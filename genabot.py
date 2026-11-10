"""genabot: a small moderation bot for Matrix (Synapse).

Commands (the prefix is configurable): !genabot help | ping | del | purge | keep.
Commands are read only in UNENCRYPTED rooms (the bot has no E2EE) and only from users who have at least
MIN_LEVEL power level in the room (default 50, moderator). To delete other people's messages the bot itself
needs at least the room's "redact" power level (default 50).
Bot replies are in English or Russian, see BOT_LANG.
"""
import asyncio, json, logging, os, re, shlex, time
from collections import defaultdict, deque
from types import SimpleNamespace
from urllib.parse import quote

import aiohttp
from nio import (AsyncClient, InviteMemberEvent, LoginResponse, MatrixRoom, MessageDirection, RoomGetEventResponse,
                 RoomMessage, RoomMessageText, RoomMessagesResponse, RoomRedactResponse)

log = logging.getLogger("genabot")
E = os.environ
HS, USER, PASSWORD = E["MATRIX_HOMESERVER"], E["MATRIX_USER"], E.get("MATRIX_PASSWORD", "")
MIN_LEVEL = int(E.get("MIN_LEVEL", "50"))
CMD = E.get("BOT_CMD", "!genabot").lower()  # commands look like "!genabot keep 100"
PREFIX = CMD + " "  # used in hint texts
RU = E.get("BOT_LANG", "en").lower().startswith("ru")  # language of the bot's replies: en (default) or ru


def T(ru, en):
    """Pick the reply text in the configured language."""
    return ru if RU else en


MAX_BULK = int(E.get("MAX_BULK", "5000"))        # max messages deleted per run
CONFIRM_OVER = int(E.get("CONFIRM_OVER", "20"))  # deleting more than this needs confirmation (the word "yes")
# Admin mode: read the whole history through the Synapse admin API (ignoring "since invited" history visibility)
# and erase history from the database. Needs ADMIN_MODE=1 AND server-admin rights on the bot account.
ADMIN_MODE = E.get("ADMIN_MODE", "0") == "1"
ALLOWED_INVITERS = {u.strip() for u in E.get("ALLOWED_INVITERS", "").split(",") if u.strip()}
MAX_LEN = int(E.get("MAX_CMD_LEN", "300"))          # longer command messages are not parsed
MAX_AGE = int(E.get("MAX_CMD_AGE", "120")) * 1000    # older commands (seconds) are ignored: no backlog after downtime
RATE_MAX, RATE_WINDOW = int(E.get("RATE_MAX", "8")), 60  # at most RATE_MAX commands per minute per user
HITS: dict[str, deque] = defaultdict(deque)
LOCKS: dict[str, asyncio.Lock] = {}  # only one deletion at a time per room
SESSION = os.path.join(E.get("DATA_DIR", "/data"), "session.json")
DOMAIN = USER.split(":", 1)[1]
mx = AsyncClient(HS, USER)

HELP = T(
    "genabot — команды (для модераторов комнаты):\n"
    f"{PREFIX}del — ответом на сообщение: удалить его\n"
    f"{PREFIX}purge [N] [from:КТО] [text:СЛОВО] [older:7d] [dry] [yes] — удалить сообщения по фильтру. "
    "N — сколько последних подходящих (без N — все подходящие). from: — часть имени отправителя "
    "(например from:webhooks или from:@user), text: — слово или фраза в тексте (фразу в кавычках), "
    "older: — старше срока (30m, 12h, 7d, 2w). dry — только посчитать, yes — подтвердить большое удаление.\n"
    "room:!идентификатор — у purge/keep: чистить другую комнату (в том числе шифрованную), команда пишется в обычной комнате. "
    "Нужны ALLOWED_INVITERS и режим админа; ваши права модератора проверяются в той комнате\n"
    f"{PREFIX}purge all yes — удалить ВСЕ сообщения комнаты (всегда нужно слово yes; без других параметров)\n"
    f"{PREFIX}keep N [from:КТО] [text:СЛОВО] [yes] — оставить последние N подходящих сообщений, более ранние удалить "
    "(без фильтров в режиме админа историю старше стирается из базы целиком, без пустых остатков)\n"
    f"{PREFIX}purge older:7d (только срок, без других фильтров и N) — стирает из базы всю историю старше срока\n"
    f"{PREFIX}ping — проверить, что бот жив\n"
    f"{PREFIX}help — эта справка\n"
    f"Примеры: {PREFIX}keep 100 yes · {PREFIX}purge from:webhooks older:7d yes · {PREFIX}purge 10",
    "genabot — commands (for room moderators):\n"
    f"{PREFIX}del — as a reply to a message: delete it\n"
    f"{PREFIX}purge [N] [from:WHO] [text:WORD] [older:7d] [dry] [yes] — delete messages matching a filter. "
    "N — how many of the latest matching messages (without N: all matching). from: — part of the sender's name "
    "(e.g. from:webhooks or from:@user), text: — a word or phrase in the text (quote phrases), "
    "older: — older than a period (30m, 12h, 7d, 2w). dry — only count, yes — confirm a large deletion.\n"
    "room:!id — for purge/keep: clean another room (including an encrypted one); write the command in a regular room. "
    "Needs ALLOWED_INVITERS and admin mode; your moderator rights are checked in that room\n"
    f"{PREFIX}purge all yes — delete ALL messages in the room (always needs the word yes; no other parameters)\n"
    f"{PREFIX}keep N [from:WHO] [text:WORD] [yes] — keep the last N matching messages, delete older ones "
    "(without filters, in admin mode the older history is erased from the database entirely, leaving no empty shells)\n"
    f"{PREFIX}purge older:7d (a period only, no other filters or N) — erases all history older than the period from the database\n"
    f"{PREFIX}ping — check that the bot is alive\n"
    f"{PREFIX}help — this help\n"
    f"Examples: {PREFIX}keep 100 yes · {PREFIX}purge from:webhooks older:7d yes · {PREFIX}purge 10",
)


async def login():
    try:
        with open(SESSION) as f:
            s = json.load(f)
        mx.restore_login(s["user_id"], s["device_id"], s["token"])
        return
    except FileNotFoundError:
        pass
    r = await mx.login(PASSWORD, device_name="genabot")
    if not isinstance(r, LoginResponse):
        raise SystemExit(f"Login failed: {r}")
    with open(SESSION, "w") as f:
        json.dump({"user_id": r.user_id, "device_id": r.device_id, "token": r.access_token}, f)
    os.chmod(SESSION, 0o600)


async def say(room_id, text):
    await mx.room_send(room_id, "m.room.message", {"msgtype": "m.notice", "body": text})


async def redact(room_id, event_id):
    """Delete an event; on a Synapse rate limit wait and retry."""
    for _ in range(6):
        r = await mx.room_redact(room_id, event_id)
        if isinstance(r, RoomRedactResponse):
            return True
        if getattr(r, "status_code", "") == "M_LIMITED_EXCEEDED":
            await asyncio.sleep((getattr(r, "retry_after_ms", None) or 1000) / 1000 + 0.2)
            continue
        log.warning("redact %s: %s", event_id, r)
        return False
    return False


def can_command(room: MatrixRoom, uid: str) -> bool:
    return room.power_levels.get_user_level(uid) >= MIN_LEVEL


def bot_can_redact(room: MatrixRoom) -> bool:
    return room.power_levels.get_user_level(mx.user_id) >= room.power_levels.defaults.redact


def clip(t, n=20):
    """A piece of someone else's input that is safe to echo: only letters, digits, - and _."""
    return re.sub(r"[^\w\-]", "?", str(t))[:n]


def parse(arg, cmd):
    """Parse and validate purge/keep arguments -> (n, filter function, dry, yes, error text)."""
    def bad(msg):
        return None, None, False, False, msg
    n, dry, yes, sender, text, older = None, False, False, None, None, None
    everything, target = False, None
    if len(arg) > MAX_LEN:
        return bad(T("Слишком длинная команда.", "The command is too long."))
    try:
        tokens = shlex.split(arg)
    except ValueError:
        return bad(T("Не закрыты кавычки.", "Unclosed quotes."))
    if len(tokens) > 8:
        return bad(T("Слишком много параметров.", "Too many parameters."))
    for t in tokens:
        tl = t.lower()
        if re.fullmatch(r"[0-9]{1,7}", t):
            if n is not None:
                return bad(T("Число N указано дважды.", "N is given twice."))
            n = int(t)
        elif tl == "dry":
            dry = True
        elif tl == "yes":
            yes = True
        elif tl == "all":
            everything = True
        elif tl.startswith("room:"):
            if target is not None:
                return bad(T("room: указан дважды.", "room: is given twice."))
            target = t[5:]  # room IDs are case-sensitive
            if not re.fullmatch(r"![\w.=+/\-]{1,100}:[\w.\-]{1,100}(:[0-9]{1,5})?", target):
                return bad(T("room: — идентификатор комнаты вида room:!abc:example.org (Element: Настройки комнаты → Расширенные).",
                             "room: takes a room ID like room:!abc:example.org (Element: Room settings → Advanced)."))
        elif tl.startswith("from:"):
            if sender is not None:
                return bad(T("from: указан дважды.", "from: is given twice."))
            sender = tl[5:]
            if not re.fullmatch(r"[@\w.:=\-]{1,100}", sender):
                return bad(T("from: — часть имени отправителя (буквы, цифры и @ . : = _ -), например from:webhooks.",
                             "from: takes part of the sender's name (letters, digits and @ . : = _ -), e.g. from:webhooks."))
        elif tl.startswith("text:"):
            if text is not None:
                return bad(T("text: указан дважды.", "text: is given twice."))
            text = tl[5:]
            if not 1 <= len(text) <= 100 or any(ord(c) < 32 for c in text):
                return bad(T('text: — от 1 до 100 символов, например text:ошибка или text:"две фразы".',
                             'text: takes 1 to 100 characters, e.g. text:error or text:"two words".'))
        elif tl.startswith("older:"):
            if older is not None:
                return bad(T("older: указан дважды.", "older: is given twice."))
            m = re.fullmatch(r"([0-9]{1,4})([mhdw])", tl[6:])
            if not m or int(m.group(1)) == 0:
                return bad(T("older: — срок вроде 30m, 12h, 7d, 2w (число от 1 до 9999).",
                             "older: takes a period like 30m, 12h, 7d, 2w (a number from 1 to 9999)."))
            older = int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)] * 1000
        else:
            return bad(T(f"Не понял «{clip(t)}».", f"Did not understand \"{clip(t)}\"."))
    if n == 0:
        return bad(T("N должно быть больше нуля.", "N must be greater than zero."))
    if everything:
        if cmd != "purge":
            return bad(T("all работает только с purge.", "all works only with purge."))
        if n is not None or sender or text or older:
            return bad(T("all (удалить всё в комнате) нельзя сочетать с N и фильтрами.",
                         "all (delete everything in the room) cannot be combined with N or filters."))
    if cmd == "purge" and n is None and not (sender or text or older or everything):
        return bad(T(f"Укажите N или фильтр: {PREFIX}purge 10 · {PREFIX}purge from:webhooks · {PREFIX}purge older:7d. "
                     f"Чтобы оставить только последние сообщения, есть {PREFIX}keep N.",
                     f"Give N or a filter: {PREFIX}purge 10 · {PREFIX}purge from:webhooks · {PREFIX}purge older:7d. "
                     f"To keep only the latest messages use {PREFIX}keep N."))
    if cmd == "keep":
        if n is None:
            return bad(T(f"Использование: {PREFIX}keep N — сколько последних сообщений оставить.",
                         f"Usage: {PREFIX}keep N — how many latest messages to keep."))
        if older:
            return bad(T("Для keep срок older: не применяется, используйте purge older:….",
                         "older: does not apply to keep, use purge older:… instead."))
    cutoff = int(time.time() * 1000) - older if older else None

    def flt(e):
        return ((not sender or sender in e.sender.lower())
                and (not text or text in (getattr(e, "body", "") or "").lower())
                and (cutoff is None or e.server_timestamp < cutoff))
    flt.plain = not (sender or text or older or everything)  # no filters: keep can erase history physically
    flt.cutoff = cutoff
    flt.all = everything  # purge all: erase everything in the room
    flt.room = target  # another room: the command came from a control room
    flt.text = text
    flt.only_older = bool(older) and not (sender or text)  # a period only: purge can erase history physically
    return n, flt, dry, yes, None


async def admin_messages(room_id, token):
    """A page of room history via the Synapse admin API -> (list of simple events, next token)."""
    params = {"dir": "b", "limit": "100"}
    if token:
        params["from"] = token
    url = f"{HS}/_synapse/admin/v1/rooms/{quote(room_id)}/messages"
    async with aiohttp.ClientSession() as http:
        async with http.get(url, params=params, headers={"Authorization": f"Bearer {mx.access_token}"}) as r:
            d = await r.json()
    if "chunk" not in d:
        log.warning("admin messages: %s", d)
        return [], None
    evs = []
    for e in d["chunk"]:
        c = e.get("content") or {}
        live = (e.get("type") == "m.room.message" and c.get("msgtype")) or (e.get("type") == "m.room.encrypted" and c.get("ciphertext"))
        if live:  # deleted events have no content; encrypted ones: text is unknown, sender and time are visible
            evs.append(SimpleNamespace(event_id=e["event_id"], sender=e["sender"], body=c.get("body", ""),
                                       server_timestamp=e.get("origin_server_ts", 0)))
    return evs, d.get("end")


async def purge_history(room_id, event_id):
    """Physically erase the room history before event_id from Synapse (admin API, leaves no empty shells)."""
    hdr = {"Authorization": f"Bearer {mx.access_token}"}
    base = f"{HS}/_synapse/admin/v1"
    async with aiohttp.ClientSession() as http:
        async with http.post(f"{base}/purge_history/{quote(room_id)}", headers=hdr,
                             json={"purge_up_to_event_id": event_id, "delete_local_events": True}) as r:
            d = await r.json()
        pid = d.get("purge_id")
        if not pid:
            log.warning("purge_history: %s", d)
            return False
        for _ in range(450):  # up to 15 minutes
            await asyncio.sleep(2)
            async with http.get(f"{base}/purge_history_status/{pid}", headers=hdr) as r:
                st = (await r.json()).get("status")
            if st == "complete":
                return True
            if st == "failed":
                return False
    return False


async def collect(room_id, cmd_id, flt, skip, limit, bound=None):
    """Walk from newest to oldest. The first `skip` matching messages are kept, the next ones (up to `limit`)
    are returned. `bound` receives the oldest kept message (the boundary)."""
    matched, out, token = 0, [], (None if ADMIN_MODE else mx.next_batch)
    first = True
    while (token or first) and len(out) < limit:
        first = False
        if ADMIN_MODE:
            chunk, token = await admin_messages(room_id, token)
            if not chunk and not token:
                break
            for e in chunk:
                if e.event_id == cmd_id or not flt(e):
                    continue
                matched += 1
                if bound is not None and matched == skip:
                    bound.append(e)
                if matched > skip:
                    out.append(e)
                    if len(out) >= limit:
                        break
            continue
        r = await mx.room_messages(room_id, token, direction=MessageDirection.back, limit=100)
        if not isinstance(r, RoomMessagesResponse) or not r.chunk:
            break
        for e in r.chunk:
            if not isinstance(e, RoomMessage) or e.event_id == cmd_id or not flt(e):
                continue
            matched += 1
            if matched > skip:
                out.append(e)
                if len(out) >= limit:
                    break
        token = r.end
    return out


async def find_edge(room_id, cmd_id, cutoff):
    """The oldest message not older than cutoff (ms): everything before it can be erased physically."""
    last, token, first = None, None, True
    while token or first:
        first = False
        chunk, token = await admin_messages(room_id, token)
        for e in chunk:
            if e.event_id == cmd_id:
                continue
            if e.server_timestamp < cutoff:
                return last
            last = e
    return last


async def physical(room: MatrixRoom, ev, kept, victims, edge, dry, yes, need_yes=False, tid=None, lab="", redact_edge=False):
    """Erase all history older than `edge` from the database (no empty shells). `kept`: what stays, in words.
    room: where we reply; tid: the room being cleaned (same room by default); redact_edge: also delete the boundary."""
    tid = tid or room.room_id
    since = time.strftime("%d.%m.%Y %H:%M UTC", time.gmtime(edge.server_timestamp / 1000))
    more = "+" if len(victims) >= MAX_BULK else ""
    if dry:
        await say(room.room_id, T(f"{lab}Останется: {kept} (с {since}). Будет стёрта вся история старше: "
                                  f"сообщений {len(victims)}{more} и все пустые остатки удалений.",
                                  f"{lab}Will remain: {kept} (since {since}). All history older than that will be erased: "
                                  f"{len(victims)}{more} messages and all empty remnants of earlier deletions."))
        return
    if (len(victims) > CONFIRM_OVER or need_yes) and not yes:
        await say(room.room_id, T(f"{lab}Будет стёрта вся история старше {since} (сообщений {len(victims)}{more}), останется: {kept}. "
                                  "Повторите команду со словом yes, чтобы подтвердить.",
                                  f"{lab}All history older than {since} will be erased ({len(victims)}{more} messages), remaining: {kept}. "
                                  "Repeat the command with the word yes to confirm."))
        return
    lock = LOCKS.setdefault(tid, asyncio.Lock())
    if lock.locked():
        await say(room.room_id, T("В этой комнате удаление уже идёт, подождите.", "A deletion is already running in this room, please wait."))
        return
    async with lock:
        await say(room.room_id, T(f"{lab}Стираю историю старше {since}, это может занять время…",
                                  f"{lab}Erasing history older than {since}, this may take a while…"))
        ok = await purge_history(tid, edge.event_id)
        if ok and redact_edge:
            await redact(tid, edge.event_id)  # the boundary itself (the latest message) is deleted the regular way
        await redact(room.room_id, ev.event_id)  # the command itself
        await say(room.room_id, (T(f"{lab}Готово: история старше {since} стёрта, осталось: {kept}. "
                                   "Если в клиенте ещё видно старое, обновите страницу (Ctrl+R).",
                                   f"{lab}Done: history older than {since} erased, remaining: {kept}. "
                                   "If the client still shows old messages, refresh the page (Ctrl+R).") if ok
                                 else T(f"{lab}Не удалось стереть историю, подробности в журнале бота.",
                                        f"{lab}Could not erase the history, see the bot log for details.")))
        log.info("%s: physical purge in %s, boundary %s, ok=%s", ev.sender, tid, edge.event_id, ok)


def remote_target(room: MatrixRoom, ev, flt):
    """The room to clean: the same one or the one given via room:. -> (room, error)."""
    if not flt.room or flt.room == room.room_id:
        return room, None
    if not ALLOWED_INVITERS:
        return None, T("Чистка другой комнаты работает только при заданном ALLOWED_INVITERS.",
                       "Cleaning another room works only when ALLOWED_INVITERS is set.")
    if not ADMIN_MODE:
        return None, T("Чистка другой комнаты работает только в режиме админа (ADMIN_MODE=1).",
                       "Cleaning another room works only in admin mode (ADMIN_MODE=1).")
    tgt = mx.rooms.get(flt.room)
    if tgt is None:
        return None, T("Бот не состоит в этой комнате.", "The bot is not in that room.")
    if ev.sender not in tgt.users or tgt.power_levels.get_user_level(ev.sender) < MIN_LEVEL:
        return None, T(f"В той комнате у вас нет прав модератора (уровень {MIN_LEVEL}+).",
                       f"You are not a moderator in that room (power level {MIN_LEVEL}+ required).")
    if not bot_can_redact(tgt):
        return None, T(f"У бота мало прав в той комнате: нужен уровень {tgt.power_levels.defaults.redact}+.",
                       f"The bot has too few rights in that room: power level {tgt.power_levels.defaults.redact}+ required.")
    if tgt.encrypted and flt.text:
        return None, T("В шифрованной комнате фильтр text: недоступен (текст сообщений неизвестен). Используйте N, from:, older: или all.",
                       "The text: filter is unavailable in an encrypted room (message text is unknown). Use N, from:, older: or all.")
    return tgt, None


async def bulk(room: MatrixRoom, ev, cmd, arg):
    n, flt, dry, yes, err = parse(arg, cmd)
    if err:
        await say(room.room_id, err + T(f" Справка: {PREFIX}help", f" Help: {PREFIX}help"))
        return
    tgt, err = remote_target(room, ev, flt)
    if err:
        await say(room.room_id, err)
        return
    tid = tgt.room_id
    remote = tid != room.room_id
    lab = f"[{tgt.display_name}] " if remote else ""
    if cmd == "keep":
        bound = []
        victims = await collect(tid, ev.event_id, flt, n, MAX_BULK, bound)
        if ADMIN_MODE and flt.plain and bound:
            await physical(room, ev, T(f"последние {n} сообщений", f"the last {n} messages"), victims, bound[0], dry, yes,
                           tid=tid, lab=lab)
            return
    else:
        victims = await collect(tid, ev.event_id, flt, 0, min(n, MAX_BULK) if n else MAX_BULK)
        if flt.all and ADMIN_MODE:  # all history before this command is erased from the database; "yes" is mandatory
            if remote:  # no command in the other room: the boundary is the newest message, deleted separately
                if not victims:
                    await say(room.room_id, T(f"{lab}Сообщений нет.", f"{lab}No messages."))
                    return
                await physical(room, ev, T("ничего", "nothing"), victims, victims[0], dry, yes, need_yes=True,
                               tid=tid, lab=lab, redact_edge=True)
            else:
                await physical(room, ev, T("ничего (кроме этой команды)", "nothing (except this command)"), victims, ev, dry, yes,
                               need_yes=True)
            return
        if ADMIN_MODE and flt.only_older and n is None and victims:
            edge = await find_edge(tid, ev.event_id, flt.cutoff)
            if edge is None and remote:
                edge = victims[0]  # no newer messages: erase everything, the newest one is the boundary
                await physical(room, ev, T("ничего", "nothing"), victims, edge, dry, yes, tid=tid, lab=lab, redact_edge=True)
            else:
                await physical(room, ev, T("сообщения новее заданного срока", "messages newer than the given period"), victims,
                               edge or ev, dry, yes, tid=tid, lab=lab)
            return
    more = T(" (достигнут предел за раз, повторите команду)", " (per-run limit reached, repeat the command)") if len(victims) >= MAX_BULK else ""
    if dry or not victims:
        empty = (T(f"Подходящих сообщений не больше {n}, удалять нечего.", f"No more than {n} matching messages, nothing to delete.")
                 if cmd == "keep" else T("Подходящих сообщений нет.", "No matching messages."))
        await say(room.room_id, T(f"{lab}Подошло бы к удалению: {len(victims)}{more}.", f"{lab}Would delete: {len(victims)}{more}.")
                  if victims else f"{lab}{empty}")
        return
    if (len(victims) > CONFIRM_OVER or flt.all) and not yes:
        await say(room.room_id, T(f"{lab}Будет удалено сообщений: {len(victims)}{more}. Повторите команду со словом yes, чтобы подтвердить.",
                                  f"{lab}Will delete {len(victims)}{more} messages. Repeat the command with the word yes to confirm."))
        return
    lock = LOCKS.setdefault(tid, asyncio.Lock())
    if lock.locked():
        await say(room.room_id, T("В этой комнате удаление уже идёт, подождите.", "A deletion is already running in this room, please wait."))
        return
    async with lock:
        if len(victims) > CONFIRM_OVER:
            await say(room.room_id, T(f"{lab}Удаляю {len(victims)} сообщений, это может занять время…",
                                      f"{lab}Deleting {len(victims)} messages, this may take a while…"))
        done = 0
        for e in victims:
            done += await redact(tid, e.event_id)
        await redact(room.room_id, ev.event_id)  # the command itself
        if len(victims) > 1 or cmd == "keep":
            await say(room.room_id, T(f"{lab}Удалено сообщений: {done} из {len(victims)}.{more}",
                                      f"{lab}Deleted messages: {done} of {len(victims)}.{more}"))
        log.info("%s: %s %s in %s, deleted %d of %d", ev.sender, cmd, arg, tid, done, len(victims))


def rate_ok(uid):
    now = time.monotonic()
    q = HITS[uid]
    while q and now - q[0] > RATE_WINDOW:
        q.popleft()
    if len(q) >= RATE_MAX:
        return False
    q.append(now)
    return True


async def on_message(room: MatrixRoom, ev: RoomMessageText):
    parts = ev.body.strip().split(None, 1)
    if ev.sender == mx.user_id or not parts or parts[0].lower() != CMD:
        return
    if ALLOWED_INVITERS and ev.sender not in ALLOWED_INVITERS:
        return  # only our own users may give commands; others get no reply
    if time.time() * 1000 - ev.server_timestamp > MAX_AGE:
        log.info("ignored an old command from %s", ev.sender)  # do not execute a backlog after downtime
        return
    if (ev.source.get("content", {}).get("m.relates_to") or {}).get("rel_type") == "m.replace":
        return  # an edit of a message, not a new command
    if not rate_ok(ev.sender):
        log.warning("too many commands from %s, ignoring", ev.sender)
        return
    if len(ev.body) > MAX_LEN:
        await say(room.room_id, T("Слишком длинное сообщение с командой.", "The command message is too long."))
        return
    if "\n" in ev.body.strip() or any(ord(c) < 32 and c != "\t" for c in ev.body):
        await say(room.room_id, T("Команда должна быть в одну строку, без служебных символов.",
                                  "The command must be a single line without control characters."))
        return
    cmd, _, arg = (parts[1] if len(parts) > 1 else "help").strip().partition(" ")
    cmd, arg = cmd.lower(), arg.strip()
    if cmd not in ("help", "ping", "del", "purge", "keep"):
        await say(room.room_id, T(f"Неизвестная команда «{clip(cmd)}». Справка: {PREFIX}help",
                                  f"Unknown command \"{clip(cmd)}\". Help: {PREFIX}help"))
        return
    if cmd in ("help", "ping", "del") and arg:
        await say(room.room_id, T(f"Команда {cmd} не принимает параметров. Справка: {PREFIX}help",
                                  f"The {cmd} command takes no parameters. Help: {PREFIX}help"))
        return
    if not can_command(room, ev.sender):
        await say(room.room_id, T(f"{ev.sender}: команда доступна модераторам комнаты (уровень {MIN_LEVEL}+).",
                                  f"{ev.sender}: this command is for room moderators (power level {MIN_LEVEL}+)."))
        return
    if cmd == "ping":
        await say(room.room_id, "pong")
    elif cmd == "help":
        await say(room.room_id, HELP)
    elif cmd in ("del", "purge", "keep"):
        if " room:" not in " " + arg.lower() and not bot_can_redact(room):
            await say(room.room_id, T(f"У бота мало прав в комнате: нужен уровень {room.power_levels.defaults.redact}+ (модератор).",
                                      f"The bot has too few rights in this room: power level {room.power_levels.defaults.redact}+ (moderator) required."))
            return
        if cmd == "del":
            target = ((ev.source.get("content", {}).get("m.relates_to") or {}).get("m.in_reply_to") or {}).get("event_id")
            if not target:
                await say(room.room_id, T(f"Ответьте командой {PREFIX}del на сообщение, которое нужно удалить.",
                                          f"Reply with {PREFIX}del to the message you want to delete."))
                return
            if not re.fullmatch(r"\$[^\s]{1,255}", str(target)):
                await say(room.room_id, T("Не удалось определить сообщение для удаления.", "Could not determine the message to delete."))
                return
            r = await mx.room_get_event(room.room_id, target)
            if not isinstance(r, RoomGetEventResponse) or not isinstance(r.event, RoomMessage):
                await say(room.room_id, T("Удалять можно только обычные сообщения.", "Only regular messages can be deleted."))  # not room state events
                return
            ok = await redact(room.room_id, target)
            await redact(room.room_id, ev.event_id)  # the command itself
            log.info("%s: del in %s, %s", ev.sender, room.room_id, ok)
        else:
            asyncio.create_task(safe(bulk(room, ev, cmd, arg)))


async def safe(coro):
    try:
        await coro
    except Exception:
        log.exception("error in command")


async def on_invite(room: MatrixRoom, ev: InviteMemberEvent):
    # accept invitations only from ALLOWED_INVITERS (if empty: from any user of the bot's own server);
    # the same users are the only ones whose commands the bot listens to
    if ev.state_key != mx.user_id or ev.membership != "invite":
        return
    if ALLOWED_INVITERS:
        if ev.sender not in ALLOWED_INVITERS:
            log.info("invitation from %s to %s declined (not in ALLOWED_INVITERS)", ev.sender, room.room_id)
            return
    elif not ev.sender.endswith(":" + DOMAIN):
        return
    r = await mx.join(room.room_id)
    log.info("invitation from %s to %s: %s", ev.sender, room.room_id, r)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    os.makedirs(os.path.dirname(SESSION), exist_ok=True)
    await login()
    await mx.sync(timeout=0, full_state=True)  # do not touch old messages
    mx.add_event_callback(on_message, RoomMessageText)
    mx.add_event_callback(on_invite, InviteMemberEvent)
    log.info("genabot started as %s%s", mx.user_id, ", ADMIN_MODE (full history)" if ADMIN_MODE else "")
    await mx.sync_forever(timeout=30000, full_state=False)


asyncio.run(main())
