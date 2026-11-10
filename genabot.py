"""genabot: небольшой модераторский бот для Matrix (Synapse).

Команды (префикс настраивается): !genabot help | ping | del | purge | keep.
Команды работают только в НЕшифрованных комнатах (бот без E2EE не видит текст в шифрованных)
и только у тех, у кого в комнате уровень прав не ниже MIN_LEVEL (по умолчанию 50, модератор).
Чтобы удалять чужие сообщения, самому боту нужен уровень не ниже порога redact комнаты (по умолчанию 50).
"""
import asyncio, json, logging, os, re, shlex, time
from types import SimpleNamespace
from urllib.parse import quote

import aiohttp

from collections import defaultdict, deque

from nio import (AsyncClient, RoomGetEventResponse, InviteMemberEvent, LoginResponse, MatrixRoom, MessageDirection,
                 RoomMessage, RoomMessageText, RoomMessagesResponse, RoomRedactResponse)

log = logging.getLogger("genabot")
E = os.environ
HS, USER, PASSWORD = E["MATRIX_HOMESERVER"], E["MATRIX_USER"], E.get("MATRIX_PASSWORD", "")
MIN_LEVEL = int(E.get("MIN_LEVEL", "50"))
CMD = E.get("BOT_CMD", "!genabot").lower()  # команды: «!genabot keep 100»
PREFIX = CMD + " "  # для текстов подсказок
MAX_BULK = int(E.get("MAX_BULK", "5000"))      # сколько сообщений максимум удалять за один запуск
CONFIRM_OVER = int(E.get("CONFIRM_OVER", "20"))  # больше стольких удаляем только с подтверждением (слово yes)
# Временный режим: читать всю историю через админский API Synapse (в обход видимости истории «с момента приглашения»).
# Включается ADMIN_MODE=1, и самой учётке бота нужно право админа сервера (выдаётся/снимается отдельно в Synapse).
ADMIN_MODE = E.get("ADMIN_MODE", "0") == "1"
ALLOWED_INVITERS = {u.strip() for u in E.get("ALLOWED_INVITERS", "").split(",") if u.strip()}
MAX_LEN = int(E.get("MAX_CMD_LEN", "300"))         # длиннее такое сообщение с командой не разбираем
MAX_AGE = int(E.get("MAX_CMD_AGE", "120")) * 1000   # команды старше (секунды) игнорируем: после простоя бота не выполняем «хвост»
RATE_MAX, RATE_WINDOW = int(E.get("RATE_MAX", "8")), 60  # не больше RATE_MAX команд в минуту от одного пользователя
HITS: dict[str, deque] = defaultdict(deque)
LOCKS: dict[str, asyncio.Lock] = {}  # в комнате одновременно идёт одна операция удаления
SESSION = os.path.join(E.get("DATA_DIR", "/data"), "session.json")
DOMAIN = USER.split(":", 1)[1]
mx = AsyncClient(HS, USER)

HELP = (
    "genabot — команды (для модераторов комнаты):\n"
    f"{PREFIX}del — ответом на сообщение: удалить его\n"
    f"{PREFIX}purge [N] [from:КТО] [text:СЛОВО] [older:7d] [dry] [yes] — удалить сообщения по фильтру. "
    "N — сколько последних подходящих (без N — все подходящие). from: — часть имени отправителя "
    "(например from:webhooks или from:@user), text: — слово или фраза в тексте (фразу в кавычках), "
    "older: — старше срока (30m, 12h, 7d, 2w). dry — только посчитать, yes — подтвердить большое удаление.\n"
    f"room:!идентификатор — у purge/keep: чистить другую комнату (в том числе шифрованную), команда пишется в обычной комнате. "
    "Нужны ALLOWED_INVITERS и режим админа; ваши права модератора проверяются в той комнате\n"
    f"{PREFIX}purge all yes — удалить ВСЕ сообщения комнаты (всегда нужно слово yes; без других параметров)\n"
    f"{PREFIX}keep N [from:КТО] [text:СЛОВО] [yes] — оставить последние N подходящих сообщений, более ранние удалить "
    "(без фильтров в режиме админа историю старше стирается из базы целиком, без пустых остатков)\n"
    f"{PREFIX}purge older:7d (только срок, без других фильтров и N) — стирает из базы всю историю старше срока\n"
    f"{PREFIX}ping — проверить, что бот жив\n"
    f"{PREFIX}help — эта справка\n"
    f"Примеры: {PREFIX}keep 100 yes · {PREFIX}purge from:webhooks older:7d yes · {PREFIX}purge 10"
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
        raise SystemExit(f"Вход не удался: {r}")
    with open(SESSION, "w") as f:
        json.dump({"user_id": r.user_id, "device_id": r.device_id, "token": r.access_token}, f)
    os.chmod(SESSION, 0o600)


async def say(room_id, text):
    await mx.room_send(room_id, "m.room.message", {"msgtype": "m.notice", "body": text})


async def redact(room_id, event_id):
    """Удалить событие, при лимите Synapse подождать и повторить."""
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
    """Безопасный для вывода кусок чужого ввода: только буквы, цифры, - и _."""
    return re.sub(r"[^\w\-]", "?", str(t))[:n]


def parse(arg, cmd):
    """Разбор и проверка аргументов purge/keep -> (n, фильтр-функция, dry, yes, текст ошибки)."""
    def bad(msg):
        return None, None, False, False, msg
    n, dry, yes, sender, text, older = None, False, False, None, None, None
    everything, target = False, None
    if len(arg) > MAX_LEN:
        return bad("Слишком длинная команда.")
    try:
        tokens = shlex.split(arg)
    except ValueError:
        return bad("Не закрыты кавычки.")
    if len(tokens) > 8:
        return bad("Слишком много параметров.")
    for t in tokens:
        tl = t.lower()
        if re.fullmatch(r"[0-9]{1,7}", t):
            if n is not None:
                return bad("Число N указано дважды.")
            n = int(t)
        elif tl == "dry":
            dry = True
        elif tl == "yes":
            yes = True
        elif tl == "all":
            everything = True
        elif tl.startswith("room:"):
            if target is not None:
                return bad("room: указан дважды.")
            target = t[5:]  # идентификатор комнаты регистрозависим
            if not re.fullmatch(r"![\w.=+/\-]{1,100}:[\w.\-]{1,100}(:[0-9]{1,5})?", target):
                return bad("room: — идентификатор комнаты вида room:!abc:example.org (Element: Настройки комнаты → Расширенные).")
        elif tl.startswith("from:"):
            if sender is not None:
                return bad("from: указан дважды.")
            sender = tl[5:]
            if not re.fullmatch(r"[@\w.:=\-]{1,100}", sender):
                return bad("from: — часть имени отправителя (буквы, цифры и @ . : = _ -), например from:webhooks.")
        elif tl.startswith("text:"):
            if text is not None:
                return bad("text: указан дважды.")
            text = tl[5:]
            if not 1 <= len(text) <= 100 or any(ord(c) < 32 for c in text):
                return bad("text: — от 1 до 100 символов, например text:ошибка или text:\"две фразы\".")
        elif tl.startswith("older:"):
            if older is not None:
                return bad("older: указан дважды.")
            m = re.fullmatch(r"([0-9]{1,4})([mhdw])", tl[6:])
            if not m or int(m.group(1)) == 0:
                return bad("older: — срок вроде 30m, 12h, 7d, 2w (число от 1 до 9999).")
            older = int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)] * 1000
        else:
            return bad(f"Не понял «{clip(t)}».")
    if n == 0:
        return bad("N должно быть больше нуля.")
    if everything:
        if cmd != "purge":
            return bad("all работает только с purge.")
        if n is not None or sender or text or older:
            return bad("all (удалить всё в комнате) нельзя сочетать с N и фильтрами.")
    if cmd == "purge" and n is None and not (sender or text or older or everything):
        return bad(f"Укажите N или фильтр: {PREFIX}purge 10 · {PREFIX}purge from:webhooks · {PREFIX}purge older:7d. "
                   f"Чтобы оставить только последние сообщения, есть {PREFIX}keep N.")
    if cmd == "keep":
        if n is None:
            return bad(f"Использование: {PREFIX}keep N — сколько последних сообщений оставить.")
        if older:
            return bad("Для keep срок older: не применяется, используйте purge older:….")
    cutoff = int(time.time() * 1000) - older if older else None

    def flt(e):
        return ((not sender or sender in e.sender.lower())
                and (not text or text in (getattr(e, "body", "") or "").lower())
                and (cutoff is None or e.server_timestamp < cutoff))
    flt.plain = not (sender or text or older or everything)  # без фильтров: для keep можно стереть историю физически
    flt.cutoff = cutoff
    flt.all = everything  # purge all: стереть вообще всё в комнате
    flt.room = target  # другая комната, команда пришла из управляющей
    flt.text = text
    flt.only_older = bool(older) and not (sender or text)  # только срок: для purge можно стереть историю физически
    return n, flt, dry, yes, None


async def admin_messages(room_id, token):
    """Страница истории через админский API Synapse -> (список простых событий, следующий токен)."""
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
        if live:  # у удалённых содержимого нет; шифрованные: текст неизвестен, видны отправитель и время
            evs.append(SimpleNamespace(event_id=e["event_id"], sender=e["sender"], body=c.get("body", ""),
                                       server_timestamp=e.get("origin_server_ts", 0)))
    return evs, d.get("end")


async def purge_history(room_id, event_id):
    """Физически стереть из Synapse всю историю комнаты до события event_id (админский API, без пустых остатков)."""
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
        for _ in range(450):  # до 15 минут
            await asyncio.sleep(2)
            async with http.get(f"{base}/purge_history_status/{pid}", headers=hdr) as r:
                st = (await r.json()).get("status")
            if st == "complete":
                return True
            if st == "failed":
                return False
    return False


async def collect(room_id, cmd_id, flt, skip, limit, bound=None):
    """Идём от новых сообщений к старым. Первые skip подходящих оставляем, следующие (до limit) возвращаем.
    В bound кладём самое старое из оставленных (граница)."""
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
    """Самое старое сообщение не старше cutoff (мс): всё до него можно стирать физически."""
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
    """Стереть из базы целиком всю историю старше edge (без пустых остатков). kept: что останется, словами.
    room: где отвечаем; tid: комната, которую чистим (по умолчанию та же); redact_edge: удалить и саму границу."""
    tid = tid or room.room_id
    since = time.strftime("%d.%m.%Y %H:%M UTC", time.gmtime(edge.server_timestamp / 1000))
    more = "+" if len(victims) >= MAX_BULK else ""
    if dry:
        await say(room.room_id, f"{lab}Останется: {kept} (с {since}). Будет стёрта вся история старше: "
                                f"сообщений {len(victims)}{more} и все пустые остатки удалений.")
        return
    if (len(victims) > CONFIRM_OVER or need_yes) and not yes:
        await say(room.room_id, f"{lab}Будет стёрта вся история старше {since} (сообщений {len(victims)}{more}), останется: {kept}. "
                                "Повторите команду со словом yes, чтобы подтвердить.")
        return
    lock = LOCKS.setdefault(tid, asyncio.Lock())
    if lock.locked():
        await say(room.room_id, "В этой комнате удаление уже идёт, подождите.")
        return
    async with lock:
        await say(room.room_id, f"{lab}Стираю историю старше {since}, это может занять время…")
        ok = await purge_history(tid, edge.event_id)
        if ok and redact_edge:
            await redact(tid, edge.event_id)  # сама граница (последнее сообщение) удаляется обычным способом
        await redact(room.room_id, ev.event_id)  # сама команда
        await say(room.room_id, (f"{lab}Готово: история старше {since} стёрта, осталось: {kept}. "
                                 "Если в клиенте ещё видно старое, обновите страницу (Ctrl+R).") if ok
                  else f"{lab}Не удалось стереть историю, подробности в журнале бота.")
        log.info("%s: физическая очистка в %s, граница %s, ok=%s", ev.sender, tid, edge.event_id, ok)


def remote_target(room: MatrixRoom, ev, flt):
    """Комната для чистки: та же или указанная через room:. -> (комната, ошибка)."""
    if not flt.room or flt.room == room.room_id:
        return room, None
    if not ALLOWED_INVITERS:
        return None, "Чистка другой комнаты работает только при заданном ALLOWED_INVITERS."
    if not ADMIN_MODE:
        return None, "Чистка другой комнаты работает только в режиме админа (ADMIN_MODE=1)."
    tgt = mx.rooms.get(flt.room)
    if tgt is None:
        return None, "Бот не состоит в этой комнате."
    if ev.sender not in tgt.users or tgt.power_levels.get_user_level(ev.sender) < MIN_LEVEL:
        return None, f"В той комнате у вас нет прав модератора (уровень {MIN_LEVEL}+)."
    if not bot_can_redact(tgt):
        return None, f"У бота мало прав в той комнате: нужен уровень {tgt.power_levels.defaults.redact}+."
    if tgt.encrypted and flt.text:
        return None, "В шифрованной комнате фильтр text: недоступен (текст сообщений неизвестен). Используйте N, from:, older: или all."
    return tgt, None


async def bulk(room: MatrixRoom, ev, cmd, arg):
    n, flt, dry, yes, err = parse(arg, cmd)
    if err:
        await say(room.room_id, err + f" Справка: {PREFIX}help")
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
            await physical(room, ev, f"последние {n} сообщений", victims, bound[0], dry, yes, tid=tid, lab=lab)
            return
    else:
        victims = await collect(tid, ev.event_id, flt, 0, min(n, MAX_BULK) if n else MAX_BULK)
        if flt.all and ADMIN_MODE:  # вся история до этой команды стирается из базы, подтверждение yes обязательно
            if remote:  # в чужой комнате команды нет: границей служит самое новое сообщение, его удаляем отдельно
                if not victims:
                    await say(room.room_id, f"{lab}Сообщений нет.")
                    return
                await physical(room, ev, "ничего", victims, victims[0], dry, yes, need_yes=True, tid=tid, lab=lab, redact_edge=True)
            else:
                await physical(room, ev, "ничего (кроме этой команды)", victims, ev, dry, yes, need_yes=True)
            return
        if ADMIN_MODE and flt.only_older and n is None and victims:
            edge = await find_edge(tid, ev.event_id, flt.cutoff)
            if edge is None and remote:
                edge = victims[0]  # новых сообщений нет: стираем всё, границей служит самое новое
                await physical(room, ev, "ничего", victims, edge, dry, yes, tid=tid, lab=lab, redact_edge=True)
            else:
                await physical(room, ev, "сообщения новее заданного срока", victims, edge or ev, dry, yes, tid=tid, lab=lab)
            return
    more = " (достигнут предел за раз, повторите команду)" if len(victims) >= MAX_BULK else ""
    if dry or not victims:
        empty = (f"Подходящих сообщений не больше {n}, удалять нечего." if cmd == "keep" else "Подходящих сообщений нет.")
        await say(room.room_id, f"{lab}Подошло бы к удалению: {len(victims)}{more}." if victims else f"{lab}{empty}")
        return
    if (len(victims) > CONFIRM_OVER or flt.all) and not yes:
        await say(room.room_id, f"{lab}Будет удалено сообщений: {len(victims)}{more}. Повторите команду со словом yes, чтобы подтвердить.")
        return
    lock = LOCKS.setdefault(tid, asyncio.Lock())
    if lock.locked():
        await say(room.room_id, "В этой комнате удаление уже идёт, подождите.")
        return
    async with lock:
        if len(victims) > CONFIRM_OVER:
            await say(room.room_id, f"{lab}Удаляю {len(victims)} сообщений, это может занять время…")
        done = 0
        for e in victims:
            done += await redact(tid, e.event_id)
        await redact(room.room_id, ev.event_id)  # сама команда
        if len(victims) > 1 or cmd == "keep":
            await say(room.room_id, f"{lab}Удалено сообщений: {done} из {len(victims)}.{more}")
        log.info("%s: %s %s в %s, удалено %d из %d", ev.sender, cmd, arg, tid, done, len(victims))


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
        return  # команды слушаем только у своих, остальным не отвечаем
    if time.time() * 1000 - ev.server_timestamp > MAX_AGE:
        log.info("старая команда от %s проигнорирована", ev.sender)  # не выполняем «хвост» после простоя бота
        return
    if (ev.source.get("content", {}).get("m.relates_to") or {}).get("rel_type") == "m.replace":
        return  # правка сообщения, не новая команда
    if not rate_ok(ev.sender):
        log.warning("слишком много команд от %s, игнорирую", ev.sender)
        return
    if len(ev.body) > MAX_LEN:
        await say(room.room_id, "Слишком длинное сообщение с командой.")
        return
    if "\n" in ev.body.strip() or any(ord(c) < 32 and c != "\t" for c in ev.body):
        await say(room.room_id, "Команда должна быть в одну строку, без служебных символов.")
        return
    cmd, _, arg = (parts[1] if len(parts) > 1 else "help").strip().partition(" ")
    cmd, arg = cmd.lower(), arg.strip()
    if cmd not in ("help", "ping", "del", "purge", "keep"):
        await say(room.room_id, f"Неизвестная команда «{clip(cmd)}». Справка: {PREFIX}help")
        return
    if cmd in ("help", "ping", "del") and arg:
        await say(room.room_id, f"Команда {cmd} не принимает параметров. Справка: {PREFIX}help")
        return
    if not can_command(room, ev.sender):
        await say(room.room_id, f"{ev.sender}: команда доступна модераторам комнаты (уровень {MIN_LEVEL}+).")
        return
    if cmd == "ping":
        await say(room.room_id, "pong")
    elif cmd == "help":
        await say(room.room_id, HELP)
    elif cmd in ("del", "purge", "keep"):
        if " room:" not in " " + arg.lower() and not bot_can_redact(room):
            await say(room.room_id, f"У бота мало прав в комнате: нужен уровень {room.power_levels.defaults.redact}+ (модератор).")
            return
        if cmd == "del":
            target = ((ev.source.get("content", {}).get("m.relates_to") or {}).get("m.in_reply_to") or {}).get("event_id")
            if not target:
                await say(room.room_id, f"Ответьте командой {PREFIX}del на сообщение, которое нужно удалить.")
                return
            if not re.fullmatch(r"\$[^\s]{1,255}", str(target)):
                await say(room.room_id, "Не удалось определить сообщение для удаления.")
                return
            r = await mx.room_get_event(room.room_id, target)
            if not isinstance(r, RoomGetEventResponse) or not isinstance(r.event, RoomMessage):
                await say(room.room_id, "Удалять можно только обычные сообщения.")  # не служебные события комнаты
                return
            ok = await redact(room.room_id, target)
            await redact(room.room_id, ev.event_id)  # сама команда
            log.info("%s: del в %s, %s", ev.sender, room.room_id, ok)
        else:
            asyncio.create_task(safe(bulk(room, ev, cmd, arg)))


async def safe(coro):
    try:
        await coro
    except Exception:
        log.exception("ошибка в команде")


async def on_invite(room: MatrixRoom, ev: InviteMemberEvent):
    # принимаем приглашения только от ALLOWED_INVITERS (если пусто, от любого пользователя своего сервера);
    # те же пользователи единственные, чьи команды бот слушает
    if ev.state_key != mx.user_id or ev.membership != "invite":
        return
    if ALLOWED_INVITERS:
        if ev.sender not in ALLOWED_INVITERS:
            log.info("приглашение от %s в %s отклонено (нет в ALLOWED_INVITERS)", ev.sender, room.room_id)
            return
    elif not ev.sender.endswith(":" + DOMAIN):
        return
    r = await mx.join(room.room_id)
    log.info("приглашение от %s в %s: %s", ev.sender, room.room_id, r)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    os.makedirs(os.path.dirname(SESSION), exist_ok=True)
    await login()
    await mx.sync(timeout=0, full_state=True)  # старые сообщения не трогаем
    mx.add_event_callback(on_message, RoomMessageText)
    mx.add_event_callback(on_invite, InviteMemberEvent)
    log.info("genabot запущен как %s%s", mx.user_id, ", ADMIN_MODE (вся история)" if ADMIN_MODE else "")
    await mx.sync_forever(timeout=30000, full_state=False)


asyncio.run(main())
