# genabot

**English** · [Русский](README.ru.md)

A small moderation bot for Matrix (Synapse): deletes messages in a room on command. Python, [matrix-nio](https://github.com/matrix-nio/matrix-nio), runs in Docker.

> Note: the bot's replies in chat are currently in Russian.

## Commands

The prefix is configurable (`BOT_CMD`, default `!genabot`). Commands are executed only for users listed in `ALLOWED_INVITERS` (if set) who also have at least `MIN_LEVEL` power level in the room (default 50).

| Command | What it does |
|---|---|
| `!genabot help` | show help |
| `!genabot ping` | check that the bot is alive |
| `!genabot del` | as a reply to a message: delete that message |
| `!genabot purge [N] [from:WHO] [text:WORD] [older:7d] [dry] [yes]` | delete messages matching a filter |
| `!genabot purge all yes` | delete **all** messages in the room (always requires `yes`; cannot be combined with other parameters) |
| `!genabot keep N [from:WHO] [text:WORD] [dry] [yes]` | keep the last N messages, delete everything older |

`purge` / `keep` parameters:

- `N`: how many of the latest matching messages (`purge` without `N` needs at least one filter);
- `from:`: part of the sender's name (`from:webhooks`, `from:@user`);
- `text:`: a word or phrase in the message text (quote phrases: `text:"two words"`);
- `older:`: older than a period: `30m`, `12h`, `7d`, `2w` (`purge` only);
- `room:!id:server`: clean **another room** (including an encrypted one) instead of the current one; see "Encrypted rooms" below;
- `dry`: only count, delete nothing;
- `yes`: confirm a deletion of more than 20 messages (`CONFIRM_OVER`).

Examples: `!genabot keep 100 dry`, `!genabot purge from:webhooks older:7d yes`, `!genabot purge 10`.

Deletion uses the normal Matrix mechanism (redaction): a message becomes an empty shell that stays in the room history. Clients have to page through such stretches for nothing, so after large cleanups the physical purge (see below) is preferable.

## Installation

1. Create the bot account on your server, for example:

   ```bash
   register_new_matrix_user -c /etc/matrix-synapse/homeserver.yaml -u genabot -p 'PASSWORD' --no-admin http://localhost:8008
   ```

2. Copy `.env.example` to `.env` and fill in `MATRIX_HOMESERVER`, `MATRIX_USER`, `MATRIX_PASSWORD`, `ALLOWED_INVITERS`.
3. Start it:

   ```bash
   docker compose up -d --build
   ```

4. Invite the bot to a room and give it moderator power level (50 or higher, so it can delete other people's messages).

The bot obtains an access token itself on first login and stores it in `data/session.json` (mode 600). It accepts invitations only from `ALLOWED_INVITERS` (if empty: from any user of its own server).

## Encrypted rooms

The bot has no E2EE, so it cannot read commands written in an encrypted room. Instead, write the command in any **unencrypted** room the bot is in and add `room:` with the target room's ID (Element: Room settings → Advanced):

```
!genabot keep 100 room:!abc123:example.org dry
!genabot purge all yes room:!abc123:example.org
```

Requirements: `ALLOWED_INVITERS` is set, `ADMIN_MODE=1`, the bot is in the target room with moderator power level, and you are a member of the target room with at least `MIN_LEVEL`. In encrypted rooms the message text is unknown, so the `text:` filter is not available; use `N`, `from:`, `older:` or `all`.

## Limitations

- Commands are read only in **unencrypted** rooms (E2EE is not implemented); encrypted rooms are cleaned remotely, see above.
- Without admin mode the bot sees history only from the moment it was invited (this is how Matrix history visibility works).
- Plain Synapse sync; no inbound ports are opened.

## Admin mode (optional, be careful)

With `ADMIN_MODE=1` and server-admin rights on the bot account, the bot:

- reads the whole room history through the Synapse admin API, ignoring the "since invited" history visibility;
- for `keep N` without filters and `purge older:…` (a period only), **physically erases** all history older than the boundary from the database (`purge_history`), leaving no empty shells. This is irreversible.

To grant the rights: `UPDATE users SET admin=1 WHERE name='@genabot:example.org';` in the Synapse database (Synapse caches this, so restart it afterwards) or use the admin API. **A server admin can do anything**: if the bot's token leaks, the whole server is at risk. Keep `ALLOWED_INVITERS` set and enable the mode only when needed.

To revoke: `ADMIN_MODE=0`, `admin=0` on the account in the database, restart the bot and Synapse.

## Input protection

Commands older than 2 minutes are ignored (after downtime the bot does not execute a backlog), at most 8 commands per minute per user, length and parameter count are limited, multi-line input is rejected, and `del` removes only regular messages. Limits are configurable in `.env`.

## Deletion speed

By default Synapse rate-limits events from a single account, so mass deletion is slow (the bot waits and retries on a limit). You can lift the limit for the bot account via a `ratelimit_override` row (`messages_per_second=0`, `burst_count=0`) or the admin API.

## License

[MIT](LICENSE)
