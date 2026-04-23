# Discord MultiLang Relay

A self-hosted Discord translation relay bot that links channels by language and reposts translated messages with the sender's display name and avatar through webhooks.

## What this first version does

- links channels into named groups
- assigns one language code to each linked channel
- translates messages from one linked channel into the other linked channels
- reposts the translated copy using the sender's name and avatar
- stores setup in SQLite
- uses LibreTranslate as the translation backend

## What this first version does not do yet

- no dashboard web panel
- no thread support
- no reply mirroring
- no real file re-upload yet; attachments are forwarded as links
- no per-user opt-out
- no auto language correction if someone writes the wrong language in the wrong channel

## Required Discord permissions

The bot should have these permissions in the linked channels:

- View Channels
- Send Messages
- Manage Webhooks
- Read Message History

Also make sure **Message Content Intent** is enabled for the bot in the Discord Developer Portal.

## Environment variables

Copy `.env.example` into your Railway variables:

- `BOT_TOKEN` = your bot token
- `GUILD_ID` = your test server ID for fast slash-command sync
- `LIBRETRANSLATE_URL` = URL of your LibreTranslate service
- `DB_PATH` = keep `/data/bot.db`
- `LOG_LEVEL` = optional, default `INFO`

## Commands

Use these slash commands inside your server:

- `/group_create`
- `/group_add`
- `/group_remove`
- `/group_list`
- `/group_delete`
- `/languages`
- `/test_translate`

## Typical setup

Example:

1. `/group_create international`
2. `/group_add international #eng-chat en`
3. `/group_add international #ara-chat ar`
4. `/group_add international #por-chat pt`
5. `/languages` if you want to check available language codes
6. send a message in one linked channel

## Railway notes

Use a persistent volume for `/data` so your SQLite database survives redeploys.

You will normally run **two Railway services**:

1. this bot service
2. a separate LibreTranslate service

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Next improvements you can ask for

- mirrored replies
- attachment re-uploading
- better admin commands
- one-click setup command
- per-group on/off toggle
- admin dashboard
