import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("multilang-relay")

BOT_TOKEN = os.getenv("BOT_TOKEN")
LIBRETRANSLATE_URL = os.getenv("LIBRETRANSLATE_URL", "http://localhost:5000").rstrip("/")
GUILD_ID_RAW = os.getenv("GUILD_ID")
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

ENFORCE_SOURCE_LANGUAGE = os.getenv("ENFORCE_SOURCE_LANGUAGE", "true").lower() in {"1", "true", "yes", "on"}
DETECT_MIN_TEXT_LENGTH = int(os.getenv("DETECT_MIN_TEXT_LENGTH", "4"))
MAX_CONCURRENT_RELAYS = max(1, int(os.getenv("MAX_CONCURRENT_RELAYS", "2")))
TRANSLATE_RETRIES = max(0, int(os.getenv("TRANSLATE_RETRIES", "2")))
TRANSLATE_RETRY_DELAY = float(os.getenv("TRANSLATE_RETRY_DELAY", "1.0"))
STRICT_LATIN_MISMATCH_MIN_CHARS = max(1, int(os.getenv("STRICT_LATIN_MISMATCH_MIN_CHARS", "18")))
STRICT_LATIN_MISMATCH_MIN_WORDS = max(1, int(os.getenv("STRICT_LATIN_MISMATCH_MIN_WORDS", "3")))
NO_DROP_FALLBACK = os.getenv("NO_DROP_FALLBACK", "true").lower() in {"1", "true", "yes", "on"}
FALLBACK_PREFIX_TEMPLATE = os.getenv("FALLBACK_PREFIX_TEMPLATE", "[{source}] ")
TRANSLATE_CHUNK_LIMIT = max(200, int(os.getenv("TRANSLATE_CHUNK_LIMIT", "1200")))
DISCORD_MESSAGE_LIMIT = max(500, int(os.getenv("DISCORD_MESSAGE_LIMIT", "2000")))


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add it in Railway Variables.")

GUILD_ID: Optional[int] = None
if GUILD_ID_RAW:
    try:
        GUILD_ID = int(GUILD_ID_RAW)
    except ValueError as exc:
        raise RuntimeError("GUILD_ID must be a number.") from exc


def is_retryable_translate_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            asyncio.TimeoutError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            aiohttp.ServerDisconnectedError,
        ),
    ):
        return True

    if isinstance(exc, RuntimeError):
        message = str(exc).lower()
        retryable_markers = (
            "libretranslate error 429",
            "libretranslate error 500",
            "libretranslate error 502",
            "libretranslate error 503",
            "libretranslate error 504",
            "server disconnected",
            "timeout",
            "temporarily unavailable",
            "internal error",
            "overloaded",
        )
        return any(marker in message for marker in retryable_markers)

    return False


def build_no_drop_fallback_content(source_lang: str, original_text: str, attachment_links: list[str]) -> str:
    parts: list[str] = []
    cleaned = original_text.strip()
    if cleaned:
        prefix = FALLBACK_PREFIX_TEMPLATE.format(source=source_lang.lower())
        parts.append(f"{prefix}{cleaned}")
    if attachment_links:
        parts.append("\n".join(attachment_links))
    return "\n\n".join(part for part in parts if part).strip()


class RelayBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True

        super().__init__(command_prefix="!", intents=intents)
        self.db: Optional[aiosqlite.Connection] = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.webhook_cache: dict[int, discord.Webhook] = {}

    async def setup_hook(self) -> None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)
        self.db.row_factory = aiosqlite.Row
        await self.init_db()

        timeout = aiohttp.ClientTimeout(total=30)
        self.http_session = aiohttp.ClientSession(timeout=timeout)

        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            log.info("Synced %s guild command(s) to guild %s", len(synced), GUILD_ID)
        else:
            synced = await self.tree.sync()
            log.info("Synced %s global command(s)", len(synced))

    async def close(self) -> None:
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        if self.db:
            await self.db.close()
        await super().close()

    async def init_db(self) -> None:
        assert self.db is not None
        await self.db.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS relay_groups (
                guild_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                PRIMARY KEY (guild_id, group_name)
            );

            CREATE TABLE IF NOT EXISTS linked_channels (
                guild_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                channel_id INTEGER NOT NULL UNIQUE,
                language_code TEXT NOT NULL,
                webhook_url TEXT,
                PRIMARY KEY (guild_id, channel_id),
                FOREIGN KEY (guild_id, group_name)
                    REFERENCES relay_groups (guild_id, group_name)
                    ON DELETE CASCADE
            );
            """
        )
        await self.db.commit()

    async def get_group_channels(self, guild_id: int, group_name: str):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT guild_id, group_name, channel_id, language_code, webhook_url
            FROM linked_channels
            WHERE guild_id = ? AND group_name = ?
            ORDER BY channel_id
            """,
            (guild_id, group_name),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def get_channel_link(self, guild_id: int, channel_id: int):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT guild_id, group_name, channel_id, language_code, webhook_url
            FROM linked_channels
            WHERE guild_id = ? AND channel_id = ?
            """,
            (guild_id, channel_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def list_groups(self, guild_id: int):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT group_name
            FROM relay_groups
            WHERE guild_id = ?
            ORDER BY group_name
            """,
            (guild_id,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def create_group(self, guild_id: int, group_name: str):
        assert self.db is not None
        await self.db.execute(
            """
            INSERT INTO relay_groups (guild_id, group_name)
            VALUES (?, ?)
            """,
            (guild_id, group_name),
        )
        await self.db.commit()

    async def delete_group(self, guild_id: int, group_name: str):
        assert self.db is not None
        await self.db.execute(
            """
            DELETE FROM relay_groups
            WHERE guild_id = ? AND group_name = ?
            """,
            (guild_id, group_name),
        )
        await self.db.commit()

    async def upsert_linked_channel(self, guild_id: int, group_name: str, channel_id: int, language_code: str):
        assert self.db is not None
        await self.db.execute(
            """
            INSERT INTO linked_channels (guild_id, group_name, channel_id, language_code)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                guild_id = excluded.guild_id,
                group_name = excluded.group_name,
                language_code = excluded.language_code
            """,
            (guild_id, group_name, channel_id, language_code.lower()),
        )
        await self.db.commit()

    async def remove_linked_channel(self, guild_id: int, channel_id: int):
        assert self.db is not None
        await self.db.execute(
            """
            DELETE FROM linked_channels
            WHERE guild_id = ? AND channel_id = ?
            """,
            (guild_id, channel_id),
        )
        await self.db.commit()

    async def set_webhook_url(self, guild_id: int, channel_id: int, webhook_url: str):
        assert self.db is not None
        await self.db.execute(
            """
            UPDATE linked_channels
            SET webhook_url = ?
            WHERE guild_id = ? AND channel_id = ?
            """,
            (webhook_url, guild_id, channel_id),
        )
        await self.db.commit()

    async def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        if not text.strip():
            return text

        if source_lang.lower() == target_lang.lower():
            return text

        assert self.http_session is not None

        translated_chunks: list[str] = []

        for piece in build_translate_chunks(text):
            payload = {
                "q": piece,
                "source": source_lang.lower(),
                "target": target_lang.lower(),
                "format": "text",
            }

            last_exc: Optional[Exception] = None

            for attempt in range(TRANSLATE_RETRIES + 1):
                try:
                    async with self.http_session.post(f"{LIBRETRANSLATE_URL}/translate", json=payload) as response:
                        body = await response.text()
                        if response.status >= 400:
                            raise RuntimeError(f"LibreTranslate error {response.status}: {body}")

                        data = await response.json()
                        translated = data.get("translatedText")
                        if not translated:
                            raise RuntimeError(f"Unexpected translation response: {data}")

                        translated_chunks.append(translated)
                        break
                except Exception as exc:
                    last_exc = exc
                    if attempt < TRANSLATE_RETRIES and is_retryable_translate_error(exc):
                        delay = TRANSLATE_RETRY_DELAY * (attempt + 1)
                        log.warning(
                            "Retrying translation %s -> %s in %.1fs after error: %s",
                            source_lang,
                            target_lang,
                            delay,
                            exc,
                        )
                        await asyncio.sleep(delay)
                        continue
                    raise

            if last_exc is not None and len(translated_chunks) == 0:
                raise last_exc

        return "".join(translated_chunks)

    async def get_languages(self) -> list[dict]:
        assert self.http_session is not None
        async with self.http_session.get(f"{LIBRETRANSLATE_URL}/languages") as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"LibreTranslate error {response.status}: {body}")

            data = await response.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected languages response: {data}")
            return data

    async def detect_language(self, text: str) -> Optional[str]:
        cleaned = text.strip()
        if len(cleaned) < DETECT_MIN_TEXT_LENGTH:
            return None

        assert self.http_session is not None

        async with self.http_session.post(f"{LIBRETRANSLATE_URL}/detect", json={"q": cleaned}) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"LibreTranslate error {response.status}: {body}")

            data = await response.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError(f"Unexpected detect response: {data}")

            best = data[0]
            if not isinstance(best, dict):
                raise RuntimeError(f"Unexpected detect response: {data}")

            language = best.get("language")
            if not language:
                raise RuntimeError(f"Unexpected detect response: {data}")

            return normalize_language_code(str(language))

    async def get_or_create_webhook(self, channel: discord.TextChannel, guild_id: int) -> discord.Webhook:
        cached = self.webhook_cache.get(channel.id)
        if cached:
            return cached

        db_row = await self.get_channel_link(guild_id, channel.id)
        if db_row and db_row["webhook_url"]:
            try:
                webhook = discord.Webhook.from_url(db_row["webhook_url"], session=self.http_session)
                self.webhook_cache[channel.id] = webhook
                return webhook
            except Exception:
                log.warning("Stored webhook for channel %s was invalid. Recreating.", channel.id)

        existing_hooks = await channel.webhooks()
        webhook = discord.utils.get(existing_hooks, name="MultiLang Relay")
        if webhook is None:
            webhook = await channel.create_webhook(name="MultiLang Relay", reason="Translation relay setup")

        await self.set_webhook_url(guild_id, channel.id, webhook.url)
        self.webhook_cache[channel.id] = webhook
        return webhook


def normalize_language_code(code: str) -> str:
    return code.strip().lower().replace("_", "-")


def language_matches(expected: str, detected: str) -> bool:
    expected_norm = normalize_language_code(expected)
    detected_norm = normalize_language_code(detected)

    if expected_norm == detected_norm:
        return True

    expected_base = expected_norm.split("-", 1)[0]
    detected_base = detected_norm.split("-", 1)[0]

    if expected_base == detected_base and expected_base != "zh":
        return True

    zh_aliases = {
        "zh": {"zh", "zh-cn", "zh-sg", "zh-hans", "zh-tw", "zh-hk", "zh-mo", "zh-hant"},
        "zh-hans": {"zh", "zh-cn", "zh-sg", "zh-hans"},
        "zh-hant": {"zh", "zh-tw", "zh-hk", "zh-mo", "zh-hant"},
    }
    if expected_norm.startswith("zh"):
        return detected_norm in zh_aliases.get(expected_norm, {expected_norm})

    return False


def script_bucket_for_language(code: str) -> str:
    norm = normalize_language_code(code)
    base = norm.split("-", 1)[0]

    if base == "ar":
        return "arabic"
    if base == "ru":
        return "cyrillic"
    if base == "ko":
        return "hangul"
    if norm.startswith("zh") or base == "zh":
        return "han"

    return "latin"


def char_script_bucket(ch: str) -> Optional[str]:
    cp = ord(ch)

    if 0x0600 <= cp <= 0x06FF or 0x0750 <= cp <= 0x077F or 0x08A0 <= cp <= 0x08FF:
        return "arabic"
    if 0x0400 <= cp <= 0x04FF or 0x0500 <= cp <= 0x052F:
        return "cyrillic"
    if 0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF:
        return "han"
    if 0x3040 <= cp <= 0x30FF:
        return "kana"
    if 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF:
        return "hangul"
    if ch.isalpha():
        return "latin"

    return None


def detect_text_script(text: str) -> Optional[str]:
    counts: dict[str, int] = {}
    for ch in text:
        bucket = char_script_bucket(ch)
        if not bucket:
            continue
        counts[bucket] = counts.get(bucket, 0) + 1

    if not counts:
        return None

    return max(counts, key=counts.get)


def should_ignore_for_language_mismatch(expected_lang: str, detected_lang: Optional[str], text: str) -> tuple[bool, Optional[str]]:
    if not detected_lang or language_matches(expected_lang, detected_lang):
        return False, None

    expected_script = script_bucket_for_language(expected_lang)
    detected_script = script_bucket_for_language(detected_lang)
    text_script = detect_text_script(text)
    cleaned = " ".join(text.split())
    word_count = len(cleaned.split())

    if text_script and expected_script and text_script != expected_script:
        return True, (
            f"text script {text_script} did not match expected channel script {expected_script} "
            f"(detected language {detected_lang})"
        )

    if expected_script != "latin" or detected_script != "latin":
        return True, (
            f"detected language {detected_lang} did not match expected channel language {expected_lang}"
        )

    if len(cleaned) >= STRICT_LATIN_MISMATCH_MIN_CHARS and word_count >= STRICT_LATIN_MISMATCH_MIN_WORDS:
        return True, (
            f"detected language {detected_lang} did not match expected channel language {expected_lang} "
            f"for a longer Latin-script message"
        )

    return False, (
        f"allowed ambiguous short Latin-script text; detected {detected_lang}, expected {expected_lang}"
    )


bot = RelayBot()


def owner_or_manage_guild() -> app_commands.check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not interaction.user:
            return False

        if interaction.user.id == interaction.guild.owner_id:
            return True

        member = interaction.guild.get_member(interaction.user.id)
        if member and member.guild_permissions.manage_guild:
            return True

        raise app_commands.CheckFailure("You need Manage Server permission to use this command.")

    return app_commands.check(predicate)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    message = str(error)
    if isinstance(error, app_commands.CheckFailure):
        message = "You need **Manage Server** permission to use this command."
    elif isinstance(error, app_commands.CommandInvokeError) and error.original:
        message = f"Error: {error.original}"

    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except Exception:
        log.exception("Failed to send app command error response")


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (%s)", bot.user, bot.user.id)


@bot.tree.command(name="group_create", description="Create a linked translation group.")
@owner_or_manage_guild()
@app_commands.describe(group_name="A short name for this linked channel set.")
async def group_create(interaction: discord.Interaction, group_name: str):
    assert interaction.guild is not None

    try:
        await bot.create_group(interaction.guild.id, group_name.strip())
    except aiosqlite.IntegrityError:
        await interaction.response.send_message(
            f"Group `{group_name}` already exists.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"Created group `{group_name}`.\nNow add channels with `/group_add`.",
        ephemeral=True,
    )


@bot.tree.command(name="group_delete", description="Delete a linked translation group.")
@owner_or_manage_guild()
@app_commands.describe(group_name="The group to delete.")
async def group_delete(interaction: discord.Interaction, group_name: str):
    assert interaction.guild is not None

    groups = await bot.list_groups(interaction.guild.id)
    existing = {row["group_name"] for row in groups}
    if group_name not in existing:
        await interaction.response.send_message(
            f"Group `{group_name}` does not exist.",
            ephemeral=True,
        )
        return

    await bot.delete_group(interaction.guild.id, group_name)
    await interaction.response.send_message(
        f"Deleted group `{group_name}` and all of its linked channels.",
        ephemeral=True,
    )


@bot.tree.command(name="group_add", description="Add a channel and language to a group.")
@owner_or_manage_guild()
@app_commands.describe(
    group_name="The group name to add this channel into.",
    channel="The channel to relay to and from.",
    language_code="Language code, for example en, ar, pt, fr, tr, es, de.",
)
async def group_add(interaction: discord.Interaction, group_name: str, channel: discord.TextChannel, language_code: str):
    assert interaction.guild is not None

    groups = await bot.list_groups(interaction.guild.id)
    existing = {row["group_name"] for row in groups}
    if group_name not in existing:
        await interaction.response.send_message(
            f"Group `{group_name}` does not exist. Create it first with `/group_create`.",
            ephemeral=True,
        )
        return

    language_code = language_code.strip().lower()
    await bot.upsert_linked_channel(interaction.guild.id, group_name, channel.id, language_code)
    await interaction.response.send_message(
        f"Added {channel.mention} to `{group_name}` with language `{language_code}`.",
        ephemeral=True,
    )


@bot.tree.command(name="group_remove", description="Remove a channel from a group.")
@owner_or_manage_guild()
@app_commands.describe(channel="The linked channel to remove.")
async def group_remove(interaction: discord.Interaction, channel: discord.TextChannel):
    assert interaction.guild is not None

    row = await bot.get_channel_link(interaction.guild.id, channel.id)
    if row is None:
        await interaction.response.send_message(
            f"{channel.mention} is not in any linked group.",
            ephemeral=True,
        )
        return

    await bot.remove_linked_channel(interaction.guild.id, channel.id)
    bot.webhook_cache.pop(channel.id, None)
    await interaction.response.send_message(
        f"Removed {channel.mention} from `{row['group_name']}`.",
        ephemeral=True,
    )


@bot.tree.command(name="group_list", description="Show all linked groups and channels.")
@owner_or_manage_guild()
async def group_list(interaction: discord.Interaction):
    assert interaction.guild is not None

    groups = await bot.list_groups(interaction.guild.id)
    if not groups:
        await interaction.response.send_message(
            "No groups yet. Start with `/group_create`.",
            ephemeral=True,
        )
        return

    lines = []
    for row in groups:
        group_name = row["group_name"]
        channels = await bot.get_group_channels(interaction.guild.id, group_name)
        if not channels:
            lines.append(f"**{group_name}**\n- no channels yet")
            continue

        chunk = [f"**{group_name}**"]
        for ch in channels:
            chunk.append(f"- <#{ch['channel_id']}> → `{ch['language_code']}`")
        lines.append("\n".join(chunk))

    await interaction.response.send_message("\n\n".join(lines), ephemeral=True)


@bot.tree.command(name="languages", description="Show the languages available from LibreTranslate.")
@owner_or_manage_guild()
async def languages(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        langs = await bot.get_languages()
    except Exception as exc:
        await interaction.followup.send(
            f"Could not reach LibreTranslate.\nError: {exc}",
            ephemeral=True,
        )
        return

    if not langs:
        await interaction.followup.send(
            "LibreTranslate returned no languages.",
            ephemeral=True,
        )
        return

    lines = []
    for item in langs:
        code = item.get("code", "?")
        name = item.get("name", code)
        lines.append(f"`{code}` — {name}")

    chunks = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > 1800:
            chunks.append(current)
            current = line
        else:
            current = f"{current}\n{line}".strip()
    if current:
        chunks.append(current)

    await interaction.followup.send(chunks[0], ephemeral=True)
    for extra in chunks[1:]:
        await interaction.followup.send(extra, ephemeral=True)


@bot.tree.command(name="test_translate", description="Translate a test message using LibreTranslate.")
@owner_or_manage_guild()
@app_commands.describe(
    target_language="Target language code, for example ar, pt, fr.",
    text="The text to translate.",
    source_language="Source language code. Keep 'auto' if unsure.",
)
async def test_translate(
    interaction: discord.Interaction,
    target_language: str,
    text: str,
    source_language: str = "auto",
):
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        translated = await bot.translate_text(text, source_language, target_language)
    except Exception as exc:
        await interaction.followup.send(
            f"Translation failed.\nError: {exc}",
            ephemeral=True,
        )
        return

    await interaction.followup.send(
        f"**Source:** `{source_language}`\n**Target:** `{target_language}`\n**Result:** {translated}",
        ephemeral=True,
    )


async def relay_to_target(
    message: discord.Message,
    source_lang: str,
    group_name: str,
    target: aiosqlite.Row,
    original_text: str,
    attachment_links: list[str],
    display_name: str,
    avatar_url: str,
) -> None:
    if message.guild is None:
        return

    if target["channel_id"] == message.channel.id:
        return

    target_channel = message.guild.get_channel(target["channel_id"])
    if not isinstance(target_channel, discord.TextChannel):
        log.warning("Linked channel %s was not found or is not a text channel", target["channel_id"])
        return

    used_fallback = False

    try:
        translated_text = await bot.translate_text(original_text, source_lang, target["language_code"])
        final_content = translated_text.strip()
    except Exception as exc:
        if not NO_DROP_FALLBACK:
            log.exception(
                "Failed to translate message from %s to %s in group %s: %s",
                source_lang,
                target["language_code"],
                group_name,
                exc,
            )
            return

        used_fallback = True
        final_content = build_no_drop_fallback_content(source_lang, original_text, attachment_links)
        log.warning(
            "Translation failed from %s to %s in group %s. Sending no-drop fallback instead. Error: %s",
            source_lang,
            target["language_code"],
            group_name,
            exc,
        )

    if not used_fallback and attachment_links:
        if final_content:
            final_content += "\n\n"
        final_content += "\n".join(attachment_links)

    if not final_content:
        return

    try:
        webhook = await bot.get_or_create_webhook(target_channel, message.guild.id)
        for chunk in build_discord_chunks(final_content):
            await webhook.send(
                content=chunk,
                username=display_name[:80],
                avatar_url=avatar_url,
                allowed_mentions=discord.AllowedMentions.none(),
            )
    except discord.Forbidden:
        log.exception("Missing permission to create or use webhooks in #%s", target_channel.name)
    except Exception:
        log.exception("Failed to relay message into #%s", target_channel.name)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild is None:
        return

    if message.author.bot:
        return

    if message.webhook_id is not None:
        return

    if not message.content and not message.attachments:
        return

    row = await bot.get_channel_link(message.guild.id, message.channel.id)
    if row is None:
        return

    group_channels = await bot.get_group_channels(message.guild.id, row["group_name"])
    if len(group_channels) < 2:
        return

    source_lang = row["language_code"]
    original_text = message.content or ""
    attachment_links = [a.url for a in message.attachments]
    display_name = message.author.display_name
    avatar_url = message.author.display_avatar.url

    if ENFORCE_SOURCE_LANGUAGE and original_text.strip():
        try:
            detected_language = await bot.detect_language(original_text)
        except Exception as exc:
            log.warning(
                "Language detection failed for message in #%s. Continuing with channel language %s. Error: %s",
                getattr(message.channel, "name", message.channel.id),
                source_lang,
                exc,
            )
            detected_language = None

        ignore_message, ignore_reason = should_ignore_for_language_mismatch(
            source_lang,
            detected_language,
            original_text,
        )
        if ignore_message:
            log.info(
                "Ignored message in #%s because %s",
                getattr(message.channel, "name", message.channel.id),
                ignore_reason,
            )
            return
        if ignore_reason:
            log.debug(
                "Language guard allowed message in #%s: %s",
                getattr(message.channel, "name", message.channel.id),
                ignore_reason,
            )

    relay_targets = [
        target
        for target in group_channels
        if target["channel_id"] != message.channel.id
    ]

    if relay_targets:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_RELAYS)

        async def run_relay(target: aiosqlite.Row) -> None:
            async with semaphore:
                await relay_to_target(
                    message=message,
                    source_lang=source_lang,
                    group_name=row["group_name"],
                    target=target,
                    original_text=original_text,
                    attachment_links=attachment_links,
                    display_name=display_name,
                    avatar_url=avatar_url,
                )

        await asyncio.gather(*(run_relay(target) for target in relay_targets), return_exceptions=True)

    await bot.process_commands(message)


if __name__ == "__main__":
    bot.run(BOT_TOKEN)
