import asyncio
import base64
import json
import logging
import os
import re
import time
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

# Optional route logging for translation debugging.
# This logs which translation path handled the message without logging private message content.
ENABLE_TRANSLATION_ROUTE_LOGS = os.getenv("ENABLE_TRANSLATION_ROUTE_LOGS", "true").lower() in {"1", "true", "yes", "on"}

BOT_TOKEN = os.getenv("BOT_TOKEN")
LIBRETRANSLATE_URL = os.getenv("LIBRETRANSLATE_URL", "http://localhost:5000").rstrip("/")
# Multi-server mode: commands are registered globally, and each server keeps its
# own configuration in the database by guild_id. Do not set a GUILD_ID.
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

# Automatic VPS-side database backups. Backups are saved inside the Docker data
# volume by default, usually visible on the VPS at:
# /opt/discord-multilang-relay/data/backups/
ENABLE_DB_BACKUPS = os.getenv("ENABLE_DB_BACKUPS", "true").lower() in {"1", "true", "yes", "on"}
DB_BACKUP_DIR = os.getenv("DB_BACKUP_DIR", str(Path(DB_PATH).parent / "backups"))
DB_BACKUP_INTERVAL_HOURS = max(1.0, float(os.getenv("DB_BACKUP_INTERVAL_HOURS", "24")))
DB_BACKUP_KEEP_DAYS = max(1, int(os.getenv("DB_BACKUP_KEEP_DAYS", "14")))

# Cleanup old relay records so edit/delete/reaction-sync metadata does not grow
# forever. This does not delete Discord messages, only old mapping records.
ENABLE_RELAY_RECORD_CLEANUP = os.getenv("ENABLE_RELAY_RECORD_CLEANUP", "true").lower() in {"1", "true", "yes", "on"}
RELAY_RECORD_RETENTION_DAYS = max(1, int(os.getenv("RELAY_RECORD_RETENTION_DAYS", "30")))
RELAY_RECORD_CLEANUP_INTERVAL_HOURS = max(1.0, float(os.getenv("RELAY_RECORD_CLEANUP_INTERVAL_HOURS", "24")))

# Recovery cleanup for missed delete events. If the bot is restarted/offline
# while a user deletes an original message, Discord will not replay that delete
# event later. This checker finds recent relay records whose original message no
# longer exists and deletes the mirrored webhook messages too.
ENABLE_ORPHAN_RELAY_CLEANUP = os.getenv("ENABLE_ORPHAN_RELAY_CLEANUP", "true").lower() in {"1", "true", "yes", "on"}
ORPHAN_RELAY_CLEANUP_INTERVAL_HOURS = max(0.25, float(os.getenv("ORPHAN_RELAY_CLEANUP_INTERVAL_HOURS", "1")))
ORPHAN_RELAY_CLEANUP_BATCH_SIZE = max(10, int(os.getenv("ORPHAN_RELAY_CLEANUP_BATCH_SIZE", "75")))
ORPHAN_RELAY_CLEANUP_GRACE_MINUTES = max(1, int(os.getenv("ORPHAN_RELAY_CLEANUP_GRACE_MINUTES", "5")))

# Persistent translation cache. This survives bot restarts and makes repeated
# phrases/messages faster, especially for AI-assisted Arabic/Korean translations.
ENABLE_TRANSLATION_CACHE = os.getenv("ENABLE_TRANSLATION_CACHE", "true").lower() in {"1", "true", "yes", "on"}
TRANSLATION_CACHE_MAX_ROWS = max(100, int(os.getenv("TRANSLATION_CACHE_MAX_ROWS", "5000")))
TRANSLATION_CACHE_TTL_DAYS = max(1, int(os.getenv("TRANSLATION_CACHE_TTL_DAYS", "30")))
TRANSLATION_CACHE_MAX_CHARS = max(50, int(os.getenv("TRANSLATION_CACHE_MAX_CHARS", "800")))


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
# Protect syntax/items that translation engines should never translate.
# Examples:
# - custom emojis: <:catglare:1485998314796089476>, <a:dance:123456789012345678>
# - plain Discord-style emoji names: :smile:, :catglare:
# - normal Unicode emojis: 👉👈, 😂, ❤️
# - mentions/channels/roles: <@123>, <@!123>, <#123>, <@&123>
# - Discord timestamps: <t:1700000000:R>
UNICODE_EMOJI_UNIT_PATTERN = (
    "[\U0001F1E6-\U0001F1FF]{2}"
    "|[\U0001F300-\U0001FAFF][\uFE0F\uFE0E]?(?:\u200D[\U0001F300-\U0001FAFF][\uFE0F\uFE0E]?)*"
    "|[\u2600-\u27BF]\uFE0F?"
)
# Match adjacent emoji as one unit, e.g. 👉👈, so RTL languages do not reorder them.
UNICODE_EMOJI_PATTERN = f"(?:{UNICODE_EMOJI_UNIT_PATTERN})+"
COLON_EMOJI_PATTERN = r":[A-Za-z0-9_]{2,32}:"
# Protect links/GIF URLs from translation. Translators can corrupt URLs by
# adding spaces, translating slug words, or appending language names.
URL_PATTERN = r"(?:https?://|www\.)[^\s<>()]+"
# Protect Discord/chat hashtag-style numbers/tags like #295, #280, #event.
# Translators can drop or move these if they are not kept as protected tokens.
HASHTAG_TOKEN_PATTERN = r"(?<![A-Za-z0-9_])#[A-Za-z0-9_]{1,64}"

DISCORD_PROTECTED_TOKEN_RE = re.compile(
    URL_PATTERN +
    r"|<a?:[A-Za-z0-9_]{2,32}:\d{15,25}>"
    r"|<@!?\d{15,25}>"
    r"|<@&\d{15,25}>"
    r"|<#\d{15,25}>"
    r"|<t:\d{1,12}(?::[tTdDfFR])?>"
    r"|" + HASHTAG_TOKEN_PATTERN +
    r"|" + COLON_EMOJI_PATTERN +
    r"|" + UNICODE_EMOJI_PATTERN
)

CUSTOM_EMOJI_RE = re.compile(r"<(?P<animated>a?):(?P<name>[A-Za-z0-9_]{2,32}):(?P<id>\d{15,25})>")
ENABLE_APP_EMOJI_CACHE = os.getenv("ENABLE_APP_EMOJI_CACHE", "true").lower() in {"1", "true", "yes", "on"}
DISCORD_API_BASE = "https://discord.com/api/v10"

# Optional local AI helper for Arabic dialect translation.
# This is self-hosted through Ollama, so it has no paid API/character quota.
# Keep it disabled until the ollama container is reachable from the bot container.
ENABLE_ARABIC_AI_TRANSLATION = os.getenv("ENABLE_ARABIC_AI_TRANSLATION", "false").lower() in {"1", "true", "yes", "on"}
ARABIC_AI_URL = os.getenv("ARABIC_AI_URL", "http://ollama:11434/api/generate")
ARABIC_AI_MODEL = os.getenv("ARABIC_AI_MODEL", "aya-expanse:8b")
ARABIC_AI_TIMEOUT = max(10.0, float(os.getenv("ARABIC_AI_TIMEOUT", "45")))
ARABIC_AI_MAX_CHARS = max(100, int(os.getenv("ARABIC_AI_MAX_CHARS", "800")))
ARABIC_AI_NUM_PREDICT = max(80, int(os.getenv("ARABIC_AI_NUM_PREDICT", "260")))
ARABIC_AI_KEEP_ALIVE = os.getenv("ARABIC_AI_KEEP_ALIVE", "30m")
ARABIC_AI_WARMUP_ON_START = os.getenv("ARABIC_AI_WARMUP_ON_START", "true").lower() in {"1", "true", "yes", "on"}

# Optional local AI helper for Korean translation quality.
# Reuses the same Ollama/Aya model by default. This fixes weak LibreTranslate
# Korean results such as "idiot" becoming unrelated words.
ENABLE_KOREAN_AI_TRANSLATION = os.getenv("ENABLE_KOREAN_AI_TRANSLATION", "false").lower() in {"1", "true", "yes", "on"}
KOREAN_AI_URL = os.getenv("KOREAN_AI_URL", ARABIC_AI_URL)
KOREAN_AI_MODEL = os.getenv("KOREAN_AI_MODEL", ARABIC_AI_MODEL)
KOREAN_AI_TIMEOUT = max(10.0, float(os.getenv("KOREAN_AI_TIMEOUT", str(ARABIC_AI_TIMEOUT))))
KOREAN_AI_MAX_CHARS = max(100, int(os.getenv("KOREAN_AI_MAX_CHARS", "800")))
KOREAN_AI_NUM_PREDICT = max(80, int(os.getenv("KOREAN_AI_NUM_PREDICT", "220")))
KOREAN_AI_KEEP_ALIVE = os.getenv("KOREAN_AI_KEEP_ALIVE", ARABIC_AI_KEEP_ALIVE)
KOREAN_AI_WARMUP_ON_START = os.getenv("KOREAN_AI_WARMUP_ON_START", "true").lower() in {"1", "true", "yes", "on"}

# Optional full-AI translation test mode.
# Default "hybrid" keeps the current setup:
# - LibreTranslate for normal translations
# - Aya/Ollama only for Arabic/Korean weak spots
#
# Set TRANSLATION_ENGINE=ai to route all text translations through the local
# Ollama/Aya model first, with LibreTranslate kept as fallback if AI fails.
TRANSLATION_ENGINE = os.getenv("TRANSLATION_ENGINE", "hybrid").strip().lower()
ENABLE_FULL_AI_TRANSLATION = TRANSLATION_ENGINE in {"ai", "full_ai", "ollama", "local_ai"}
FULL_AI_URL = os.getenv("FULL_AI_URL", ARABIC_AI_URL)
FULL_AI_MODEL = os.getenv("FULL_AI_MODEL", ARABIC_AI_MODEL)
FULL_AI_TIMEOUT = max(10.0, float(os.getenv("FULL_AI_TIMEOUT", str(ARABIC_AI_TIMEOUT))))
FULL_AI_MAX_CHARS = max(100, int(os.getenv("FULL_AI_MAX_CHARS", "800")))
FULL_AI_NUM_PREDICT = max(80, int(os.getenv("FULL_AI_NUM_PREDICT", "260")))
FULL_AI_KEEP_ALIVE = os.getenv("FULL_AI_KEEP_ALIVE", ARABIC_AI_KEEP_ALIVE)
FULL_AI_WARMUP_ON_START = os.getenv("FULL_AI_WARMUP_ON_START", "true").lower() in {"1", "true", "yes", "on"}
FULL_AI_BATCH_TRANSLATION = os.getenv("FULL_AI_BATCH_TRANSLATION", "true").lower() in {"1", "true", "yes", "on"}
FULL_AI_BATCH_MAX_TARGETS = max(1, int(os.getenv("FULL_AI_BATCH_MAX_TARGETS", "12")))
FULL_AI_BATCH_NUM_PREDICT = max(200, int(os.getenv("FULL_AI_BATCH_NUM_PREDICT", "900")))
FULL_AI_BATCH_TIMEOUT = max(5.0, float(os.getenv("FULL_AI_BATCH_TIMEOUT", "15")))
FULL_AI_PER_TARGET_AFTER_BATCH_FAILURE = os.getenv("FULL_AI_PER_TARGET_AFTER_BATCH_FAILURE", "false").lower() in {"1", "true", "yes", "on"}

# Webhook messages cannot reliably create Discord's native reply bubble while
# also preserving the original user's name/avatar. Instead, mirrored replies can
# include a small translated context line above the message.
ENABLE_REPLY_CONTEXT = os.getenv("ENABLE_REPLY_CONTEXT", "true").lower() in {"1", "true", "yes", "on"}
REPLY_CONTEXT_MAX_CHARS = max(40, int(os.getenv("REPLY_CONTEXT_MAX_CHARS", "180")))


def make_app_emoji_name(original_name: str, emoji_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", original_name).strip("_") or "emoji"
    suffix = str(emoji_id)[-8:]
    prefix = "r"
    max_base_len = max(2, 32 - len(prefix) - 1 - len(suffix))
    safe = safe[:max_base_len].strip("_") or "emoji"
    name = f"{prefix}_{safe}_{suffix}"
    return name[:32]


def app_emoji_token(name: str, emoji_id: str, animated: bool = False) -> str:
    return f"<{'a' if animated else ''}:{name}:{emoji_id}>"


RTL_LANGUAGE_BASES = {"ar", "he", "fa", "ur"}
LTR_ISOLATE = "\u2066"
POP_DIRECTIONAL_ISOLATE = "\u2069"


def is_rtl_language_code(code: str) -> bool:
    return canonical_language_code(code).split("-", 1)[0] in RTL_LANGUAGE_BASES


def should_direction_wrap_token(token: str) -> bool:
    # Wrap normal Unicode emoji clusters and hashtag tokens for RTL targets.
    #
    # Discord custom/app emojis like <:name:id> should NOT be wrapped with
    # invisible direction marks. Those hidden marks can make Discord render
    # emoji-only custom emoji messages as small inline emojis in Arabic channels.
    # Custom emojis are parsed by Discord before display, so they do not need
    # LTR isolate wrapping.
    #
    # Hashtag/game-state tokens such as #295 and #280 should be wrapped in RTL
    # languages so they keep their left-to-right visual order.
    return (
        re.fullmatch(UNICODE_EMOJI_PATTERN, token) is not None
        or re.fullmatch(HASHTAG_TOKEN_PATTERN, token) is not None
    )


def wrap_protected_token_for_target(token: str, target_lang: str) -> str:
    # Discord/Unicode RTL rendering can visually reverse emoji clusters like 👉👈
    # around Arabic text. LTR isolate keeps the emoji/token order stable without
    # changing the visible text.
    if not is_rtl_language_code(target_lang):
        return token
    if not should_direction_wrap_token(token):
        return token
    return f"{LTR_ISOLATE}{token}{POP_DIRECTIONAL_ISOLATE}"


def normalize_hashtag_spacing(text: str) -> str:
    """Keep protected hashtag tokens readable after translation.

    Translators often glue protected placeholders back to punctuation, e.g.
    "us?#295". Add a space before visible or LTR-isolated hashtags without
    touching URL fragments like example.com/page#section.
    """
    if not text:
        return text

    # Visible hashtags: "text?#295" -> "text? #295"
    text = re.sub(r"(?<=[^\sA-Za-z0-9_/\\])(?=#[A-Za-z0-9_]{1,64}\b)", " ", text)

    # RTL wrapped hashtags: "معنا؟\u2066#295\u2069" -> "معنا؟ \u2066#295\u2069"
    text = re.sub(
        rf"(?<=[^\sA-Za-z0-9_/\\])(?={re.escape(LTR_ISOLATE)}#[A-Za-z0-9_]{{1,64}}{re.escape(POP_DIRECTIONAL_ISOLATE)})",
        " ",
        text,
    )

    return text


def normalize_translated_output_for_target(text: str, target_lang: str) -> str:
    text = normalize_hashtag_spacing(text)

    # LibreTranslate sometimes returns duplicated punctuation for Arabic, e.g.
    # "??" or "؟؟" after a translated question. Collapse those to one Arabic
    # question mark without touching normal text.
    if canonical_language_code(target_lang).split("-", 1)[0] == "ar":
        # Only collapse repeated question punctuation. This avoids changing normal
        # protected links that may contain a single ? in the URL query string.
        text = re.sub(r"[؟?]{2,}", "؟", text)

    return normalize_hashtag_spacing(text)


CHAT_SLANG_REPLACEMENTS: dict[str, str] = {
    "u": "you",
    "ur": "your",
    "r": "are",
    "pls": "please",
    "plz": "please",
    "ty": "thank you",
    "thx": "thanks",
    "thanx": "thanks",
    "idk": "I don't know",
    "imo": "in my opinion",
    "rn": "right now",
    "btw": "by the way",
    "ppl": "people",
    "msg": "message",
    "cuz": "because",
    "coz": "because",
    "bc": "because",
    "bcs": "because",
    "wanna": "want to",
    "gonna": "going to",
    "gotta": "have to",
    "lemme": "let me",
    "kinda": "kind of",
    "tho": "though",
    "tbh": "to be honest",
}

CHAT_SLANG_RE = re.compile(
    r"\b(" + "|".join(re.escape(key) for key in sorted(CHAT_SLANG_REPLACEMENTS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


def match_case_replacement(original: str, replacement: str) -> str:
    if original.isupper():
        return replacement.upper()
    if original[:1].isupper():
        return replacement[:1].upper() + replacement[1:]
    return replacement


def normalize_chat_slang_for_translation(text: str, source_lang: str) -> str:
    # Only normalize common English chat slang before sending text to LibreTranslate.
    # This helps casual messages like "Hope u like..." translate correctly.
    if canonical_language_code(source_lang).split("-", 1)[0] != "en":
        return text

    def replace(match: re.Match) -> str:
        original = match.group(0)
        replacement = CHAT_SLANG_REPLACEMENTS.get(original.lower())
        if not replacement:
            return original
        return match_case_replacement(original, replacement)

    return CHAT_SLANG_RE.sub(replace, text)


LANGUAGE_DISPLAY_NAMES: dict[str, str] = {
    "auto": "the source language",
    "en": "English",
    "ar": "Arabic",
    "pt": "Portuguese",
    "es": "Spanish",
    "ru": "Russian",
    "pl": "Polish",
    "tr": "Turkish",
    "ko": "Korean",
    "zh": "Simplified Chinese",
    "zh-hans": "Simplified Chinese",
    "zh-cn": "Simplified Chinese",
    "zh-sg": "Simplified Chinese",
    "zt": "Traditional Chinese",
    "zh-hant": "Traditional Chinese",
    "zh-tw": "Traditional Chinese",
    "zh-hk": "Traditional Chinese",
    "zh-mo": "Traditional Chinese",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "ja": "Japanese",
    "nl": "Dutch",
    "id": "Indonesian",
    "hi": "Hindi",
    "ur": "Urdu",
}


def language_display_name(code: str) -> str:
    normalized = canonical_language_code(code)
    if normalized in LANGUAGE_DISPLAY_NAMES:
        return LANGUAGE_DISPLAY_NAMES[normalized]

    base = normalized.split("-", 1)[0]
    if base in LANGUAGE_DISPLAY_NAMES:
        return LANGUAGE_DISPLAY_NAMES[base]

    # Last-resort label for custom language codes.
    return normalized.upper()



def log_translation_route(route: str, source_lang: str, target_lang: str, text: str, detail: str = "") -> None:
    """Log the translation path without exposing message content."""
    if not ENABLE_TRANSLATION_ROUTE_LOGS:
        return

    try:
        source_norm = canonical_language_code(source_lang)
    except Exception:
        source_norm = str(source_lang)

    try:
        target_norm = canonical_language_code(target_lang)
    except Exception:
        target_norm = str(target_lang)

    suffix = f" {detail}" if detail else ""
    log.info(
        "translation_route route=%s %s->%s chars=%s%s",
        route,
        source_norm,
        target_norm,
        len(text or ""),
        suffix,
    )


def normalize_ai_cache_text(text: str) -> str:
    """Make tiny/simple repeated chat messages reuse the same AI cache entry.

    This makes "Hello", "hello", "HELLO", and " hello " share a cached
    translation, while avoiding aggressive lowercasing for longer messages where
    casing can carry meaning.
    """
    cleaned = " ".join(text.strip().split())
    if not cleaned:
        return cleaned

    if "\n" not in cleaned and len(cleaned) <= 80 and not DISCORD_PROTECTED_TOKEN_RE.search(cleaned):
        return cleaned.lower()

    return cleaned


def should_use_persistent_translation_cache(text: str) -> bool:
    """Return True when a translation is safe/useful to persist in SQLite."""
    if not ENABLE_TRANSLATION_CACHE:
        return False

    cleaned = text.strip()
    if not cleaned:
        return False

    if len(cleaned) > TRANSLATION_CACHE_MAX_CHARS:
        return False

    # Avoid caching messages that contain Discord IDs, URLs, or emoji tokens.
    # Those can be user/server-specific and are already protected separately.
    if DISCORD_PROTECTED_TOKEN_RE.search(cleaned):
        return False

    return True


COMMON_PHRASE_RE = re.compile(r"^(?P<prefix>\s*)(?P<body>[A-Za-z' ]+?)(?P<punct>[.!?。！？…]*)\s*$", re.DOTALL)

COMMON_PHRASE_TRANSLATIONS: dict[str, dict[str, str]] = {
    "hello": {
        "ar": "مرحبًا",
        "es": "Hola",
        "pt": "Olá",
        "ru": "Привет",
        "pl": "Cześć",
        "tr": "Merhaba",
        "ko": "안녕하세요",
        "zh": "你好",
        "zh-hans": "你好",
        "zt": "你好",
        "zh-hant": "你好",
    },
    "hi": {
        "ar": "مرحبًا",
        "es": "Hola",
        "pt": "Olá",
        "ru": "Привет",
        "pl": "Cześć",
        "tr": "Merhaba",
        "ko": "안녕하세요",
        "zh": "你好",
        "zh-hans": "你好",
        "zt": "你好",
        "zh-hant": "你好",
    },
    "hey": {
        "ar": "مرحبًا",
        "es": "Hola",
        "pt": "Olá",
        "ru": "Привет",
        "pl": "Cześć",
        "tr": "Merhaba",
        "ko": "안녕하세요",
        "zh": "嘿",
        "zh-hans": "嘿",
        "zt": "嘿",
        "zh-hant": "嘿",
    },
    "bye": {
        "ar": "وداعًا",
        "es": "Adiós",
        "pt": "Tchau",
        "ru": "Пока",
        "pl": "Cześć",
        "tr": "Görüşürüz",
        "ko": "안녕히 가세요",
        "zh": "再见",
        "zh-hans": "再见",
        "zt": "再見",
        "zh-hant": "再見",
    },
    "goodbye": {
        "ar": "وداعًا",
        "es": "Adiós",
        "pt": "Adeus",
        "ru": "До свидания",
        "pl": "Do widzenia",
        "tr": "Hoşça kal",
        "ko": "안녕히 가세요",
        "zh": "再见",
        "zh-hans": "再见",
        "zt": "再見",
        "zh-hant": "再見",
    },
    "thanks": {
        "ar": "شكرًا",
        "es": "Gracias",
        "pt": "Obrigado",
        "ru": "Спасибо",
        "pl": "Dzięki",
        "tr": "Teşekkürler",
        "ko": "감사합니다",
        "zh": "谢谢",
        "zh-hans": "谢谢",
        "zt": "謝謝",
        "zh-hant": "謝謝",
    },
    "thank you": {
        "ar": "شكرًا",
        "es": "Gracias",
        "pt": "Obrigado",
        "ru": "Спасибо",
        "pl": "Dziękuję",
        "tr": "Teşekkür ederim",
        "ko": "감사합니다",
        "zh": "谢谢",
        "zh-hans": "谢谢",
        "zt": "謝謝",
        "zh-hant": "謝謝",
    },
    "yes": {
        "ar": "نعم",
        "es": "Sí",
        "pt": "Sim",
        "ru": "Да",
        "pl": "Tak",
        "tr": "Evet",
        "ko": "네",
        "zh": "是的",
        "zh-hans": "是的",
        "zt": "是的",
        "zh-hant": "是的",
    },
    "no": {
        "ar": "لا",
        "es": "No",
        "pt": "Não",
        "ru": "Нет",
        "pl": "Nie",
        "tr": "Hayır",
        "ko": "아니요",
        "zh": "不",
        "zh-hans": "不",
        "zt": "不",
        "zh-hant": "不",
    },
    "ok": {
        "ar": "حسنًا",
        "es": "Está bien",
        "pt": "Tudo bem",
        "ru": "Хорошо",
        "pl": "Okej",
        "tr": "Tamam",
        "ko": "알겠습니다",
        "zh": "好的",
        "zh-hans": "好的",
        "zt": "好的",
        "zh-hant": "好的",
    },
    "okay": {
        "ar": "حسنًا",
        "es": "Está bien",
        "pt": "Tudo bem",
        "ru": "Хорошо",
        "pl": "Okej",
        "tr": "Tamam",
        "ko": "알겠습니다",
        "zh": "好的",
        "zh-hans": "好的",
        "zt": "好的",
        "zh-hant": "好的",
    },
    "lol": {
        "ar": "هههه",
        "es": "jajaja",
        "pt": "kkkk",
        "ru": "хаха",
        "pl": "haha",
        "tr": "haha",
        "ko": "ㅋㅋ",
        "zh": "哈哈",
        "zh-hans": "哈哈",
        "zt": "哈哈",
        "zh-hant": "哈哈",
    },
    "good morning": {
        "ar": "صباح الخير",
        "es": "Buenos días",
        "pt": "Bom dia",
        "ru": "Доброе утро",
        "pl": "Dzień dobry",
        "tr": "Günaydın",
        "ko": "좋은 아침입니다",
        "zh": "早上好",
        "zh-hans": "早上好",
        "zt": "早安",
        "zh-hant": "早安",
    },
    "good night": {
        "ar": "تصبح على خير",
        "es": "Buenas noches",
        "pt": "Boa noite",
        "ru": "Спокойной ночи",
        "pl": "Dobranoc",
        "tr": "İyi geceler",
        "ko": "좋은 밤 되세요",
        "zh": "晚安",
        "zh-hans": "晚安",
        "zt": "晚安",
        "zh-hant": "晚安",
    },
}


def common_phrase_target_key(target_lang: str) -> str:
    normalized = canonical_language_code(target_lang)
    if normalized in TRADITIONAL_CHINESE_CODES:
        return "zh-hant"
    if normalized in SIMPLIFIED_CHINESE_CODES:
        return "zh"
    return normalized.split("-", 1)[0]


def get_common_phrase_translation(text: str, source_lang: str, target_lang: str) -> Optional[str]:
    """Instant translations for very common short English chat messages.

    This keeps full-AI mode fast for repeated/basic Discord messages without
    reducing quality for longer or more nuanced messages.
    """
    source_base = canonical_language_code(source_lang).split("-", 1)[0]
    target_norm = canonical_language_code(target_lang)
    target_base = target_norm.split("-", 1)[0]

    if source_base != "en" or target_base == "en":
        return None

    if "\n" in text or len(text.strip()) > 40:
        return None

    match = COMMON_PHRASE_RE.match(text)
    if not match:
        return None

    phrase = " ".join((match.group("body") or "").strip().lower().split())
    translations = COMMON_PHRASE_TRANSLATIONS.get(phrase)
    if not translations:
        return None

    target_key = common_phrase_target_key(target_norm)
    translated = translations.get(target_key)
    if translated is None:
        return None

    prefix = match.group("prefix") or ""
    punct = match.group("punct") or ""
    return normalize_translated_output_for_target(f"{prefix}{translated}{punct}", target_norm)


# LibreTranslate/Argos can be weak with Arabic dialects such as Egyptian Arabic.
# These rules convert common dialect chat phrases into clear English "pivot"
# phrases first, then the bot translates that pivot to the target language.
ARABIC_LETTER_RE = re.compile(r"[\u0600-\u06FF]")
KOREAN_LETTER_RE = re.compile(r"[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]")
LATIN_LETTER_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ſ]")
MEANINGFUL_TEXT_LETTER_RE = re.compile(
    r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ſ]"
    r"|[\u0400-\u04FF]"
    r"|[\u0600-\u06FF]"
    r"|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]"
    r"|[\u3040-\u30FF]"
    r"|[\u4E00-\u9FFF]"
)
MIXED_LANGUAGE_AUTO_DETECT = os.getenv("MIXED_LANGUAGE_AUTO_DETECT", "true").lower() in {"1", "true", "yes", "on"}
MIXED_LANGUAGE_MIN_CHARS = max(12, int(os.getenv("MIXED_LANGUAGE_MIN_CHARS", "18")))
ARABIC_DIACRITICS_RE = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]")
ARABIC_LINE_RE = re.compile(r"^(?P<prefix>\s*)(?P<body>.*?)(?P<punct>[.!?؟،,…]*)\s*$", re.DOTALL)

ARABIC_CHAR_TRANSLATION = str.maketrans(
    {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ى": "ي",
        "ئ": "ي",
        "ؤ": "و",
        "ة": "ه",
        "گ": "ك",
        "چ": "ج",
        "پ": "ب",
    }
)

ARABIC_DIALECT_EXACT_PIVOTS: dict[str, str] = {
    # Greetings / "how are you?" variations.
    "كيف حالكم": "How are you guys doing?",
    "كيف حالكم اليوم": "How are you guys doing today?",
    "كيف حالكم اليوم يا رفاق": "How are you guys doing today?",
    "كيف حالكم اليوم يا شباب": "How are you guys doing today?",
    "كيف حالكم اليوم يا جماعه": "How are you guys doing today?",
    "كيف حالكم يا رفاق": "How are you guys doing?",
    "كيف حالكم يا شباب": "How are you guys doing?",
    "كيف حالكم يا جماعه": "How are you guys doing?",
    "عاملين ايه": "How are you guys doing?",
    "عاملين اي": "How are you guys doing?",
    "عاملين ايه يا جماعه": "How are you guys doing?",
    "عاملين اي يا جماعه": "How are you guys doing?",
    "عاملين ايه يا شباب": "How are you guys doing?",
    "عاملين اي يا شباب": "How are you guys doing?",
    "عاملين ايه النهارده": "How are you guys doing today?",
    "عاملين اي النهارده": "How are you guys doing today?",
    "عاملين ايه اليوم": "How are you guys doing today?",
    "عاملين اي اليوم": "How are you guys doing today?",
    "عاملين ايه يا جماعه النهارده": "How are you guys doing today?",
    "عاملين اي يا جماعه النهارده": "How are you guys doing today?",
    "عاملين ايه يا شباب النهارده": "How are you guys doing today?",
    "عاملين اي يا شباب النهارده": "How are you guys doing today?",
    "ازيكم يا جماعه عاملين ايه النهارده": "How are you guys doing today?",
    "ازيكم يا جماعه عاملين اي النهارده": "How are you guys doing today?",
    "ازيكو يا جماعه عاملين ايه النهارده": "How are you guys doing today?",
    "ازيكوا يا جماعه عاملين ايه النهارده": "How are you guys doing today?",
    "ازيك يا جماعه عاملين ايه النهارده": "How are you guys doing today?",
    "ازيكم يا شباب عاملين ايه النهارده": "How are you guys doing today?",
    "ازيكو يا شباب عاملين ايه النهارده": "How are you guys doing today?",
    "ازيكوا يا شباب عاملين ايه النهارده": "How are you guys doing today?",
    "ازيك يا شباب عاملين ايه النهارده": "How are you guys doing today?",
    "عامل ايه": "How are you doing?",
    "عامل اي": "How are you doing?",
    "عامله ايه": "How are you doing?",
    "عامله اي": "How are you doing?",
    "ازيكم": "How are you guys doing?",
    "ازيكو": "How are you guys doing?",
    "ازيكوا": "How are you guys doing?",
    "ازيكو يا شباب": "How are you guys doing?",
    "ازيكم يا شباب": "How are you guys doing?",
    "ازيكوا يا شباب": "How are you guys doing?",
    "ازيكو يا جماعه": "How are you guys doing?",
    "ازيكم يا جماعه": "How are you guys doing?",
    "ازيكوا يا جماعه": "How are you guys doing?",
    "ازيك يا شباب": "How are you guys doing?",
    "ازيك يا جماعه": "How are you guys doing?",
    "ازيك": "How are you doing?",
    "ايه الاخبار": "What's up?",
    "اي الاخبار": "What's up?",
    "ايه اخبارك": "How are you doing?",
    "اي اخبارك": "How are you doing?",
    "ايه اخباركم": "How are you guys doing?",
    "اي اخباركم": "How are you guys doing?",
    "اخبارك ايه": "How are you doing?",
    "اخباركم ايه": "How are you guys doing?",

    # Common casual replies / small phrases.
    "تمام": "I'm good.",
    "تمام الحمد لله": "I'm good, thank God.",
    "الحمد لله": "Thank God.",
    "ماشي": "Okay.",
    "معلش": "Sorry.",
    "ولا يهمك": "No worries.",
    "براحتك": "Take your time.",
    "وحشتني": "I missed you.",
    "وحشتوني": "I missed you all.",
    "حبيبي": "My friend.",
    "يا جماعه": "guys",
    "يا شباب": "guys",
}

ARABIC_DIALECT_REGEX_PIVOTS: list[tuple[re.Pattern, str]] = [
    (
        re.compile(r"^(ازيك|ازيكم|ازيكو|ازيكوا)( يا (شباب|جماعه|ناس|رفاق|رجاله|صحاب|اصحابي|حبايب))?$"),
        "How are you guys doing?",
    ),
    (
        re.compile(
            r"^(ازيك|ازيكم|ازيكو|ازيكوا)( يا (شباب|جماعه|ناس|رفاق|رجاله|صحاب|اصحابي|حبايب))? "
            r"(عاملين ايه|عاملين اي|بتعملوا ايه|بتعملو ايه|بتعملوا اي|بتعملو اي)"
            r"( (النهارده|انهارده|اليوم|دلوقتي))?$"
        ),
        "How are you guys doing today?",
    ),
    (
        re.compile(
            r"^(عاملين ايه|عاملين اي|بتعملوا ايه|بتعملو ايه|بتعملوا اي|بتعملو اي)"
            r"( يا (شباب|جماعه|ناس|رفاق|رجاله|صحاب|اصحابي|حبايب))?"
            r"( (النهارده|انهارده|اليوم|دلوقتي))?$"
        ),
        "How are you guys doing today?",
    ),
    (
        re.compile(r"^(عامل ايه|عامل اي|عامله ايه|عامله اي|اخبارك ايه|ايه اخبارك|اي اخبارك)$"),
        "How are you doing?",
    ),
    (
        re.compile(r"^(ايه الاخبار|اي الاخبار|ايه اخباركم|اي اخباركم|اخباركم ايه)( يا (شباب|جماعه|ناس|رفاق|رجاله|صحاب|اصحابي|حبايب))?$"),
        "What's up?",
    ),
    (
        re.compile(r"^(صباح الخير|صباحو)$"),
        "Good morning.",
    ),
    (
        re.compile(r"^(مساء الخير|مسا الخير)$"),
        "Good evening.",
    ),
]


def normalize_arabic_for_rule_matching(text: str) -> str:
    cleaned = ARABIC_DIACRITICS_RE.sub("", text)
    cleaned = cleaned.replace("ـ", "")
    cleaned = cleaned.translate(ARABIC_CHAR_TRANSLATION)
    cleaned = re.sub(r"[^\u0600-\u06FFA-Za-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def normalize_arabic_output_punctuation(punct: str) -> str:
    if not punct:
        return ""
    return punct.replace("؟", "?").replace("،", ",")


def get_arabic_dialect_pivot_for_line(line: str) -> Optional[str]:
    if not ARABIC_LETTER_RE.search(line):
        return None

    match = ARABIC_LINE_RE.match(line)
    if not match:
        return None

    prefix = match.group("prefix") or ""
    body = (match.group("body") or "").strip()
    punct = normalize_arabic_output_punctuation(match.group("punct") or "")

    if not body:
        return None

    key = normalize_arabic_for_rule_matching(body)
    if not key:
        return None

    pivot = ARABIC_DIALECT_EXACT_PIVOTS.get(key)
    if pivot is None:
        for pattern, candidate in ARABIC_DIALECT_REGEX_PIVOTS:
            if pattern.fullmatch(key):
                pivot = candidate
                break

    if pivot is None:
        return None

    # Do not double punctuation if the pivot already ends with punctuation.
    if punct and pivot[-1:] in ".!?":
        return f"{prefix}{pivot}"
    return f"{prefix}{pivot}{punct}"


# Very short Arabic messages are risky for AI translation. A single Arabic
# letter or a short unknown one-word token is often a name, typo, or chat
# fragment. Local AI models may hallucinate explanations for those, so keep
# unknown short fragments unchanged. Known short chat words are handled here
# first through a safe English pivot.
ARABIC_SHORT_CHAT_PIVOTS: dict[str, str] = {
    "نعم": "Yes.",
    "ايوه": "Yes.",
    "ايوا": "Yes.",
    "يب": "Yes.",
    "لا": "No.",
    "كلا": "No.",
    "تمام": "Okay.",
    "تم": "Done.",
    "اوكي": "Okay.",
    "اوك": "Okay.",
    "ماشي": "Okay.",
    "شكرا": "Thank you.",
    "شكرا لك": "Thank you.",
    "يعطيك العافيه": "Thank you.",
    "العفو": "You're welcome.",
    "مرحبا": "Hello.",
    "هلا": "Hello.",
    "اهلا": "Hello.",
    "السلام عليكم": "Peace be upon you.",
    "وعليكم السلام": "Peace be upon you too.",
    "صباح الخير": "Good morning.",
    "مساء الخير": "Good evening.",
    "تصبح على خير": "Good night.",
    "وين": "Where?",
    "فين": "Where?",
    "مين": "Who?",
    "منو": "Who?",
    "متى": "When?",
    "ليش": "Why?",
    "ليه": "Why?",
    "كيف": "How?",
    "كم": "How much?",
    "ايه": "What?",
    "ايش": "What?",
    "وش": "What?",
    "هاها": "Haha.",
    "هه": "Haha.",
    "هههه": "Haha.",
    "ههههه": "Haha.",
    "هههههه": "Haha.",
}


def get_arabic_short_chat_pivot(text: str) -> Optional[str]:
    """Return a safe English pivot for common very short Arabic chat messages."""
    if not ARABIC_LETTER_RE.search(text):
        return None

    if "\n" in text or len(text.strip()) > 40:
        return None

    match = ARABIC_LINE_RE.match(text)
    if not match:
        return None

    prefix = match.group("prefix") or ""
    body = (match.group("body") or "").strip()
    punct = normalize_arabic_output_punctuation(match.group("punct") or "")

    if not body:
        return None

    key = normalize_arabic_for_rule_matching(body)
    if not key:
        return None

    pivot = ARABIC_SHORT_CHAT_PIVOTS.get(key)

    # Arabic laughter can be written with many repeated ه characters.
    if pivot is None and re.fullmatch(r"ه{2,}", key):
        pivot = "Haha."

    if pivot is None:
        return None

    if punct and pivot[-1:] in ".!?":
        return f"{prefix}{pivot}"
    return f"{prefix}{pivot}{punct}"


def is_arabic_ambiguous_fragment_passthrough(text: str, source_lang: str, target_lang: str) -> bool:
    """Return True for tiny Arabic fragments that should be mirrored unchanged.

    Examples: "ا", "ل..", and short one-word names such as "ايكا".
    Translating these gives the AI too little context and can produce
    explanations instead of translations.
    """
    source_base = canonical_language_code(source_lang).split("-", 1)[0]
    target_base = canonical_language_code(target_lang).split("-", 1)[0]

    if source_base != "ar" or target_base == "ar":
        return False

    cleaned = (text or "").strip()
    if not cleaned or "\n" in cleaned:
        return False

    if not ARABIC_LETTER_RE.search(cleaned):
        return False

    # Let protected token splitting handle messages that mix mentions/URLs/emojis
    # with Arabic text.
    if DISCORD_PROTECTED_TOKEN_RE.search(cleaned):
        return False

    # Known Arabic chat words/phrases should still translate.
    if get_arabic_short_chat_pivot(cleaned) is not None:
        return False
    if get_arabic_dialect_pivot_for_line(cleaned) is not None:
        return False

    match = ARABIC_LINE_RE.match(cleaned)
    if not match:
        return False

    body = (match.group("body") or "").strip()
    key = normalize_arabic_for_rule_matching(body)
    if not key:
        return False

    words = key.split()
    arabic_letter_count = len(ARABIC_LETTER_RE.findall(key))

    # One Arabic letter, or two-letter fragments, are too ambiguous to translate.
    if arabic_letter_count <= 2 and all(re.fullmatch(r"[\u0600-\u06FF]+", word) for word in words):
        return True

    # A single short Arabic token that is not a known word is more likely a name
    # or fragment than a sentence. Preserve it exactly.
    if len(words) == 1 and re.fullmatch(r"[\u0600-\u06FF]{1,6}", words[0]):
        return True

    return False


# LibreTranslate can mistranslate very short chat phrases, especially into Korean.
# These exact short-phrase overrides avoid broken outputs like "이름 *" for "Hi/Bye".
SHORT_PHRASE_OVERRIDES: dict[str, dict[str, str]] = {
    "ko": {
        "hi": "안녕하세요",
        "hello": "안녕하세요",
        "hey": "안녕하세요",
        "hiya": "안녕하세요",
        "yo": "안녕하세요",
        "bye": "안녕히 가세요",
        "goodbye": "안녕히 가세요",
        "see you": "또 만나요",
        "see you later": "나중에 봐요",
        "thanks": "감사합니다",
        "thank you": "감사합니다",
        "ty": "감사합니다",
        "yes": "네",
        "yeah": "네",
        "yep": "네",
        "no": "아니요",
        "nope": "아니요",
        "ok": "알겠습니다",
        "okay": "알겠습니다",
        "lol": "ㅋㅋ",
        "gg": "수고하셨습니다",
        "good morning": "좋은 아침입니다",
        "good night": "좋은 밤 되세요",
        "welcome": "환영합니다",
    }
}

SHORT_PHRASE_RE = re.compile(r"^(?P<prefix>\s*)(?P<body>.*?)(?P<punct>[.!?。！？…]*)\s*$", re.DOTALL)


def get_short_phrase_override(text: str, target_lang: str) -> Optional[str]:
    target_norm = canonical_language_code(target_lang)
    overrides = SHORT_PHRASE_OVERRIDES.get(target_norm)
    if not overrides:
        return None

    # Only override simple one-line short phrases. Full sentences should still
    # go through LibreTranslate.
    if "\n" in text or len(text.strip()) > 40:
        return None

    match = SHORT_PHRASE_RE.match(text)
    if not match:
        return None

    prefix = match.group("prefix") or ""
    body = " ".join((match.group("body") or "").strip().lower().split())
    punct = match.group("punct") or ""

    if body not in overrides:
        return None

    return f"{prefix}{overrides[body]}{punct}"



KOREAN_EMOTICON_LINE_RE = re.compile(
    r"^(?P<prefix>\s*)(?P<body>[ㅠㅜㅋㅎ]+)(?P<punct>[.!?~。！？…]*)\s*$"
)


def get_korean_emoticon_override(text: str, source_lang: str, target_lang: str) -> Optional[str]:
    """Convert Korean chat emoticons like ㅠㅠ before AI translation.

    Aya/Ollama can sometimes explain that these are emoticons instead of
    returning a clean Discord-friendly result. These are universal enough to
    mirror as emoji across all target languages.
    """
    source_base = canonical_language_code(source_lang).split("-", 1)[0]
    target_base = canonical_language_code(target_lang).split("-", 1)[0]

    if source_base != "ko" or target_base == "ko":
        return None

    if len(text.strip()) > 40:
        return None

    output_lines: list[str] = []
    saw_override = False

    for raw_line in text.splitlines() or [text]:
        if not raw_line.strip():
            output_lines.append(raw_line)
            continue

        match = KOREAN_EMOTICON_LINE_RE.fullmatch(raw_line)
        if not match:
            return None

        body = match.group("body") or ""
        punct = match.group("punct") or ""
        prefix = match.group("prefix") or ""

        if body and all(ch in {"ㅠ", "ㅜ"} for ch in body):
            output_lines.append(f"{prefix}😭{punct}")
            saw_override = True
            continue

        if body and all(ch == "ㅋ" for ch in body):
            output_lines.append(f"{prefix}😂{punct}")
            saw_override = True
            continue

        if body and all(ch == "ㅎ" for ch in body):
            output_lines.append(f"{prefix}😄{punct}")
            saw_override = True
            continue

        return None

    if not saw_override:
        return None

    return "\n".join(output_lines)


AI_EXPLANATION_MARKERS = (
    "no translation needed",
    "not a meaningful",
    "not meaningful",
    "rather an emoji",
    "rather an emoticon",
    "not a korean text",
    "not korean text",
    "not an arabic text",
    "not arabic text",
    "cannot be translated",
    "can't be translated",
    "does not need translation",
    "doesn't need translation",
    "is an emoji",
    "is an emoticon",
    "this appears to be",
    "appears to be a name",
    "appears to be a term",
    "term of endearment",
    "remains unchanged",
    "i understand your request",
    "please provide",
    "provide the text",
    "original message is missing",
    "source message is missing",
    "original text is missing",
    "compreendo o seu pedido",
    "forneça o texto",
    "eu ficarei feliz em ajudar",
    "我理解你的要求",
    "请提供",
    "原始信息似乎不见了",
    "我很乐意提供帮助",
)


def looks_like_ai_explanation_output(text: str) -> bool:
    # Detect AI commentary that should never be mirrored into Discord.
    cleaned = " ".join((text or "").strip().split())
    if not cleaned:
        return False

    lowered = cleaned.lower()

    if any(marker in lowered for marker in AI_EXPLANATION_MARKERS):
        return True

    # Catch label/template output in English and Korean, for example:
    # "Original message:", "Translated message:", "원본 메시지:", "번역된 메시지:".
    # These usually mean the model answered like an assistant instead of acting
    # as a pure translation engine.
    if re.search(
        r"(?im)(^|\n)\s*(?:"
        r"original\s+message|translated\s+message|source\s+message|source\s+text|"
        r"input\s+message|output\s+message|translation|translated\s+text|"
        r"원본\s*메시지|번역된\s*메시지|번역\s*메시지"
        r")\s*[:：]",
        text or "",
    ):
        return True

    # If the model repeats assistant-style labels in one answer, reject it.
    label_hits = len(re.findall(
        r"(?i)(original\s+message|translated\s+message|source\s+text|translated\s+text|"
        r"원본\s*메시지|번역된\s*메시지)",
        text or "",
    ))
    if label_hits >= 1:
        return True

    # Extra guard for parenthesized AI explanations.
    if cleaned.startswith("(") and cleaned.endswith(")") and len(cleaned) < 240:
        explanation_words = ("translation", "meaningful", "emoji", "emoticon", "text", "message")
        if sum(1 for word in explanation_words if word in lowered) >= 2:
            return True

    return False


# LibreTranslate/Argos can sometimes leak ASS/SSA subtitle styling tags into
# Chinese output, e.g. {\fn黑體\fs22\bord1...}. These should never be shown in
# Discord. Clean them before sending/caching.
ASS_STYLE_TAG_RE = re.compile(r"\{\\[^{}]*\}")
ASS_STYLE_COMMAND_RE = re.compile(r"\\(?:fn|fs|bord|shad|[1234]?a|fscx|fscy|[1234]?c)[A-Za-z0-9&]+", re.IGNORECASE)


def extract_hashtag_tokens(text: str) -> list[str]:
    return re.findall(HASHTAG_TOKEN_PATTERN, text or "")


def missing_hashtag_tokens(source_text: str, translated_text: str) -> list[str]:
    """Return source hashtag tokens that disappeared from a translation."""
    source_tokens = extract_hashtag_tokens(source_text)
    if not source_tokens:
        return []

    translated = translated_text or ""
    missing: list[str] = []
    for token in source_tokens:
        if token not in translated and token not in missing:
            missing.append(token)
    return missing


def clean_translation_engine_artifacts(text: str) -> str:
    """Remove known garbage formatting leaked by local translation engines."""
    if not text:
        return text

    cleaned = text

    # Remove full ASS/SSA tags, including Chinese font names inside the tag.
    # Some engines emit tags such as:
    # {\fn黑體\fs22\bord1\shad0\3ahbe\4ah00\fscx67\fscy66...}
    cleaned = re.sub(r"\{\\[^{}]*\}", "", cleaned)
    cleaned = re.sub(r"\{(?:\\[^{}\\]*){2,}[^{}]*\}", "", cleaned)

    # If a broken tag was partially emitted without braces, strip common style
    # commands while preserving normal translated words around them.
    cleaned = ASS_STYLE_COMMAND_RE.sub("", cleaned)
    cleaned = re.sub(
        r"\\(?:fn|fs|bord|shad|[1234]?a|fscx|fscy|[1234]?c)[^\s{}，。,.!?؛:;]+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    # Remove empty braces or leftover tag fragments.
    cleaned = cleaned.replace("{}", "")
    cleaned = re.sub(r"\{+\s*\}+", "", cleaned)

    # Normalize whitespace around lines after tag removal.
    lines = []
    for line in cleaned.splitlines():
        line = re.sub(r"[ \t]{2,}", " ", line).strip()
        lines.append(line)

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned

def looks_like_translation_artifact_garbage(text: str) -> bool:
    """Detect formatting/code garbage that should not be cached or sent."""
    cleaned = text or ""
    if not cleaned.strip():
        return False

    if re.search(r"\{\\[^{}]*\}", cleaned):
        return True

    # ASS commands showing up in visible text are a strong sign of bad output.
    command_hits = len(ASS_STYLE_COMMAND_RE.findall(cleaned))
    extra_command_hits = len(re.findall(
        r"\\(?:fn|fs|bord|shad|[1234]?a|fscx|fscy|[1234]?c)",
        cleaned,
        flags=re.IGNORECASE,
    ))
    if command_hits + extra_command_hits >= 2:
        return True

    # Guard against repeated raw formatting braces/backslashes.
    if cleaned.count("\\") >= 3 and ("{" in cleaned or "}" in cleaned):
        return True

    return False



def is_non_language_passthrough_text(text: str) -> bool:
    """Return True for signs/separators/IDs that should be mirrored unchanged.

    Examples: "---", "_", "____", "...", "#295 #280", emoji-only lines.
    These are not real language content, so translation engines should not touch
    them.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        return False

    if is_expressive_chat_reaction(cleaned):
        return True

    # Remove protected Discord tokens first. A line that is only emojis,
    # hashtags, mentions, URLs, etc. should be copied as-is.
    without_tokens = DISCORD_PROTECTED_TOKEN_RE.sub("", cleaned).strip()
    if not without_tokens:
        return True

    # If there are no meaningful text letters at all, it is just punctuation,
    # numbers, separators, or symbols.
    return MEANINGFUL_TEXT_LETTER_RE.search(without_tokens) is None


def split_preserving_line_endings(text: str) -> list[tuple[str, str]]:
    """Split text into (line_without_newline, newline_suffix) pairs."""
    if text == "":
        return [("", "")]

    parts: list[tuple[str, str]] = []
    for raw in text.splitlines(keepends=True):
        if raw.endswith("\r\n"):
            parts.append((raw[:-2], "\r\n"))
        elif raw.endswith("\n") or raw.endswith("\r"):
            parts.append((raw[:-1], raw[-1:]))
        else:
            parts.append((raw, ""))

    return parts


COMMON_SPANISH_WORD_RE = re.compile(
    r"\b("
    r"a|al|algo|algunos|amigos|como|con|de|del|dejar|el|en|es|esa|ese|estado|"
    r"están|estan|fueron|la|las|lo|los|meses|no|nosotros|personas|por|porque|"
    r"puede|que|son|uno|veces|ver|viejos|vuelvo|y"
    r")\b",
    re.IGNORECASE,
)


def normalize_spanish_chat_phrase_key(text: str) -> str:
    cleaned = (text or "").strip().lower()
    cleaned = cleaned.strip("¿¡")
    cleaned = re.sub(r"[.!?¿¡…~،。！？]+$", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    # Small accent normalization for common Spanish chat phrases.
    cleaned = (
        cleaned.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return cleaned


SPANISH_CHAT_PHRASE_PIVOTS: dict[str, str] = {
    "que tierno": "how cute",
    "que lindo": "how cute",
    "que bonito": "how nice",
    "que dulce": "how sweet",
    "a veces uno no puede dejar de ver a los viejos amigos": "sometimes you cannot stop seeing old friends",
}


def get_spanish_chat_phrase_pivot(text: str) -> Optional[str]:
    """Return an English pivot for short/common Spanish chat phrases."""
    match = SHORT_PHRASE_RE.match(text)
    if not match:
        return None

    prefix = match.group("prefix") or ""
    body = (match.group("body") or "").strip()
    punct = match.group("punct") or ""

    key = normalize_spanish_chat_phrase_key(body)
    pivot = SPANISH_CHAT_PHRASE_PIVOTS.get(key)
    if pivot is None:
        return None

    return f"{prefix}{pivot}{punct}"


def is_spanish_chat_phrase(text: str) -> bool:
    return get_spanish_chat_phrase_pivot(text) is not None


EXPRESSIVE_CHAT_REACTION_RE = re.compile(
    r"^\s*(?:a+w+|aw+|x+d+|x+|xd+)(?:[.!?…~]*)\s*$",
    re.IGNORECASE,
)


def is_expressive_chat_reaction(text: str) -> bool:
    """Return True for expressive reaction sounds that are safer copied as-is.

    Examples: "aaawww....", "awww", "XD". These are not worth sending through
    translation engines, which can turn them into random words in some targets.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    return EXPRESSIVE_CHAT_REACTION_RE.fullmatch(cleaned) is not None


def looks_like_spanish_latin_text(text: str) -> bool:
    """Lightweight guard for Spanish pasted into another Latin channel."""
    cleaned = " ".join((text or "").strip().split())
    if is_spanish_chat_phrase(cleaned):
        return True

    if len(cleaned) < MIXED_LANGUAGE_MIN_CHARS:
        return False

    if not LATIN_LETTER_RE.search(cleaned):
        return False

    words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ſ']+", cleaned)
    if len(words) < 3:
        return False

    spanish_hits = len(COMMON_SPANISH_WORD_RE.findall(cleaned))
    accented_spanish = bool(re.search(r"[áéíóúñ¿¡]", cleaned, flags=re.IGNORECASE))

    # Require a few Spanish function words, or accented Spanish punctuation.
    return spanish_hits >= 3 or (accented_spanish and spanish_hits >= 1)


def should_try_mixed_language_detection(text: str, source_lang: str) -> bool:
    """Avoid expensive detection unless the line could be the wrong Latin language."""
    if not MIXED_LANGUAGE_AUTO_DETECT:
        return False

    cleaned = (text or "").strip()
    if len(cleaned) < MIXED_LANGUAGE_MIN_CHARS:
        return False

    source_base = canonical_language_code(source_lang).split("-", 1)[0]
    if source_base not in {"en", "es", "pt", "fr", "de", "it", "nl", "pl", "tr"}:
        return False

    if is_non_language_passthrough_text(cleaned):
        return False

    # We only auto-detect Latin text here. Arabic/Korean/script mismatches are
    # handled above with script-specific checks.
    return bool(LATIN_LETTER_RE.search(cleaned))


def should_translate_line_by_line(text: str) -> bool:
    """Return True when per-line translation can preserve separators/mixed languages."""
    if "\n" not in text:
        return False

    lines = split_preserving_line_endings(text)
    non_empty = [line for line, _nl in lines if line.strip()]
    if len(non_empty) <= 1:
        return False

    if any(is_non_language_passthrough_text(line) for line in non_empty):
        return True

    if any(looks_like_spanish_latin_text(line) for line in non_empty):
        return True

    return False



if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add it in your .env or environment variables.")

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


def get_message_sticker_links(message: discord.Message) -> list[str]:
    """Return CDN URLs for stickers attached to a Discord message.

    Webhooks cannot resend a Discord sticker as a real sticker, so the safest
    mirror is the sticker CDN URL. Discord will usually render static/APNG/GIF
    sticker URLs as an image/GIF preview in the target channels.
    """
    links: list[str] = []

    for sticker in getattr(message, "stickers", []) or []:
        url = getattr(sticker, "url", None)
        if url:
            links.append(str(url))
            continue

        sticker_id = getattr(sticker, "id", None)
        if sticker_id is not None:
            links.append(f"https://media.discordapp.net/stickers/{sticker_id}.png?size=160")

    return links



def get_object_or_dict_value(obj, key: str, default=None):
    """Read an attribute from discord.py objects or a key from raw Discord dicts."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def get_url_from_discord_media(obj) -> Optional[str]:
    """Return the best URL from attachments/embed media objects or raw dicts."""
    for key in ("url", "proxy_url"):
        value = get_object_or_dict_value(obj, key)
        if value:
            return str(value)
    return None


def extract_embed_media_links(embed) -> list[str]:
    """Extract useful preview/media links from a Discord embed or raw embed dict."""
    links: list[str] = []

    direct_url = get_object_or_dict_value(embed, "url")
    if direct_url:
        links.append(str(direct_url))

    for field_name in ("image", "thumbnail", "video"):
        media = get_object_or_dict_value(embed, field_name)
        if not media:
            continue
        media_url = get_url_from_discord_media(media)
        if media_url:
            links.append(media_url)

    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def iter_forwarded_message_snapshots(message: discord.Message):
    """Yield forwarded-message snapshot objects if Discord provides them.

    discord.py exposes forwarded messages as message snapshots in newer builds.
    Keep this helper flexible so it works whether snapshots are rich objects or
    raw dict-like payloads.
    """
    for attr_name in ("message_snapshots", "snapshots"):
        snapshots = getattr(message, attr_name, None)
        if not snapshots:
            continue
        for snapshot in snapshots:
            # Some wrappers expose the actual snapshot/message under .message.
            yield get_object_or_dict_value(snapshot, "message", snapshot)


def get_forwarded_message_parts(message: discord.Message) -> tuple[str, list[str]]:
    """Return text/media links from Discord forwarded-message snapshots.

    Webhooks cannot recreate Discord's native "Forwarded" card UI, so the bot
    mirrors the forwarded content/media as normal translated text and links.
    """
    text_parts: list[str] = []
    links: list[str] = []

    for snapshot in iter_forwarded_message_snapshots(message):
        content = get_object_or_dict_value(snapshot, "content", "") or ""
        if content and str(content).strip():
            text_parts.append(str(content).strip())

        for attachment in get_object_or_dict_value(snapshot, "attachments", []) or []:
            url = get_url_from_discord_media(attachment)
            if url:
                links.append(url)

        for embed in get_object_or_dict_value(snapshot, "embeds", []) or []:
            links.extend(extract_embed_media_links(embed))

    deduped_links: list[str] = []
    seen_links: set[str] = set()
    for link in links:
        if link in seen_links:
            continue
        seen_links.add(link)
        deduped_links.append(link)

    return "\n\n".join(text_parts).strip(), deduped_links


def message_has_forwarded_content(message: discord.Message) -> bool:
    """Return True when a message contains Discord forwarded-message snapshots."""
    forwarded_text, forwarded_links = get_forwarded_message_parts(message)
    return bool(forwarded_text or forwarded_links)


def message_has_syncable_content(message: discord.Message) -> bool:
    """Return True if a message has content/media worth mirroring."""
    if message.content:
        return True
    if message.attachments:
        return True
    if getattr(message, "stickers", None):
        return True

    forwarded_text, forwarded_links = get_forwarded_message_parts(message)
    return bool(forwarded_text or forwarded_links)



async def get_webhook_author_identity(message: discord.Message) -> tuple[str, str]:
    """Return the display name/avatar that webhook mirrors should use.

    Prefer the server member profile when available. Fetched messages from raw
    edit events can sometimes expose the author more like a global user, which
    can make edited mirrors switch from server nickname/avatar to global
    profile. This keeps relay and edit-sync consistent.
    """
    author = message.author
    member: Optional[discord.Member] = author if isinstance(author, discord.Member) else None

    if message.guild is not None and member is None:
        member = message.guild.get_member(author.id)
        if member is None:
            try:
                member = await message.guild.fetch_member(author.id)
            except (discord.NotFound, discord.Forbidden):
                member = None
            except Exception:
                log.exception("Failed to fetch guild member %s for webhook identity", author.id)
                member = None

    identity = member or author
    display_name = (
        getattr(identity, "display_name", None)
        or getattr(author, "display_name", None)
        or getattr(author, "name", "Unknown")
    )

    avatar = getattr(identity, "display_avatar", None) or getattr(author, "display_avatar", None)
    avatar_url = avatar.url if avatar is not None else author.default_avatar.url

    return str(display_name), str(avatar_url)


def compact_reply_context_text(text: str, max_chars: int = REPLY_CONTEXT_MAX_CHARS) -> str:
    """Make a one-line reply preview that is short enough for Discord mirrors."""
    compacted = " ".join((text or "").strip().split())
    if not compacted:
        return ""

    if len(compacted) <= max_chars:
        return compacted

    return compacted[: max_chars - 1].rstrip() + "…"


def get_message_reply_preview(message: discord.Message) -> str:
    """Build a short preview for the message being replied to."""
    content = compact_reply_context_text(message.content or "")
    if content:
        return content

    if getattr(message, "stickers", None):
        return "sticker"

    if getattr(message, "attachments", None):
        return "attachment"

    return "message"


async def get_reply_context_for_message(message: discord.Message) -> Optional[dict[str, str]]:
    """Return author/text preview for a replied-to message, if any."""
    if not ENABLE_REPLY_CONTEXT:
        return None

    # Discord forwarded messages can expose a reference/snapshot that looks like
    # a reply to the bot. Do not build reply context for forwarded messages,
    # otherwise the forwarded text appears once in the quote and once as the
    # actual mirrored message.
    if message_has_forwarded_content(message):
        return None

    reference = getattr(message, "reference", None)
    if reference is None or getattr(reference, "message_id", None) is None:
        return None

    referenced = getattr(reference, "resolved", None)
    if not isinstance(referenced, discord.Message):
        channel_id = getattr(reference, "channel_id", None) or message.channel.id
        channel = await resolve_text_channel(int(channel_id))
        if channel is None:
            return None

        try:
            referenced = await channel.fetch_message(int(reference.message_id))
        except discord.NotFound:
            return None
        except Exception:
            log.exception("Failed to fetch replied-to message %s for reply context", reference.message_id)
            return None

    author_name = getattr(referenced.author, "display_name", None) or getattr(referenced.author, "name", "Unknown")
    author_name = compact_reply_context_text(str(author_name), 48)
    preview = get_message_reply_preview(referenced)

    if not preview:
        return None

    author_id = getattr(referenced.author, "id", None)

    return {
        "author": author_name,
        "author_id": str(author_id) if author_id is not None else "",
        "text": preview,
    }


async def build_reply_context_prefix(
    reply_context: Optional[dict[str, str]],
    source_lang: str,
    target_lang: str,
) -> str:
    """Build a translated quote-style line for mirrored replies."""
    if not reply_context:
        return ""

    author = reply_context.get("author") or "Unknown"
    author_id = (reply_context.get("author_id") or "").strip()
    preview = reply_context.get("text") or ""
    preview = compact_reply_context_text(preview)
    if not preview:
        return ""

    translated_preview = preview
    try:
        translated_preview = await bot.translate_text(preview, source_lang, target_lang)
    except Exception as exc:
        # Reply context is nice-to-have. Never block the main relay because of it.
        log.warning(
            "Could not translate reply context from %s to %s. Using original preview. Error: %s",
            source_lang,
            target_lang,
            exc,
        )

    translated_preview = compact_reply_context_text(translated_preview)

    # Show a Discord-style user mention when possible.
    # Webhook sends still use AllowedMentions.none(), so this is meant as a
    # tag-style context label without causing notification spam. If a real
    # mention is available, do not repeat the display name after it.
    author_label = f"<@{author_id}>" if author_id.isdigit() else author
    return f"> ↪ {author_label}: {translated_preview}\n\n"


def split_text_for_limit(text: str, limit: int) -> list[str]:
    cleaned = text.strip()
    if not cleaned:
        return []

    if len(cleaned) <= limit:
        return [cleaned]

    paragraphs = cleaned.split("\n")
    chunks: list[str] = []
    current = ""

    def flush_current() -> None:
        nonlocal current
        if current:
            chunks.append(current)
            current = ""

    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            if current and len(current) + 2 <= limit:
                current += "\n\n"
            else:
                flush_current()
            continue

        if len(paragraph) <= limit:
            candidate = f"{current}\n{paragraph}".strip() if current else paragraph
            if len(candidate) <= limit:
                current = candidate
            else:
                flush_current()
                current = paragraph
            continue

        words = paragraph.split()
        piece = ""
        for word in words:
            candidate = f"{piece} {word}".strip()
            if len(candidate) <= limit:
                piece = candidate
                continue

            if piece:
                candidate = f"{current}\n{piece}".strip() if current else piece
                if len(candidate) <= limit:
                    current = candidate
                else:
                    flush_current()
                    current = piece
                piece = ""

            if len(word) <= limit:
                piece = word
            else:
                for i in range(0, len(word), limit):
                    sub = word[i:i + limit]
                    candidate = f"{current}\n{sub}".strip() if current else sub
                    if len(candidate) <= limit:
                        current = candidate
                    else:
                        flush_current()
                        current = sub

        if piece:
            candidate = f"{current}\n{piece}".strip() if current else piece
            if len(candidate) <= limit:
                current = candidate
            else:
                flush_current()
                current = piece

    flush_current()
    return [chunk for chunk in chunks if chunk]


def build_translate_chunks(text: str) -> list[str]:
    return split_text_for_limit(text, TRANSLATE_CHUNK_LIMIT)


def build_discord_chunks(text: str) -> list[str]:
    return split_text_for_limit(text, DISCORD_MESSAGE_LIMIT)


def canonical_language_code(code: str) -> str:
    return code.strip().lower().replace("_", "-")


TRADITIONAL_CHINESE_CODES = {"zt", "zh-hant", "zh-tw", "zh-hk", "zh-mo", "traditional"}
SIMPLIFIED_CHINESE_CODES = {"zh", "zh-hans", "zh-cn", "zh-sg", "simplified"}

try:
    from opencc import OpenCC

    OPENCC_S2T = OpenCC("s2t")
    OPENCC_T2S = OpenCC("t2s")
except Exception:
    OPENCC_S2T = None
    OPENCC_T2S = None


def is_traditional_chinese_code(code: str) -> bool:
    return canonical_language_code(code) in TRADITIONAL_CHINESE_CODES


def is_simplified_chinese_code(code: str) -> bool:
    return canonical_language_code(code) in SIMPLIFIED_CHINESE_CODES


def libretranslate_language_code(code: str) -> str:
    normalized = canonical_language_code(code)
    if normalized in SIMPLIFIED_CHINESE_CODES:
        return "zh"
    if normalized in TRADITIONAL_CHINESE_CODES:
        return "zt"
    return normalized


def simplified_to_traditional(text: str) -> str:
    if OPENCC_S2T is None:
        raise RuntimeError(
            "Traditional Chinese conversion requires opencc-python-reimplemented. "
            "Add it to requirements.txt and rebuild the Docker image."
        )
    return OPENCC_S2T.convert(text)


def traditional_to_simplified(text: str) -> str:
    if OPENCC_T2S is None:
        raise RuntimeError(
            "Simplified Chinese conversion requires opencc-python-reimplemented. "
            "Add it to requirements.txt and rebuild the Docker image."
        )
    return OPENCC_T2S.convert(text)


class RelayBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.reactions = True

        super().__init__(command_prefix="!", intents=intents)
        self.db: Optional[aiosqlite.Connection] = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.webhook_cache: dict[int, discord.Webhook] = {}
        self.app_emoji_lock = asyncio.Lock()
        self.arabic_ai_lock = asyncio.Lock()
        self.arabic_ai_pivot_cache: dict[str, str] = {}
        self.korean_ai_lock = asyncio.Lock()
        self.korean_ai_cache: dict[tuple[str, str, str], str] = {}
        self.full_ai_lock = asyncio.Lock()
        self.full_ai_cache: dict[tuple[str, str, str], str] = {}
        self.full_ai_batch_cache: dict[tuple[str, tuple[str, ...], str], dict[str, str]] = {}
        self.full_ai_batch_failed_messages: set[tuple[str, str]] = set()
        self.source_message_locks: dict[tuple[int, int, int], asyncio.Lock] = {}
        self.source_message_locks_guard = asyncio.Lock()
        self.language_detect_cache: dict[str, str] = {}

    async def get_source_message_lock(self, guild_id: int, channel_id: int, message_id: int) -> asyncio.Lock:
        """Return a per-source-message lock to prevent send/edit relay races.

        If a user edits a message while the initial mirror is still being sent,
        Discord can deliver the edit event before relay records are saved. The
        lock makes edit-sync wait for the initial relay, so it updates existing
        mirrored messages instead of creating duplicate mirrors.
        """
        key = (int(guild_id), int(channel_id), int(message_id))
        async with self.source_message_locks_guard:
            lock = self.source_message_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self.source_message_locks[key] = lock

            # Lightweight cleanup so the lock map does not grow forever.
            if len(self.source_message_locks) > 10000:
                removed = 0
                for old_key, old_lock in list(self.source_message_locks.items()):
                    if old_key == key:
                        continue
                    if not old_lock.locked():
                        self.source_message_locks.pop(old_key, None)
                        removed += 1
                    if removed >= 2000:
                        break

            return lock

    async def setup_hook(self) -> None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)
        self.db.row_factory = aiosqlite.Row
        await self.init_db()

        timeout = aiohttp.ClientTimeout(total=30)
        self.http_session = aiohttp.ClientSession(timeout=timeout)

        if ENABLE_ARABIC_AI_TRANSLATION and ARABIC_AI_WARMUP_ON_START:
            asyncio.create_task(self.warmup_arabic_ai_model())

        if ENABLE_KOREAN_AI_TRANSLATION and KOREAN_AI_WARMUP_ON_START:
            # Same model as Arabic by default. This keeps Aya warm for Korean too.
            asyncio.create_task(self.warmup_korean_ai_model())

        if ENABLE_FULL_AI_TRANSLATION and FULL_AI_WARMUP_ON_START:
            asyncio.create_task(self.warmup_full_ai_model())

        if ENABLE_DB_BACKUPS:
            asyncio.create_task(self.database_backup_loop())

        if ENABLE_RELAY_RECORD_CLEANUP or ENABLE_ORPHAN_RELAY_CLEANUP or ENABLE_TRANSLATION_CACHE:
            asyncio.create_task(self.database_maintenance_loop())

        synced = await self.tree.sync()
        log.info(
            "Synced %s global command(s). Multi-server mode is enabled; "
            "each Discord server keeps separate groups/channels in the database.",
            len(synced),
        )

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

            CREATE TABLE IF NOT EXISTS relayed_messages (
                guild_id INTEGER NOT NULL,
                source_channel_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                target_channel_id INTEGER NOT NULL,
                target_message_id INTEGER NOT NULL,
                target_index INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (target_message_id),
                UNIQUE (source_message_id, target_channel_id, target_index)
            );

            CREATE TABLE IF NOT EXISTS app_emoji_cache (
                original_emoji_id TEXT PRIMARY KEY,
                original_name TEXT NOT NULL,
                original_animated INTEGER NOT NULL DEFAULT 0,
                app_emoji_id TEXT NOT NULL,
                app_emoji_name TEXT NOT NULL,
                app_emoji_animated INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS translation_cache (
                source_lang TEXT NOT NULL,
                target_lang TEXT NOT NULL,
                cache_text TEXT NOT NULL,
                translated_text TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_used_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                uses INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (source_lang, target_lang, cache_text)
            );
            """
        )
        await self.ensure_db_migrations()
        await self.db.commit()

    async def ensure_db_migrations(self) -> None:
        """Apply lightweight SQLite migrations for existing bot.db files."""
        assert self.db is not None

        cursor = await self.db.execute("PRAGMA table_info(relayed_messages)")
        relayed_columns = {row[1] for row in await cursor.fetchall()}
        await cursor.close()

        if "created_at" not in relayed_columns:
            await self.db.execute("ALTER TABLE relayed_messages ADD COLUMN created_at TEXT")
            await self.db.execute(
                "UPDATE relayed_messages SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
            )

        cursor = await self.db.execute("PRAGMA table_info(translation_cache)")
        cache_columns = {row[1] for row in await cursor.fetchall()}
        await cursor.close()

        # Future-proofing in case an old test version created this table partially.
        required_cache_columns = {
            "source_lang",
            "target_lang",
            "cache_text",
            "translated_text",
            "created_at",
            "last_used_at",
            "uses",
        }
        if cache_columns and not required_cache_columns.issubset(cache_columns):
            log.warning(
                "translation_cache table exists but has an unexpected schema. "
                "Persistent translation cache may be skipped until the table is fixed."
            )

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


    async def replace_relay_records(
        self,
        guild_id: int,
        source_channel_id: int,
        source_message_id: int,
        target_channel_id: int,
        target_message_ids: list[int],
    ) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            DELETE FROM relayed_messages
            WHERE guild_id = ? AND source_channel_id = ? AND source_message_id = ? AND target_channel_id = ?
            """,
            (guild_id, source_channel_id, source_message_id, target_channel_id),
        )
        for index, target_message_id in enumerate(target_message_ids):
            await self.db.execute(
                """
                INSERT OR REPLACE INTO relayed_messages (
                    guild_id,
                    source_channel_id,
                    source_message_id,
                    target_channel_id,
                    target_message_id,
                    target_index,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    guild_id,
                    source_channel_id,
                    source_message_id,
                    target_channel_id,
                    target_message_id,
                    index,
                ),
            )
        await self.db.commit()

    async def get_relay_records_for_source(self, guild_id: int, source_channel_id: int, source_message_id: int):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT guild_id, source_channel_id, source_message_id, target_channel_id, target_message_id, target_index
            FROM relayed_messages
            WHERE guild_id = ? AND source_channel_id = ? AND source_message_id = ?
            ORDER BY target_channel_id, target_index
            """,
            (guild_id, source_channel_id, source_message_id),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def get_relay_record_by_target_message(self, guild_id: int, target_message_id: int):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT guild_id, source_channel_id, source_message_id, target_channel_id, target_message_id, target_index
            FROM relayed_messages
            WHERE guild_id = ? AND target_message_id = ?
            """,
            (guild_id, target_message_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def get_related_message_locations(self, guild_id: int, channel_id: int, message_id: int) -> list[tuple[int, int]]:
        """Return the original message and all mirrored message IDs related to one linked message."""
        source_channel_id = channel_id
        source_message_id = message_id

        source_rows = await self.get_relay_records_for_source(guild_id, source_channel_id, source_message_id)
        if not source_rows:
            target_row = await self.get_relay_record_by_target_message(guild_id, message_id)
            if target_row is None:
                return []
            source_channel_id = target_row["source_channel_id"]
            source_message_id = target_row["source_message_id"]
            source_rows = await self.get_relay_records_for_source(guild_id, source_channel_id, source_message_id)

        locations: list[tuple[int, int]] = [(source_channel_id, source_message_id)]
        for row in source_rows:
            locations.append((row["target_channel_id"], row["target_message_id"]))

        deduped: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()
        for location in locations:
            if location in seen:
                continue
            seen.add(location)
            deduped.append(location)

        return deduped

    async def get_application_id_for_emojis(self) -> int:
        app_id = self.application_id
        if app_id:
            return int(app_id)

        if self.user:
            # For normal Discord bots, the bot user ID is the application/client ID.
            return int(self.user.id)

        raise RuntimeError("Could not determine Discord application ID for app emoji cache.")

    async def discord_api_request(self, method: str, path: str, **kwargs):
        assert self.http_session is not None

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bot {BOT_TOKEN}"
        headers["User-Agent"] = "discord-multilang-relay (app emoji cache)"

        url = f"{DISCORD_API_BASE}{path}"

        for attempt in range(2):
            async with self.http_session.request(method, url, headers=headers, **kwargs) as response:
                body = await response.text()

                if response.status == 429 and attempt == 0:
                    try:
                        retry_after = float((await response.json()).get("retry_after", 1.0))
                    except Exception:
                        retry_after = 1.0
                    await asyncio.sleep(min(max(retry_after, 0.5), 10.0))
                    continue

                if response.status >= 400:
                    raise RuntimeError(f"Discord API error {response.status} {method} {path}: {body}")

                if response.status == 204 or not body.strip():
                    return None

                return await response.json()

        raise RuntimeError(f"Discord API request failed after retry: {method} {path}")

    async def get_cached_app_emoji(self, original_emoji_id: str):
        assert self.db is not None
        cursor = await self.db.execute(
            """
            SELECT original_emoji_id, original_name, original_animated,
                   app_emoji_id, app_emoji_name, app_emoji_animated
            FROM app_emoji_cache
            WHERE original_emoji_id = ?
            """,
            (str(original_emoji_id),),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def set_cached_app_emoji(
        self,
        original_emoji_id: str,
        original_name: str,
        original_animated: bool,
        app_emoji_id: str,
        app_emoji_name: str,
        app_emoji_animated: bool,
    ) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            INSERT INTO app_emoji_cache (
                original_emoji_id,
                original_name,
                original_animated,
                app_emoji_id,
                app_emoji_name,
                app_emoji_animated
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_emoji_id) DO UPDATE SET
                original_name = excluded.original_name,
                original_animated = excluded.original_animated,
                app_emoji_id = excluded.app_emoji_id,
                app_emoji_name = excluded.app_emoji_name,
                app_emoji_animated = excluded.app_emoji_animated
            """,
            (
                str(original_emoji_id),
                original_name,
                1 if original_animated else 0,
                str(app_emoji_id),
                app_emoji_name,
                1 if app_emoji_animated else 0,
            ),
        )
        await self.db.commit()

    async def download_custom_emoji_image(self, emoji_id: str, animated: bool) -> tuple[bytes, str]:
        assert self.http_session is not None

        # GIF preserves animated emojis. WebP is best for static emojis.
        formats = ["gif", "webp"] if animated else ["webp", "png"]
        last_error: Optional[Exception] = None

        for ext in formats:
            url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{ext}"
            if ext == "webp" and animated:
                url += "?animated=true"

            try:
                async with self.http_session.get(url) as response:
                    body = await response.read()
                    if response.status >= 400:
                        raise RuntimeError(f"CDN error {response.status} while downloading emoji {emoji_id}.{ext}")

                    if len(body) > 256 * 1024:
                        raise RuntimeError(
                            f"Emoji {emoji_id}.{ext} is {len(body)} bytes; Discord app emojis must be 256 KiB or less."
                        )

                    content_type = {
                        "gif": "image/gif",
                        "webp": "image/webp",
                        "png": "image/png",
                    }[ext]
                    return body, content_type
            except Exception as exc:
                last_error = exc
                continue

        raise RuntimeError(f"Could not download usable emoji image {emoji_id}: {last_error}")

    async def find_existing_app_emoji_by_name(self, app_emoji_name: str):
        application_id = await self.get_application_id_for_emojis()
        data = await self.discord_api_request("GET", f"/applications/{application_id}/emojis")
        items = data.get("items", []) if isinstance(data, dict) else []
        for item in items:
            if item.get("name") == app_emoji_name:
                return item
        return None

    async def create_or_get_app_emoji_token(self, original_name: str, original_emoji_id: str, original_animated: bool) -> str:
        if not ENABLE_APP_EMOJI_CACHE:
            return app_emoji_token(original_name, original_emoji_id, original_animated)

        cached = await self.get_cached_app_emoji(original_emoji_id)
        if cached:
            return app_emoji_token(
                cached["app_emoji_name"],
                cached["app_emoji_id"],
                bool(cached["app_emoji_animated"]),
            )

        async with self.app_emoji_lock:
            cached = await self.get_cached_app_emoji(original_emoji_id)
            if cached:
                return app_emoji_token(
                    cached["app_emoji_name"],
                    cached["app_emoji_id"],
                    bool(cached["app_emoji_animated"]),
                )

            app_emoji_name = make_app_emoji_name(original_name, original_emoji_id)

            try:
                existing = await self.find_existing_app_emoji_by_name(app_emoji_name)
                if existing:
                    app_emoji_id = str(existing["id"])
                    app_emoji_animated = bool(existing.get("animated", False))
                    await self.set_cached_app_emoji(
                        original_emoji_id,
                        original_name,
                        original_animated,
                        app_emoji_id,
                        app_emoji_name,
                        app_emoji_animated,
                    )
                    return app_emoji_token(app_emoji_name, app_emoji_id, app_emoji_animated)

                image_bytes, content_type = await self.download_custom_emoji_image(original_emoji_id, original_animated)
                encoded = base64.b64encode(image_bytes).decode("ascii")
                image_data = f"data:{content_type};base64,{encoded}"

                application_id = await self.get_application_id_for_emojis()
                created = await self.discord_api_request(
                    "POST",
                    f"/applications/{application_id}/emojis",
                    json={"name": app_emoji_name, "image": image_data},
                )

                app_emoji_id = str(created["id"])
                app_emoji_animated = bool(created.get("animated", False))

                await self.set_cached_app_emoji(
                    original_emoji_id,
                    original_name,
                    original_animated,
                    app_emoji_id,
                    app_emoji_name,
                    app_emoji_animated,
                )

                log.info("Cached external emoji :%s: (%s) as app emoji :%s: (%s)", original_name, original_emoji_id, app_emoji_name, app_emoji_id)
                return app_emoji_token(app_emoji_name, app_emoji_id, app_emoji_animated)
            except Exception as exc:
                log.warning(
                    "Could not create app emoji cache for :%s: (%s). Keeping original token. Error: %s",
                    original_name,
                    original_emoji_id,
                    exc,
                )
                return app_emoji_token(original_name, original_emoji_id, original_animated)

    async def replace_custom_emojis_with_app_emojis(self, text: str) -> str:
        if not text or not ENABLE_APP_EMOJI_CACHE:
            return text

        matches = list(CUSTOM_EMOJI_RE.finditer(text))
        if not matches:
            return text

        parts: list[str] = []
        last_index = 0

        for match in matches:
            parts.append(text[last_index:match.start()])
            original_name = match.group("name")
            original_emoji_id = match.group("id")
            original_animated = bool(match.group("animated"))

            replacement = await self.create_or_get_app_emoji_token(
                original_name,
                original_emoji_id,
                original_animated,
            )
            parts.append(replacement)
            last_index = match.end()

        parts.append(text[last_index:])
        return "".join(parts)

    async def warmup_arabic_ai_model(self) -> None:
        """Preload the local Ollama model so the first Arabic message is faster."""
        # Wait a little so the bot finishes starting and Ollama has time to be reachable.
        await asyncio.sleep(2)

        if not ENABLE_ARABIC_AI_TRANSLATION:
            return

        assert self.http_session is not None

        payload = {
            "model": ARABIC_AI_MODEL,
            "stream": False,
            "keep_alive": ARABIC_AI_KEEP_ALIVE,
            "prompt": "",
        }

        timeout = aiohttp.ClientTimeout(total=min(max(ARABIC_AI_TIMEOUT, 10.0), 60.0))

        try:
            async with self.http_session.post(ARABIC_AI_URL, json=payload, timeout=timeout) as response:
                body = await response.text()
                if response.status >= 400:
                    raise RuntimeError(f"Ollama warmup error {response.status}: {body}")
            log.info("Arabic AI model warmup requested for %s with keep_alive=%s.", ARABIC_AI_MODEL, ARABIC_AI_KEEP_ALIVE)
        except Exception as exc:
            log.warning("Arabic AI model warmup failed. The bot will still fallback normally. Error: %s", exc)

    async def warmup_korean_ai_model(self) -> None:
        """Preload the local Ollama model so Korean AI translations are faster."""
        await asyncio.sleep(3)

        if not ENABLE_KOREAN_AI_TRANSLATION:
            return

        assert self.http_session is not None

        payload = {
            "model": KOREAN_AI_MODEL,
            "stream": False,
            "keep_alive": KOREAN_AI_KEEP_ALIVE,
            "prompt": "",
        }

        timeout = aiohttp.ClientTimeout(total=min(max(KOREAN_AI_TIMEOUT, 10.0), 60.0))

        try:
            async with self.http_session.post(KOREAN_AI_URL, json=payload, timeout=timeout) as response:
                body = await response.text()
                if response.status >= 400:
                    raise RuntimeError(f"Ollama warmup error {response.status}: {body}")
            log.info("Korean AI model warmup requested for %s with keep_alive=%s.", KOREAN_AI_MODEL, KOREAN_AI_KEEP_ALIVE)
        except Exception as exc:
            log.warning("Korean AI model warmup failed. The bot will still fallback normally. Error: %s", exc)

    async def warmup_full_ai_model(self) -> None:
        """Preload the local Ollama model for full-AI translation test mode."""
        await asyncio.sleep(4)

        if not ENABLE_FULL_AI_TRANSLATION:
            return

        assert self.http_session is not None

        payload = {
            "model": FULL_AI_MODEL,
            "stream": False,
            "keep_alive": FULL_AI_KEEP_ALIVE,
            "prompt": "",
        }

        timeout = aiohttp.ClientTimeout(total=min(max(FULL_AI_TIMEOUT, 10.0), 60.0))

        try:
            async with self.http_session.post(FULL_AI_URL, json=payload, timeout=timeout) as response:
                body = await response.text()
                if response.status >= 400:
                    raise RuntimeError(f"Ollama warmup error {response.status}: {body}")
            log.info(
                "Full AI translation model warmup requested for %s with keep_alive=%s.",
                FULL_AI_MODEL,
                FULL_AI_KEEP_ALIVE,
            )
        except Exception as exc:
            log.warning("Full AI model warmup failed. The bot will still fallback normally. Error: %s", exc)

    async def backup_database_once(self) -> Optional[Path]:
        """Create one SQLite backup on the VPS disk."""
        assert self.db is not None

        backup_dir = Path(DB_BACKUP_DIR)
        backup_dir.mkdir(parents=True, exist_ok=True)

        stamp = time.strftime("%Y-%m-%d-%H%M%S")
        backup_path = backup_dir / f"bot-{stamp}.db"
        tmp_path = backup_path.with_suffix(".db.tmp")

        try:
            if tmp_path.exists():
                tmp_path.unlink()

            async with aiosqlite.connect(str(tmp_path)) as backup_db:
                await self.db.backup(backup_db)

            tmp_path.replace(backup_path)
            await self.cleanup_old_database_backups()
            log.info("Created database backup: %s", backup_path)
            return backup_path
        except Exception as exc:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            log.warning("Database backup failed: %s", exc)
            return None

    async def cleanup_old_database_backups(self) -> None:
        backup_dir = Path(DB_BACKUP_DIR)
        if not backup_dir.exists():
            return

        cutoff = time.time() - (DB_BACKUP_KEEP_DAYS * 86400)
        removed = 0

        for backup_file in backup_dir.glob("bot-*.db"):
            try:
                if backup_file.stat().st_mtime < cutoff:
                    backup_file.unlink()
                    removed += 1
            except Exception as exc:
                log.warning("Could not remove old DB backup %s: %s", backup_file, exc)

        if removed:
            log.info("Removed %s old database backup(s).", removed)

    async def database_backup_loop(self) -> None:
        await asyncio.sleep(30)

        while not self.is_closed():
            await self.backup_database_once()
            await asyncio.sleep(DB_BACKUP_INTERVAL_HOURS * 3600)

    async def cleanup_old_relay_records(self) -> int:
        if not ENABLE_RELAY_RECORD_CLEANUP:
            return 0

        assert self.db is not None
        cutoff_modifier = f"-{RELAY_RECORD_RETENTION_DAYS} days"
        cursor = await self.db.execute(
            """
            DELETE FROM relayed_messages
            WHERE created_at IS NOT NULL
              AND created_at < datetime('now', ?)
            """,
            (cutoff_modifier,),
        )
        removed = cursor.rowcount if cursor.rowcount is not None else 0
        await cursor.close()
        await self.db.commit()

        if removed:
            log.info("Cleaned up %s old relay record(s).", removed)

        return int(removed or 0)

    async def cleanup_orphaned_relay_messages(self) -> int:
        """Delete mirrors whose original source message was deleted while the bot missed the event.

        Normal delete-sync is instant through Discord delete events. This is a
        safety net for cases where the bot was restarting/offline, or where a
        delete event arrived before relay records were saved.
        """
        if not ENABLE_ORPHAN_RELAY_CLEANUP:
            return 0

        assert self.db is not None

        grace_modifier = f"-{ORPHAN_RELAY_CLEANUP_GRACE_MINUTES} minutes"
        cursor = await self.db.execute(
            """
            SELECT
                guild_id,
                source_channel_id,
                source_message_id,
                MIN(created_at) AS first_created_at
            FROM relayed_messages
            WHERE created_at IS NOT NULL
              AND created_at < datetime('now', ?)
            GROUP BY guild_id, source_channel_id, source_message_id
            ORDER BY first_created_at DESC
            LIMIT ?
            """,
            (grace_modifier, ORPHAN_RELAY_CLEANUP_BATCH_SIZE),
        )
        rows = await cursor.fetchall()
        await cursor.close()

        cleaned = 0

        for row in rows:
            guild_id = int(row["guild_id"])
            source_channel_id = int(row["source_channel_id"])
            source_message_id = int(row["source_message_id"])

            channel = await resolve_text_channel(source_channel_id)
            if channel is None:
                continue

            try:
                await channel.fetch_message(source_message_id)
                continue
            except discord.NotFound:
                pass
            except discord.Forbidden:
                log.warning(
                    "Missing permission to check source message %s in channel %s for orphan cleanup.",
                    source_message_id,
                    source_channel_id,
                )
                continue
            except discord.HTTPException as exc:
                log.warning(
                    "Could not check source message %s in channel %s for orphan cleanup: %s",
                    source_message_id,
                    source_channel_id,
                    exc,
                )
                continue
            except Exception:
                log.exception(
                    "Unexpected orphan cleanup check error for source message %s in channel %s.",
                    source_message_id,
                    source_channel_id,
                )
                continue

            lock = await self.get_source_message_lock(guild_id, source_channel_id, source_message_id)
            async with lock:
                await delete_mirrored_messages(guild_id, source_channel_id, source_message_id)
                cleaned += 1

        if cleaned:
            log.info("Cleaned up mirrored messages for %s deleted source message(s).", cleaned)

        return cleaned

    async def prune_translation_cache(self) -> None:
        if not ENABLE_TRANSLATION_CACHE:
            return

        assert self.db is not None

        ttl_modifier = f"-{TRANSLATION_CACHE_TTL_DAYS} days"
        await self.db.execute(
            """
            DELETE FROM translation_cache
            WHERE last_used_at < datetime('now', ?)
            """,
            (ttl_modifier,),
        )

        await self.db.execute(
            """
            DELETE FROM translation_cache
            WHERE rowid IN (
                SELECT rowid
                FROM translation_cache
                ORDER BY last_used_at DESC, uses DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (TRANSLATION_CACHE_MAX_ROWS,),
        )
        await self.db.commit()

    async def database_maintenance_loop(self) -> None:
        await asyncio.sleep(45)

        intervals = []
        if ENABLE_RELAY_RECORD_CLEANUP:
            intervals.append(RELAY_RECORD_CLEANUP_INTERVAL_HOURS)
        if ENABLE_ORPHAN_RELAY_CLEANUP:
            intervals.append(ORPHAN_RELAY_CLEANUP_INTERVAL_HOURS)
        if ENABLE_TRANSLATION_CACHE:
            intervals.append(24.0)

        sleep_hours = min(intervals) if intervals else 24.0

        while not self.is_closed():
            try:
                await self.cleanup_old_relay_records()
                await self.cleanup_orphaned_relay_messages()
                await self.prune_translation_cache()
            except Exception as exc:
                log.warning("Database maintenance failed: %s", exc)

            await asyncio.sleep(sleep_hours * 3600)

    async def get_cached_translation(self, text: str, source_lang: str, target_lang: str) -> Optional[str]:
        if not should_use_persistent_translation_cache(text):
            return None

        assert self.db is not None

        source_norm = canonical_language_code(source_lang)
        target_norm = canonical_language_code(target_lang)
        cache_text = normalize_ai_cache_text(text)

        cursor = await self.db.execute(
            """
            SELECT translated_text
            FROM translation_cache
            WHERE source_lang = ? AND target_lang = ? AND cache_text = ?
            """,
            (source_norm, target_norm, cache_text),
        )
        row = await cursor.fetchone()
        await cursor.close()

        if row is None:
            return None

        await self.db.execute(
            """
            UPDATE translation_cache
            SET last_used_at = CURRENT_TIMESTAMP,
                uses = uses + 1
            WHERE source_lang = ? AND target_lang = ? AND cache_text = ?
            """,
            (source_norm, target_norm, cache_text),
        )
        await self.db.commit()

        log.debug("Persistent translation cache hit for %s -> %s.", source_norm, target_norm)
        return str(row["translated_text"])

    async def set_cached_translation(self, text: str, source_lang: str, target_lang: str, translated_text: str) -> None:
        if not should_use_persistent_translation_cache(text):
            return

        cleaned_translation = clean_translation_engine_artifacts(translated_text).strip()
        if not cleaned_translation:
            return

        if looks_like_translation_artifact_garbage(cleaned_translation):
            log.warning("Skipped caching translation artifact output for %s -> %s.", source_lang, target_lang)
            return

        if looks_like_ai_explanation_output(cleaned_translation):
            log.warning("Skipped caching explanation-like translation output for %s -> %s.", source_lang, target_lang)
            return

        assert self.db is not None

        source_norm = canonical_language_code(source_lang)
        target_norm = canonical_language_code(target_lang)

        if source_norm == target_norm:
            return

        cache_text = normalize_ai_cache_text(text)

        await self.db.execute(
            """
            INSERT INTO translation_cache (
                source_lang,
                target_lang,
                cache_text,
                translated_text,
                created_at,
                last_used_at,
                uses
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            ON CONFLICT(source_lang, target_lang, cache_text) DO UPDATE SET
                translated_text = excluded.translated_text,
                last_used_at = CURRENT_TIMESTAMP,
                uses = translation_cache.uses + 1
            """,
            (source_norm, target_norm, cache_text, cleaned_translation),
        )

        await self.db.execute(
            """
            DELETE FROM translation_cache
            WHERE rowid IN (
                SELECT rowid
                FROM translation_cache
                ORDER BY last_used_at DESC, uses DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (TRANSLATION_CACHE_MAX_ROWS,),
        )
        await self.db.commit()

    async def remember_translation_result(
        self,
        original_text: str,
        source_lang: str,
        target_lang: str,
        translated_text: str,
    ) -> str:
        cleaned = clean_translation_engine_artifacts(translated_text)
        normalized = normalize_translated_output_for_target(cleaned, target_lang)

        missing_hashes = missing_hashtag_tokens(original_text, normalized)
        if missing_hashes:
            # This should be rare now because hashtag tokens are protected before
            # translation. If an engine still drops them, append them rather than
            # losing important game/state numbers like #295 or #280.
            normalized = f"{normalized} {' '.join(missing_hashes)}".strip()
            log.warning(
                "Translation output was missing protected hashtag token(s) for %s -> %s; restored: %s",
                canonical_language_code(source_lang),
                canonical_language_code(target_lang),
                ", ".join(missing_hashes),
            )

        await self.set_cached_translation(original_text, source_lang, target_lang, normalized)
        return normalized



    def clean_arabic_ai_response(self, response_text: str) -> str:
        cleaned = response_text.strip()

        # Some local models occasionally add labels or code fences even when told
        # not to. Strip common wrappers so the bot forwards only the translation.
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[A-Za-z0-9_-]*\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)

        label_patterns = (
            r"^translation\s*:\s*",
            r"^english\s*translation\s*:\s*",
            r"^translated\s*text\s*:\s*",
            r"^korean\s*translation\s*:\s*",
            r"^번역\s*[:：]\s*",
        )
        for pattern in label_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()

        return cleaned.strip()

    async def get_arabic_ai_english_pivot(self, text: str) -> Optional[str]:
        """Use local Ollama/Aya to translate Arabic dialect text to English.

        The English pivot is cached per exact message text so one Arabic message
        relayed to many target channels calls Ollama only once.
        """
        cleaned_text = text.strip()
        if not ENABLE_ARABIC_AI_TRANSLATION:
            return None

        if not cleaned_text or not ARABIC_LETTER_RE.search(cleaned_text):
            return None

        if len(cleaned_text) > ARABIC_AI_MAX_CHARS:
            log.info(
                "Arabic AI helper skipped message because it is too long (%s > %s chars).",
                len(cleaned_text),
                ARABIC_AI_MAX_CHARS,
            )
            return None

        cached = self.arabic_ai_pivot_cache.get(cleaned_text)
        if cached:
            return cached

        async with self.arabic_ai_lock:
            cached = self.arabic_ai_pivot_cache.get(cleaned_text)
            if cached:
                return cached

            assert self.http_session is not None

            prompt = (
                "You are a professional Arabic dialect translator for Discord chat. "
                "Translate the Arabic message into natural English. Understand Egyptian Arabic, "
                "Gulf Arabic, UAE Arabic, Levantine Arabic, and casual slang. Preserve line breaks. "
                "Preserve names, mentions, links, numbers, and emoji text. Return only the English "
                "translation with no explanation.\n\n"
                f"{cleaned_text}"
            )

            payload = {
                "model": ARABIC_AI_MODEL,
                "stream": False,
                "keep_alive": ARABIC_AI_KEEP_ALIVE,
                "options": {
                    "temperature": 0,
                    "num_predict": ARABIC_AI_NUM_PREDICT,
                },
                "prompt": prompt,
            }

            timeout = aiohttp.ClientTimeout(total=ARABIC_AI_TIMEOUT)

            try:
                async with self.http_session.post(ARABIC_AI_URL, json=payload, timeout=timeout) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise RuntimeError(f"Ollama Arabic AI error {response.status}: {body}")

                    data = await response.json()
                    response_text = data.get("response")
                    if not response_text:
                        raise RuntimeError(f"Unexpected Ollama Arabic AI response: {data}")

                    english_pivot = self.clean_arabic_ai_response(str(response_text))
                    if not english_pivot:
                        raise RuntimeError("Ollama Arabic AI returned an empty translation.")

                    if looks_like_ai_explanation_output(english_pivot):
                        log.warning("Arabic AI helper returned explanation-like output; ignoring it.")
                        return None

                    # Very small cache to avoid repeated Ollama calls when one
                    # source message is being relayed to many channels.
                    if len(self.arabic_ai_pivot_cache) > 256:
                        self.arabic_ai_pivot_cache.clear()
                    self.arabic_ai_pivot_cache[cleaned_text] = english_pivot

                    log.info("Arabic AI helper translated Arabic message to English pivot.")
                    return english_pivot
            except Exception as exc:
                log.warning("Arabic AI helper failed; falling back to LibreTranslate/rules. Error: %s", exc)
                return None

    async def translate_arabic_with_ai_if_needed(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        if canonical_language_code(source_norm).split("-", 1)[0] != "ar":
            return None

        if not ARABIC_LETTER_RE.search(text):
            return None

        # Arabic -> Arabic does not need AI translation.
        if canonical_language_code(target_norm).split("-", 1)[0] == "ar":
            return None

        english_pivot = await self.get_arabic_ai_english_pivot(text)
        if not english_pivot:
            return None

        if canonical_language_code(target_norm).split("-", 1)[0] == "en":
            return english_pivot

        return await self.translate_text(english_pivot, "en", target_norm)

    async def get_korean_ai_translation(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        """Use local Ollama/Aya for Korean-related translation.

        This is used for:
        - Korean source messages -> English pivot
        - Any source language -> Korean target
        """
        cleaned_text = text.strip()
        if not ENABLE_KOREAN_AI_TRANSLATION:
            return None

        if not cleaned_text:
            return None

        source_base = canonical_language_code(source_norm).split("-", 1)[0]
        target_base = canonical_language_code(target_norm).split("-", 1)[0]

        if source_base != "ko" and target_base != "ko":
            return None

        if source_base == target_base:
            return None

        if len(cleaned_text) > KOREAN_AI_MAX_CHARS:
            log.info(
                "Korean AI helper skipped message because it is too long (%s > %s chars).",
                len(cleaned_text),
                KOREAN_AI_MAX_CHARS,
            )
            return None

        # Do not run Korean-source mode unless the text actually contains Hangul.
        # English -> Korean and other languages -> Korean do not need Hangul in source.
        if source_base == "ko" and not KOREAN_LETTER_RE.search(cleaned_text):
            return None

        if source_base == "ko":
            target_language_name = "natural English"
            prompt_task = (
                "Translate the Korean Discord chat message into natural English. "
                "Preserve line breaks, names, mentions, links, numbers, and emoji text. "
                "Return only the English translation with no explanation."
            )
            cache_target = "en"
        else:
            source_language_name = language_display_name(source_norm)
            target_language_name = "natural Korean"
            prompt_task = (
                f"Translate the {source_language_name} Discord chat message into natural Korean. "
                "Preserve line breaks, names, mentions, links, hashtags, numbers, and emoji text. "
                "Use casual but clear Korean suitable for Discord chat. "
                "Do not leave source-language words untranslated unless they are names, links, mentions, or protected tokens. "
                "Return only the Korean translation with no explanation."
            )
            cache_target = "ko"

        cache_key = (source_base, cache_target, cleaned_text)
        cached = self.korean_ai_cache.get(cache_key)
        if cached:
            return cached

        async with self.korean_ai_lock:
            cached = self.korean_ai_cache.get(cache_key)
            if cached:
                return cached

            assert self.http_session is not None

            prompt = (
                "You are a strict translation engine for Discord chat. "
                f"{prompt_task}\n"
                "Rules: Output only the translated message. "
                "Do not add greetings, labels, examples, notes, explanations, or sections. "
                "Do not write 'Original message', 'Translated message', '원본 메시지', or '번역된 메시지'. "
                "Do not answer the message; only translate it. "
                "Translate only the content between <message> and </message>.\n\n"
                "<message>\n"
                f"{cleaned_text}\n"
                "</message>"
            )

            payload = {
                "model": KOREAN_AI_MODEL,
                "stream": False,
                "keep_alive": KOREAN_AI_KEEP_ALIVE,
                "options": {
                    "temperature": 0,
                    "num_predict": KOREAN_AI_NUM_PREDICT,
                },
                "prompt": prompt,
            }

            timeout = aiohttp.ClientTimeout(total=KOREAN_AI_TIMEOUT)

            try:
                async with self.http_session.post(KOREAN_AI_URL, json=payload, timeout=timeout) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise RuntimeError(f"Ollama Korean AI error {response.status}: {body}")

                    data = await response.json()
                    response_text = data.get("response")
                    if not response_text:
                        raise RuntimeError(f"Unexpected Ollama Korean AI response: {data}")

                    translated = self.clean_arabic_ai_response(str(response_text))
                    if not translated:
                        raise RuntimeError("Ollama Korean AI returned an empty translation.")

                    if looks_like_ai_explanation_output(translated):
                        log.warning(
                            "Korean AI helper returned explanation-like output for %s -> %s; ignoring it.",
                            source_base,
                            cache_target,
                        )
                        return None

                    if len(self.korean_ai_cache) > 256:
                        self.korean_ai_cache.clear()
                    self.korean_ai_cache[cache_key] = translated

                    log.info("Korean AI helper translated %s -> %s.", source_base, cache_target)
                    return translated
            except Exception as exc:
                log.warning("Korean AI helper failed; falling back to LibreTranslate. Error: %s", exc)
                return None

    async def translate_korean_with_ai_if_needed(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        source_base = canonical_language_code(source_norm).split("-", 1)[0]
        target_base = canonical_language_code(target_norm).split("-", 1)[0]

        if source_base != "ko" and target_base != "ko":
            return None

        if source_base == target_base:
            return None

        korean_ai_translation = await self.get_korean_ai_translation(text, source_norm, target_norm)
        if not korean_ai_translation:
            return None

        # Korean source goes to English first. If the final target is not English,
        # continue from that English pivot through the normal translator path.
        if source_base == "ko":
            if target_base == "en":
                return korean_ai_translation
            return await self.translate_text(korean_ai_translation, "en", target_norm)

        # Any non-Korean source directly to Korean.
        return korean_ai_translation

    async def get_full_ai_translation(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        """Use local Ollama/Aya for full translation-engine=ai test mode.

        LibreTranslate remains available as fallback. This method only returns a
        value when TRANSLATION_ENGINE=ai and the local AI request succeeds.
        """
        cleaned_text = text.strip()
        if not ENABLE_FULL_AI_TRANSLATION:
            return None

        if not cleaned_text:
            return None

        source_norm = canonical_language_code(source_norm)
        target_norm = canonical_language_code(target_norm)
        if source_norm == target_norm:
            return text

        common_translation = get_common_phrase_translation(cleaned_text, source_norm, target_norm)
        if common_translation is not None:
            return common_translation

        if len(cleaned_text) > FULL_AI_MAX_CHARS:
            log.info(
                "Full AI translator skipped message because it is too long (%s > %s chars).",
                len(cleaned_text),
                FULL_AI_MAX_CHARS,
            )
            return None

        cache_text = normalize_ai_cache_text(cleaned_text)
        if (
            not FULL_AI_PER_TARGET_AFTER_BATCH_FAILURE
            and (source_norm, cache_text) in self.full_ai_batch_failed_messages
        ):
            return None

        cache_key = (source_norm, target_norm, cache_text)
        cached = self.full_ai_cache.get(cache_key)
        if cached:
            return cached

        async with self.full_ai_lock:
            cached = self.full_ai_cache.get(cache_key)
            if cached:
                return cached

            assert self.http_session is not None

            source_name = language_display_name(source_norm)
            target_name = language_display_name(target_norm)

            prompt = (
                "You are a professional Discord chat translator. "
                f"Translate the message from {source_name} to {target_name}. "
                "Use natural language suitable for Discord chat while preserving the exact meaning, tone, and slang. "
                "Preserve line breaks, names, mentions, role/channel mentions, links, numbers, emoji text, and custom emoji tokens. "
                "Do not add explanations, quotes, labels, markdown code fences, or extra commentary. "
                "Return only the translated message.\n\n"
                f"{cleaned_text}"
            )

            payload = {
                "model": FULL_AI_MODEL,
                "stream": False,
                "keep_alive": FULL_AI_KEEP_ALIVE,
                "options": {
                    "temperature": 0,
                    "num_predict": FULL_AI_NUM_PREDICT,
                },
                "prompt": prompt,
            }

            timeout = aiohttp.ClientTimeout(total=FULL_AI_TIMEOUT)

            try:
                async with self.http_session.post(FULL_AI_URL, json=payload, timeout=timeout) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise RuntimeError(f"Ollama full AI error {response.status}: {body}")

                    data = await response.json()
                    response_text = data.get("response")
                    if not response_text:
                        raise RuntimeError(f"Unexpected Ollama full AI response: {data}")

                    translated = self.clean_arabic_ai_response(str(response_text))
                    if not translated:
                        raise RuntimeError("Ollama full AI returned an empty translation.")

                    if len(self.full_ai_cache) > 512:
                        self.full_ai_cache.clear()
                    self.full_ai_cache[cache_key] = translated

                    log.info("Full AI translator handled %s -> %s.", source_norm, target_norm)
                    return translated
            except Exception as exc:
                log.warning("Full AI translator failed; falling back to hybrid/LibreTranslate. Error: %s", exc)
                return None

    def parse_full_ai_batch_response(self, response_text: str, requested_targets: list[str]) -> dict[str, str]:
        """Parse a JSON-first batch translation response from the local AI model."""
        cleaned = self.clean_arabic_ai_response(response_text)
        requested = {canonical_language_code(code) for code in requested_targets}
        parsed: dict[str, str] = {}

        # Preferred format: {"ar": "...", "es": "..."}
        try:
            json_text = cleaned
            if "{" in cleaned and "}" in cleaned:
                json_text = cleaned[cleaned.find("{"): cleaned.rfind("}") + 1]
            data = json.loads(json_text)
            if isinstance(data, dict):
                for key, value in data.items():
                    norm_key = canonical_language_code(str(key))
                    if norm_key in requested and isinstance(value, str) and value.strip():
                        parsed[norm_key] = value.strip()
        except Exception:
            parsed = {}

        if parsed:
            return parsed

        # Fallback format: ar: translation
        for raw_line in cleaned.splitlines():
            line = raw_line.strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            norm_key = canonical_language_code(key.strip().strip('"').strip("'"))
            if norm_key in requested and value.strip():
                parsed[norm_key] = value.strip().strip('"').strip("'")

        return parsed

    async def get_full_ai_batch_translations(
        self,
        text: str,
        source_norm: str,
        target_norms: list[str],
    ) -> dict[str, str]:
        """Translate one source message into many target languages with one AI call.

        This is the main speed improvement for TRANSLATION_ENGINE=ai. Instead of
        making one Ollama request per linked channel, the bot asks Aya for all
        target languages at once and caches the individual results.
        """
        cleaned_text = text.strip()
        if not ENABLE_FULL_AI_TRANSLATION or not FULL_AI_BATCH_TRANSLATION:
            return {}

        if not cleaned_text:
            return {}

        if DISCORD_PROTECTED_TOKEN_RE.search(cleaned_text):
            # Let translate_text handle protected-token splitting safely.
            return {}

        if len(cleaned_text) > FULL_AI_MAX_CHARS:
            return {}

        source_norm = canonical_language_code(source_norm)
        unique_targets: list[str] = []
        seen_targets: set[str] = set()

        for target in target_norms:
            target_norm = canonical_language_code(target)
            if target_norm == source_norm:
                continue
            if target_norm in seen_targets:
                continue
            seen_targets.add(target_norm)
            unique_targets.append(target_norm)

        if not unique_targets or len(unique_targets) > FULL_AI_BATCH_MAX_TARGETS:
            return {}

        cache_text = normalize_ai_cache_text(cleaned_text)
        results: dict[str, str] = {}

        for target_norm in unique_targets:
            common_translation = get_common_phrase_translation(cleaned_text, source_norm, target_norm)
            if common_translation is not None:
                results[target_norm] = common_translation
                self.full_ai_cache[(source_norm, target_norm, cache_text)] = common_translation
                log_translation_route("full-ai-batch-common-phrase", source_norm, target_norm, cleaned_text)
                continue

            cached = self.full_ai_cache.get((source_norm, target_norm, cache_text))
            if cached:
                results[target_norm] = cached
                log_translation_route("full-ai-batch-memory-cache", source_norm, target_norm, cleaned_text)

        missing_targets = [target for target in unique_targets if target not in results]
        if not missing_targets:
            return results

        batch_key = (source_norm, tuple(missing_targets), cache_text)
        cached_batch = self.full_ai_batch_cache.get(batch_key)
        if cached_batch:
            results.update(cached_batch)
            for target_norm in cached_batch:
                log_translation_route("full-ai-batch-cache", source_norm, target_norm, cleaned_text)
            return results

        async with self.full_ai_lock:
            cached_batch = self.full_ai_batch_cache.get(batch_key)
            if cached_batch:
                results.update(cached_batch)
                return results

            # Another coroutine may have filled individual cache while waiting.
            still_missing: list[str] = []
            for target_norm in missing_targets:
                cached = self.full_ai_cache.get((source_norm, target_norm, cache_text))
                if cached:
                    results[target_norm] = cached
                else:
                    still_missing.append(target_norm)

            if not still_missing:
                return results

            assert self.http_session is not None

            source_name = language_display_name(source_norm)
            target_descriptions = {
                target: language_display_name(target)
                for target in still_missing
            }
            target_json = json.dumps(target_descriptions, ensure_ascii=False)

            prompt = (
                "You are a professional Discord chat translator. "
                f"Translate the message from {source_name} into every target language listed in this JSON object: {target_json}. "
                "Return ONLY a compact JSON object. The JSON keys must be exactly the same target language codes. "
                "The JSON values must be only the translated messages. "
                "Use natural language suitable for Discord chat while preserving exact meaning, tone, and slang. "
                "Preserve line breaks, names, mentions, role/channel mentions, links, numbers, emoji text, and custom emoji tokens. "
                "Do not add explanations, markdown, quotes outside JSON, or extra commentary.\n\n"
                f"Message:\n{cleaned_text}"
            )

            payload = {
                "model": FULL_AI_MODEL,
                "stream": False,
                "keep_alive": FULL_AI_KEEP_ALIVE,
                "options": {
                    "temperature": 0,
                    "num_predict": FULL_AI_BATCH_NUM_PREDICT,
                },
                "prompt": prompt,
            }

            timeout = aiohttp.ClientTimeout(total=FULL_AI_BATCH_TIMEOUT)

            try:
                async with self.http_session.post(FULL_AI_URL, json=payload, timeout=timeout) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise RuntimeError(f"Ollama full AI batch error {response.status}: {body}")

                    data = await response.json()
                    response_text = data.get("response")
                    if not response_text:
                        raise RuntimeError(f"Unexpected Ollama full AI batch response: {data}")

                    parsed = self.parse_full_ai_batch_response(str(response_text), still_missing)
                    if not parsed:
                        raise RuntimeError(f"Could not parse full AI batch response: {response_text}")

                    batch_results: dict[str, str] = {}
                    for target_norm, translated in parsed.items():
                        translated = normalize_translated_output_for_target(translated, target_norm)
                        batch_results[target_norm] = translated
                        self.full_ai_cache[(source_norm, target_norm, cache_text)] = translated
                        log_translation_route("full-ai-batch", source_norm, target_norm, cleaned_text)

                    if len(self.full_ai_cache) > 1024:
                        self.full_ai_cache.clear()
                    if len(self.full_ai_batch_cache) > 128:
                        self.full_ai_batch_cache.clear()

                    self.full_ai_batch_cache[batch_key] = batch_results
                    results.update(batch_results)

                    log.info(
                        "Full AI batch translator handled %s -> %s target language(s).",
                        source_norm,
                        len(batch_results),
                    )
                    return results
            except Exception as exc:
                # If a batch request times out or fails, do not then run slow
                # per-target AI translations for the same source message unless
                # explicitly enabled. Fall back to the normal hybrid/LibreTranslate
                # path instead, so one failed batch does not cause a long message
                # delay across every linked channel.
                if not FULL_AI_PER_TARGET_AFTER_BATCH_FAILURE:
                    self.full_ai_batch_failed_messages.add((source_norm, cache_text))
                    if len(self.full_ai_batch_failed_messages) > 512:
                        self.full_ai_batch_failed_messages.clear()
                log.warning("Full AI batch translator failed; falling back to hybrid/LibreTranslate path. Error: %r", exc)
                return results

    async def translate_full_ai_if_needed(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        if not ENABLE_FULL_AI_TRANSLATION:
            return None

        translation = await self.get_full_ai_translation(text, source_norm, target_norm)
        if translation is None:
            return None

        return normalize_translated_output_for_target(translation, target_norm)

    async def translate_arabic_dialect_text_if_needed(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        """Translate known Arabic/Egyptian dialect phrases through an English pivot.

        This avoids bad literal translations for common dialect lines like
        "ازيك يا شباب" or "عاملين ايه يا جماعة" while keeping normal Arabic
        text on the regular LibreTranslate path.
        """
        if canonical_language_code(source_norm).split("-", 1)[0] != "ar":
            return None

        if not ARABIC_LETTER_RE.search(text):
            return None

        parts = text.splitlines(keepends=True)
        if not parts:
            return None

        # Only take the dialect override path when every non-empty line matches
        # a known dialect rule. If any line is normal Arabic, return None and let
        # the regular LibreTranslate Arabic path handle the whole text. This also
        # avoids recursive calls back into translate_text for unmatched lines.
        pivot_parts: list[tuple[str, str]] = []
        blank_parts: dict[int, str] = {}

        for index, part in enumerate(parts):
            line = part.rstrip("\r\n")
            newline = part[len(line):]

            if not line.strip():
                blank_parts[index] = part
                continue

            pivot = get_arabic_dialect_pivot_for_line(line)
            if pivot is None:
                return None

            pivot_parts.append((pivot, newline))

        if not pivot_parts:
            return None

        translated_parts: list[str] = []
        pivot_index = 0

        for index, part in enumerate(parts):
            if index in blank_parts:
                translated_parts.append(blank_parts[index])
                continue

            pivot, newline = pivot_parts[pivot_index]
            pivot_index += 1

            if canonical_language_code(target_norm).split("-", 1)[0] == "en":
                translated_parts.append(pivot)
            else:
                translated_parts.append(await self.translate_text(pivot, "en", target_norm))
            translated_parts.append(newline)

        return "".join(translated_parts)

    async def delete_relay_records_for_source(self, guild_id: int, source_channel_id: int, source_message_id: int) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            DELETE FROM relayed_messages
            WHERE guild_id = ? AND source_channel_id = ? AND source_message_id = ?
            """,
            (guild_id, source_channel_id, source_message_id),
        )
        await self.db.commit()

    async def delete_relay_record_by_target_message(self, target_message_id: int) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            DELETE FROM relayed_messages
            WHERE target_message_id = ?
            """,
            (target_message_id,),
        )
        await self.db.commit()

    async def detect_language_cached(self, text: str) -> Optional[str]:
        """Detect language with a small in-memory cache to avoid repeated calls."""
        cleaned = " ".join((text or "").strip().split())
        if len(cleaned) < DETECT_MIN_TEXT_LENGTH:
            return None

        cache_key = cleaned[:500].lower()
        cached = self.language_detect_cache.get(cache_key)
        if cached:
            return cached

        detected = await self.detect_language(cleaned)
        if detected:
            self.language_detect_cache[cache_key] = detected

            if len(self.language_detect_cache) > 2000:
                for old_key in list(self.language_detect_cache.keys())[:500]:
                    self.language_detect_cache.pop(old_key, None)

        return detected

    async def infer_effective_source_language(self, text: str, source_norm: str) -> str:
        """Detect wrong-language Latin lines posted in a linked channel.

        Example: a Spanish line in the English channel should be translated as
        Spanish, not forced through English.
        """
        source_norm = canonical_language_code(source_norm)
        cleaned = (text or "").strip()

        if not cleaned or is_non_language_passthrough_text(cleaned):
            return source_norm

        # Script-based overrides for single lines.
        if ARABIC_LETTER_RE.search(cleaned) and source_norm.split("-", 1)[0] != "ar":
            return "ar"

        if KOREAN_LETTER_RE.search(cleaned) and source_norm.split("-", 1)[0] != "ko":
            return "ko"

        # Fast explicit Spanish guard for common pasted Spanish lines. This fixes
        # cases where a user writes Spanish in the English channel.
        if looks_like_spanish_latin_text(cleaned) and source_norm.split("-", 1)[0] != "es":
            return "es"

        if not should_try_mixed_language_detection(cleaned, source_norm):
            return source_norm

        try:
            detected = await self.detect_language_cached(cleaned)
        except Exception as exc:
            log.debug("Mixed-language line detection failed. Keeping source %s. Error: %s", source_norm, exc)
            return source_norm

        if not detected:
            return source_norm

        detected_norm = canonical_language_code(detected)
        if language_matches(source_norm, detected_norm):
            return source_norm

        # Only auto-switch between Latin languages here. Different-script cases
        # are handled above.
        source_script = script_bucket_for_language(source_norm)
        detected_script = script_bucket_for_language(detected_norm)
        if source_script == detected_script == "latin":
            return detected_norm

        return source_norm

    async def translate_mixed_language_lines_if_needed(self, text: str, source_norm: str, target_norm: str) -> Optional[str]:
        """Translate multi-line messages line-by-line when needed.

        This keeps separator/sign lines unchanged and fixes mixed-language
        messages where one line is Spanish inside an English channel, etc.
        """
        if not should_translate_line_by_line(text):
            return None

        output: list[str] = []
        changed_route = False

        for line, newline in split_preserving_line_endings(text):
            if not line.strip():
                output.append(line + newline)
                continue

            if is_non_language_passthrough_text(line):
                output.append(line + newline)
                changed_route = True
                continue

            effective_source = await self.infer_effective_source_language(line, source_norm)
            if effective_source != source_norm:
                changed_route = True

            translated_line = await self.translate_text(line, effective_source, target_norm)
            output.append(translated_line + newline)

        if not changed_route:
            return None

        translated = "".join(output)
        log_translation_route("mixed-line", source_norm, target_norm, text)
        return normalize_translated_output_for_target(clean_translation_engine_artifacts(translated), target_norm)

    async def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        if not text.strip():
            return text

        source_norm = canonical_language_code(source_lang)
        target_norm = canonical_language_code(target_lang)

        if is_non_language_passthrough_text(text):
            log_translation_route("non-language-passthrough", source_norm, target_norm, text)
            return text

        mixed_line_translation = await self.translate_mixed_language_lines_if_needed(text, source_norm, target_norm)
        if mixed_line_translation is not None:
            return await self.remember_translation_result(text, source_norm, target_norm, mixed_line_translation)

        inferred_source = await self.infer_effective_source_language(text, source_norm)
        if inferred_source != source_norm:
            log_translation_route("mixed-source-detected", source_norm, target_norm, text, detail=f"detected_source={inferred_source}")
            source_norm = inferred_source
            source_lang = inferred_source

        # If someone writes Arabic text in the "wrong" linked channel, do not
        # force LibreTranslate to treat that Arabic as the channel language
        # (for example, Arabic text in an English channel). That can produce
        # random/garbled output. Treat Arabic-script text as Arabic instead.
        if ARABIC_LETTER_RE.search(text) and source_norm.split("-", 1)[0] != "ar":
            source_norm = "ar"
            source_lang = "ar"

        # Same protection for Korean text posted in the wrong linked channel.
        if KOREAN_LETTER_RE.search(text) and source_norm.split("-", 1)[0] != "ko":
            source_norm = "ko"
            source_lang = "ko"

        if source_norm == target_norm:
            log_translation_route("same-language", source_norm, target_norm, text)
            return text

        arabic_short_chat_pivot = get_arabic_short_chat_pivot(text) if source_norm.split("-", 1)[0] == "ar" else None
        if arabic_short_chat_pivot is not None:
            log_translation_route("arabic-short-chat", source_norm, target_norm, text)
            if target_norm.split("-", 1)[0] == "en":
                return await self.remember_translation_result(text, source_norm, target_norm, arabic_short_chat_pivot)
            translated_from_pivot = await self.translate_text(arabic_short_chat_pivot, "en", target_norm)
            return await self.remember_translation_result(text, source_norm, target_norm, translated_from_pivot)

        if is_arabic_ambiguous_fragment_passthrough(text, source_norm, target_norm):
            log_translation_route("arabic-ambiguous-passthrough", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, text)

        spanish_chat_pivot = get_spanish_chat_phrase_pivot(text) if source_norm.split("-", 1)[0] == "es" else None
        if spanish_chat_pivot is not None:
            log_translation_route("spanish-chat-phrase", source_norm, target_norm, text)
            if target_norm.split("-", 1)[0] == "en":
                return await self.remember_translation_result(text, source_norm, target_norm, spanish_chat_pivot)
            translated_from_pivot = await self.translate_text(spanish_chat_pivot, "en", target_norm)
            return await self.remember_translation_result(text, source_norm, target_norm, translated_from_pivot)

        korean_emoticon_override = get_korean_emoticon_override(text, source_norm, target_norm)
        if korean_emoticon_override is not None:
            log_translation_route("korean-emoticon", source_norm, target_norm, text)
            return await self.remember_translation_result(
                text,
                source_norm,
                target_norm,
                korean_emoticon_override,
            )

        cached_translation = await self.get_cached_translation(text, source_norm, target_norm)
        if cached_translation is not None:
            missing_hashes = missing_hashtag_tokens(text, cached_translation)
            if looks_like_ai_explanation_output(cached_translation):
                log.warning(
                    "Ignoring cached AI explanation output for %s -> %s.",
                    source_norm,
                    target_norm,
                )
            elif looks_like_translation_artifact_garbage(cached_translation):
                log.warning(
                    "Ignoring cached translation artifact output for %s -> %s.",
                    source_norm,
                    target_norm,
                )
            elif missing_hashes:
                log.warning(
                    "Ignoring cached translation that dropped protected hashtag token(s) for %s -> %s: %s",
                    source_norm,
                    target_norm,
                    ", ".join(missing_hashes),
                )
            else:
                log_translation_route("persistent-cache", source_norm, target_norm, text)
                return normalize_translated_output_for_target(
                    clean_translation_engine_artifacts(cached_translation),
                    target_norm,
                )

        common_translation = get_common_phrase_translation(text, source_norm, target_norm)
        if common_translation is not None:
            log_translation_route("common-phrase", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, common_translation)

        short_override = get_short_phrase_override(text, target_norm)
        if short_override is not None:
            log_translation_route("short-phrase", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, short_override)

        # Preserve Discord custom emojis/mentions/timestamps by translating only
        # the normal text around them, then putting the original token back.
        # Without this, translation can corrupt <:emoji_name:emoji_id> syntax.
        protected_matches = list(DISCORD_PROTECTED_TOKEN_RE.finditer(text))
        if protected_matches:
            translated_parts: list[str] = []
            last_index = 0

            for match in protected_matches:
                before = text[last_index:match.start()]
                if before:
                    if before.strip():
                        translated_parts.append(await self.translate_text(before, source_lang, target_lang))
                    else:
                        translated_parts.append(before)

                token = match.group(0)

                # Keep emoji-only lines clean so Discord can still render them
                # as large/jumbo emojis in RTL target channels. Directional
                # isolates are only needed when Unicode emojis are mixed inline
                # with Arabic/RTL text; adding hidden marks to standalone emoji
                # lines can make Discord render them as small inline emoji.
                line_start = text.rfind("\n", 0, match.start()) + 1
                line_end = text.find("\n", match.end())
                if line_end == -1:
                    line_end = len(text)
                line_text = text[line_start:line_end].strip()

                if re.fullmatch(UNICODE_EMOJI_PATTERN, token) and re.fullmatch(UNICODE_EMOJI_PATTERN, line_text):
                    translated_parts.append(token)
                else:
                    translated_parts.append(wrap_protected_token_for_target(token, target_norm))
                last_index = match.end()

            after = text[last_index:]
            if after:
                if after.strip():
                    translated_parts.append(await self.translate_text(after, source_lang, target_lang))
                else:
                    translated_parts.append(after)

            translated_with_tokens = "".join(translated_parts)
            log_translation_route("protected-token-split", source_norm, target_norm, text)
            return normalize_translated_output_for_target(translated_with_tokens, target_norm)

        full_ai_translation = await self.translate_full_ai_if_needed(text, source_norm, target_norm)
        if full_ai_translation is not None:
            log_translation_route("full-ai", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, full_ai_translation)

        arabic_ai_translation = await self.translate_arabic_with_ai_if_needed(text, source_norm, target_norm)
        if arabic_ai_translation is not None:
            log_translation_route("arabic-ai", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, arabic_ai_translation)

        arabic_dialect_translation = await self.translate_arabic_dialect_text_if_needed(text, source_norm, target_norm)
        if arabic_dialect_translation is not None:
            log_translation_route("arabic-dialect-rules", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, arabic_dialect_translation)

        korean_ai_translation = await self.translate_korean_with_ai_if_needed(text, source_norm, target_norm)
        if korean_ai_translation is not None:
            log_translation_route("korean-ai", source_norm, target_norm, text)
            return await self.remember_translation_result(text, source_norm, target_norm, korean_ai_translation)

        original_text_for_cache = text
        text = normalize_chat_slang_for_translation(text, source_norm)

        # LibreTranslate's direct Traditional Chinese target model can produce
        # repeated/garbled output. Translate to Simplified Chinese first, then
        # convert locally to Traditional with OpenCC.
        source_is_traditional = is_traditional_chinese_code(source_norm)
        target_is_traditional = is_traditional_chinese_code(target_norm)
        source_is_simplified = is_simplified_chinese_code(source_norm)
        target_is_simplified = is_simplified_chinese_code(target_norm)

        if source_is_traditional and target_is_simplified:
            log_translation_route("opencc-traditional-to-simplified", source_norm, target_norm, original_text_for_cache)
            return await self.remember_translation_result(
                original_text_for_cache,
                source_norm,
                target_norm,
                traditional_to_simplified(text),
            )

        if source_is_simplified and target_is_traditional:
            log_translation_route("opencc-simplified-to-traditional", source_norm, target_norm, original_text_for_cache)
            return await self.remember_translation_result(
                original_text_for_cache,
                source_norm,
                target_norm,
                simplified_to_traditional(text),
            )

        libre_source = libretranslate_language_code(source_norm)
        libre_target = "zh" if target_is_traditional else libretranslate_language_code(target_norm)

        if libre_source == libre_target:
            translated_text = text
        else:
            assert self.http_session is not None

            translated_chunks: list[str] = []

            for piece in build_translate_chunks(text):
                payload = {
                    "q": piece,
                    "source": libre_source,
                    "target": libre_target,
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
                                libre_source,
                                libre_target,
                                delay,
                                exc,
                            )
                            await asyncio.sleep(delay)
                            continue
                        raise

                if last_exc is not None and len(translated_chunks) == 0:
                    raise last_exc

            translated_text = "".join(translated_chunks)

        if target_is_traditional:
            translated_text = simplified_to_traditional(translated_text)
            log_translation_route("libretranslate-opencc-traditional", source_norm, target_norm, original_text_for_cache)
        else:
            route_name = "libretranslate" if libre_source != libre_target else "libretranslate-same-code"
            log_translation_route(route_name, source_norm, target_norm, original_text_for_cache)

        return await self.remember_translation_result(
            original_text_for_cache,
            source_norm,
            target_norm,
            translated_text,
        )

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
    return canonical_language_code(code)


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


def group_relay_rows_by_target(rows: list[aiosqlite.Row]) -> dict[int, list[int]]:
    grouped: dict[int, list[int]] = {}
    for row in rows:
        grouped.setdefault(row["target_channel_id"], []).append(row["target_message_id"])
    return grouped


async def resolve_text_channel(channel_id: int) -> Optional[discord.TextChannel]:
    channel = bot.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        return channel
    try:
        fetched = await bot.fetch_channel(channel_id)
    except Exception:
        return None
    return fetched if isinstance(fetched, discord.TextChannel) else None


bot = RelayBot()


def owner_or_manage_guild() -> app_commands.check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not interaction.user:
            return False

        # Server owner always allowed.
        if interaction.user.id == interaction.guild.owner_id:
            return True

        # In slash commands, interaction.user is usually a discord.Member.
        permissions = getattr(interaction.user, "guild_permissions", None)
        if permissions and (permissions.administrator or permissions.manage_guild):
            return True

        # Fallback if Discord did not include complete member permissions.
        try:
            member = await interaction.guild.fetch_member(interaction.user.id)
            if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
                return True
        except Exception:
            pass

        raise app_commands.CheckFailure(
            "You need Administrator or Manage Server permission to use this command."
        )

    return app_commands.check(predicate)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    message = str(error)
    if isinstance(error, app_commands.CheckFailure):
        message = "You need **Administrator** or **Manage Server** permission to use this command."
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
    log.info("Currently connected to %s Discord server(s).", len(bot.guilds))


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    log.info("Joined new server: %s (%s). Use /group_create and /group_add there to configure linked channels.", guild.name, guild.id)


@bot.tree.command(name="help", description="Show setup steps and available commands.")
async def help_command(interaction: discord.Interaction):
    """Show a private, simple help page for users and server admins."""
    embed = discord.Embed(
        title="MultiLang Relay Help",
        description=(
            "I mirror messages between linked language channels and translate them automatically."
        ),
        color=discord.Color.blurple(),
    )

    embed.add_field(
        name="What I do",
        value=(
            "• Mirror messages from one linked language channel to the others.\n"
            "• Translate text into each channel's language.\n"
            "• Keep normal chat flow easy for international communities.\n"
            "• Preserve common Discord items like mentions, emojis, stickers, GIFs, and links when possible."
        ),
        inline=False,
    )

    embed.add_field(
        name="Quick setup",
        value=(
            "1. Create a group: `/group_create group_name:international`\n"
            "2. Add English: `/group_add group_name:international channel:#english language_code:en`\n"
            "3. Add other channels with codes like `ar`, `es`, `pt`, `ru`, `pl`, `tr`, `ko`, `zh-hans`, `zh-hant`.\n"
            "4. Send a message in any linked channel and I will mirror it to the others."
        ),
        inline=False,
    )

    embed.add_field(
        name="Commands",
        value=(
            "`/help` — show this help message\n"
            "`/group_create` — create a translation group\n"
            "`/group_add` — link a channel with a language code\n"
            "`/group_list` — show this server's groups/channels\n"
            "`/group_remove` — remove a linked channel\n"
            "`/group_delete` — delete a whole group\n"
            "`/languages` — show available translation languages\n"
            "`/test_translate` — test a translation privately"
        ),
        inline=False,
    )

    embed.add_field(
        name="Language code examples",
        value=(
            "`en` English, `ar` Arabic, `es` Spanish, `pt` Portuguese, `ru` Russian, "
            "`pl` Polish, `tr` Turkish, `ko` Korean, `zh-hans` Simplified Chinese, "
            "`zh-hant` Traditional Chinese."
        ),
        inline=False,
    )

    embed.set_footer(text="Only you can see this help message.")

    await interaction.response.send_message(embed=embed, ephemeral=True)


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
    existing_message_ids: Optional[list[int]] = None,
    pretranslated_text: Optional[str] = None,
    reply_context: Optional[dict[str, str]] = None,
    is_forwarded_message: bool = False,
) -> tuple[int, list[int]] | None:
    if message.guild is None:
        return None

    if target["channel_id"] == message.channel.id:
        return None

    target_channel = message.guild.get_channel(target["channel_id"])
    if not isinstance(target_channel, discord.TextChannel):
        log.warning("Linked channel %s was not found or is not a text channel", target["channel_id"])
        return None

    used_fallback = False

    try:
        if pretranslated_text is not None:
            translated_text = pretranslated_text
            log_translation_route("pretranslated-batch-result", source_lang, target["language_code"], original_text)
        else:
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
            return None

        used_fallback = True
        final_content = build_no_drop_fallback_content(source_lang, original_text, attachment_links)
        log_translation_route("no-drop-fallback", source_lang, target["language_code"], original_text)
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

    if is_forwarded_message:
        forwarded_prefix = "> ↪ Forwarded\n\n"
        final_content = f"{forwarded_prefix}{final_content}".strip()

    reply_prefix = await build_reply_context_prefix(
        reply_context,
        source_lang,
        target["language_code"],
    )
    if reply_prefix:
        final_content = f"{reply_prefix}{final_content}".strip()

    final_content = clean_translation_engine_artifacts(final_content).strip()

    if not final_content:
        return None

    # Convert custom emojis from servers the bot/webhook may not be able to use
    # into bot application-owned emoji copies. This keeps them inline instead of
    # falling back to :emoji_name: text when possible.
    final_content = await bot.replace_custom_emojis_with_app_emojis(final_content)

    try:
        webhook = await bot.get_or_create_webhook(target_channel, message.guild.id)
        sent_ids: list[int] = []
        chunks = build_discord_chunks(final_content)
        previous_ids = existing_message_ids or []

        for index, chunk in enumerate(chunks):
            if index < len(previous_ids):
                previous_id = previous_ids[index]
                try:
                    await webhook.edit_message(
                        previous_id,
                        content=chunk,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    sent_ids.append(previous_id)
                    continue
                except discord.NotFound:
                    pass

            sent_message = await webhook.send(
                content=chunk,
                username=display_name[:80],
                avatar_url=avatar_url,
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
            )
            sent_ids.append(sent_message.id)

        for extra_message_id in previous_ids[len(chunks):]:
            try:
                await webhook.delete_message(extra_message_id)
            except discord.NotFound:
                pass

        return target["channel_id"], sent_ids
    except discord.Forbidden:
        log.exception("Missing permission to create or use webhooks in #%s", target_channel.name)
    except Exception:
        log.exception("Failed to relay message into #%s", target_channel.name)
    return None


async def sync_linked_message(message: discord.Message, existing_records_by_target: Optional[dict[int, list[int]]] = None) -> None:
    if message.guild is None:
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
    sticker_links = get_message_sticker_links(message)
    if sticker_links:
        attachment_links.extend(sticker_links)

    forwarded_text, forwarded_links = get_forwarded_message_parts(message)
    is_forwarded_message = bool(forwarded_text or forwarded_links)
    if forwarded_text:
        if original_text.strip():
            original_text = f"{original_text.rstrip()}\n\n{forwarded_text}"
        else:
            original_text = forwarded_text
    if forwarded_links:
        attachment_links.extend(forwarded_links)

    display_name, avatar_url = await get_webhook_author_identity(message)
    reply_context = None if is_forwarded_message else await get_reply_context_for_message(message)

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
    if not relay_targets:
        return

    existing_records_by_target = existing_records_by_target or {}
    pretranslated_by_channel: dict[int, str] = {}

    if ENABLE_FULL_AI_TRANSLATION and FULL_AI_BATCH_TRANSLATION and original_text.strip():
        batch_targets = [
            canonical_language_code(target["language_code"])
            for target in relay_targets
        ]
        batch_translations = await bot.get_full_ai_batch_translations(
            original_text,
            source_lang,
            batch_targets,
        )
        for target in relay_targets:
            target_norm = canonical_language_code(target["language_code"])
            translated = batch_translations.get(target_norm)
            if translated is not None:
                pretranslated_by_channel[target["channel_id"]] = translated

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_RELAYS)

    async def run_relay(target: aiosqlite.Row):
        async with semaphore:
            return await relay_to_target(
                message=message,
                source_lang=source_lang,
                group_name=row["group_name"],
                target=target,
                original_text=original_text,
                attachment_links=attachment_links,
                display_name=display_name,
                avatar_url=avatar_url,
                existing_message_ids=existing_records_by_target.get(target["channel_id"]),
                pretranslated_text=pretranslated_by_channel.get(target["channel_id"]),
                reply_context=reply_context,
                is_forwarded_message=is_forwarded_message,
            )

    results = await asyncio.gather(*(run_relay(target) for target in relay_targets), return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            continue
        if not result:
            continue
        target_channel_id, sent_ids = result
        await bot.replace_relay_records(
            message.guild.id,
            message.channel.id,
            message.id,
            target_channel_id,
            sent_ids,
        )


async def delete_mirrored_messages(guild_id: int, source_channel_id: int, source_message_id: int) -> None:
    rows = await bot.get_relay_records_for_source(guild_id, source_channel_id, source_message_id)
    if not rows:
        return

    grouped = group_relay_rows_by_target(rows)

    for target_channel_id, target_message_ids in grouped.items():
        target_channel = await resolve_text_channel(target_channel_id)
        if not target_channel:
            continue

        try:
            webhook = await bot.get_or_create_webhook(target_channel, guild_id)
        except Exception:
            log.exception("Failed to prepare webhook for delete-sync in channel %s", target_channel_id)
            continue

        for target_message_id in target_message_ids:
            try:
                await webhook.delete_message(target_message_id)
            except discord.NotFound:
                pass
            except Exception:
                log.exception("Failed to delete mirrored message %s in channel %s", target_message_id, target_channel_id)

    await bot.delete_relay_records_for_source(guild_id, source_channel_id, source_message_id)


@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent) -> None:
    if payload.guild_id is None:
        return

    # Wait for an in-progress relay/edit of the same original message to finish.
    # Without this, a very fast delete can arrive before relay records are saved,
    # leaving mirrored webhook messages behind.
    lock = await bot.get_source_message_lock(payload.guild_id, payload.channel_id, payload.message_id)
    async with lock:
        await delete_mirrored_messages(payload.guild_id, payload.channel_id, payload.message_id)
        await bot.delete_relay_record_by_target_message(payload.message_id)


@bot.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent) -> None:
    if payload.guild_id is None:
        return

    for message_id in payload.message_ids:
        lock = await bot.get_source_message_lock(payload.guild_id, payload.channel_id, message_id)
        async with lock:
            await delete_mirrored_messages(payload.guild_id, payload.channel_id, message_id)
            await bot.delete_relay_record_by_target_message(message_id)



async def resolve_reaction_emoji_for_mirror(emoji: discord.PartialEmoji | str):
    """Return an emoji usable by the bot for reaction-sync.

    Unicode reactions are returned unchanged. Custom emoji reactions are copied
    into the bot application emoji cache first, so reactions from servers the bot
    cannot access can still be mirrored.
    """
    if isinstance(emoji, str):
        return emoji

    emoji_id = getattr(emoji, "id", None)
    if emoji_id is None:
        return emoji

    original_name = getattr(emoji, "name", None) or "emoji"
    original_animated = bool(getattr(emoji, "animated", False))

    try:
        token = await bot.create_or_get_app_emoji_token(
            original_name=original_name,
            original_emoji_id=str(emoji_id),
            original_animated=original_animated,
        )
    except Exception as exc:
        log.warning(
            "Could not prepare app emoji for reaction-sync :%s: (%s). Using original reaction emoji. Error: %s",
            original_name,
            emoji_id,
            exc,
        )
        return emoji

    match = CUSTOM_EMOJI_RE.fullmatch(token)
    if not match:
        return emoji

    return discord.PartialEmoji(
        name=match.group("name"),
        id=int(match.group("id")),
        animated=bool(match.group("animated")),
    )


async def mirror_reaction_to_linked_messages(payload: discord.RawReactionActionEvent, add: bool) -> None:
    if payload.guild_id is None:
        return

    if bot.user and payload.user_id == bot.user.id:
        return

    related_locations: list[tuple[int, int]] = []
    for attempt in range(3):
        related_locations = await bot.get_related_message_locations(
            payload.guild_id,
            payload.channel_id,
            payload.message_id,
        )
        if related_locations:
            break
        if attempt < 2:
            await asyncio.sleep(1)

    if len(related_locations) < 2:
        return

    mirror_emoji = await resolve_reaction_emoji_for_mirror(payload.emoji)

    for channel_id, message_id in related_locations:
        if channel_id == payload.channel_id and message_id == payload.message_id:
            continue

        target_channel = await resolve_text_channel(channel_id)
        if not target_channel:
            continue

        try:
            target_message = await target_channel.fetch_message(message_id)
        except discord.NotFound:
            continue
        except Exception:
            log.exception("Failed to fetch message %s for reaction-sync in channel %s", message_id, channel_id)
            continue

        try:
            if add:
                await target_message.add_reaction(mirror_emoji)
            else:
                if bot.user is not None:
                    await target_message.remove_reaction(mirror_emoji, bot.user)
        except discord.NotFound:
            pass
        except discord.Forbidden:
            log.warning(
                "Missing permission to %s reaction %s on message %s in channel %s",
                "add" if add else "remove",
                mirror_emoji,
                message_id,
                channel_id,
            )
        except discord.HTTPException as exc:
            log.warning(
                "Could not %s reaction %s on message %s in channel %s: %s",
                "add" if add else "remove",
                mirror_emoji,
                message_id,
                channel_id,
                exc,
            )
        except Exception:
            log.exception(
                "Unexpected reaction-sync error for message %s in channel %s",
                message_id,
                channel_id,
            )


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    await mirror_reaction_to_linked_messages(payload, add=True)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent) -> None:
    await mirror_reaction_to_linked_messages(payload, add=False)


@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent) -> None:
    if payload.guild_id is None:
        return

    if (
        "content" not in payload.data
        and "attachments" not in payload.data
        and "stickers" not in payload.data
        and "embeds" not in payload.data
        and "message_snapshots" not in payload.data
    ):
        return

    channel = await resolve_text_channel(payload.channel_id)
    if not channel:
        return

    row = await bot.get_channel_link(payload.guild_id, payload.channel_id)
    if row is None:
        return

    try:
        message = await channel.fetch_message(payload.message_id)
    except discord.NotFound:
        await delete_mirrored_messages(payload.guild_id, payload.channel_id, payload.message_id)
        return
    except Exception:
        log.exception("Failed to fetch source message %s for edit-sync", payload.message_id)
        return

    if message.author.bot or message.webhook_id is not None:
        return

    lock = await bot.get_source_message_lock(payload.guild_id, payload.channel_id, payload.message_id)
    async with lock:
        existing_rows = await bot.get_relay_records_for_source(payload.guild_id, payload.channel_id, payload.message_id)
        existing_records_by_target = group_relay_rows_by_target(existing_rows)

        if not message_has_syncable_content(message):
            await delete_mirrored_messages(payload.guild_id, payload.channel_id, payload.message_id)
            return

        await sync_linked_message(message, existing_records_by_target)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild is None:
        return

    if message.author.bot:
        return

    if message.webhook_id is not None:
        return

    if not message_has_syncable_content(message):
        return

    lock = await bot.get_source_message_lock(message.guild.id, message.channel.id, message.id)
    async with lock:
        await sync_linked_message(message)

    await bot.process_commands(message)


if __name__ == "__main__":
    bot.run(BOT_TOKEN)
