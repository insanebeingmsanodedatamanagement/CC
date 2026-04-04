import asyncio
import functools
import logging
import os
import pymongo
import random
import re
import secrets
import string
import time
import traceback
import sys
from datetime import datetime, timedelta
from pymongo.errors import DuplicateKeyError

# Fix Windows console encoding for emojis (prevents UnicodeEncodeError with cp1252)
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
from zoneinfo import ZoneInfo
import aiohttp
from aiohttp import web as aiohttp_web

# --- 🔧 NEW: UNIFIED BACKUP SYSTEM FINAL ARCHITECTURE ---
try:
    from backup_manager import BackupManager
    bot_backup = BackupManager("bot1")
except ImportError:
    bot_backup = None
    print("⚠️ backup_manager module not found — Automatic backups disabled")
from aiogram import Bot, Dispatcher, types, F

# ── Unified weekly backup system ──
try:
    from backup_schedulers import weekly_backup_scheduler, monthly_export_scheduler
except ImportError:
    print("⚠️ backup_schedulers module not found — weekly backups disabled")
    weekly_backup_scheduler = None
    monthly_export_scheduler = None
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter, TelegramNetworkError, TelegramUnauthorizedError
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application



# ==========================================
# ⚡ CONFIGURATION  — all values from env vars
# ==========================================
BOT_TOKEN = os.getenv("BOT_1_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "MSANodeDB")  # MongoDB database name
# Dedicated backup cluster — isolated from prod; backup writes go here only
BACKUP_MONGO_URI     = os.getenv("BACKUP_MONGO_URI")
BACKUP_MONGO_DB_NAME = os.getenv("BACKUP_MONGO_DB_NAME", "MSANodeBackups")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))           # Vault channel numeric ID
CHANNEL_LINK = os.getenv("CHANNEL_LINK")               # Telegram vault invite link
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK", "")
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK", "")
REVIEW_LOG_CHANNEL = int(os.getenv("REVIEW_LOG_CHANNEL", 0))   # Support ticket log channel
# Fallback link shown to users when no content link is stored in DB
BOT_FALLBACK_LINK = os.getenv("BOT_FALLBACK_LINK", "https://t.me/msanodebot")
# Render web-service health check port (Render sets PORT automatically)
PORT = int(os.getenv("PORT", 8088))

# ==========================================
# 🌐 WEBHOOK CONFIGURATION
# ==========================================
_WEBHOOK_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
_WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
_WEBHOOK_URL = f"{_WEBHOOK_BASE_URL}{_WEBHOOK_PATH}" if _WEBHOOK_BASE_URL else ""

# ==========================================
# ⚠️ STARTUP VALIDATION - Fail fast
# ==========================================
_REQUIRED_ENV = {
    "BOT_1_TOKEN": BOT_TOKEN,
    "MONGO_URI": MONGO_URI,
    "OWNER_ID": os.getenv("OWNER_ID"),
    "CHANNEL_ID": os.getenv("CHANNEL_ID"),
    "CHANNEL_LINK": CHANNEL_LINK,
}
_missing = [k for k, v in _REQUIRED_ENV.items() if not v]
if _missing:
    print(f"ERROR: Missing required env vars: {', '.join(_missing)}")
    sys.exit(1)

# ==========================================
# 🕐 TIMEZONE CONFIGURATION
# ==========================================
# Set your timezone here — used for 8:40 AM/PM daily reports
REPORT_TIMEZONE = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")  # Change via env var
try:
    TZ = ZoneInfo(REPORT_TIMEZONE)
except Exception:
    TZ = ZoneInfo("Asia/Kolkata")
    logging.warning(f"Invalid REPORT_TIMEZONE '{REPORT_TIMEZONE}', falling back to Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(TZ).replace(tzinfo=None)

# Daily report times (24h format)
REPORT_HOUR_AM = 8   # 8 AM
REPORT_MIN_AM = 40   # :40
REPORT_HOUR_PM = 20  # 8 PM
REPORT_MIN_PM = 40   # :40

# ==========================================
# ⏱️ TIMING CONSTANTS (Animation speeds)
# ==========================================
ANIM_FAST = 0.2      # Fast animations
ANIM_MEDIUM = 0.3    # Medium animations
ANIM_SLOW = 0.5      # Slow animations
ANIM_PAUSE = 0.4     # Pause between sections
ANIM_DELAY = 1.0     # Long delay before delete

# Dead-user lifecycle: days after MSA-ID release before user_verification record is purged
DEAD_USER_CLEANUP_DAYS = 90
# Ghost-user cleanup: users who /started but never joined vault, idle this many days
GHOST_USER_CLEANUP_DAYS = 180

# ==========================================
# 🛡️ ANTI-SPAM SYSTEM
# ==========================================
# Track users currently processing commands (prevents spam)
user_processing: dict[int, str] = {}  # {user_id: "command_name"}

# Rate limiting: Track last command time per user (prevents flood bans)
user_last_command: dict[int, float] = {}  # {user_id: timestamp}
COMMAND_COOLDOWN = 2.0  # seconds between commands (prevents Telegram FloodWait)

# ==========================================
# 🧊 PROGRESSIVE AUTO-FREEZE SYSTEM
# ==========================================
# Freeze durations per offense level (seconds)
_FREEZE_LEVELS  = [60, 60, 60, 60]   # Fixed 60s anti-flood cooldown on trigger
_FREEZE_WINDOW  = 4.0   # sliding window (seconds) — lenient for slow internet
_FREEZE_TRIGGER = 5     # rapid taps within window needed to trip first freeze
_FREEZE_DECAY   = 600   # seconds of clean behavior before offense count resets

# Per-user state: {user_id: {offense, frozen_until, taps, window_start}}
_freeze_tracker: dict[int, dict] = {}
_freeze_notice_tracker: dict[int, float] = {}  # throttle freeze notice spam per user

# Support security hardening: progressive warnings + temporary lock for repeated abuse
_SUPPORT_SECURITY_WINDOW_SECS = 24 * 3600
_SUPPORT_SECURITY_MAX_WARNINGS = 3
_SUPPORT_SECURITY_LOCK_SECS = 6 * 3600
# NOTE: Lock is now stored in MongoDB (bot1_support_tickets, type='security_lock').
# _support_security_tracker is kept as a fast in-process cache only.
_support_security_tracker: dict[int, dict] = {}

# Cooldown live-refresh hardening (prevents Telegram flood during heavy traffic)
_COOLDOWN_REFRESH_INTERVAL_SECS = 5
_COOLDOWN_REFRESH_MAX_SECS = 30
_ticket_cooldown_live_tasks: dict[int, asyncio.Task] = {}

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy library loggers — keep only WARNING+ for aiogram
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("aiogram.event").setLevel(logging.WARNING)
logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)
logging.getLogger("aiogram.client").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# Suppress noisy pymongo background pool/network warnings (auto-recovered by pymongo itself)
logging.getLogger("pymongo.client").setLevel(logging.CRITICAL)
logging.getLogger("pymongo.pool").setLevel(logging.CRITICAL)
logging.getLogger("pymongo.topology").setLevel(logging.CRITICAL)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==========================================
# 🖥️ Bot 1 LIVE TERMINAL MIDDLEWARE
# Logs every user interaction to MongoDB — visible in Bot 2 Terminal from Render
# ==========================================
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from typing import Callable, Dict, Any, Awaitable

class Bot1TerminalMiddleware(BaseMiddleware):
    """Intercepts every message and logs it to shared MongoDB live_terminal_logs collection."""
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Only log Message events with text
        msg = getattr(event, 'message', event) if not hasattr(event, 'text') else event
        user = getattr(msg, 'from_user', None)
        text = getattr(msg, 'text', None) or getattr(msg, 'caption', None) or "[media]"
        if user and user.id:
            try:
                # Trim long messages
                display_text = text[:60] + "..." if len(text) > 60 else text
                # We call log_to_terminal only after DB is ready (guarded by try/except inside)
                log_to_terminal(
                    action_type=f"MSG from {user.full_name or user.id}",
                    user_id=user.id,
                    details=display_text
                )
            except Exception:
                pass
        return await handler(event, data)

class BanGateMiddleware(BaseMiddleware):
    """
    Global hard gate — runs BEFORE every message AND callback_query handler.
    Permanently banned users: ALL interaction is silently dropped (no response).
    Temporarily banned users: passed through — their per-handler support flow applies.
    Bot owner is always exempted.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get("event_from_user")
        if user and user.id and user.id != OWNER_ID:
            try:
                ban_doc = col_banned_users.find_one(
                    {"user_id": user.id, "scope": {"$ne": "bot2"}}
                )
                if ban_doc:
                    ban_type = ban_doc.get("ban_type", "permanent")
                    # Temp ban: let through — per-handler logic handles it
                    if ban_type == "temporary":
                        pass
                    else:
                        # Permanent ban: hard drop — no response, no bypass
                        # For callback_query, silently answer to dismiss Telegram spinner
                        if hasattr(event, "answer") and callable(event.answer):
                            try:
                                await event.answer()
                            except Exception:
                                pass
                        return  # Do NOT call handler under any circumstances
            except Exception:
                pass  # DB error: fail-open (never block legitimate users)
        return await handler(event, data)

from typing import Any
# ==========================================
health_stats: dict[str, Any] = {
    "errors_caught": 0,
    "auto_healed": 0,
    "owner_notified": 0,
    "last_error": None,
    "last_error_msg": "",
    "bot_start_time": datetime.now(TZ),
    "db_reconnects": 0,
    "reports_sent": 0,
}

# ==========================================
# 📊 DATABASE CONNECTION  
# ==========================================
try:
    import certifi
    client = pymongo.MongoClient(
        MONGO_URI,
        maxPoolSize=50,          # Up to 50 concurrent connections
        minPoolSize=5,           # Keep 5 always alive
        maxIdleTimeMS=30000,     # Close idle connections after 30 s
        serverSelectionTimeoutMS=5000,  # Fail fast on unavailable
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        retryWrites=True,
        retryReads=True,
        w="majority",            # Write concern – durable
        tlsCAFile=certifi.where()
    )
    db = client[MONGO_DB_NAME]
    # Guard: refuse to start if pointed at the wrong database
    if db.name != "MSANodeDB":
        logger.critical(f"❌ FATAL: MONGO_DB_NAME is '{db.name}' — must be 'MSANodeDB'. Fix your env vars and restart.")
        sys.exit(1)
    logger.info(f"✅ Database guard passed: writing to '{db.name}'")
    # Single database — all bots (bot1, bot2, bot3) use MSANodeDB on Render
    col_user_verification = db["bot1_user_verification"]
    col_msa_ids = db["bot1_msa_ids"]  # Collection for MSA+ ID tracking
    col_pdfs = db["bot3_pdfs"]          # Bot 3 PDFs (same MSANodeDB)
    col_ig_content = db["bot3_ig_content"] # Bot 3 IG content (same MSANodeDB)
    col_support_tickets = db["bot1_support_tickets"]  # Collection for support ticket tracking
    col_banned_users = db["bot1_banned_users"]  # Collection for banned users (managed by Bot 2)
    col_suspended_features = db["bot1_suspended_features"]  # Collection for suspended features (managed by Bot 2)
    col_bot1_settings = db["bot1_settings"]  # Bot 1 global settings (Maintenance Mode)
    col_live_logs = db["bot2_live_terminal_logs"]  # Shared live logs for Bot 2 terminal (Render-safe)
    logger.info("✅ MongoDB connected successfully")

    # ── Dedicated BACKUP cluster (writes go here, never to MSANodeDB) ──
    _bk_uri  = BACKUP_MONGO_URI or MONGO_URI
    _bk_db   = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
    if not BACKUP_MONGO_URI:
        logger.warning("⚠️ BACKUP_MONGO_URI not set — bot1 backup collections falling back to PROD cluster!")
        backup_client_b1 = client
    else:
        import certifi
        backup_client_b1 = pymongo.MongoClient(
            _bk_uri,
            maxPoolSize=10, minPoolSize=1,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=10000, socketTimeoutMS=30000,
            retryWrites=True, w="majority",
            tlsCAFile=certifi.where()
        )
    backup_db_b1 = backup_client_b1[_bk_db]  # MSANodeBackups
    logger.info(f"✅ Backup cluster connected: {_bk_db}")
    col_bot1_backups = backup_db_b1["bot1_backups"]         # Bot 1 auto-backups → BACKUP cluster only
    col_bot1_restore_data = backup_db_b1["bot1_restore_data"]  # Bot 1 restore snapshot → BACKUP cluster only
    col_broadcasts = db["bot2_broadcasts"]        # Broadcasts sent via Bot 2 (read-only here)
    
    # ==========================================
    # 🔍 CREATE DATABASE INDEXES (Performance)
    # ==========================================
    try:
        col_user_verification.create_index("user_id", unique=True)
        col_msa_ids.create_index("user_id", unique=True)
        col_msa_ids.create_index("msa_number")
        col_pdfs.create_index("ig_start_code")
        col_pdfs.create_index("yt_start_code")
        col_pdfs.create_index("index")
        col_ig_content.create_index("cc_code")
        col_ig_content.create_index("start_code")
        col_support_tickets.create_index("user_id")
        col_support_tickets.create_index("status")
        # Enterprise extra indexes
        col_banned_users.create_index("user_id", unique=True)
        col_banned_users.create_index("ban_expires")  # TTL hint only
        col_support_tickets.create_index([("user_id", 1), ("status", 1)])
        col_support_tickets.create_index("created_at")
        # Note: resolved_at index intentionally omitted — tickets are permanent, no TTL needed
        db["bot2_user_tracking"].create_index("user_id", unique=True)
        db["bot1_state_persistence"].create_index("key", unique=True)
        col_bot1_backups.create_index([("backup_date", -1)])
        col_bot1_backups.create_index([("backup_type", 1)])
        col_broadcasts.create_index([("index", -1)])
        col_broadcasts.create_index("broadcast_id", unique=True)
        # ── Unique dedup index: prevents duplicate click-tracking rows even under concurrent load
        db["bot3_user_activity"].create_index(
            [("user_id", 1), ("item_id", 1), ("click_type", 1)],
            unique=True,
            name="unique_user_item_click"
        )
        logger.info("✅ Database indexes created/verified")
    except Exception as idx_error:
        logger.warning(f"⚠️ Index creation warning: {idx_error}")

    # ── Drop any legacy TTL index on resolved_at (all variants) ───────────────────────
    try:
        for _idx_name in ("resolved_at_1", "resolved_at_ttl_180d", "resolved_at_ttl_30d"):
            try:
                col_support_tickets.drop_index(_idx_name)
            except Exception:
                pass  # Already gone — that's fine
        logger.info("✅ Ticket TTL cleared — tickets are permanent, no auto-deletion")
    except Exception as ttl_err:
        logger.warning(f"⚠️ Ticket TTL drop warning: {ttl_err}")

    # ── Partial unique: prevent duplicate open tickets per user ──────────
    try:
        col_support_tickets.create_index(
            [("user_id", 1)],
            unique=True,
            partialFilterExpression={"status": "open"},
            name="unique_open_ticket_per_user"
        )
        logger.info("✅ Unique partial index: one open ticket per user enforced")
    except Exception as uniq_err:
        logger.warning(f"⚠️ Partial unique index warning (may already exist): {uniq_err}")

    # ── Backup dedup index: one backup summary per bot/window key ───────────
    try:
        col_bot1_backups.create_index(
            [("bot", 1), ("window_key", 1)],
            unique=True,
            sparse=True,
            name="unique_bot_window_key"
        )
        logger.info("✅ Backup dedup index active: unique (bot, window_key)")
    except Exception as bidx_err:
        logger.warning(f"⚠️ Backup dedup index warning: {bidx_err}")

except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    sys.exit(1)

# ==========================================
# 🖥️ LIVE TERMINAL LOGGER (shared with Bot 2)
# ==========================================
_BOT1_LOG_MAX = 100  # Keep last 100 bot1 logs in MongoDB

def log_to_terminal(action_type: str, user_id: int, details: str = ""):
    """Write a log entry to the shared live_terminal_logs collection so Bot 2 can display it live."""
    try:
        timestamp = now_local().strftime('%I:%M:%S %p')
        col_live_logs.insert_one({
            "timestamp": timestamp,
            "created_at": now_local(),
            "bot": "bot1",
            "action": action_type,
            "user_id": user_id,
            "details": details,
        })
        # Trim: keep newest _BOT1_LOG_MAX entries for bot1
        count = col_live_logs.count_documents({"bot": "bot1"})
        if count > _BOT1_LOG_MAX:
            oldest = list(col_live_logs.find({"bot": "bot1"}, {"_id": 1}).sort("created_at", 1).limit(count - _BOT1_LOG_MAX))
            if oldest:
                col_live_logs.delete_many({"_id": {"$in": [d["_id"] for d in oldest]}})
    except Exception:
        pass  # Never let logging crash the bot

# ==========================================
# 🔐 VERIFICATION FUNCTIONS
# ==========================================

# ==========================================
# USER SOURCE TRACKING (permanent first-source lock)
# ==========================================

def _is_new_unique_click(user_id: int, item_id, click_type: str) -> bool:
    """
    Per-user click deduplication — race-condition-proof.
    Uses upsert + unique index instead of find_one+insert_one so concurrent
    clicks can never produce a duplicate row, even under high load.
    Returns True on the FIRST click, False on every subsequent click.
    """
    try:
        from pymongo.errors import DuplicateKeyError
        col_dedup = db["bot3_user_activity"]
        key = {"user_id": user_id, "item_id": str(item_id), "click_type": click_type}
        result = col_dedup.update_one(
            key,
            {"$setOnInsert": {**key, "first_click_at": now_local()}},
            upsert=True
        )
        # upserted_id is set ONLY when a new doc was inserted (first click)
        return result.upserted_id is not None
    except Exception as e:
        # DuplicateKeyError = race lost = already exists = not a new click
        from pymongo.errors import DuplicateKeyError
        if isinstance(e, DuplicateKeyError):
            return False
        logger.warning(f"Dedup check failed ({click_type}): {e}; allowing increment")
        return True  # On any other error, fail-open (never block a user)

def _store_initial_source(user_id: int, source: str) -> None:
    """
    Lightweight first-touch source recorder — writes ONLY to col_user_verification.
    Does NOT write to bot2_user_tracking (that happens only at vault join).

    Priority: Specific sources (IG, YT, YTCODE, IGCC) beat UNKNOWN.
    Once a specific source is stored it is never overwritten.
    """
    _SPECIFIC = {"IG", "YT", "YTCODE", "IGCC"}
    try:
        existing = col_user_verification.find_one({"user_id": user_id}, {"initial_source": 1})
        if existing is None:
            # Brand-new user — create record with grace pass included
            col_user_verification.update_one(
                {"user_id": user_id},
                {"$set": {
                    "initial_source": source,
                    "grace_allowed": True,   # All new users get one free pass
                    "grace_consumed": False,
                }},
                upsert=True
            )
        else:
            current = existing.get("initial_source", "UNKNOWN")
            if current not in _SPECIFIC and source in _SPECIFIC:
                # Upgrade UNKNOWN → specific (first real source wins)
                col_user_verification.update_one(
                    {"user_id": user_id},
                    {"$set": {"initial_source": source}}
                )
            # else: specific already stored — never overwrite
    except Exception as e:
        logger.warning(f"_store_initial_source failed ({source}): {e}")

def track_user_source(user_id: int, source: str, username: str, first_name: str, msa_id: str):
    """
    Record traffic source with FIRST-TOUCH PRIORITY lock.

    Source Priority Tiers (HIGH → LOW):
      Tier 1 (Specific / Permanent): IG | YT | YTCODE | IGCC
      Tier 2 (Placeholder):          UNKNOWN

    Rules:
    - Brand new user → insert full record with whichever source comes first.
    - User has UNKNOWN source → upgrade to any specific source freely.
    - User has a specific source → NEVER overwrite, regardless of what comes next.
    - Always update `last_start` and `msa_id` on every call.
    """
    _SPECIFIC_SOURCES = {"IG", "YT", "YTCODE", "IGCC"}
    new_source_is_specific = source in _SPECIFIC_SOURCES

    try:
        col = db["bot2_user_tracking"]
        existing = col.find_one({"user_id": user_id}, {"source": 1, "first_start": 1})

        if existing is None:
            # ── Brand new user: insert with whatever source arrives first ──
            col.insert_one({
                "user_id": user_id,
                "source": source,
                "first_start": now_local(),
                "username": username,
                "first_name": first_name,
                "msa_id": msa_id,
                "last_start": now_local(),
            })
        else:
            current_source = existing.get("source", "UNKNOWN")
            current_is_specific = current_source in _SPECIFIC_SOURCES

            if current_is_specific:
                # ── Specific source already locked — only refresh last_start ──
                col.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_start": now_local(), "username": username, "first_name": first_name}}
                )
            elif new_source_is_specific:
                # ── Upgrade UNKNOWN → specific source, lock it permanently ──
                col.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "source": source,
                        "first_start": now_local(),  # Reset to first real interaction
                        "last_start": now_local(),
                        "username": username,
                        "first_name": first_name,
                        "msa_id": msa_id,
                    }}
                )
                logger.info(f"📌 Source upgraded: user {user_id} UNKNOWN → {source}")
            else:
                # ── UNKNOWN stays UNKNOWN — just update mutable fields ──
                col.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_start": now_local(), "username": username, "first_name": first_name}}
                )
    except Exception as e:
        logger.error(f"Warning: track_user_source failed: {e}")

async def _sync_pre_vault_user(user_id: int, username: str, first_name: str) -> tuple:
    """
    One-time DB sync for users who joined the vault BEFORE ever starting bot1.

    Called whenever is_in_vault=True is detected in cmd_start.
    Idempotent — if vault_joined is already True in DB, skips silently.
    Never sends any messages — only syncs database state.

    Returns: (msa_id: str|None, newly_synced: bool)
      newly_synced=True  → first time sync; caller should send the menu keyboard
      newly_synced=False → already synced; no extra action needed

    Actions (only when not already synced):
      1. Sets vault_joined=True, verified=True, ever_verified=True
      2. Allocates MSA+ ID (requires vault_joined=True, now satisfied)
      3. Calls track_user_source → writes to bot2_user_tracking with first-touch source
      4. Clears stale left/reminder fields
    """
    try:
        rec = col_user_verification.find_one(
            {"user_id": user_id}, {"vault_joined": 1, "initial_source": 1}
        )
        # Already synced — nothing to do
        if rec and rec.get("vault_joined"):
            return get_user_msa_id(user_id), False  # (msa_id, newly_synced=False)

        # --- Sync vault status in DB ---
        update_verification_status(
            user_id,
            vault_joined=True,
            verified=True,
            ever_verified=True,
        )
        # Clear any stale lifecycle fields
        col_user_verification.update_one(
            {"user_id": user_id},
            {"$unset": {"vault_left_at": "", "reminder1_sent": "", "reminder2_sent": "", "reminder3_sent": ""}}
        )

        # --- Allocate MSA+ ID (vault_joined=True now satisfies the guard) ---
        msa_id = allocate_msa_id(user_id, username, first_name)

        # --- Write first-touch source to bot2_user_tracking ---
        source = (rec or {}).get("initial_source", "UNKNOWN")
        track_user_source(user_id, source, username, first_name, msa_id)

        logger.info(
            f"[PRE-VAULT SYNC] user={user_id} NEWLY SYNCED: source={source!r} msa_id={msa_id}"
        )
        return msa_id, True  # (msa_id, newly_synced=True)
    except Exception as e:
        logger.error(f"[PRE-VAULT SYNC] Failed for user {user_id}: {e}")
        return None, False


async def check_channel_membership(user_id: int) -> bool:
    """Check if user is a member of the vault channel"""
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        return False

class SearchCodeStates(StatesGroup):
    waiting_for_code = State()
    waiting_for_first_code = State()

class SupportStates(StatesGroup):
    waiting_for_issue = State()  # Waiting for user to describe their issue

class GuideStates(StatesGroup):
    viewing_bot1 = State()  # paginated Agent Guide

class RulesStates(StatesGroup):
    viewing_rules = State()  # paginated Rules

class ResetDataStates(StatesGroup):
    selecting_reset_target = State()   # Choose Bot 1 or Bot 2
    waiting_for_confirm1   = State()   # Type CONFIRM
    waiting_for_confirm2   = State()   # Type DELETE

# ==========================================
# 🛡️ TICKET VALIDATION & FILTERS — ENTERPRISE GRADE
# ==========================================

# ---------------------------------------------------------------------------
# PROFANITY DATABASE
# Multi-layer: exact words, leetspeak/substitutions, phrase patterns, threats,
# scam patterns, and hate speech — all checked independently.
# ---------------------------------------------------------------------------

# Layer 1 — Core profanity (English, common)
_PROFANITY_CORE = {
    "fuck", "fucking", "fucked", "fucker", "fucks", "fuckin", "fuckoff",
    "shit", "shitting", "shitty", "bullshit", "horseshit", "shithead", "shitstorm",
    "bitch", "bitches", "bitchy", "son of a bitch",
    "asshole", "ass", "asses", "arsehole", "arse",
    "bastard", "bastards",
    "crap", "crappy",
    "piss", "pissed", "pissing",
    "dick", "dicks", "dickhead", "dickface",
    "cock", "cocks", "cocksucker", "cockhead",
    "pussy", "pussies",
    "slut", "sluts", "slutty",
    "whore", "whores", "whorish",
    "cunt", "cunts",
    "twat", "twats",
    "motherfucker", "motherfucking", "mf",
    "dipshit", "dumbass", "dumbfuck", "jackass", "numbnuts",
    "prick", "pricks",
    "douche", "douchebag", "douchebags",
    "wanker", "wankers", "tosser", "tossers",
    "skank", "skanky",
    "bimbo", "bimbos",
    "moron", "morons", "idiot", "idiots", "imbecile",
    "shitface", "shitbag", "cumshot", "cumface",
    "jerkoff", "jerk off", "jackoff", "jack off",
    "asshat", "asswipe", "assfuck",
    "fuckface", "fuckwit", "fuckhead", "fuckboy",
    "clusterfuck", "mindfuck",
    "dumbshit", "dumb shit", "holy shit",
}

# Layer 2 — Hate speech / slurs
_HATE_SPEECH = {
    "nigger", "nigga", "niggas", "niggers",
    "fag", "faggot", "faggots", "fags",
    "retard", "retarded", "retards",
    "spic", "spics", "wetback", "wetbacks",
    "chink", "chinks", "gook", "gooks",
    "kike", "kikes",
    "tranny", "trannies",
    "dyke", "dykes",
    "cracker", "crackers",
    "coon", "coons",
    "towelhead", "sandnigger",
    "zipperhead",
    "raghead",
    "beaner", "beaners",
    "gringo", "gringos",
    "honky", "honkies",
    "jap", "japs",
    "nazi", "nazis",
    "white trash",
}

# Layer 3 — Sexual content / adult material
_SEXUAL_CONTENT = {
    "porn", "porno", "pornography", "pornographic",
    "nude", "nudes", "nudity",
    "naked", "nakedpics",
    "xxx", "x-rated",
    "dildo", "dildos",
    "vibrator", "vibrators",
    "blowjob", "blow job", "handjob", "hand job",
    "cumming", "cum", "cumslut",
    "orgasm", "orgasms",
    "masturbate", "masturbation", "masturbating",
    "erection", "erotic",
    "hentai",
    "onlyfans", "only fans",
    "sexting", "sext",
    "stripclub", "strip club",
    "hooker", "hookers", "escort", "prostitute", "prostitution",
}

# Layer 4 — Threats and violence
_THREATS = {
    "kill yourself", "kys", "go kill yourself",
    "kill you", "i will kill", "gonna kill", "going to kill",
    "i will hurt", "gonna hurt", "going to hurt",
    "beat you up", "beat your ass",
    "shoot you", "stab you", "i will stab",
    "bomb", "bombing", "blow up", "blowing up",
    "die", "you should die", "hope you die",
    "suicide", "hang yourself", "slit your wrists",
    "murder", "murdering", "gonna murder",
    "attack you", "come for you", "find you",
    "i know where you live", "dox you", "doxxed",
    "ddos", "hack you", "hacking you",
}

# Layer 5 — Scam / phishing / spam
_SCAM_PATTERNS = {
    "free money", "free cash", "free bitcoin", "free crypto",
    "click here", "click this link", "click the link",
    "bit.ly", "tinyurl", "shorturl", "is.gd", "t.co/",
    "get rich", "get rich quick", "earn money fast",
    "make money fast", "make $", "make dollars",
    "investment opportunity", "guaranteed profit", "guaranteed returns",
    "binary options", "forex signals", "crypto signals",
    "send me money", "send me btc", "send bitcoin",
    "wire transfer", "western union", "moneygram",
    "nigerian prince", "lottery winner", "you've won",
    "claim your prize", "congratulations you won",
    "account suspended", "verify your account now",
    "your account will be deleted",
    "whatsapp me", "telegram me at", "contact me on",
    "100% safe", "100% legit", "zero risk",
    "passive income", "work from home earn",
    "mlm", "pyramid scheme", "ponzi",
    "cheap followers", "buy followers", "buy likes",
}

# Layer 6 — Leetspeak / character substitution variants
# These are checked after normalizing the text (see _normalize below)
_LEET_NORMALIZED = {
    # These are canonical forms; normalizer converts leet → plain before matching
    "fuck", "shit", "bitch", "ass", "dick", "cock", "cunt", "piss",
    "nigger", "nigga", "faggot", "retard",
    "porn", "sex", "nude",
}

# Combined master set for quick single-pass check
BAD_WORDS: set[str] = (
    _PROFANITY_CORE
    | _HATE_SPEECH
    | _SEXUAL_CONTENT
    | _THREATS
    | _SCAM_PATTERNS
)

# ---------------------------------------------------------------------------
# NORMALIZATION — converts leet/unicode tricks to plain ASCII before matching
# Handles: 4→a, 3→e, 1→i/l, 0→o, 5→s, 7→t, @→a, $→s, +→t, etc.
# ---------------------------------------------------------------------------
_LEET_MAP = str.maketrans({
    '4': 'a', '@': 'a', 'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a',
    '3': 'e', 'é': 'e', 'è': 'e', 'ë': 'e',
    '1': 'i', '!': 'i', 'í': 'i', 'ì': 'i', 'î': 'i',
    '0': 'o', 'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o',
    '5': 's', '$': 's',
    '7': 't', '+': 't',
    '6': 'g',
    '8': 'b',
    '9': 'p',
    'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
    'ç': 'c',
    'ñ': 'n',
    'ý': 'y',
    'ß': 'ss',
    # Zero-width / invisible chars
    '\u200b': '', '\u200c': '', '\u200d': '', '\ufeff': '',
    # Common obfuscation
    '|': 'i', '(': 'c',
})

def _normalize(text: str) -> str:
    """Normalize text: lowercase, leet decode, collapse repeated chars, strip spaces between letters."""
    t = text.lower()
    t = t.translate(_LEET_MAP)
    # Remove zero-width spaces and soft hyphens
    t = re.sub(r'[\u00ad\u200b-\u200d\ufeff]', '', t)
    # Collapse 3+ repeated same characters → 2 (catches "fuuuuck" → "fuuck" → still matches "fuck")
    t = re.sub(r'(.)\1{2,}', r'\1\1', t)
    # Remove spaces/dots/dashes used to obfuscate (f.u.c.k, f-u-c-k, f u c k)
    t_nospace = re.sub(r'(?<=[a-z])[\s.\-_*#]{1,3}(?=[a-z])', '', t)
    return t_nospace

# Maximum safe message length for Telegram
MAX_TICKET_LENGTH = 4000
MIN_TICKET_LENGTH = 20  # Raised: 10 chars is too little to be a real support message

# ---------------------------------------------------------------------------
# PROFANITY DETECTION — multi-layer
# ---------------------------------------------------------------------------
def contains_profanity(text: str) -> tuple[bool, list[str]]:
    """
    Multi-layer profanity check:
    1. Direct match on original lowercase
    2. Leet/unicode-normalized match
    3. Phrase-level match (for multi-word patterns like "kill yourself")

    Returns (has_profanity: bool, found_terms: list)
    """
    original_lower = text.lower()
    normalized = _normalize(text)
    found = []

    for term in BAD_WORDS:
        term_lower = term.lower()
        term_norm  = _normalize(term)

        # Multi-word phrases: substring match (no word boundary needed)
        if " " in term_lower:
            if term_lower in original_lower or term_norm in normalized:
                found.append(term)
            continue

        # Single words: word-boundary match on both original and normalized
        pattern_orig = r'(?<![a-z])' + re.escape(term_lower) + r'(?![a-z])'
        pattern_norm = r'(?<![a-z])' + re.escape(term_norm)  + r'(?![a-z])'

        if re.search(pattern_orig, original_lower):
            found.append(term)
        elif re.search(pattern_norm, normalized):
            found.append(term)

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped = []
    for w in found:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    return (len(deduped) > 0, deduped)


# ---------------------------------------------------------------------------
# SPAM / GIBBERISH DETECTION
# ---------------------------------------------------------------------------
def is_spam_or_gibberish(text: str) -> tuple[bool, str]:
    """
    Multi-signal spam and gibberish detector.
    Returns (is_spam: bool, reason: str)
    """
    stripped = text.strip()
    words = stripped.split()
    total_chars = len(stripped)

    # --- 1. Excessive repeated characters (aaaaaaa, !!!!!, hahahahaha) ---
    if re.search(r'(.)\1{5,}', stripped):
        return (True, "Excessive repeated characters — looks like spam")

    # --- 2. Entire message is a single repeated word ---
    if len(words) >= 4:
        unique_words = set(w.lower() for w in words)
        if len(unique_words) == 1:
            return (True, "Single word repeated over and over")

    # --- 3. Excessive ALL-CAPS (>65% uppercase, ignoring spaces/punctuation) ---
    if total_chars > 10:
        letters = [c for c in stripped if c.isalpha()]
        if letters:
            caps_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
            if caps_ratio > 0.65:
                return (True, "Excessive caps — please write normally")

    # --- 4. Excessive special characters (>35%) ---
    if total_chars > 0:
        special = sum(1 for c in stripped if not c.isalnum() and not c.isspace())
        if special / total_chars > 0.35:
            return (True, "Too many special characters")

    # --- 5. Excessive emojis (>40% of characters are emoji/non-BMP) ---
    if total_chars > 0:
        emoji_chars = sum(1 for c in stripped if ord(c) > 0x1F000)
        if emoji_chars / total_chars > 0.40:
            return (True, "Too many emojis — describe your issue in words")

    # --- 6. Very short words dominate (random noise: "lol ok hi ya oh") ---
    if len(words) > 6:
        short = [w for w in words if len(w.strip('.,!?')) <= 3]
        if len(short) / len(words) > 0.75:
            return (True, "Message is mostly very short/meaningless words")

    # --- 7. Keyboard mashing patterns ---
    keyboard_rows = [
        "qwertyuiop", "asdfghjkl", "zxcvbnm",
        "qazwsx", "wsxedc", "edcrfv", "rfvtgb", "tgbyhn", "yhnujm",
        "1234567890", "0987654321",
        "qwerty", "azerty", "dvorak",
    ]
    t_nospace = stripped.lower().replace(" ", "")
    for kp in keyboard_rows:
        if len(kp) >= 6 and (kp in t_nospace or "".join(reversed(kp)) in t_nospace):
            return (True, "Keyboard mashing detected")

    # --- 8. No vowels in a long stretch (pure consonant gibberish: "jksdfjkl") ---
    # Check each word individually so real abbreviations (e.g. "lol") don't trigger
    if total_chars > 20:
        long_words = [w for w in words if len(w) > 6]
        for w in long_words:
            w_alpha = re.sub(r'[^a-z]', '', w.lower())
            if len(w_alpha) > 6:
                vowels = sum(1 for c in w_alpha if c in 'aeiou')
                if vowels == 0:
                    return (True, f"Gibberish word detected: '{w}'")

    # --- 9. URL/link injection (phishing, external links not from Telegram) ---
    url_pattern = re.compile(
        r'(https?://|www\.)'                          # http(s):// or www.
        r'(?!t\.me|telegram\.(me|org|dog))'           # exclude Telegram itself
        r'[^\s]{4,}',
        re.IGNORECASE
    )
    if url_pattern.search(stripped):
        return (True, "External links are not allowed in support messages")

    # --- 10. Phone number injection (scam bait: share your number privately) ---
    phone_pattern = re.compile(
        r'(\+?[0-9]{1,3}[\s\-.]?)?'                  # optional country code
        r'(\(?\d{3}\)?[\s\-.]?)'                      # area code
        r'\d{3}[\s\-.]?\d{4,}',                       # local number
        re.IGNORECASE
    )
    if phone_pattern.search(stripped):
        return (True, "Phone numbers are not allowed in support messages")

    # --- 11. Email injection ---
    email_pattern = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    if email_pattern.search(stripped):
        return (True, "Email addresses are not allowed in support messages")

    # --- 12. Excessive number blocks (card numbers, account numbers) ---
    # 12+ consecutive digits → likely a card/account number
    if re.search(r'\d{12,}', stripped.replace(' ', '').replace('-', '')):
        return (True, "Long number sequences are not allowed — do not share financial data")

    # --- 13. Completely non-meaningful (only punctuation/numbers, no real words) ---
    alpha_chars = sum(1 for c in stripped if c.isalpha())
    if total_chars > 10 and alpha_chars / total_chars < 0.25:
        return (True, "Message has almost no readable text")

    return (False, "")


# ---------------------------------------------------------------------------
# RATE LIMITING for support tickets — 24-hour cooldown, DB-backed (survives restarts)
# ---------------------------------------------------------------------------
TICKET_COOLDOWN_HOURS = 24# User must wait 24 hours between ticket submissions

def _format_hms(total_seconds: int) -> str:
    """Format seconds as Hh Mm Ss for premium cooldown/readability messages."""
    total_seconds = max(0, int(total_seconds))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h}h {m}m {s}s"

def _build_remaining_bar(remaining_seconds: int, total_seconds: int, width: int = 20) -> tuple[str, float]:
    """Build a decreasing progress bar based on remaining time."""
    if total_seconds <= 0:
        return ("▱" * width, 0.0)
    ratio = max(0.0, min(1.0, remaining_seconds / total_seconds))
    filled = int(round(ratio * width))
    filled = max(0, min(width, filled))
    return ("▰" * filled + "▱" * (width - filled), ratio * 100)

def _support_lock_key(user_id: int) -> dict:
    return {"type": "security_lock", "user_id": user_id}


def _get_support_lock_remaining(user_id: int) -> int:
    """Return active support lock remaining seconds (0 = no lock).
    Checks in-memory cache first, then MongoDB for persistence across restarts.
    This means the lock survives: bot restarts, user leaving vault, rejoining.
    """
    now_ts = int(time.time())

    # Fast path: in-memory cache
    cached = _support_security_tracker.get(user_id)
    if cached and cached.get("lock_until", 0) > now_ts:
        return max(0, int(cached["lock_until"]) - now_ts)

    # DB fallback — authoritative source
    try:
        doc = col_support_tickets.find_one(_support_lock_key(user_id))
        if doc:
            lock_until = int(doc.get("lock_until", 0))
            warnings   = int(doc.get("warnings", 0))
            window_start = int(doc.get("window_start", 0))
            # Sync back into in-memory cache
            _support_security_tracker[user_id] = {
                "lock_until":   lock_until,
                "warnings":     warnings,
                "window_start": window_start,
            }
            return max(0, lock_until - now_ts)
    except Exception:
        pass

    return 0


def _register_support_violation(user_id: int) -> tuple[int, int, bool]:
    """Register a support abuse violation.
    Persists lock state to MongoDB so it survives bot restarts and vault leave/rejoin.
    Returns: (warning_count, lock_remaining_seconds, lock_triggered_now)
    """
    now_ts = int(time.time())

    # Load from DB first (authoritative)
    state: dict = {"window_start": now_ts, "warnings": 0, "lock_until": 0}
    try:
        doc = col_support_tickets.find_one(_support_lock_key(user_id))
        if doc:
            state = {
                "window_start": int(doc.get("window_start", now_ts)),
                "warnings":     int(doc.get("warnings", 0)),
                "lock_until":   int(doc.get("lock_until", 0)),
            }
    except Exception:
        # DB unavailable — fall back to in-memory cache
        state = _support_security_tracker.get(user_id, state)

    # Reset rolling window if it has expired
    if now_ts - state.get("window_start", now_ts) > _SUPPORT_SECURITY_WINDOW_SECS:
        state["window_start"] = now_ts
        state["warnings"] = 0

    # If already locked, return stable lock state
    active_lock = max(0, int(state.get("lock_until", 0)) - now_ts)
    if active_lock > 0:
        _support_security_tracker[user_id] = state
        return (int(state.get("warnings", 0)), active_lock, False)

    # Increment warning count
    warnings = int(state.get("warnings", 0)) + 1
    state["warnings"] = warnings
    lock_triggered = False

    if warnings >= _SUPPORT_SECURITY_MAX_WARNINGS:
        state["lock_until"] = now_ts + _SUPPORT_SECURITY_LOCK_SECS
        state["warnings"] = 0
        lock_triggered = True

    # Persist to MongoDB — this is what survives restarts and vault exits
    try:
        col_support_tickets.update_one(
            _support_lock_key(user_id),
            {"$set": {
                "type":         "security_lock",
                "user_id":      user_id,
                "window_start": state["window_start"],
                "warnings":     state["warnings"],
                "lock_until":   state["lock_until"],
                "updated_at":   now_ts,
            }},
            upsert=True,
        )
    except Exception:
        pass  # If DB write fails, in-memory state still works for this session

    # Sync in-memory cache
    _support_security_tracker[user_id] = state
    lock_remaining = max(0, int(state.get("lock_until", 0)) - now_ts)
    return (warnings, lock_remaining, lock_triggered)

def _build_support_security_notice(user_name: str, reason: str, warning_count: int, lock_remaining: int = 0) -> str:
    """Premium security warning/lock notice for support abuse detection."""
    if lock_remaining > 0:
        bar, pct = _build_remaining_bar(lock_remaining, _SUPPORT_SECURITY_LOCK_SECS)
        return (
            f"🛡️ **SUPPORT SECURITY LOCK ENABLED**\n\n"
            f"**{user_name}**, repeated unsafe/spam submissions were detected.\n\n"
            f"**Latest reason:** {reason}\n"
            f"⏳ **Lock remaining:** {_format_hms(lock_remaining)}\n"
            f"📉 **Lock cooldown bar:** `{bar}` ({pct:.1f}% remaining)\n\n"
            f"Please return with a clean, professional issue report after the lock expires."
        )

    return (
        f"⚠️ **SECURITY WARNING ({warning_count}/{_SUPPORT_SECURITY_MAX_WARNINGS})**\n\n"
        f"**{user_name}**, your submission violated support safety policy.\n"
        f"**Reason:** {reason}\n\n"
        f"Please send only relevant, professional support details.\n"
        f"Repeated violations can temporarily lock ticket submissions."
    )

def _detect_nsfw_caption_terms(text: str) -> list[str]:
    """Return matched NSFW terms from text/caption (for media protection)."""
    if not text:
        return []
    normalized = _normalize(text)
    nsfw_terms = {
        "nude", "nudes", "nudity", "naked", "porn", "porno", "pornography", "xxx",
        "sex", "sexual", "explicit", "blowjob", "handjob", "hentai", "onlyfans",
        "boobs", "breasts", "nipples", "dick", "cock", "pussy", "cum", "sexting",
    }
    found = []
    for term in nsfw_terms:
        if _normalize(term) in normalized:
            found.append(term)
    # Deduplicate in insertion order
    dedup = []
    seen = set()
    for term in found:
        if term not in seen:
            seen.add(term)
            dedup.append(term)
    return dedup

def check_ticket_rate_limit(user_id: int, user_name: str = "You") -> tuple[bool, str]:
    """
    Returns (allowed: bool, error_msg: str).
    DB-backed: queries the support_tickets collection for the user's last submission.
    This survives bot restarts and is accurate even across multiple instances.
    """
    now = now_local()
    cutoff = now - timedelta(hours=TICKET_COOLDOWN_HOURS)

    # Find the most recently submitted ticket by this user
    last_ticket = col_support_tickets.find_one(
        {"user_id": user_id},
        sort=[("created_at", -1)]
    )

    if last_ticket:
        last_at = last_ticket.get("created_at")
        if last_at and last_at > cutoff:
            remaining = timedelta(hours=TICKET_COOLDOWN_HOURS) - (now - last_at)
            remaining_secs = max(0, int(remaining.total_seconds()))
            unlock_dt = last_at + timedelta(hours=TICKET_COOLDOWN_HOURS)
            unlock_at = unlock_dt.strftime("%I:%M:%S %p")
            cooldown_total_secs = TICKET_COOLDOWN_HOURS * 3600
            bar, pct = _build_remaining_bar(remaining_secs, cooldown_total_secs)
            return (False,
                f"⏳ **COOLDOWN ACTIVE**\n\n"
                f"**{user_name}**, you already submitted a support ticket recently.\n\n"
                f"⏰ **Live time remaining:** {_format_hms(remaining_secs)}\n"
                f"🔓 **Unlocks at:** {unlock_at}\n\n"
                f"📉 **Live cooldown bar:** `{bar}` ({pct:.2f}% remaining)\n"
                f"🕒 _Server-time based and recalculated on every submission attempt._\n\n"
                f"_One ticket per {TICKET_COOLDOWN_HOURS} hours keeps our support queue manageable.\n"
                f"You will be able to submit a new ticket once this period ends._"
            )

    return (True, "")

async def _cooldown_refresh_runner(msg: types.Message, user_id: int, user_name: str, seconds: int):
    """Background runner for cooldown live refresh with low API call rate."""
    refresh_every = max(2, _COOLDOWN_REFRESH_INTERVAL_SECS)
    loops = max(1, seconds // refresh_every)

    for _ in range(loops):
        await asyncio.sleep(refresh_every)
        allowed, refreshed = check_ticket_rate_limit(user_id, user_name)
        if allowed:
            try:
                await msg.edit_text(
                    "✅ **COOLDOWN COMPLETE**\n\n"
                    "Your support cooldown has ended.\n"
                    "You can now submit a new ticket.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
            return
        try:
            await msg.edit_text(refreshed, parse_mode=ParseMode.MARKDOWN)
        except TelegramRetryAfter as e:
            await asyncio.sleep(max(1, int(e.retry_after)))
        except Exception:
            # Message may be unchanged/deleted; stop this runner quietly.
            return


async def _live_refresh_ticket_cooldown(msg: types.Message, user_id: int, user_name: str, seconds: int = 20):
    """Schedule one low-frequency live cooldown refresh task per user."""
    existing = _ticket_cooldown_live_tasks.get(user_id)
    if existing and not existing.done():
        return

    seconds = max(_COOLDOWN_REFRESH_INTERVAL_SECS, min(seconds, _COOLDOWN_REFRESH_MAX_SECS))
    task = asyncio.create_task(
        _cooldown_refresh_runner(msg, user_id, user_name, seconds),
        name=f"cooldown_live_{user_id}"
    )
    _ticket_cooldown_live_tasks[user_id] = task

    def _cleanup(_):
        cur = _ticket_cooldown_live_tasks.get(user_id)
        if cur is task:
            _ticket_cooldown_live_tasks.pop(user_id, None)

    task.add_done_callback(_cleanup)

def record_ticket_submission(user_id: int):
    """No-op — submission is recorded directly in the tickets collection (DB-backed)."""
    pass


# ---------------------------------------------------------------------------
# MASTER VALIDATION — called before every ticket submission
# ---------------------------------------------------------------------------
def validate_ticket_content(text: str, user_name: str = "User") -> tuple[bool, str]:
    """
    Full multi-layer ticket content validation.
    Order: length → profanity → spam/gibberish
    Returns (is_valid: bool, rejection_message: str)
    """
    # 1. Length checks
    if len(text) < MIN_TICKET_LENGTH:
        return (False,
            f"⚠️ **MESSAGE TOO SHORT**\n\n"
            f"{user_name}, please describe your issue in more detail.\n\n"
            f"• Minimum: **{MIN_TICKET_LENGTH} characters**\n"
            f"• Your message: **{len(text)} characters**\n\n"
            f"_Include what happened, when it happened, and what you need help with._"
        )

    if len(text) > MAX_TICKET_LENGTH:
        return (False,
            f"⚠️ **MESSAGE TOO LONG**\n\n"
            f"{user_name}, your message is too long.\n\n"
            f"• Maximum: **{MAX_TICKET_LENGTH} characters**\n"
            f"• Your message: **{len(text)} characters**\n\n"
            f"_Please shorten your message and focus on the key issue._"
        )

    # 2. Profanity / hate speech / threat check
    has_profanity, found_terms = contains_profanity(text)
    if has_profanity:
        # Censor found terms to not expose the full list in messages
        display = ", ".join([f"`{'*' * len(found_terms[i])}`" for i in range(min(3, len(found_terms)))])
        return (False,
            f"🚫 **INAPPROPRIATE CONTENT DETECTED**\n\n"
            f"{user_name}, your message was blocked by our content filter.\n\n"
            f"**Reason:** Offensive, hateful, or threatening language detected.\n\n"
            f"⚠️ Please rewrite your message respectfully.\n\n"
            f"_Repeated violations may result in your support access being restricted._"
        )

    # 3. Spam / gibberish / injection check
    is_spam, reason = is_spam_or_gibberish(text)
    if is_spam:
        return (False,
            f"🚫 **MESSAGE REJECTED**\n\n"
            f"{user_name}, your message was flagged as invalid.\n\n"
            f"**Reason:** {reason}\n\n"
            f"⚠️ Please write a clear, genuine description of your issue.\n\n"
            f"_Our system requires real, readable support messages._"
        )

    return (True, "")

def get_user_verification_status(user_id: int, tg_user=None) -> dict:
    """Get user verification status from database.
    If tg_user (Telegram user object) is provided, updates premium/language fields on every call.
    """
    user_data = col_user_verification.find_one({"user_id": user_id})
    if not user_data:
        # Capture Telegram-provided analytics fields (zero extra API calls needed)
        is_premium = bool(getattr(tg_user, "is_premium", False)) if tg_user else False
        language_code = getattr(tg_user, "language_code", None) if tg_user else None

        # Create new record for new user
        user_data = {
            "user_id": user_id,
            "vault_joined": False,
            "verified": False,
            "ever_verified": False,  # Track if user was EVER verified (for old user detection)
            "verification_msg_id": None,  # Store verification message ID for deletion
            "rejoin_msg_id": None,  # Store rejoin message ID for deletion when user rejoins
            "first_start": now_local(),
            # Telegram analytics (captured once on first start, free data)
            "is_premium": is_premium,       # Telegram Premium subscriber flag
            "language_code": language_code,  # e.g. "en", "hi", "ar"
            # Grace-pass fields: One free content delivery before vault join is mandatory
            "grace_allowed": True,  # All new users get one free pass
            "grace_consumed": False,  # Set to true after first successful delivery
            "grace_consumed_at": None,  # Timestamp when grace was consumed
            "grace_consumed_via": None,  # How grace was consumed: "IG", "YT", "IGCC", "YTCODE", "UNKNOWN_SEARCH"
            # Abandonment lifecycle fields: Track 30/60/90-day notifications
            "vault_left_at": None,  # When user left vault (trigger for 30/60/90-day countdown)
            "reminder1_sent": False,  # First reminder sent at day 30
            "reminder2_sent": False,  # Second reminder sent at day 60
            "reminder3_sent": False  # Third reminder sent at day 90 (before auto-delete)
        }
        col_user_verification.insert_one(user_data)
    elif tg_user:
        # Returning user — silently update language/premium in case they changed
        is_premium = bool(getattr(tg_user, "is_premium", False))
        language_code = getattr(tg_user, "language_code", None)
        col_user_verification.update_one(
            {"user_id": user_id},
            {"$set": {"is_premium": is_premium, "language_code": language_code, "last_seen": now_local()}}
        )
    return user_data

def update_verification_status(user_id: int, **kwargs):
    """Update user verification fields (prevents duplicates with upsert)"""
    col_user_verification.update_one(
        {"user_id": user_id},
        {"$set": kwargs},
        upsert=True  # Create if doesn't exist, update if exists
    )


async def send_psychological_vault_lock_message(user_id: int):
    try:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from aiogram.enums import ParseMode
        import asyncio
        await asyncio.sleep(1.5)
        msg = (
            f"⚡ **YOUR FIRST BLUEPRINT WAS JUST DELIVERED.**\n\n"
            f"That was a preview. What the full system holds is on another level entirely.\n\n"
            f"Join the Vault now — it's 100% free — and unlock everything:\n\n"
            f"📂 **All Blueprints**: Every premium PDF delivered instantly.\n"
            f"🤖 **Elite AI Tools**: Private automation scripts the public never sees.\n"
            f"💎 **Insider Strategies**: Reserved strictly for Vault members.\n"
            f"🔓 **Full Agent Menu**: Dashboard, search, and live updates unlocked.\n\n"
            f"This is the one action that separates access from opportunity.\n"
            f"Join free. Stay informed. Scale faster.\n\n"
            f"*The Vault is open. Your next move decides everything.*"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 JOIN MSA VAULT (Free)", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="📸 INSTAGRAM", url=INSTAGRAM_LINK),
             InlineKeyboardButton(text="▶️ YOUTUBE", url=YOUTUBE_LINK)]
        ])
        await bot.send_message(user_id, msg, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to send psycho lock msg: {e}")

# ==========================================
# 🛑 MAINTENANCE MODE CHECK


# ==========================================
_maintenance_cache: dict = {"value": None, "set_at": 0.0, "bot3_settings": None}
_MAINTENANCE_CACHE_TTL = 30  # seconds

async def check_maintenance_mode(message: types.Message) -> bool:
    """
    Check if maintenance mode is enabled.
    Returns True if maintenance is ON and user should be blocked.
    """
    try:
        # 1. Check if user is MASTER_ADMIN (Bypass)
        user_id = message.from_user.id
        if user_id == OWNER_ID:  # Owner can always access
            return False

        # 2. Use cached result if fresh
        now_ts = time.time()
        if _maintenance_cache["value"] is not None and (now_ts - _maintenance_cache["set_at"]) < _MAINTENANCE_CACHE_TTL:
            if not _maintenance_cache["value"]:
                return False
            settings = _maintenance_cache["bot3_settings"]
        else:
            # 3. Refresh from DB
            settings = col_bot1_settings.find_one({"setting": "maintenance_mode"})
            _maintenance_cache["value"] = bool(settings and settings.get("value", False))
            _maintenance_cache["set_at"] = now_ts
            _maintenance_cache["bot3_settings"] = settings
            if not _maintenance_cache["value"]:
                return False

        if settings and settings.get("value", False):
            # Maintenance is ON
            try:
                user_name = message.from_user.first_name or "Valued Member"
                maintenance_msg = settings.get("maintenance_message", "")
                eta = settings.get("eta", "")
                
                # Build premium maintenance message
                msg_lines = [
                    "🔴  **SYSTEM TEMPORARILY OFFLINE**\n",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n",
                    f"👤  **Dear {user_name},**\n\n",
                    "**MSA NODE AGENT** is currently paused for a scheduled maintenance or system upgrade. "
                    "All services are temporarily suspended so our team can deliver you a superior experience upon return.\n\n",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    "🔒  **CURRENT STATUS**\n\n",
                    "• 🔴  Bot features .............. Offline\n",
                    "• 🔴  Start links ............... Inactive\n",
                    "• 🔴  Support queue ............. On hold\n",
                    "• 🟢  Your data ................. Fully secure\n\n",
                ]

                if maintenance_msg:
                    msg_lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                    msg_lines.append(f"💬  **Message from Admin:**\n_{maintenance_msg}_\n\n")

                if eta:
                    msg_lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
                    msg_lines.append(f"⏳  **Estimated Return:** {eta}\n\n")
                else:
                    msg_lines.append(f"⏳  **Status:** We'll be back online very soon.\n\n")

                msg_lines += [
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n",
                    "We appreciate your patience, {user_name}. Our team is working rapidly to restore full service.\n\n",
                    "_You will be notified the moment the agent is back online._\n\n",
                    "_— MSA NODE Systems_",
                ]

                final_msg = "".join(msg_lines).replace("{user_name}", user_name)

                await message.answer(final_msg, parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
                logger.info(f"🚫 Maintenance Block: User {user_id} blocked.")
            except Exception as e:
                logger.error(f"Error sending maintenance message: {e}")
            return True
        
        return False
    except Exception as e:
        logger.error(f"Error checking maintenance mode: {e}")
        return False

# ==========================================
# 🛡️ ANTI-SPAM & UTILITY FUNCTIONS
# ==========================================
async def safe_delete_message(message: types.Message):
    """Safely delete a message without raising exceptions"""
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Could not delete message: {e}")
        pass

def is_user_processing(user_id: int) -> bool:
    """Check if user is currently processing a command"""
    return user_id in user_processing

def set_user_processing(user_id: int, command: str):
    """Mark user as processing a command"""
    user_processing[user_id] = command
    logger.debug(f"User {user_id} started processing: {command}")

def clear_user_processing(user_id: int):
    """Clear user's processing state"""
    if user_id in user_processing:
        command = user_processing.pop(user_id)
        logger.debug(f"User {user_id} finished processing: {command}")

def rate_limit(cooldown: float = COMMAND_COOLDOWN):
    """Decorator to enforce cooldown between commands (prevents Telegram FloodWait bans)"""
    def decorator(handler):
        @functools.wraps(handler)
        
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            now = time.time()
            last_time = user_last_command.get(user_id, 0)

            # If user is already frozen, block with throttled reminder.
            if await _guard_flood_from_wrapper(message, "rate_limit", record_tap=False):
                return
            
            # Check if user is within cooldown period
            time_since_last = now - last_time
            if time_since_last < cooldown:
                # Rejected rapid tap: feed anti-flood tracker (non-punitive, no ban)
                await _guard_flood_from_wrapper(message, "rate_limit", record_tap=True)
                return
            
            # Update last command time
            user_last_command[user_id] = now
            
            # Execute the handler
            return await handler(message, *args, **kwargs)
        
        return wrapper
    return decorator

def anti_spam(command_name: str):
    """Decorator to prevent command spam - blocks if user is already processing"""
    def decorator(handler):
        @functools.wraps(handler)
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id

            # If user is already frozen, block with throttled reminder.
            if await _guard_flood_from_wrapper(message, "anti_spam", record_tap=False):
                return
            
            # Check if user is already processing
            if is_user_processing(user_id):
                # Rejected parallel tap: feed anti-flood tracker (non-punitive, no ban)
                await _guard_flood_from_wrapper(message, "anti_spam", record_tap=True)
                return
            
            # Mark as processing
            set_user_processing(user_id, command_name)
            
            try:
                # Execute the actual handler
                await handler(message, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {command_name} for user {user_id}: {e}")
            finally:
                # Always clear processing state
                clear_user_processing(user_id)
        
        return wrapper
    return decorator

# ==========================================
# 🧊 FREEZE ENGINE FUNCTIONS
# ==========================================
def _record_spam_tap(user_id: int) -> tuple[bool, int]:
    """
    Record one tap for this user.
    Returns (triggered_new_freeze: bool, freeze_seconds: int).
    Lenient: needs _FREEZE_TRIGGER taps inside _FREEZE_WINDOW seconds.
    """
    now = time.time()
    state = _freeze_tracker.setdefault(user_id, {
        "offense": 0, "frozen_until": 0.0,
        "taps": 0, "window_start": now, "last_tap": now
    })

    # If already frozen just return
    if now < state["frozen_until"]:
        return False, 0

    # Decay offense count if user was clean for _FREEZE_DECAY seconds
    if now - state.get("last_tap", now) > _FREEZE_DECAY:
        state["offense"] = 0

    state["last_tap"] = now

    # Sliding window: reset tap counter when window expires
    if now - state["window_start"] > _FREEZE_WINDOW:
        state["taps"] = 1
        state["window_start"] = now
    else:
        state["taps"] += 1

    # Check if threshold crossed
    if state["taps"] >= _FREEZE_TRIGGER:
        level   = min(state["offense"], len(_FREEZE_LEVELS) - 1)
        secs    = _FREEZE_LEVELS[level]
        state["frozen_until"]  = now + secs
        state["offense"]       = min(state["offense"] + 1, len(_FREEZE_LEVELS))
        state["taps"]          = 0          # reset tap window after freeze
        state["window_start"]  = now
        return True, secs   # ← freeze was triggered

    return False, 0


async def _guard_flood_from_wrapper(message: types.Message, source: str, record_tap: bool = False) -> bool:
    """Decorator-level flood guard.
    Returns True when the request should be blocked.
    When record_tap=True, this rejected action is counted toward anti-flood freeze.
    """
    user_id = message.from_user.id
    now = time.time()

    # Already frozen: show throttled reminder
    state = _freeze_tracker.get(user_id, {})
    frozen_until = state.get("frozen_until", 0)
    if now < frozen_until:
        remaining = int(max(0, frozen_until - now))
        last_notice = _freeze_notice_tracker.get(user_id, 0)
        if now - last_notice >= 3:
            _freeze_notice_tracker[user_id] = now
            mins, secs = divmod(remaining, 60)
            time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
            unfreeze_dt = datetime.now(TZ) + timedelta(seconds=remaining)
            unfreeze_str = unfreeze_dt.strftime("%I:%M:%S %p")
            try:
                await message.answer(
                    f"🛡️ <b>FLOOD PROTECTION ACTIVE</b>\n\n"
                    f"Too many rapid taps were detected.\n"
                    f"⏳ <b>Cooldown:</b> {time_str} (until {unfreeze_str})\n\n"
                    f"<i>Please do not spam buttons.\n"
                    f"If your internet is lagging, wait a few seconds and try once.</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return True

    # Non-rejected path: no tap should be counted.
    if not record_tap:
        return False

    # Rejected action path: record one tap and trigger fixed 60s freeze on rapid abuse
    triggered, freeze_secs = _record_spam_tap(user_id)
    if not triggered:
        return True

    state = _freeze_tracker.get(user_id, {})
    offense = state.get("offense", 1)
    logger.warning(f"🧊 FLOOD LOCK: User {user_id} frozen for {freeze_secs}s via {source} (offense #{offense})")
    _freeze_notice_tracker[user_id] = now

    mins, secs = divmod(freeze_secs, 60)
    time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
    unfreeze_dt = datetime.now(TZ) + timedelta(seconds=freeze_secs)
    unfreeze_str = unfreeze_dt.strftime("%I:%M:%S %p")
    try:
        await message.answer(
            f"🧊 <b>ANTI-FLOOD TIMER STARTED</b>\n\n"
            f"We detected rapid button spam and paused input for safety or pleaase check your internet connection is strong to avoid these kind of issues.\n"
            f"⏳ <b>Cooldown:</b> {time_str} (until {unfreeze_str})\n\n"
            f"<i>No ban was applied.\n"
            f"Please send one action at a time to avoid Telegram policy/flood issues.</i>",
            parse_mode="HTML"
        )
    except Exception:
        pass
    return True


async def _check_freeze(message: types.Message) -> bool:
    """
    Call at the top of every user handler.
    Records the tap.  If the user is currently frozen → sends a 12h-format
    warning and returns True (handler should return immediately).
    If a new freeze is triggered → also sends warning + notifies owner.
    Returns False when the user is clear to proceed.
    """
    user_id  = message.from_user.id
    now      = time.time()

    # Check if already frozen (without recording a new tap)
    state = _freeze_tracker.get(user_id, {})
    if now < state.get("frozen_until", 0):
        remaining = int(state["frozen_until"] - now)
        mins, secs = divmod(remaining, 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        # Calculate unfreeze clock in 12h format
        unfreeze_dt = datetime.now(TZ) + timedelta(seconds=remaining)
        unfreeze_str = unfreeze_dt.strftime("%I:%M %p")
        offense = state.get("offense", 1)
        level_label = ["1st", "2nd", "3rd", "4th"][min(offense - 1, 3)]
        try:
            await message.answer(
                f"🧊 <b>You are temporarily frozen.</b>\n\n"
                f"Rapid button presses detected — please slow down.\n\n"
                f"⏳ <b>Unfreeze in:</b> {time_str}  (at {unfreeze_str})\n"
                f"⚠️ <b>Offense level:</b> {level_label}\n\n"
                f"<i>All features are paused during freeze.\n"
                f"Internet lag? No worries — freeze times reset after 10 min of normal use.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return True  # ← caller should return

    # Record the tap and check if this triggers a new freeze
    triggered, freeze_secs = _record_spam_tap(user_id)
    if triggered:
        state  = _freeze_tracker[user_id]
        offense = state.get("offense", 1)
        level_label = ["1st", "2nd", "3rd", "4th"][min(offense - 1, 3)]
        mins, secs = divmod(freeze_secs, 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        unfreeze_dt = datetime.now(TZ) + timedelta(seconds=freeze_secs)
        unfreeze_str = unfreeze_dt.strftime("%I:%M %p")
        logger.warning(f"🧊 FREEZE: User {user_id} frozen for {freeze_secs}s (offense #{offense})")
        try:
            await message.answer(
                f"🧊 <b>Auto-Freeze Activated!</b>\n\n"
                f"Too many rapid button presses detected.\n\n"
                f"⏳ <b>Frozen for:</b> {time_str}  (until {unfreeze_str})\n"
                f"⚠️ <b>Offense level:</b> {level_label} — repeated spam will restart the timer.\n\n"
                f"<i>All features are paused during freeze.\n"
                f"Slow internet? No worry — 5+ taps in 4s needed to trigger. "
                f"After 10 min of normal use the count resets completely.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        # Notify owner on 3rd+ offense
        if offense >= 3:
            try:
                user_mention = f"@{message.from_user.username}" if message.from_user.username else f"ID {user_id}"
                await bot.send_message(
                    OWNER_ID,
                    f"🧊 <b>REPEAT SPAMMER — {level_label} OFFENSE</b>\n\n"
                    f"User: {user_mention} (ID: <code>{user_id}</code>)\n"
                    f"Frozen for: {time_str}\n"
                    f"Total offenses: {offense}\n\n"
                    f"<i>Not banned — temporary 60s anti-flood lock only.</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return True  # ← caller should return

    return False  # ← user is clean, proceed

# ==========================================
# 📦 CONTENT PACKS (20 Items Each)
# ==========================================
CONTENT_PACKS: dict[str, list] = {
    "IGCCC_CODES": [
        "👁️ **THE GLIMPSE:** You just saw a piece of it. The full picture is much bigger. More free content is waiting — grab your YT MSA CODE and get all of it.",
        "⚡ **CHARGED:** That post was the spark. The real fire is on YouTube. Watch the video, get the MSA CODE, and access everything we promised.",
        "🧩 **ONE PIECE:** You have one part of the puzzle. The rest is on YouTube. Get the MSA CODE and complete what you started.",
        "🌊 **GO DEEPER:** You just touched the surface. The real value is inside. Watch the YouTube video, get the MSA CODE, and dive in.",
        "🗝️ **THE KEY:** The door is right in front of you. The key is the MSA CODE on YouTube. Watch the video and step inside.",
        "🚀 **READY TO LAUNCH:** You have the fuel. Now start the engine. Get the MSA CODE from YouTube and unlock what's next.",
        "💎 **HIDDEN VALUE:** The best content is not out in the open. It's reserved for members. Watch the video, get the MSA CODE, and claim your spot.",
        "📡 **FIND THE SOURCE:** You caught the signal. Now follow it to the source. The MSA CODE is in the YouTube video. Go get it.",
        "🧠 **MORE THAN DATA:** That post gave you information. YouTube gives you the full strategy. Get the MSA CODE and level up for free.",
        "🔒 **AT THE GATE:** You are standing at the entrance. The MSA CODE from YouTube is your pass. Watch the video and walk in.",
        "🌐 **JOIN THE NETWORK:** One post connects you to the community. The MSA CODE connects you to everything. It's free — get it on YouTube.",
        "💼 **OWN THE VAULT:** You saw the preview. Now own the full asset. The MSA CODE is your key — find it in the YouTube video.",
        "🧬 **THE REAL THING:** That was just a sample. Come see the full version. Watch YouTube, grab the MSA CODE, and access the rest for free.",
        "🔌 **POWER UP:** You are running on the free clips. The full power is on YouTube. Get the MSA CODE and plug into the main grid.",
        "🐺 **THE PACK IS INSIDE:** You saw the teaser. The community is on the other side. Get the MSA CODE on YouTube and join us.",
        "🦅 **FLY HIGHER:** You are at ground level right now. There is much more above. Watch the YouTube video and get the MSA CODE to rise up.",
        "⚔️ **BETTER TOOLS WAIT:** What you saw is just the start. The real strategy is inside. Get the MSA CODE from YouTube and come prepared.",
        "🛡️ **GET PROTECTED:** Free content keeps you exposed. The full system protects you. Watch the video, get the MSA CODE, and get covered.",
        "🩸 **DEEPLY COMMITTED:** You clicked, you watched, you stayed. That matters. Now go all in — get the MSA CODE from YouTube and lock in your access.",
        "🌪️ **MORE IS COMING:** That was just the beginning. The best content is still ahead. Get your MSA CODE on YouTube and be ready when it drops."
    ],
    "PDF_TITLES": [
        "📫 **DELIVERED:** Your resource is here, {name}. This is a clear, step-by-step guide. Open it, follow the steps, and take action.",
        "🗺️ **YOUR ROADMAP:** You have the full plan, {name}. It is straightforward. Follow it from start to finish and reach your goal.",
        "✅ **ALL YOURS:** Your download is ready, {name}. No filler, no fluff — just practical steps you can use today.",
        "📘 **THE BLUEPRINT:** This is not theory, {name}. It is a real plan built for real results. Read it and start building.",
        "⚡ **FAST TRACK:** This guide was made for speed, {name}. You can read it in under 10 minutes and put it to work the same day.",
        "🗝️ **THE ANSWER:** You have been looking for this, {name}. Everything you need is inside. Open it and see for yourself.",
        "🧠 **CLEAR DIRECTION:** Too much information creates confusion, {name}. This PDF cuts out the noise and gives you one clear path forward.",
        "🏗️ **SOLID START:** Every strong result starts with a strong foundation, {name}. This PDF gives you exactly that. Build from here.",
        "🛡️ **STAY AHEAD:** Others are guessing and losing time, {name}. This guide keeps you informed and moving in the right direction.",
        "🧭 **YOUR GUIDE:** Not sure where to start? This PDF answers that, {name}. It points you in the right direction from page one.",
        "🩸 **YOUR COMMITMENT:** You took the first step, {name}. This PDF is your next move. Do the work and get the result you want.",
        "🔋 **READY TO USE:** This is not just a document, {name}. It is a working system. Put it into action and watch things move.",
        "🕶️ **SEE THE FULL PICTURE:** You already saw the opportunity, {name}. This PDF shows you how to act on it the right way.",
        "🧬 **THE FORMULA:** There is a repeatable formula behind every good result, {name}. You just received it. Now apply it.",
        "🎓 **REAL KNOWLEDGE:** Formal education teaches you to memorize, {name}. This guide teaches you to build. Read and apply.",
        "💼 **PROFESSIONAL LEVEL:** This is the kind of resource that serious people use, {name}. Treat it that way and get serious results.",
        "🚦 **YOUR GREEN LIGHT:** Everything is set, {name}. The guide is simple and direct. There is nothing stopping you now. Start.",
        "🧩 **THE COMPLETE PICTURE:** The information you needed is all in one place, {name}. Read it and see how everything connects.",
        "💎 **REAL VALUE:** You earned this, {name}. Do not let it sit unopened. The value is there — but only if you use it.",
        "🚀 **TIME TO MOVE:** You have the plan, {name}. Everything is ready. The only thing left is to take action. Go."
    ],
    "PDF_BUTTONS": [
        "📂 OPEN YOUR GUIDE", "🔓 ACCESS THE GUIDE", "🎒 GET YOUR FILE",
        "📦 DOWNLOAD NOW", "🗝️ OPEN THE GUIDE", "👓 VIEW THE GUIDE",
        "🤝 GET YOUR GUIDE", "✊ TAKE GUIDE NOW",
        "🔦 SEE INSIDE GUIDE", "💵 CLAIM THE GUIDE", "📥 DOWNLOAD FREE GUIDE", "💼 OPEN THE FILE", 
        "🔐 UNLOCK THE GUIDE", "🎣 GRAB YOUR GUIDE", "💿 SAVE YOUR GUIDE",
        "🗄️ ACCESS THE GUIDE", "🚪 OPEN AND READ", "🔬 VIEW GUIDE NOW", "🕯️ FOLLOW THE GUIDE",
        "🗡️ USE THIS NOW", "🩸 COMMIT DOWNLOAD",
        "💠 CLAIM YOUR GUIDE"
    ],
    "PDF_FOOTERS": [
        "⚠️ This guide was sent directly to you, {name}.", "🔒 Your access is confirmed, {name}. Use it wisely.", "⏱️ The right time to start is right now.",
        "🕶️ No noise. No guessing. Just the plan.", "🩸 You committed. Here is what you asked for, {name}.", "🧾 Delivery confirmed. Your guide is ready to read.",
        "🛡️ You are covered, {name}. The guide has everything you need.", "🧬 This was built for people like you, {name}.", "🔋 Energy is full. Now put it to work, {name}.",
        "🧊 Stay calm and focused. The answers are inside.", "🐺 You made the move, {name}. Finish what you started.", "🦅 High performers act fast. The guide is open — move.",
        "🗝️ Full access granted, {name}. Everything is inside.", "🚫 No shortcuts here. Just real steps that work.", "🧠 Sharp minds read first and act second. Your turn, {name}.",
        "🏹 The target is clear. The guide shows you how to hit it.", "⚖️ You decided to show up. Now decide to follow through, {name}.", "🌪️ Everything you need is here. Clear path ahead.",
        "🧱 The wall is gone. The guide shows you what is on the other side.", "🔦 You are in the right place, {name}. Keep reading.", "💊 Real solutions. No hype. Just results.",
        "💉 The truth is inside, {name}. Read every page.", "🧩 It all comes together in this guide, {name}.", "🏆 This is the level you wanted to reach. Start here, {name}.",
        "🎫 Your spot is confirmed. Make it count, {name}.", "🎬 Your first move starts with this guide, {name}.", "🎤 You have the floor, {name}. Use this to make your move.",
        "🥊 You showed up ready, {name}. Now read and execute.", "🚦 Everything is green. Start reading and keep going.", "🏁 You are already ahead of most people, {name}. Finish the job."
    ],
    "AFFILIATE_TITLES": [
        "🤖 **THE WORKFLOW:** I previously paid $1,500/month for a VA. Now this $29/month AI does the exact same tasks better. That’s a $17,000/year raise. Click to secure yours.",
        "💸 **ROI ALERT:** This is an investment, not an expense. If a $40 tool brings in one $50 sale, the rest is pure profit. Don’t cut corners on your growth.",
        "🚀 **SPEED:** Corporate competitors have meetings; you have this AI. While they talk, you build. Get the tool and execute faster.",
        "💰 **ASSET BUILDING:** Stop trading time for gigs. Start building automated assets that pay you 24/7. This is the engine behind those assets.",
        "🧬 **CLONE YOURSELF:** Your time is limited. This AI clones your voice, ideas, and output at scale. It’s the smartest way to duplicate your effort.",
        "📈 **COMPOUND RESULTS:** One post does nothing; consistent output changes your life. This tool guarantees you hit your volume without burning out. Start compounding.",
        "🏦 **THE MATH:** If a $20 tool saves you 20 hours, it’s paying you to use it. Ignoring automation is literally costing you money. Run the numbers.",
        "💎 **HIDDEN GEM:** Most tools just wrap ChatGPT. This is a full-stack automation system that executes real tasks. I only share what works. This works.",
        "🧾 **EXPENSE IT:** If you own a business, write this off. If you don’t, this is how you build one. It costs less than dinner. Take the step.",
        "🏗️ **FOUNDATION:** You wouldn't skip the foundation on a house. Don't skip the automation foundation on your business. This software is the concrete.",
        "🧠 **PSYCHOLOGY:** Consistency builds trust. But humans are inconsistent by nature. This AI solves that flaw. Automate your consistency.",
        "⚡ **FRICTION:** Creating from scratch causes friction. This tool removes the friction entirely. One click, one result. Clear the barrier.",
        "🕵️ **SECRET ADVANTAGE:** Top creators don't work 100x harder; they use better leverage. This system is leverage. Pull the lever.",
        "📝 **WRITING HACK:** I stopped writing emails manually. I trained this AI on my voice, and now it sends emails natively. My open rates increased. Try it out.",
        "🎨 **NO SKILL NEEDED:** You don't need technical skills or design experience. You just need to be smart enough to apply the tool. It bridges the gap.",
        "🧹 **AUTOMATE THE BORING:** Life is too short for data entry and manual formatting. Let the software handle operations while you focus on strategy.",
        "🚿 **PASSIVE INCOME:** 'Passive income' requires active systems. Doing manual work is active income. Build an active system with this system today.",
        "⚙️ **SYSTEM:** You rise to the level of your systems. A system of 'doing it later' fails. A system of automation wins. Upgrade yours.",
        "📅 **CONSISTENCY:** Motivation gets you started, but automated habits keep you going. This tool runs even when you take the day off. Secure your consistency.",
        "📂 **DIGITAL REAL ESTATE:** Every post is a digital asset. This tool builds your real estate portfolio 24/7. Build while you sleep.",
        "😨 **THE WARNING:** People wait until the opportunity is crowded. This tool gives you an edge *right now*. Act before the window closes.",
        "🦖 **DINOSAUR:** Running a fully manual business in 5 years will be obsolete. Adapt with AI now, or get left behind. Make the shift.",
        "📉 **INFLATION:** Costs are rising. Manual hourly work can't scale fast enough to beat inflation. You need scalable systems to protect your margin.",
        "🚫 **DON'T GET LEFT BEHIND:** Half the people reading this will take action; half won't. The action-takers will win the market. Decide which side you are on.",
        "⚠️ **PRICE HIKE:** Good software raises prices as it grows. Lock in your legacy rate right now before the monthly cost doubles. Secure the price.",
        "🛑 **STOP SCROLLING:** You just spent 20 minutes consuming for $0. Spend the next 20 minutes setting up this tool to build an asset instead. Switch gears.",
        "⏳ **TIME IS MONEY:** Any manual task you do is time you just priced at zero. Stop giving away your margin. Automate the routine.",
        "🌪️ **THE WAVE:** AI is a tidal wave. You either ride it or get swept away. This system is your board. Get on.",
        "👋 **FIRE YOUR BOSS:** You can't replace a full-time salary with another full-time grind. You need leverage. Leverage starts here.",
        "🤜 **PUNCH BACK:** Build a revenue stream that isn't tied to your hourly wage. This system gives you the leverage to punch back at the economy.",
        "🧪 **TESTED BY ME:** I don't push junk. I rely on this tool daily. If it breaks, I lose revenue. It hasn't broken. That is the highest endorsement.",
        "📊 **RESULTS:** Someone set this up in 20 minutes last week and generated their first automated result today. It works fast. Test it yourself.",
        "👨🔬 **THE LAB:** I spend thousands validating software so you don't have to guess. This is the top performer in its class. Skip the testing phase.",
        "🔬 **VETTED:** Everything I share is fully vetted. This tool passed every standard. Trust the process and implement it, {name}."
    ],
    "AFFILIATE_FOOTERS": [
        "Take action now, {name}. Hesitation pays zero.",
        "Every day without systems is a day of lost leverage, {name}.",
        "This is the exact resource you've been looking for, {name}.",
        "Execute based on facts, not fear, {name}.",
        "You can deploy this, or watch someone else win with it, {name}.",
        "The timeline is moving. Claim your spot now, {name}.",
        "Delaying a good decision is a bad decision, {name}.",
        "Results require action. Make the move.",
        "This is your unfair advantage, {name}. Apply it.",
        "Success follows proven systems. This is the system.",
        "The value is obvious, {name}. The next step is yours.",
        "Treat this as a core investment, {name}. It pays off.",
        "Your competitors are scaling. Catch up, {name}.",
        "The market rewards speed. Move fast, {name}.",
        "Commitment drives results. Lock it in, {name}.",
        "The ROI starts the moment you implement this, {name}.",
        "Stop analyzing. Start executing.",
        "The window is open. Walk through, {name}.",
        "You need this infrastructure. Bring it online.",
        "Zero excuses left. Click to start."
    ],
    "AFFILIATE_TITLES_EXTRA": [
        "✅ **VERIFIED:** Avoid knock-off tools and scams. This is the verified, official suite I actually use. Secure your access through this link.",
        "📜 **MY STACK:** If you want to know my tech stack, this is the core. Without this infrastructure, the system stops. It is mandatory.",
        "👨🏫 **LESSON:** You can either buy time or sell it. A $29 software buys you 100 hours of leverage. That is the highest-return trade available.",
        "🏆 **WINNER:** Decisiveness separates operators from observers. Lock in the tool and become an operator. Make the decision.",
        "🥇 **TOP TIER:** Free tools cost you manual labor. Premium tools save you labor. Upgrade to the tier that creates leverage.",
        "🤝 **TRUST ME:** My reputation relies on sharing what actually works. I stake my performance on this software. It is legitimate.",
        "🗣️ **FINAL WORD:** You can continue grinding manually, but there is a smarter way. If you are ready to scale efficiently, click the button."
    ],
    "AFFILIATE_BUTTONS": [
        "🚀 ACTIVATE ACCOUNT NOW",
        "⚡ START YOUR ENGINE",
        "🏗️ BEGIN INSTALLATION",
        "🔧 CONFIGURE THE SYSTEM",
        "🔌 PLUG INTO THE TOOL",
        "💻 START THE BUILD",
        "✅ COMPLETE SETUP",
        "🎯 EXECUTE SETUP",
                
        "🔓 UNLOCK AND INSTALL",
        "🔌 CONNECT YOUR TOOL",
        "⚙️ DEPLOY FULL SYSTEM",
        "🛠️ ASSEMBLE YOUR STACK",
        "🚀 INITIATE STARTUP",
        "⚡ QUICK INSTALLATION",
        "🏗️ SET UP FOUNDATION",
        "🔧 BEGIN INTEGRATION",
        "✅ I AM READY TO SETUP"
    ],
    "YT_VIDEO_TITLES": [
        "👁️ **THE SOURCE:** You saw the short clips on Instagram, {name}. The full strategy is on the Main Channel. Go explore it.",
        "📡 **MAIN FREQUENCY:** Instagram is for the highlights, {name}. YouTube is the full broadcast. Tune into the Main Channel.",
        "🧠 **THE ARCHIVE:** You are only scratching the surface on Instagram, {name}. The YouTube Channel holds the full archive. Go deep.",
        "🏗️ **HEADQUARTERS:** Instagram is the outpost, {name}. YouTube is Headquarters. Report there for the full briefing.",
        "🌊 **DEEP DIVE:** Instagram is the shallow end, {name}. YouTube is where the real depth is. Dive into the Main Channel.",
        "📚 **THE LIBRARY:** You read the headlines on Instagram, {name}. Now read the book on YouTube. The Channel holds the blueprint.",
        "⚡ **FULL POWER:** Instagram is a preview, {name}. YouTube is the full system. Switch to the Main Channel for everything.",
        "🔥 **UNCENSORED:** The short-form content is limited, {name}. The full, uncensored strategies are on the YouTube Channel. Watch now.",
        "🔐 **THE VAULT:** The quick tips are on Instagram, {name}. The real assets are in the YouTube vault. Enter the Main Channel.",
        "🧬 **ORIGIN STORY:** You know the brand from the feed, {name}. Learn the philosophy behind it on YouTube. Watch the Main Channel.",
        "🕸️ **THE NETWORK:** Instagram is the starting point, {name}. YouTube is the center. Come to the core of the network.",
        "🎓 **HIGHER LEARNING:** Instagram is for attention, {name}. YouTube is for education. Class is in session on the Main Channel.",
        "🛫 **LAUNCHPAD:** You are preparing on Instagram, {name}. Take off on YouTube. The Main Channel is the runway.",
        "🔭 **BIGGER PICTURE:** Expand your perspective, {name}. Instagram is a keyhole. YouTube is the open door. Walk through.",
        "🗺️ **EXPEDITION:** The introduction is on Insta, {name}. The actual journey happens on YouTube. Join the Channel.",
        "🥊 **HEAVYWEIGHT:** Instagram is practice, {name}. YouTube is the main event. Step into the arena on the Main Channel.",
        "🎹 **FULL SYMPHONY:** You heard the notes on Instagram, {name}. Hear the full composition on YouTube. Listen to the Main Channel.",
        "🍳 **THE KITCHEN:** You saw the result on Instagram, {name}. See how we built it on YouTube. Watch the process.",
        "🏎️ **FULL THROTTLE:** You are cruising on Insta, {name}. It's time to accelerate on YouTube. Hit the gas on the Main Channel.",
        "🌎 **THE UNIVERSE:** You are orbiting on Instagram, {name}. Land and explore the full ecosystem on YouTube."
    ],
    "YT_CODES_BUTTONS": [
        "📺 ACCESS THE CHANNEL",
        "📺 VISIT THE HUB",
        "📺 OPEN THE ARCHIVE",
        "📺 ENTER THE VAULT",
        "📺 JOIN THE PLATFORM",
        "📺 VIEW FULL STRATEGY",
        "📺 GO TO THE STRATEGY",
        "📺 UNLOCK THE CHANNEL",
        "📺 WATCH THE INTEL",
        "📺 OPEN MAIN CHANNEL"
    ],
    "IG_VIDEO_TITLES": [
        "➕ **GET MORE:** If you found value here, {name}, there is much more on Instagram. Experience the daily operations.",
        "🤝 **CONNECT:** You consumed the content, {name}. Now connect directly. Follow the operation on Instagram.",
        "🏠 **THE HOUSE:** YouTube provides the framework, {name}. Instagram shows the daily execution. Come inside.",
        "🔥 **THE ENERGY:** YouTube is the strategy, {name}. Instagram is the execution and energy. Experience both.",
        "🧬 **FULL CIRCLE:** You learned the lesson, {name}. Now see it applied in real time on Instagram. Complete the loop.",
        "🫂 **THE COMMUNITY:** YouTube is public, {name}. Instagram is where the community operates closely. Join us.",
        "📸 **UNFILTERED:** YouTube has high production, {name}. Instagram is raw and real-time. See the actual day-to-day.",
        "🧠 **INSIDE MY HEAD:** Receive daily insights and raw thoughts on Instagram, {name}. Understand the mindset.",
        "❤️ **PASSION:** The videos show the work, {name}. Instagram shows the relentless drive behind it. Follow along.",
        "🆙 **LEVEL UP:** You want more frequent updates? I post the daily mechanics on Instagram, {name}. Level up.",
        "🎁 **BONUS:** The YouTube video was the foundation, {name}. The daily updates and bonuses happen on Instagram.",
        "🗣️ **CONVERSATION:** YouTube is a broadcast, {name}. Instagram is a dialogue. Join the conversation there.",
        "👀 **CLOSER LOOK:** Want to see the behind-the-scenes, {name}? Instagram zooms in on the operations. Watch closely.",
        "🛡️ **MY NETWORK:** See the environment I operate in on Instagram, {name}. Your network is your net worth.",
        "💎 **MORE GEMS:** Daily tactical drops happen exclusively on Instagram, {name}. Don't miss the real-time value.",
        "🚀 **THE RIDE:** Follow the actual journey as it happens, {name}. The documentation lives on Instagram.",
        "🚪 **BACKSTAGE:** You saw the final product on YouTube, {name}. Come backstage on Instagram to see the build.",
        "🔌 **PLUG IN:** YouTube is the long-form asset, {name}. Instagram is the daily pulse. Plug into the feed.",
        "🌊 **IMMERSE:** Don't just watch passively, {name}. Immerse yourself in the daily reality. Follow the Instagram.",
        "🔑 **ACCESS GRANTED:** Experience the unedited daily life of the operation, {name}. Accept the invite on Instagram."
    ],
    "IG_CODES_BUTTONS": [
        "📸 SEE THE REALITY",
        "📸 JOIN THE INSIDERS",
        "📸 WATCH IT HAPPEN",
        "📸 SEE DAILY PIPELINE",
        "📸 VERIFY THE WORK",
        "📸 CHECK THE PROCESS",
        "📸 FOLLOW THE FOUNDER",
        "📸 VIEW THE LIFESTYLE",
        "📸 ACCESS REAL TIME",
        "📸 ENTER THE DAILY LAB"
    ],
    "IG_VIDEO_FOOTERS": [
        "Take the next step, {name}. See the daily action.",
        "This is where the real-time execution happens.",
        "The operation is fully transparent, {name}. Look inside.",
        "Stop passively watching. Start engaging.",
        "Missing this means missing the daily mechanics, {name}.",
        "The daily network feed is waiting for you.",
        "One click gives you the behind-the-scenes view, {name}.",
        "Follow the reality. Apply the lessons.",
        "You are already invested. Lock in the daily updates.",
        "This is pure access, {name}. Take advantage of it.",
        "Observing the daily process is how you learn.",
        "The daily proof is published on the feed, {name}.",
        "You have the blueprint. Now watch the execution, {name}.",
        "Instagram is where the daily work is shown, {name}.",
        "Stay close to the source to maintain your momentum.",
        "You made it this far, {name}. Enter the daily circle.",
        "Success requires daily immersion. This is your immersion.",
        "This is the missing daily link you needed, {name}.",
        "Stay plugged into the daily frequency.",
        "If you are serious about scale, you will watch the daily execution."
    ],
    "MSACODE": [
        "🔍 **THE SOURCE:** {name}, YouTube delivers the **MSA CODES**. Instagram provides the **STRATEGY**. You need both to execute.",
        "🗝️ **KEYS & MAPS:** The Keys (**MSA CODES**) are in the YouTube briefings, {name}. The execution Map is on Instagram. Connect them.",
        "💎 **DOUBLE THREAT:** {name}, track YouTube for the **MSA CODES** and follow Instagram for the **DAILY OPS**. Master the full system.",
        "📡 **SIGNAL:** YouTube broadcasts the **MSA CODES**, {name}. Instagram broadcasts the **CULTURE**. Stay tuned to both.",
        "🛑 **MISSING DATA:** {name}, the PDF is only part of the equation. YouTube holds the **MSA CODES**. Instagram holds the context.",
        "🐺 **HUNTING GROUNDS:** We deploy **MSA CODES** in YouTube videos, {name}. We display the **RESULTS** on Instagram. Follow both.",
        "👁️ **ALWAYS WATCHING:** Did you catch the **MSA CODE** in the last video, {name}? YouTube has the code. Instagram shows the application.",
        "⚡ **POWER SUPPLY:** {name}, YouTube is the **ENGINE** (MSA CODES). Instagram is the **FUEL** (Execution). You require both.",
        "🧠 **FULL ACCESS:** To access more **MSA CODES**, {name}, go to YouTube. To access the network, go to Instagram.",
        "📦 **THE DROP:** The Asset is delivered, {name}. Find the next **MSA CODE** on YouTube. See the **DEPLOYMENT** on Instagram.",
        "🔐 **TWO KEYS:** Full access requires two components, {name}. The **MSA CODE** is on YouTube. The daily insight is on Instagram.",
        "🌐 **THE SYSTEM:** The System distributes **MSA CODES** via YouTube and **UPDATES** via Instagram. Stay in the loop, {name}.",
        "🧬 **DNA:** The structure of this operation, {name}: **MSA CODES** unlock the doors, and Instagram shows you the room.",
        "🕵️ **CLUES:** {name}, the latest **MSA CODE** is embedded in the YouTube video. The context is waiting on Instagram.",
        "🏆 **THE PRIZE:** The next level is locked, {name}. YouTube provides the **MSA CODE**. Instagram provides the roadmap.",
        "🔌 **DISCONNECTED:** Without YouTube, you miss the **MSA CODES**. Without Instagram, you miss the **MOMENTUM**. Reconnect now, {name}.",
        "📢 **BRIEFING:** The core briefing is on YouTube (find the **MSA CODE**), {name}. The daily debrief is on Instagram.",
        "⏳ **COUNTDOWN:** The next **MSA CODE** drops on YouTube soon, {name}. Watch Instagram for the notification. Be prepared.",
        "🤝 **THE DEAL:** Watch YouTube for the **MSA CODES**, {name}. Follow Instagram for the **COMMUNITY**. That is the framework.",
        "🚪 **DUAL ENTRY:** One door opens with an **MSA CODE** (YouTube). The daily room opens with presence (Instagram). Enter both, {name}.",
        "🔦 **SEARCH PARTY:** {name}, the search is active. **MSA CODES** are on YouTube. Daily insights are on Instagram.",
        "💼 **THE BRIEFCASE:** The asset is locked, {name}. The combination is the **MSA CODE** (YouTube). The location is Instagram.",
        "🚁 **EXTRACTION:** The extraction point is set, {name}. Your ticket is the **MSA CODE** (YouTube). The route is on Instagram.",
        "📡 **FREQUENCY:** {name}, ensure you are on the right frequencies. YouTube for **MSA CODES**. Instagram for **DAILY UPDATES**.",
        "🧱 **THE WALL:** Hit a wall, {name}? Break through with an **MSA CODE** from YouTube. Build momentum on Instagram.",
        "💊 **RED PILL:** The truth is the **MSA CODE** (YouTube). The reality is built on Instagram. Wake up and execute, {name}.",
        "🕰️ **TIK TOK:** The clock is running, {name}. Secure the **MSA CODE** from YouTube before the window closes. Updates on Instagram.",
        "🗺️ **COMPASS:** If you are stalled, {name}, YouTube is your direction (**MSA CODES**). Instagram is your terrain.",
        "⚖️ **JUDGMENT:** Your progress is tracked, {name}. The proof: **MSA CODES** (YouTube). The result: Instagram.",
        "🌪️ **CHAOS:** Bring order to the chaos, {name}. Structure comes from **MSA CODES** (YouTube). Execution comes from Instagram.",
        "🔑 **MASTER KEY:** The master key exists, {name}. It's an **MSA CODE** on YouTube. The community is on Instagram.",
        "👁️‍🗨️ **VISION:** Gain clarity, {name}. Locate the **MSA CODE** on YouTube. See the daily operation on Instagram.",
        "🩸 **BLOODLINE:** It's in the system, {name}. **MSA CODES** (YouTube) secure access. The Network (Instagram) provides leverage.",
        "🛡️ **SHIELD:** Protect your progress, {name}. Gain access with **MSA CODES** (YouTube). Maintain ground on Instagram.",
        "⚔️ **SWORD:** Take the offensive, {name}. Your tool: **MSA CODE** (YouTube). Your arena: Instagram.",
        "👑 **CROWN:** Build your authority, {name}. Secure it with **MSA CODES** (YouTube). Demonstrate it on Instagram.",
        "🦁 **ROAR:** Make your move, {name}. Unlock the next phase with an **MSA CODE** (YouTube). Lead your market on Instagram.",
        "🦅 **ALTITUDE:** Gain a new perspective, {name}. Lift off with **MSA CODES** (YouTube). Navigate the airspace via Instagram.",
        "🌑 **ECLIPSE:** Outperform the rest, {name}. The spark is the **MSA CODE** (YouTube). The sustained energy is Instagram.",
        "🚀 **IGNITION:** Prepare for launch, {name}. The ignition sequence is the **MSA CODE** (YouTube). The trajectory is on Instagram."
    ],
    "MSACODE_BUTTONS": [
        ("📺 FIND THE CODE", "📸 SEE THE RESULTS"),
        ("📺 WATCH THE VIDEO", "📸 JOIN THE INSIDERS"),
        ("📺 VIEW FULL BRIEFING", "📸 SEE DAILY UPDATES"),
        ("📺 ACCESS THE INTEL", "📸 ENTER THE NETWORK"),
        ("📺 OPEN THE SYSTEM", "📸 VERIFY YOUR ACCESS"),
        ("📺 WATCH IT NOW", "📸 CHECK THE PROOF"),
        ("📺 CLAIM THE ASSET", "📸 CONNECT WITH US"),
        ("📺 GET THE BLUEPRINT", "📸 JOIN THE COMMUNITY"),
        ("📺 LAUNCH THE PLAN", "📸 EXECUTE THE WORK"),
        ("📺 UNLOCK THE ARCHIVE", "📸 CONFIRM YOUR SPOT"),
        ("📺 SECURE YOUR ACCESS", "📸 VERIFY DEPLOYMENT"),
        ("📺 START YOUR MISSION", "📸 JOIN THE SQUAD"),
        ("📺 DECODE THE SYSTEM", "📸 REVIEW DAILY OPS"),
        ("📺 OPEN THE VAULT", "📸 ACCESS THE FLOOR"),
        ("📺 LEARN THE STRATEGY", "📸 SEE THE TACTICS"),
        ("📺 DOWNLOAD THE KEY", "📸 UPLOAD PROGRESS"),
        ("📺 ACTIVATE FULL PLAN", "📸 DEPLOY YOUR ASSET"),
        ("📺 WATCH FULL FOOTAGE", "📸 EXAMINE THE PROOF"),
        ("📺 ENTER THE SYSTEM", "📸 SEE THE REALITY"),
        ("📺 UNLOCK THE GATE", "📸 ACCESS THE PIPELINE"),
        ("📺 RETRIEVE THE PASS", "📸 CONFIRM YOUR ENTRY"),
        ("📺 CLAIM THE REWARD", "📸 JOIN THE FACTION"),
        ("📺 WATCH THE FULL SETUP", "📸 READ THE DAILY LOG"),
        ("📺 GET THE MASTER KEY", "📸 ENTER THE WORKSPACE"),
        ("📺 ACCESS MAIN SOURCE", "📸 JOIN THE CHANNEL"),
        ("📺 VIEW PRIMARY FEED", "📸 VERIFY YOUR STATUS"),
        ("📺 OPEN SECURE FILE", "📸 READ THE MEMO"),
        ("📺 GET ELITE CLEARANCE", "📸 JOIN THE ELITE LIST"),
        ("📺 UNLOCK YOUR ACCESS", "📸 GAIN INSIDER STATUS"),
        ("📺 VIEW ALL CODES", "📸 SEE THE FULL NETWORK"),
        ("📺 START FULL DOWNLOAD", "📸 INITIATE YOUR UPLOAD"),
        ("📺 GET THE FULL SCOOP", "📸 VERIFY YOUR RANK"),
        ("📺 ACCESS THE TERMINAL", "📸 JOIN THE SERVER"),
        ("📺 WATCH THE FULL PLAY", "📸 SEE THE METRICS"),
        ("📺 SECURE YOUR CODE", "📸 JOIN THE WINNING TEAM"),
        ("📺 ENTER THE NEW CODE", "📸 ENTER THE REAL WORLD"),
        ("📺 UNLOCK IT NOW", "📸 JOIN US NOW"),
        ("📺 ACCESS THE MASTER KEY", "📸 ACCESS THE CENTRAL HUB"),
        ("📺 VIEW THE ROADMAP", "📸 FIND THE CLEAR PATH"),
        ("📺 GET THE COORDINATES", "📸 JOIN THE EXACT LOCATION"),
        ("📺 START YOUR ENGINE", "📸 JOIN THE FAST TRACT"),
        ("📺 LOAD THE PROGRAM", "📸 RUN THE ACTIVE SYSTEM"),
        ("📺 EXECUTE THE CODE", "📸 CONFIRM THE CLEARANCE"),
        ("📺 ACCESS SECURE DATA", "📸 READ THE ACTIVE LOGS"),
        ("📺 GET YOUR CREDENTIALS", "📸 VERIFY YOUR PASSWORD"),
        ("📺 OPEN THE MAIN DOOR", "📸 ENTER THE LIVE ROOM"),
        ("📺 START THE SEQUENCE", "📸 JOIN THE OPERATIONS"),
        ("📺 UNLOCK YOUR POWER", "📸 CLAIM YOUR POSITION"),
        ("📺 ACCESS THE PAYOUT", "📸 RANK UP YOUR SYSTEM"),
        ("📺 TAKE THE FINAL STEP", "📸 COMPLETE THE PROCESS")
    ],
    "MSACODE_FOOTERS": [
        "🛡️ Clearance: VERIFIED | Status: ACTIVE",
        "👁️ System: ONLINE | User: {name}",
        "⚡ Connection: SECURE | Uplink: STABLE",
        "🔒 Access Level: FULL | User: {name}",
        "🕶️ Deployment: ACTIVE | Access: GRANTED",
        "🧬 Identity: CONFIRMED | Phase: EXECUTING",
        "📡 Signal: STRONG | Priority: HIGH",
        "🗝️ Asset: ALLOCATED | Session: SECURE",
        "🩸 Agreement: BOUND | Verification: COMPLETE",
        "🏛️ Network: PRIVATE | Entry: AUTHORIZED",
        " Phase: ACTIVE | Code: VALID",
        "🧪 Environment: SECURE | Test: PASSED",
        "🧹 System: CLEAN | Status: OPTIMAL",
        "🧗 Operations: SCALING | Trajectory: UP",
        "⚓ Infrastructure: SOLID | Ready: YES",
        "🥊 Market: COMPETITIVE | Position: SECURE",
        "🏁 Plan: EXECUTING | Operator: {name}",
        "🐺 Community: ACTIVE | Growth: ON",
        "🦅 View: CLEAR | Focus: SHARP",
        "🕯️ Progress: VISIBLE | Direction: SET",
        "🗡️ Strategy: SHARP | Execution: PRECISE",
        "🏆 Metrics: MET | Target: HIT",
        "👻 Operations: BACKEND | Noise: MINIMAL",
        "🚫 Distractions: ZERO | Focus: HIGH",
        "🔋 Systems: 100% | Capacity: MAX",
        "🤖 Automation: ACTIVE | Status: ONLINE",
        "💸 Asset: SECURE | Integrity: HIGH",
        "🏗️ Build: COMPLETE | Foundation: VERIFIED",
        "🧠 Mindset: FOCUSED | Vision: CLEAR",
        "🌪️ Momentum: BUILDING | Direction: FORWARD",
        "🌊 Trend: RIDING | Trajectory: SCALING",
        "🔥 Execution: RAPID | Progress: CONSTANT",
        "❄️ Processes: COLD | Logic: SOUND",
        "☁️ Infrastructure: SYNCED | Data: SECURE",
        "🌞 Launch: IMMINENT | Status: GO",
        "🌚 Coverage: COMPLETE | Operations: LIVE",
        "⭐ Trajectory: RISING | Performance: PEAK",
        "🌀 Engagement: HIGH | Pull: STRONG"
    ],
    "MSACODE_INVALID": [
        "❌ **INVALID:** That MSA CODE does not exist in the system, {name}. Stop guessing. Click below to retrieve the correct code.",
        "🚫 **ACCESS DENIED:** We checked the database, {name}. That MSA CODE is incorrect. You missed the information. Click below to watch the video again.",
        "⚠️ **WARNING:** Incorrect input detected, {name}. Do not guess the codes. Click below to retrieve the exact MSA CODE.",
        "🛑 **HALT:** You cannot bypass the system, {name}. There are no shortcuts. Click below to secure the real MSA CODE.",
        "📉 **FAILURE:** You missed the proper MSA CODE, {name}. It was displayed in the video. Click below and find the correct one.",
        "🔒 **LOCKED OUT:** Your access remains restricted, {name}. You supplied the wrong key. The real key is in the video. Click below.",
        "📵 **NO MATCH:** Your submitted MSA CODE is incorrect, {name}. We need accurate data. Click below to connect to the correct source.",
        "🧩 **INCOMPLETE:** You are trying to proceed without the correct data, {name}. Click below to get the exact code.",
        "📉 **ERROR 404:** MSA CODE not recognized, {name}. The correct strategy: Click below. Watch the video. Enter the exact code.",
        "👀 **NO ENTRY:** You are guessing blindly, {name}. The exact code is in the briefing. Click below to see it.",
        "🧱 **BLOCKED:** Your request was blocked, {name}. Enter the exact, correct MSA CODE to proceed. Click below to find it.",
        "🕸️ **STUCK:** You entered an incorrect code, {name}. Correct your input. Click below to do the work.",
        "⚖️ **VERDICT:** The system has ruled your MSA CODE: INVALID. Fix your input by clicking below, {name}.",
        "⏳ **TIME WASTED:** Guessing codes wastes your time, {name}. Stop. Click below, get the exact code, and proceed.",
        "🔌 **UNRECOGNIZED:** Your entry is not in our system, {name}. Click below to retrieve the recognized code.",
        "🔦 **NOT FOUND:** The code you entered was not found, {name}. It is visible in the video. Click below to locate it.",
        "🗑️ **REJECTED:** That MSA CODE data is incorrect, {name}. Only accurate codes are accepted. Click below to find it.",
        "🚩 **FLAGGED:** Your attempt was flagged as invalid, {name}. Adjust your input. Click below to find the correct details.",
        "📉 **DECLINED:** Your code submission was declined, {name}. Incorrect format or value. Click below to fetch the accurate code.",
        "🚪 **WRONG ENTRY:** That code does not open this stage, {name}. Click below to find the matching code.",
        "🔇 **NO RESPONSE:** The system rejected your input, {name}. Your MSA CODE is wrong. Click below to fetch the correct one.",
        "👻 **NON-EXISTENT:** You entered a code that doesn't exist, {name}. Click below to find the active, real MSA CODE.",
        "🌪️ **INCORRECT:** That MSA CODE is a mistake, {name}. It does not work. Click below to find the guaranteed code.",
        "🕸️ **VOIDED:** Your request was voided due to bad input, {name}. There is nothing here for that code. Click below.",
        "⚡ **MISMATCH:** The code you sent does not match our records, {name}. Fix your input. Click below to find the signal.",
        "🐛 **ERROR:** You submitted an invalid format, {name}. That MSA CODE is incorrect. Click below to correct it.",
        "🛑 **RESTRICTED:** Security protocol rejected your entry, {name}. MSA CODE unrecognized. Click below to resolve this.",
        "🧊 **PAUSED:** Your progress is paused due to an incorrect code, {name}. Click below to locate the correct code.",
        "🎭 **INVALID DATA:** The data provided is incorrect, {name}. Enter the verified code. Click below to find it.",
        "🕰️ **NO MATCH:** Your code was not found in the manifest, {name}. We require exact inputs. Click below to secure the exact code."
    ]
}

# ==========================================
# 🆔 MSA+ ID ALLOCATION SYSTEM
# ==========================================

def get_next_msa_id() -> tuple[str, int]:
    """Get the next available MSA+ ID — randomly allocated, never repeats."""
    # Build a set of all already-allocated numbers for O(1) lookup
    allocated_set = {
        doc["msa_number"]
        for doc in col_msa_ids.find({}, {"msa_number": 1})
    }

    # Generate a unique random 9-digit number (100000000–999999999)
    max_attempts = 1000
    for _ in range(max_attempts):
        candidate = random.randint(100_000_000, 999_999_999)
        if candidate not in allocated_set:
            msa_id = f"MSA{candidate:09d}"
            return msa_id, candidate

    raise RuntimeError("Could not generate a unique MSA ID after exhaustive attempts")

def allocate_msa_id(user_id: int, username: str, first_name: str) -> str:
    """Allocate MSA+ ID to a user (prevents duplicates)
    
    CRITICAL: Can only allocate to users who have vault_joined=True.
    This ensures MSA ID is never assigned before vault membership.
    """
    # Check if user is vault member - MUST be true to allocate
    user_data = col_user_verification.find_one({"user_id": user_id})
    if not user_data or not user_data.get("vault_joined", False):
        logger.warning(f"Cannot allocate MSA ID to user {user_id}: must join vault first")
        raise ValueError(f"User {user_id} must join vault before MSA ID allocation")
    
    # Check if user already has an MSA+ ID
    existing = col_msa_ids.find_one({"user_id": user_id})
    if existing:
        logger.info(f"User {user_id} already has MSA+ ID: {existing['msa_id']}")
        return existing['msa_id']
    
    # Get next available ID
    msa_id, msa_number = get_next_msa_id()
    
    # Insert into database
    col_msa_ids.insert_one({
        "user_id": user_id,
        "msa_id": msa_id,
        "msa_number": msa_number,
        "assigned_at": now_local(),
        "username": username,
        "first_name": first_name
    })
    
    # Update user verification record
    update_verification_status(user_id, msa_id=msa_id)
    
    logger.info(f"Allocated {msa_id} to user {user_id} ({first_name})")
    return msa_id

def get_user_msa_id(user_id: int) -> str | None:
    """Get user's MSA+ ID from database"""
    msa_record = col_msa_ids.find_one({"user_id": user_id})
    if msa_record:
        return msa_record['msa_id']
    return None

def get_verification_keyboard(user_id: int, user_data: dict, show_all: bool = True) -> InlineKeyboardMarkup:
    """Create inline keyboard - All 3 are URL buttons, no callbacks"""
    if show_all:
        # For NEW users - show all 3 buttons (all caps)
        keyboard = [
            [InlineKeyboardButton(text="📺 YOUTUBE — JOIN NOW", url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text="📸 INSTAGRAM — JOIN NOW", url=INSTAGRAM_LINK)],
            [InlineKeyboardButton(text="💎 MSA NODE VAULT — JOIN NOW", url=CHANNEL_LINK)]
        ]
    else:
        # For OLD users who left - show ONLY rejoin button (all caps)
        keyboard = [
            [InlineKeyboardButton(text="💎 MSA NODE VAULT — REJOIN NOW", url=CHANNEL_LINK)]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================
# 🔒 VAULT ACCESS CONTROL MIDDLEWARE
# ==========================================

# Per-user cooldown: track the last time we sent the rejoin message.
# Key = user_id (int), Value = datetime of last send.
# The rejoin/block message fires AT MOST once per 60 seconds per user.
# All other non-member interactions in the cooldown window are silently dropped.
_VAULT_REJOIN_COOLDOWN_SECS = 60
_vault_rejoin_last_sent: dict[int, datetime] = {}

async def _send_vault_rejoin_message(message_or_callback, user_id: int, user_name: str) -> bool:
    """Send the vault-rejoin prompt ONLY when the 60s per-user cooldown has expired.
    Returns True if the message was sent, False if suppressed (in cooldown — silent drop)."""
    now = datetime.now(TZ)
    last = _vault_rejoin_last_sent.get(user_id)
    if last and (now - last).total_seconds() < _VAULT_REJOIN_COOLDOWN_SECS:
        # Still in cooldown — silently block without sending anything
        return False

    # Cooldown cleared — record timestamp and show the rejoin prompt
    _vault_rejoin_last_sent[user_id] = now
    user_data = get_user_verification_status(user_id)
    was_ever_verified = user_data.get('ever_verified', False)

    if was_ever_verified:
        # Old user who left the vault
        await message_or_callback.answer(
            f"🔒 **{user_name}, YOU LEFT THE VAULT**\n\n"
            f"You had access. You gave it up.\n"
            f"Now the system won't let you in.\n\n"
            f"**You know the drill:**\n"
            f"No vault = No features. No exceptions.\n\n"
            f"💎 **Get back in. Restore your status.**",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        # New user who never joined
        await message_or_callback.answer(
            f"🔒 **{user_name}, AGENT ACCESS PAUSED**\n\n"
            f"You are currently missing out on the full **MSA Node Ecosystem**.\n\n"
            f"By securing your connection, you instantly unlock:\n\n"
            f"📂 **Unlimited Blueprints:** Seamless delivery of all future PDFs and guides.\n"
            f"🤖 **Elite AI Tools:** Access to our private arsenal of automation scripts.\n"
            f"💎 **The Inner Circle:** Strategies reserved strictly for the Vault.\n\n"
            f"To unlock the entire system at zero cost, simply join the Vault below.\n\n"
            f"*Don't stay on the outside. Secure your access now.*",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=True),
            parse_mode=ParseMode.MARKDOWN
        )
    return True

async def _require_vault_check(message) -> bool:
    """Vault gate for Message handlers.
    Sends rejoin message (respecting 60s cooldown) and returns True if user is NOT in vault.
    Returns False if user IS in vault (allow the handler to continue)."""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    is_in_vault = await check_channel_membership(user_id)
    if not is_in_vault:
        await _send_vault_rejoin_message(message, user_id, user_name)
        return True   # blocked
    return False      # allowed

async def require_vault_access(handler_func):
    """Decorator to ensure user is in vault before accessing any feature"""
    async def wrapper(message_or_callback):
        # Get user ID
        if hasattr(message_or_callback, 'from_user'):
            user_id = message_or_callback.from_user.id
            user_name = message_or_callback.from_user.first_name or "User"
        else:
            return

        # Check vault membership; send rejoin at most once per cooldown
        is_in_vault = await check_channel_membership(user_id)
        if not is_in_vault:
            await _send_vault_rejoin_message(message_or_callback, user_id, user_name)
            return   # blocked

        # User is in vault — allow access
        await handler_func(message_or_callback)

    return wrapper

async def check_if_banned(user_id: int) -> dict | None:
    """Check if user is banned. Returns ban doc if banned, None otherwise. Auto-unbans expired temporary bans."""
    try:
        # Only check bans that apply to Bot 1 (exclude bans scoped to bot2 admin panel only)
        ban_doc = col_banned_users.find_one({"user_id": user_id, "scope": {"$ne": "bot2"}})
        
        if ban_doc:
            # Check if it's a temporary ban that has expired
            if ban_doc.get('ban_type') == 'temporary' and ban_doc.get('ban_expires'):
                if now_local() > ban_doc['ban_expires']:
                    # Temporary ban has expired - auto-unban
                    col_banned_users.delete_one({"user_id": user_id})
                    logger.info(f"Auto-unbanned user {user_id} - temporary ban expired")
                    return None  # User is no longer banned
            
            # Ban is still active
            return ban_doc
        
        return None
    except Exception as e:
        logger.error(f"Ban check failed for user {user_id}: {e}")
        return None



# ==========================================
# 📋 MENU KEYBOARDS
# ==========================================
def get_main_menu():
    """Create the main menu keyboard — 6 core buttons"""
    keyboard = [
        [KeyboardButton(text="📊 DASHBOARD")],
        [KeyboardButton(text="🔍 SEARCH CODE")],
        [KeyboardButton(text="📺 WATCH TUTORIAL")],
        [KeyboardButton(text="📖 AGENT GUIDE")],
        [KeyboardButton(text="📜 RULES")],
        [KeyboardButton(text="📞 SUPPORT")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_user_menu(user_id: int):
    """Create menu based on user's ban/suspension status"""
    from aiogram.types import ReplyKeyboardRemove
    
    # Check if user is banned (only bans that apply to Bot 1, not bot2-only admin bans)
    ban_doc = col_banned_users.find_one({"user_id": user_id, "scope": {"$ne": "bot2"}})
    
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        
        # Temporary ban: Show only SUPPORT button
        if ban_type == "temporary":
            keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        
        # Permanent ban: Hide all buttons (no keyboard)
        else:
            return ReplyKeyboardRemove()
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": user_id})
    
    if suspend_doc:
        suspended = suspend_doc.get("bot1_suspended_features", [])
        
        # Build menu excluding suspended features
        keyboard = []
        
        if "DASHBOARD" not in suspended:
            keyboard.append([KeyboardButton(text="📊 DASHBOARD")])
        if "SEARCH_CODE" not in suspended:
            keyboard.append([KeyboardButton(text="🔍 SEARCH CODE")])
        if "TUTORIAL" not in suspended:
            keyboard.append([KeyboardButton(text="📺 WATCH TUTORIAL")])
        if "GUIDE" not in suspended:
            keyboard.append([KeyboardButton(text="📖 AGENT GUIDE")])
        if "RULES" not in suspended:
            keyboard.append([KeyboardButton(text="📜 RULES")])
        
        # Always show SUPPORT
        keyboard.append([KeyboardButton(text="📞 SUPPORT")])
        
        if keyboard:
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        else:
            # If all features suspended, show only support
            keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    # No restrictions: return full menu
    return get_main_menu()

def get_banned_user_keyboard(ban_type="permanent"):
    """Create keyboard for banned users based on ban type"""
    from aiogram.types import ReplyKeyboardRemove
    
    if ban_type == "temporary":
        # Temporary ban: Show only SUPPORT button
        keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    else:
        # Permanent ban: No buttons at all
        return ReplyKeyboardRemove()

def get_support_menu():
    """Create the support menu with issue categories"""
    keyboard = [
        [KeyboardButton(text="📄 PDF/LINK ISSUES")],
        [KeyboardButton(text="🔧 TROUBLESHOOTING")],
        [KeyboardButton(text="❓ OTHER ISSUES")],
        [KeyboardButton(text="🎫 RAISE A TICKET"), KeyboardButton(text="📋 MY TICKET")],
        [KeyboardButton(text="🔙 BACK TO MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_resolution_keyboard():
    """Create resolution keyboard after showing help"""
    keyboard = [
        [KeyboardButton(text="✅ RESOLVED")],
        [KeyboardButton(text="🔍 CHECK OTHER")],
        [KeyboardButton(text="🎫 RAISE A TICKET")],
        [KeyboardButton(text="🏠 MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==========================================
# 🎬 HANDLERS
# ==========================================

# ==========================================
# 🧩 DYNAMIC PAYLOAD PARSING
# ==========================================
def parse_start_payload(payload: str):
    """
    Parse the start payload with strict validation and partial matching.
    Returns a dict with 'status' and 'data'.
    """
    if not payload:
        return {"status": "invalid", "data": None}

    # 1. Try Exact Match
    # Format: CODE_source_NAME (e.g., cGdBXAN9_ig_PF2)
    match = re.search(r"^(.+)_(ig|yt)_(.+)$", payload)
    if match:
        return {
            "status": "valid",
            "data": {
                "code": match.group(1),
                "source": match.group(2).lower(),
                "pdf_name": match.group(3)
            }
        }

    # 2. Try YT Code Prompt Match
    # Format: CODE_YTCODE (e.g., 80919449_YTCODE)
    match_yt = re.search(r"^(.+)_YTCODE$", payload)
    if match_yt:
        return {
            "status": "yt_code_prompt",
            "data": {
                "user_code": match_yt.group(1)
            }
        }
    
    # 3. Try IGCC Deep Link Match
    # Format: USERID_igcc_CCCODE (e.g. 84797415_igcc_CC1)
    match_igcc = re.search(r"^(.+)_igcc_(.+)$", payload)
    if match_igcc:
        return {
            "status": "igcc_deep_link",
            "data": {
                "user_id_ref": match_igcc.group(1),
                "cc_code": match_igcc.group(2)
            }
        }

    # 4. Try Partial/Broken Match (Source Detection)
    if "_ig_" in payload.lower():
        return {"status": "broken_ig", "data": None}
    
    if "_yt_" in payload.lower():
        return {"status": "broken_yt", "data": None}
        
    if "ytcode" in payload.lower():
        return {"status": "broken_yt_prompt", "data": None}

    return {"status": "invalid", "data": payload}

def generate_alphanumeric(length=8):
    """Generate random alphanumeric code"""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))

def generate_digits(length=8):
    """Generate random digit code"""
    return "".join(secrets.choice(string.digits) for _ in range(length))

async def ensure_pdf_codes(pdf):
    """Ensure PDF has all start codes - creates them if missing"""
    from typing import Any
    pdf_doc: dict[str, Any] = dict(pdf)
    updates: dict[str, Any] = {}
    if not pdf_doc.get("ig_start_code"):
        updates["ig_start_code"] = generate_alphanumeric(8)
    if not pdf_doc.get("yt_start_code"):
        updates["yt_start_code"] = generate_digits(8)
    if not pdf_doc.get("aff_start_code"):
        updates["aff_start_code"] = generate_digits(8)
    if not pdf_doc.get("orig_start_code"):
        updates["orig_start_code"] = generate_digits(8)
    
    if updates:
        col_pdfs.update_one({"_id": pdf_doc["_id"]}, {"$set": updates})
        return {**pdf_doc, **updates}
    return pdf_doc

async def ensure_ig_cc_code(content) -> dict:
    """Ensure IG content has start_code"""
    if not content.get("start_code"):
        code = generate_digits(8)
        col_ig_content.update_one({"_id": content["_id"]}, {"$set": {"start_code": code}})
        return dict({**content, "start_code": code})
    return dict(content)

async def show_access_denied_animation(message: types.Message, user_id: int, payload: str = "", expected: str = ""):
    """Reusable ACCESS DENIED animation and message"""
    # 🎬 ANIMATION: ACCESS DENIED
    msg = await message.answer("🚫")
    await asyncio.sleep(ANIM_MEDIUM)
    await msg.edit_text("🚫 **SYSTEM ALERT**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    await msg.edit_text("🔒 **SECURITY BREACH DETECTED**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    await safe_delete_message(msg)

    # Error message
    error_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 INSTAGRAM", url=INSTAGRAM_LINK)],
        [InlineKeyboardButton(text="📺 YOUTUBE", url=YOUTUBE_LINK)]
    ])
    await message.answer(
        f"⚠️ **ACCESS DENIED: INVALID LINK**\n\n"
        f"The link you provided is **unrecognized** by the Agent.\n"
        f"Please obtain the **CORRECT LINK** from our official channels:\n\n"
        f"📸 **Instagram**: For exclusive Deep Links.\n"
        f"📺 **YouTube**: For Video Access.\n\n"
        f"OR enter a valid **MSA CODE** manually.\n\n"
        f"💬 Need help? Check vault announcements",
        reply_markup=error_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Log security breach
    if payload and expected:
        logger.warning(f"SECURITY BREACH: User {user_id} tried payload '{payload}' but expected '{expected}'")
    else:
        logger.warning(f"SECURITY BREACH: User {user_id} tried invalid link")

def get_pdf_content(index: int):
    """Fetch PDF content by index from bot3_pdfs collection"""
    return col_pdfs.find_one({"index": index})

# ==========================================
# 🎬 HANDLERS
# ==========================================

@dp.message(CommandStart())
@rate_limit(cooldown=2.0)
@anti_spam("start")
async def cmd_start(message: types.Message, state: FSMContext):
    
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    
    # ==========================================
    # 🚫 BAN CHECK - Highest Priority
    # ==========================================
    ban_doc = await check_if_banned(user_id)
    if ban_doc:
        banned_at = ban_doc.get("banned_at", now_local())
        ban_type = ban_doc.get("ban_type", "permanent")
        
        # Build ban message based on type
        if ban_type == "temporary" and ban_doc.get("ban_expires"):
            ban_expires = ban_doc["ban_expires"]
            time_diff = ban_expires - now_local()
            
            # Calculate time remaining
            days = time_diff.days
            hours = time_diff.seconds // 3600
            minutes = (time_diff.seconds % 3600) // 60
            
            time_remaining = ""
            if days > 0:
                time_remaining = f"{days} day{'s' if days > 1 else ''}, {hours} hour{'s' if hours != 1 else ''}"
            elif hours > 0:
                time_remaining = f"{hours} hour{'s' if hours != 1 else ''}, {minutes} minute{'s' if minutes != 1 else ''}"
            else:
                time_remaining = f"{minutes} minute{'s' if minutes != 1 else ''}"
            
            # Calculate progress bar
            ban_duration_hours = ban_doc.get("ban_duration_hours", 24)
            total_seconds = ban_duration_hours * 3600
            elapsed_seconds = total_seconds - time_diff.total_seconds()
            progress_percentage = max(0, min(100, (elapsed_seconds / total_seconds) * 100))
            
            # Generate progress bar (20 blocks)
            filled = int((progress_percentage / 100) * 20)
            empty = 20 - filled
            progress_bar = "▰" * filled + "▱" * empty
            
            ban_message = (
                "⏰ **TEMPORARY RESTRICTION**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Hi {user_name}, your account access is temporarily limited.\n\n"
                f"🕐 **Ban Start:** {banned_at.strftime('%b %d at %I:%M %p')}\n"
                f"🕐 **Ban Expires:** {ban_expires.strftime('%b %d at %I:%M %p')}\n"
                f"⏳ **Time Remaining:** {time_remaining}\n\n"
                f"**Ban Progress**\n"
                f"`[{progress_bar}]` {progress_percentage:.0f}%\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ **Auto-Unban:** Your access will be automatically restored when the timer expires.\n\n"
                f"⚠️ **Support:** You can use **📞 SUPPORT** to contact us.\n\n"
                f"📋 **Note:** Review community guidelines to avoid future restrictions."
            )
        else:
            ban_message = (
                "🚫 **ACCOUNT PERMANENTLY BANNED**\n\n"
                f"Hi {user_name}, your account has been permanently banned.\n\n"
                f"🕐 **Banned:** {banned_at.strftime('%b %d, %Y at %I:%M:%S %p')}\n\n"
                "⚠️ **All features and buttons are disabled.**\n"
                "This action is permanent."
            )
        
        await message.answer(
            ban_message,
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"🚫 Banned user {user_id} ({ban_type}) attempted to access bot")
        return
    
    just_consumed_grace = False
    args = message.text.split()
    payload = args[1] if len(args) > 1 else None
    
    # Check for Dynamic Payload (Priority)
    parse_result = parse_start_payload(payload or "")
    
    if parse_result["status"] == "valid":
        _raw_data = parse_result.get("data")
        parsed_data: dict = _raw_data if isinstance(_raw_data, dict) else {}
        input_code = str(parsed_data.get("code", ""))
        source = str(parsed_data.get("source", "")) # 'ig' or 'yt'
        
        # 1. Fetch Content by CODE (not by index)
        # Determine which DB field to check based on source
        if source == "ig":
            pdf_data = col_pdfs.find_one({"ig_start_code": input_code})
        elif source == "yt":
            pdf_data = col_pdfs.find_one({"yt_start_code": input_code})
        else:
            pdf_data = None
        
        if pdf_data:
            # ✅ CODE FOUND - Now VALIDATE ALL REQUIRED FIELDS
            # 🔒 STRICT FIELD VALIDATION
            # Check if PDF has ALL required data before allowing access
            
            missing_fields = []
            
            # Check for Affiliate Link
            if not pdf_data.get('affiliate_link'):
                missing_fields.append("Affiliate Link")
            
            # Check for MSA Code
            if not pdf_data.get('msa_code'):
                missing_fields.append("MSA Code")
            
            # If any field is missing, deny access
            if missing_fields:
                error_msg = (
                    "⚠️ **LINK INVALID**\n\n"
                    f"{user_name}, this content is no longer available because required information is missing:\n\n"
                )
                for field in missing_fields:
                    error_msg += f"• {field}\n"
                
                error_msg += (
                    "\n━━━━━━━━━━━━━━━━━━━━\n\n"
                    "🛠️ **Status:** This link has been disabled.\n\n"
                    "📞 **Support:** Use the Menu button to access support if you need assistance."
                )
                
                logger.warning(f"🚫 Deep link denied for user {user_id}: Missing fields {missing_fields} for PDF '{pdf_data.get('name')}'")
                
                await message.answer(
                    error_msg,
                    reply_markup=get_main_menu(),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # ✅ ALL REQUIRED FIELDS PRESENT - Now validate FULL payload structure
            # 🔒 STRICT FULL LINK VALIDATION
            # Reconstruct the expected payload and compare with input
            
            # Sanitize PDF name (same logic as bot3.py)
            pdf_name = pdf_data.get("name", "")
            sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', pdf_name)
            sanitized_name = re.sub(r'_+', '_', sanitized_name).strip('_')
            
            # Build expected payload
            expected_payload = f"{input_code}_{source}_{sanitized_name}"
            
            # STRICT COMPARISON: Must match EXACTLY
            if payload != expected_payload:
                # 🚫 INVALID LINK (Tampered suffix/structure)
                await show_access_denied_animation(message, user_id, payload or "", expected_payload)
                return
            
            # ✅ FULL VALIDATION PASSED
            # 📊 TRACK CLICK ANALYTICS
            # Vault membership checked FIRST — MSA ID only allocated for confirmed vault members
            username = message.from_user.username or "unknown"
            first_name = message.from_user.first_name or "User"
            is_in_vault = await check_channel_membership(user_id)
            try:
                # ── FIRST-TOUCH SOURCE: MUST run BEFORE sync so the sync reads
                # the correct source (IG/YT) when writing to bot2_user_tracking.
                # Stored permanently on first click — never overwritten.
                if source == "ig":
                    _store_initial_source(user_id, "IG")
                elif source == "yt":
                    _store_initial_source(user_id, "YT")

                # ── PRE-VAULT SYNC (idempotent) — reads the source just stored above ──
                # For pre-vault users: syncs vault_joined, allocates MSA ID, writes
                # bot2_user_tracking with the correct first-touch source.
                # For non-vault users: no-op.
                _newly_synced = False
                if is_in_vault:
                    msa_id, _newly_synced = await _sync_pre_vault_user(user_id, username, first_name)

                msa_id = msa_id if is_in_vault else get_user_msa_id(user_id)


                # 📊 EXCLUSIVE VAULT-MEMBER ANALYTICS TRACKING
                # Click counters only run for confirmed vault members (no bloat from non-members)
                if is_in_vault:
                    if source == "ig":
                        # Deduplicated IG start click — only count each user once per PDF
                        if _is_new_unique_click(user_id, pdf_data["_id"], "ig_start"):
                            col_pdfs.update_one(
                                {"_id": pdf_data["_id"]},
                                {
                                    "$inc": {"ig_start_clicks": 1, "clicks": 1},
                                    "$set": {"last_ig_click": now_local(), "last_clicked_at": now_local()}
                                }
                            )
                    elif source == "yt":
                        # Deduplicated YT start click — only count each user once per PDF
                        if _is_new_unique_click(user_id, pdf_data["_id"], "yt_start"):
                            col_pdfs.update_one(
                                {"_id": pdf_data["_id"]},
                                {
                                    "$inc": {"yt_start_clicks": 1, "clicks": 1},
                                    "$set": {"last_yt_click": now_local(), "last_clicked_at": now_local()}
                                }
                            )
                    logger.info(f"📊 Analytics: Vault user {user_id} clicked {source.upper()} link for PDF '{pdf_data.get('name')}'")
            except Exception as analytics_err:
                logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")
            
            # 🔒 VAULT ACCESS CHECK — Allow grace-pass users
            # ==========================================
            if not is_in_vault:
                # Check if user has grace-pass available
                user_data = get_user_verification_status(user_id)
                grace_allowed = user_data.get('grace_allowed', False)
                grace_consumed = user_data.get('grace_consumed', False)
                # Brand-new users (no DB record at all) always get a free first pass
                is_brand_new = not bool(user_data)
                has_grace = is_brand_new or (grace_allowed and not grace_consumed)
                
                if not has_grace:
                    # No grace available — block with vault lock message
                    col_user_verification.update_one({"user_id": user_id}, {"$set": {"pending_payload": payload}}, upsert=True)
                    
                    was_ever_verified = user_data.get('ever_verified', False)
                    vault_kb = get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified)
                    if was_ever_verified:
                        vault_msg = (
                            f"🔐 **{user_name}, THE VAULT IS CLOSED TO YOU**\n\n"
                            f"You clicked the link. The content is right there.\n"
                            f"But the system doesn't deliver to those who walked out.\n\n"
                            f"**You left the Vault.**\n"
                            f"That means you left your privileges at the door.\n\n"
                            f"💎 **One action separates you from everything:**\n"
                            f"Rejoin the Vault → Unlock full delivery. Instantly.\n\n"
                            f"*The content waits. The clock doesn't.*"
                        )
                    else:
                        vault_msg = (
                            f"🔒 **{user_name}, AGENT ACCESS PAUSED**\n\n"
                            f"You tried to access another premium asset, but your complimentary pass has been used.\n\n"
                            f"**The MSA Node Agent is an exclusive ecosystem.** By verifying your free membership, you will instantly unlock:\n\n"
                            f"📂 **Unlimited Blueprints:** Seamless delivery of all future PDFs and guides.\n"
                            f"🤖 **Elite AI Tools:** Access to our private arsenal of automation scripts.\n"
                            f"💎 **The Inner Circle:** Strategies reserved strictly for the Vault.\n\n"
                            f"To resume your delivery and unlock the entire system at zero cost, simply join the Vault below and return here.\n\n"
                            f"*The content is waiting. The choice is yours.*"
                        )
                    _vault_ans = await message.answer(
                        vault_msg,
                        reply_markup=vault_kb,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    _locked_ans = await message.answer(
                        "🔒 Menu locked until you rejoin the Vault.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    col_user_verification.update_one(
                        {"user_id": user_id},
                        {"$set": {"pending_delete_msg_ids": [_vault_ans.message_id, _locked_ans.message_id]}},
                        upsert=True
                    )
                    return
                
                # User has grace — consume it now atomically before delivering content
                try:
                    col_user_verification.update_one(
                        {
                            "user_id": user_id,
                            "grace_allowed": True,
                            "grace_consumed": False  # Ensure not already consumed (atomic check)
                        },
                        {
                            "$set": {
                                "grace_consumed": True,
                                "grace_consumed_at": now_local(),
                                "grace_consumed_via": f"{source.upper()}"  # Track which source consumed the grace
                            }
                        }
                    )
                    just_consumed_grace = True
                    logger.info(f"✅ Grace-pass consumed for user {user_id} via {source.upper()} link")
                except Exception as grace_err:
                    logger.error(f"⚠️ Failed to consume grace for user {user_id}: {grace_err}")
                # Continue to deliver content (grace consumed successfully)

            # =================================================================================
            # 🚀 EXACT SEARCH CODE DELIVERY FORMAT (Dynamic Cross-Platform)
            # =================================================================================
            
            # 1. PREPARE CONTENT
            first_name = message.from_user.first_name
            
            # PDF Title
            pdf_title_template = CONTENT_PACKS["PDF_TITLES"][secrets.randbelow(len(CONTENT_PACKS["PDF_TITLES"]))]
            try:
                pdf_title_text = pdf_title_template.format(name=first_name)
            except:
                pdf_title_text = pdf_title_template
            
            # Affiliate Title
            aff_title_text = CONTENT_PACKS["AFFILIATE_TITLES"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_TITLES"]))]
            
            # Dynamic Cross-Platform Logic for Text AND Final Button
            if source == 'ig':
                # IG -> YT (Use YT_VIDEO_TITLES for text, YT_CODES_BUTTONS for action)
                msa_code_template = CONTENT_PACKS["YT_VIDEO_TITLES"][secrets.randbelow(len(CONTENT_PACKS["YT_VIDEO_TITLES"]))]
                target_btn_text = CONTENT_PACKS["YT_CODES_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["YT_CODES_BUTTONS"]))]
                target_link = YOUTUBE_LINK
                footer_suffix = "| Source: IG -> YT" 
                
            elif source == 'yt':
                # YT -> IG (Use IG_VIDEO_TITLES for text, IG_CODES_BUTTONS for action)
                msa_code_template = CONTENT_PACKS["IG_VIDEO_TITLES"][secrets.randbelow(len(CONTENT_PACKS["IG_VIDEO_TITLES"]))]
                target_btn_text = CONTENT_PACKS["IG_CODES_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["IG_CODES_BUTTONS"]))]
                target_link = INSTAGRAM_LINK
                footer_suffix = "| Source: YT -> IG"
                
            else:
                # Fallback (legacy/unknown)
                msa_code_template = CONTENT_PACKS["MSACODE"][secrets.randbelow(len(CONTENT_PACKS["MSACODE"]))]
                target_btn_text = "📢 JOIN VAULT"
                target_link = CHANNEL_LINK
                footer_suffix = ""

            # Format the selected MSA Code text
            try:
                msa_code_text = msa_code_template.format(name=first_name)
            except:
                msa_code_text = msa_code_template
                
            # Links
            pdf_link = pdf_data.get("link") or BOT_FALLBACK_LINK
            affiliate_link = pdf_data.get("affiliate_link") or BOT_FALLBACK_LINK

            # 🎬 ANIMATION: DECRYPTION
            msg = await message.answer("◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text(f"📸 **CONNECTING SOURCE...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"🔓 **DECRYPTING ASSET...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"✅ **IDENTITY CONFIRMED: {first_name}**\n\n`Secure Delivery In Progress...`", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_DELAY)
            await safe_delete_message(msg)

            # ---------------------------------------------------------
            # 1️⃣ MESSAGE 1: PDF DELIVERY
            # ---------------------------------------------------------
            pdf_btn_text = CONTENT_PACKS["PDF_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["PDF_BUTTONS"]))]
            pdf_footer_template = CONTENT_PACKS["PDF_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["PDF_FOOTERS"]))]
            try:
                pdf_footer_text = pdf_footer_template.format(name=first_name)
            except:
                pdf_footer_text = pdf_footer_template
                
            pdf_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=pdf_btn_text, url=pdf_link)]])
            await message.answer(
                f"{pdf_title_text}\n\n`{pdf_footer_text}`",
                reply_markup=pdf_kb,
                parse_mode=ParseMode.MARKDOWN
            )

            # ⏳ DOT ANIMATION 1
            wait_msg = await message.answer("▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 2️⃣ MESSAGE 2: AFFILIATE OPPORTUNITY
            # ---------------------------------------------------------
            if affiliate_link:
                # Select Random Affiliate Footer
                aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
                try:
                    aff_footer_text = aff_footer_template.format(name=first_name)
                except:
                    aff_footer_text = aff_footer_template

                aff_btn_text = CONTENT_PACKS["AFFILIATE_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_BUTTONS"]))]
                aff_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=aff_btn_text, url=affiliate_link)]])
                await message.answer(
                    f"{aff_title_text}\n\n━━━━━━━━━━━━━━━━\n`{aff_footer_text}`",
                    reply_markup=aff_kb,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # ⏳ DOT ANIMATION 2
                wait_msg = await message.answer("▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 3️⃣ MESSAGE 3: NETWORK / CROSS-PLATFORM
            # ---------------------------------------------------------
            # Select Random Affiliate Footer
            aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
            try:
                base_footer = aff_footer_template.format(name=first_name)
            except:
                base_footer = aff_footer_template
            
            final_footer = base_footer 

            # Final message — random button text from packs, always both IG + YT links
            ig_btn_text = CONTENT_PACKS["IG_CODES_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["IG_CODES_BUTTONS"]))]
            yt_btn_text = CONTENT_PACKS["YT_CODES_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["YT_CODES_BUTTONS"]))]
            network_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text=ig_btn_text, url=INSTAGRAM_LINK),
                    InlineKeyboardButton(text=yt_btn_text, url=YOUTUBE_LINK)
                ]
            ])
            
            await message.answer(
                f"{msa_code_text}\n\n`{final_footer}`",
                reply_markup=network_kb,
                parse_mode=ParseMode.MARKDOWN
            )
            
            logger.info(f"User {user_id} triggered dynamic start: Source={source}, Code={input_code}")
            if just_consumed_grace:
                await send_psychological_vault_lock_message(user_id)
            # ── PRE-VAULT FIRST-INTERACTION MENU UNLOCK ──────────────────────
            # User was in vault BEFORE starting bot1. Content just delivered.
            # Send the reply menu now so they don't need a second /start.
            if _newly_synced:
                _sync_msa = get_user_msa_id(user_id) or "Assigned"
                await message.answer(
                    f"✅ **VAULT ACCESS CONFIRMED**\n\n"
                    f"Your account is now fully activated.\n"
                    f"🆔 **MSA+ ID:** `{_sync_msa}`\n\n"
                    f"Your full menu is unlocked below ⬇️",
                    reply_markup=get_user_menu(user_id),
                    parse_mode=ParseMode.MARKDOWN
                )
            return
        else:
            # 🚫 PDF NOT FOUND - Invalid Code
            await show_access_denied_animation(message, user_id)
            return

    # 🚫 ERROR HANDLING: BROKEN IG LINK
    elif parse_result["status"] == "broken_ig":
        await show_access_denied_animation(message, user_id)
        return

    # 🚫 ERROR HANDLING: BROKEN YT LINK
    elif parse_result["status"] == "broken_yt":
        await show_access_denied_animation(message, user_id)
        return

    # 🚫 ERROR HANDLING: INVALID / UNKNOWN SOURCE
    elif parse_result["status"] == "invalid" and payload:
        await show_access_denied_animation(message, user_id)
        return

    # 🎥 NEW FLOW: YT CODE PROMPT (Force MSA Code Entry)
    elif parse_result["status"] == "yt_code_prompt":
        # ──────────────────────────────────────────────────────────────────────
        # 🔒 STRICT YTCODE LINK VALIDATION
        # The prefix embedded in the link MUST match the persistent home_yt_code
        # stored in bot3_settings by Bot 3. This is the ONLY valid YTCODE prefix.
        # Any other value (guessed, incremented, tampered) is instantly denied.
        # ──────────────────────────────────────────────────────────────────────
        _embedded_code = str(parse_result.get("data", {}).get("user_code", ""))
        try:
            _home_yt_setting = db["bot3_settings"].find_one({"key": "home_yt_code"})
            _valid_yt_code = str(_home_yt_setting["value"]) if _home_yt_setting else None
        except Exception as _ytval_err:
            logger.warning(f"[YTCODE] home_yt_code DB lookup failed: {_ytval_err}")
            _valid_yt_code = None

        if not _valid_yt_code or _embedded_code != _valid_yt_code:
            logger.warning(
                f"[YTCODE] INVALID CODE: embedded={_embedded_code!r} "
                f"valid={_valid_yt_code!r} — access denied"
            )
            await show_access_denied_animation(message, user_id)
            return

        # ✅ Ownership verified — proceed
        # RECORD FIRST-TOUCH SOURCE — lightweight, no bot2_user_tracking write yet
        _store_initial_source(user_id, "YTCODE")
        # 🔒 VAULT ACCESS CHECK — Block non-members for YTCODE links
        is_in_vault = await check_channel_membership(user_id)
        # ── PRE-VAULT SYNC (idempotent) ──
        _ytcode_newly_synced = False
        if is_in_vault:
            _uname_yt = message.from_user.username or "unknown"
            _fname_yt = message.from_user.first_name or "User"
            _, _ytcode_newly_synced = await _sync_pre_vault_user(user_id, _uname_yt, _fname_yt)
        if not is_in_vault:

            # ── GRACE-PASS CHECK ──────────────────────────────────────────────
            # First-time users get one free pass through to the MSA code prompt.
            # Grace is NOT consumed here — it is consumed only after a valid code
            # is successfully entered (in process_search_code with is_yt_flow).
            user_data = get_user_verification_status(user_id)
            grace_consumed = user_data.get('grace_consumed', False)
            # Also block users who left the vault (ever_verified but not in vault)
            was_ever_verified = user_data.get('ever_verified', False)
            
            if was_ever_verified or grace_consumed:
                # Returning/lapsed user OR grace already used → hard block
                col_user_verification.update_one({"user_id": user_id}, {"$set": {"pending_payload": payload}}, upsert=True)
                vault_kb = get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified)
                if was_ever_verified:
                    vault_msg = (
                        f"🔐 **{user_name}, THE VAULT IS CLOSED TO YOU**\n\n"
                        f"You clicked the link. The content is right there.\n"
                        f"But the system doesn't deliver to those who walked out.\n\n"
                        f"**You left the Vault.**\n"
                        f"That means you left your privileges at the door.\n\n"
                        f"💎 **One action separates you from everything:**\n"
                        f"Rejoin the Vault → Unlock full delivery. Instantly.\n\n"
                        f"*The content waits. The clock doesn't.*"
                    )
                else:
                    vault_msg = (
                        f"🔒 **{user_name}, AGENT ACCESS PAUSED**\n\n"
                        f"You have already used your complimentary content pass.\n\n"
                        f"**The MSA Node Agent is an exclusive ecosystem.** By joining free, you instantly unlock:\n\n"
                        f"📂 **Unlimited Blueprints:** Seamless delivery of all future PDFs and guides.\n"
                        f"🤖 **Elite AI Tools:** Access to our private arsenal of automation scripts.\n"
                        f"💎 **The Inner Circle:** Strategies reserved strictly for the Vault.\n\n"
                        f"Join the Vault below and return here to continue.\n\n"
                        f"*The content is waiting. The choice is yours.*"
                    )
                _vault_ans = await message.answer(vault_msg, reply_markup=vault_kb, parse_mode=ParseMode.MARKDOWN)
                _locked_ans = await message.answer("🔒 Menu locked until you join the Vault.", reply_markup=ReplyKeyboardRemove())
                col_user_verification.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "pending_payload": payload,
                        "pending_delete_msg_ids": [_vault_ans.message_id, _locked_ans.message_id]
                    }},
                    upsert=True
                )
                return
            # ── First-time user: grace available → fall through to MSA code prompt ──
        msg = await message.answer("📡")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("📡 **CONNECTING TO SOURCE...**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("🔒 **SECURE CONNECTION ESTABLISHED**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await safe_delete_message(msg)

        # Prompt for MSA Code
        first_name = message.from_user.first_name
        
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
        
        # ── Choose prompt based on user type ─────────────────────────────────
        # First-time user (not in vault, grace available) → onboarding message, NO cancel button
        # Vault member / returning user → standard MSA code required message WITH cancel button
        if not is_in_vault:
            # 🚀 FIRST-TIME USER — AGENT ACTIVATED onboarding (no cancel button in menu)
            await message.answer(
                f"⚡ **AGENT ACTIVATED, {first_name}**\n\n"
                f"You've just unlocked access to the **MSA NODE** system.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔑 **ENTER YOUR MSA CODE**\n\n"
                f"Every piece of premium content — blueprints, AI tools, guides — is unlocked with a unique **MSA Code**.\n\n"
                f"📸 Find your code on **Instagram** or **YouTube**.\n"
                f"Then type it below to unlock your first blueprint instantly.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"`TYPE YOUR MSA CODE BELOW:`",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            # 🔒 RETURNING / VAULT USER — Standard MSA code prompt with cancel button
            await message.answer(
                f"🔒 **MSA CODE REQUIRED**\n\n"
                f"{first_name}, the agent is waiting.\n"
                f"Enter correct **MSA CODE** and get your blueprints Instantly!.\n\n"
                f"*Precision is key.*\n\n"
                f"`ENTER MSA CODE BELOW:`\n\n"
                f"⚪️ Press '**CANCEL**' to cancel this search operation.",
                reply_markup=cancel_kb,
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Set state to waiting for code
        await state.set_state(SearchCodeStates.waiting_for_code)
        # Set context flag: User came from YT, so we treat them as a YT source user
        await state.update_data(is_yt_flow=True)
        # ── PRE-VAULT FIRST-INTERACTION MENU UNLOCK ──────────────────────────
        # Send menu immediately — no need to wait for a second /start.
        if _ytcode_newly_synced:
            _yt_msa = get_user_msa_id(user_id) or "Assigned"
            await message.answer(
                f"✅ **VAULT ACCESS CONFIRMED**\n\n"
                f"Your account is fully activated.\n"
                f"🆔 **MSA+ ID:** `{_yt_msa}`\n\n"
                f"Your full menu is unlocked below ⬇️",
                reply_markup=get_user_menu(user_id),
                parse_mode=ParseMode.MARKDOWN
            )
        return

    # 🚫 ERROR HANDLING: BROKEN YT CODE PROMPT
    elif parse_result["status"] == "broken_yt_prompt":
        # Log the specific broken payload
        logger.warning(f"BROKEN YT PROMPT from {user_id}: {payload}")

        # 🎬 ANIMATION: ERROR DETECTION
        msg = await message.answer("⚠️")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⚠️ **DETECTING ERROR...**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("⚙️ **BYPASSING SECURITY...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⚡ **PROXY CONNECTION ESTABLISHED...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("🔍 **SEARCHING DATABASE...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⛔ **ERROR: ENCRYPTION KEY INVALID**")
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("⚠️ **ACCESS DENIED**")
        await asyncio.sleep(ANIM_SLOW)
        await safe_delete_message(msg)

        # Select Random Affiliate Footer
        aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
        try:
            error_footer = aff_footer_template.format(name=message.from_user.first_name)
        except:
            error_footer = aff_footer_template

        error_msg = (
            f"⚠️ **ACCESS DENIED: LINK FRACTURED**\n\n"
            f"The Neural Link you attempted to access is **INVALID**.\n"
            f"The agent cannot verify the requested Asset.\n\n"
            f"**DIAGNOSTIC:**\n"
            f"• Check the characters in your link.\n"
            f"• Ensure no digits are missing.\n"
            f"• Verify the source of your intelligence.\n\n"
            f"**PROTOCOL:**\n"
            f"Re-examine your data. Correct the vector. Execute again.\n\n"
            f"💬 Still stuck? Ask in vault channel\n\n"
            f"`{error_footer}`"
        )

        # Select Random Affiliate Button
        aff_btn_text = CONTENT_PACKS["AFFILIATE_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_BUTTONS"]))]
        aff_link = BOT_FALLBACK_LINK

        error_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 GET CORRECT LINK", url=INSTAGRAM_LINK)]
        ])
        
        await message.answer(error_msg, reply_markup=error_kb, parse_mode=ParseMode.MARKDOWN)
        return
    
    # 📸 NEW FLOW: IGCC DEEP LINK (Instant Content + Upsell)
    elif parse_result["status"] == "igcc_deep_link":
        _raw_igcc_data = parse_result.get("data")
        parsed_data: dict = _raw_igcc_data if isinstance(_raw_igcc_data, dict) else {}
        cc_code = str(parsed_data.get("cc_code", ""))
        user_id_ref = str(parsed_data.get("user_id_ref", ""))
        
        # Fetch Content
        ig_content = col_ig_content.find_one({"cc_code": cc_code})
        
        if ig_content:
            # ✅ ENSURE CODE EXISTS - Auto-generate if missing
            content_doc: dict = await ensure_ig_cc_code(ig_content)
            
            # 🔒 STRICT FULL LINK VALIDATION
            # Reconstruct expected payload and compare
            db_start_code = content_doc.get("start_code", "")
            expected_payload = f"{db_start_code}_igcc_{cc_code}"
            
            # STRICT COMPARISON: Must match EXACTLY
            if not db_start_code or payload != expected_payload:
                # 🚫 INVALID LINK (Tampered or Mismatch)
                await show_access_denied_animation(message, user_id, payload or "", expected_payload)
                return
            
            # ✅ VALIDATION PASSED - Continue with content delivery
            # 📊 TRACK CLICK ANALYTICS
            # Vault membership checked FIRST — MSA ID only allocated for confirmed vault members
            user_name = message.from_user.first_name or "User"
            username = message.from_user.username or "unknown"
            first_name = message.from_user.first_name or "User"
            is_in_vault = await check_channel_membership(user_id)
            try:
                # ── FIRST-TOUCH SOURCE: MUST run BEFORE sync so the sync reads
                # the correct source (IGCC) when writing to bot2_user_tracking.
                _store_initial_source(user_id, "IGCC")

                # ── PRE-VAULT SYNC (idempotent) — reads the source just stored above ──
                _igcc_newly_synced = False
                if is_in_vault:
                    msa_id, _igcc_newly_synced = await _sync_pre_vault_user(user_id, username, first_name)

                msa_id = msa_id if is_in_vault else get_user_msa_id(user_id)

                # Deduplicated IG CC click — only count each user once per IG content
                if _is_new_unique_click(user_id, content_doc["_id"], "ig_cc"):
                    col_ig_content.update_one(
                        {"_id": content_doc["_id"]},
                        {
                            "$inc": {"ig_cc_clicks": 1},
                            "$set": {"last_ig_cc_click": now_local()}
                        }
                    )

                logger.info(f"📊 Analytics: User {user_id} clicked IGCC link for '{content_doc.get('name')}'")
            except Exception as analytics_err:
                logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")


            
            # ==========================================
            # 🔒 VAULT ACCESS CHECK — Block non-members (already resolved above)
            # ==========================================
            if not is_in_vault:
                # Check if user has grace-pass available
                user_data = get_user_verification_status(user_id)
                grace_allowed = user_data.get('grace_allowed', False)
                grace_consumed = user_data.get('grace_consumed', False)
                has_grace = grace_allowed and not grace_consumed
                
                if not has_grace:
                    # No grace available — block with vault lock message
                    col_user_verification.update_one({"user_id": user_id}, {"$set": {"pending_payload": payload}}, upsert=True)
                    was_ever_verified = user_data.get('ever_verified', False)
                    vault_kb = get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified)
                    if was_ever_verified:
                        vault_msg = (
                            f"🔐 **{user_name}, THE VAULT IS CLOSED TO YOU**\n\n"
                            f"You clicked the link. The content is right there.\n"
                            f"But the system doesn't deliver to those who walked out.\n\n"
                            f"**You left the Vault.**\n"
                            f"That means you left your privileges at the door.\n\n"
                            f"💎 **One action separates you from everything:**\n"
                            f"Rejoin the Vault → Unlock full delivery. Instantly.\n\n"
                            f"*The content waits. The clock doesn't.*"
                        )
                    else:
                        vault_msg = (
                            f"🔒 **{user_name}, AGENT ACCESS PAUSED**\n\n"
                            f"You tried to access another premium asset, but your complimentary pass has been used.\n\n"
                            f"**The MSA Node Agent is an exclusive ecosystem.** By verifying your free membership, you will instantly unlock:\n\n"
                            f"📂 **Unlimited Blueprints:** Seamless delivery of all future PDFs and guides.\n"
                            f"🤖 **Elite AI Tools:** Access to our private arsenal of automation scripts.\n"
                            f"💎 **The Inner Circle:** Strategies reserved strictly for the Vault.\n\n"
                            f"To resume your delivery and unlock the entire system at zero cost, simply join the Vault below and return here.\n\n"
                            f"*The content is waiting. The choice is yours.*"
                        )
                    _vault_ans = await message.answer(
                        vault_msg,
                        reply_markup=vault_kb,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    _locked_ans = await message.answer(
                        "🔒 Menu locked until you rejoin the Vault.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    col_user_verification.update_one(
                        {"user_id": user_id},
                        {"$set": {"pending_delete_msg_ids": [_vault_ans.message_id, _locked_ans.message_id]}},
                        upsert=True
                    )
                    return
                
                # User has grace — consume it now atomically before delivering content
                try:
                    col_user_verification.update_one(
                        {
                            "user_id": user_id,
                            "grace_allowed": True,
                            "grace_consumed": False  # Ensure not already consumed (atomic check)
                        },
                        {
                            "$set": {
                                "grace_consumed": True,
                                "grace_consumed_at": now_local(),
                                "grace_consumed_via": "IGCC"
                            }
                        }
                    )
                    just_consumed_grace = True
                    logger.info(f"✅ Grace-pass consumed for user {user_id} via IGCC")
                except Exception as grace_err:
                    logger.error(f"⚠️ Failed to consume grace for user {user_id}: {grace_err}")
                # Continue to deliver content (grace consumed successfully)

            # 🎬 ANIMATION: ACCESSING CONTENT
            msg = await message.answer("◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text(f"📸 **CONNECTING TO SOURCE...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"🔓 **ACCESSING CONTENT...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await safe_delete_message(msg)

            # ---------------------------------------------------------
            # 1️⃣ MESSAGE 1: CONTENT DELIVERY
            # Safe delivery: handles long text (>4096 chars) and Markdown parse errors
            # ---------------------------------------------------------
            _MAX_TG = 4096
            _raw_content = ig_content.get("name", "")
            # Split into chunks so we never exceed Telegram's limit
            _chunks = [_raw_content[i:i+_MAX_TG] for i in range(0, max(len(_raw_content), 1), _MAX_TG)]
            for _chunk in _chunks:
                try:
                    await message.answer(_chunk, parse_mode="Markdown")
                except Exception:
                    # Markdown parse failed (e.g. # headers, unmatched * etc) — send as plain text
                    try:
                        await message.answer(_chunk)
                    except Exception as _e:
                        logger.error(f"IGCC content delivery failed for {cc_code}: {_e}")

            # ⏳ DOT ANIMATION 1
            wait_msg = await message.answer("▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 2️⃣ MESSAGE 2: AFFILIATE UPSELL (Only if affiliate link exists)
            # ---------------------------------------------------------
            aff_link = ig_content.get("affiliate_link", "")
            has_affiliate = bool(aff_link and len(aff_link) >= 5)
            
            if has_affiliate:
                title_text = CONTENT_PACKS["AFFILIATE_TITLES"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_TITLES"]))]
                footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
                try:
                    footer_text = footer_template.format(name=user_name)
                except:
                    footer_text = footer_template
                    
                aff_msg = f"{title_text}\n\n`{footer_text}`"
                
                aff_btn_text = CONTENT_PACKS["AFFILIATE_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_BUTTONS"]))]
                kb_aff = [[InlineKeyboardButton(text=aff_btn_text, url=aff_link)]]
                
                await message.answer(aff_msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_aff), parse_mode="Markdown")
                
                # ⏳ DOT ANIMATION 2
                wait_msg = await message.answer("▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 3️⃣ MESSAGE 3: NETWORK CONNECTION (IG + YT)
            # ---------------------------------------------------------
            # Static Psychological "System" Message
            network_msg = (
                f"📡 **SYSTEM STATUS: ASSET SECURED**\n\n"
                f"{user_name}, the tool is in your hands.\n"
                f"But a tool without a master is just metal.\n\n"
                f"You are here to build an **EMPIRE**, not a hobby.\n"
                f"We provide the blueprints. You provide the labor.\n\n"
                f"📺 **YouTube**: THE BLUEPRINT (Strategy & Execution).\n"
                f"📸 **Instagram**: THE NETWORK (Connections & Alpha).\n\n"
                f"The game is rigged. We are teaching you how to play.\n"
                f"**🚀 GET IN THE GAME NOW, {user_name}. Before it's too late.**"
            )
            
            # Select Random Affiliate Footer
            aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
            try:
                network_footer = aff_footer_template.format(name=user_name)
            except:
                network_footer = aff_footer_template
            
            final_network_msg = f"{network_msg}\n\n`{network_footer}`"
            
            # Always just 2 buttons — no affiliate button in this message
            kb_network = [
                [
                    InlineKeyboardButton(text="📸 EXPLORE MORE IG", url=INSTAGRAM_LINK),
                    InlineKeyboardButton(text="▶️ EXPLORE MORE YT", url=YOUTUBE_LINK)
                ]
            ]
            
            await message.answer(final_network_msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_network), parse_mode="Markdown")

            logger.info(f"User {user_id} triggered IGCC deep link for {cc_code}")
            if just_consumed_grace:
                await send_psychological_vault_lock_message(user_id)
            # ── PRE-VAULT FIRST-INTERACTION MENU UNLOCK ──────────────────────
            if _igcc_newly_synced:
                _igcc_msa = get_user_msa_id(user_id) or "Assigned"
                await message.answer(
                    f"✅ **VAULT ACCESS CONFIRMED**\n\n"
                    f"Your account is now fully activated.\n"
                    f"🆔 **MSA+ ID:** `{_igcc_msa}`\n\n"
                    f"Your full menu is unlocked below ⬇️",
                    reply_markup=get_user_menu(user_id),
                    parse_mode=ParseMode.MARKDOWN
                )
            return
        else:
            # 🚫 IG Content not found
            await show_access_denied_animation(message, user_id)
            return

    # Fallback to Standard Flow (Welcome / Verification)
    
    # Always show animation first for everyone
    # Step 1: Initial box
    msg = await message.answer("◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 2: Loading boxes
    await msg.edit_text("◻️ ◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 3: Full boxes
    await msg.edit_text("◻️ ◻️ ◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 4: System activation
    await msg.edit_text("🔒 **AUTHENTICATING**\n\n`Verifying identity...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    
    # Step 5: Identity confirmed
    await msg.edit_text(f"✅ **VERIFIED**\n\n`Welcome, {user_name}`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    
    # Step 6: Interface loading
    await msg.edit_text("⚙️ **INITIALIZING**\n\n`Loading workspace...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    
    # Now check verification status
    user_data = get_user_verification_status(user_id)
    
    # ALWAYS check if user is in vault channel (real-time check)
    is_in_vault = await check_channel_membership(user_id)

    # ── PRE-VAULT SYNC (idempotent) — runs only once, skips if already synced ──
    # Handles the special case: user was already in vault BEFORE ever starting bot1.
    # For these users: sets vault_joined=True, allocates MSA ID, writes bot2_user_tracking.
    # For normal users (not in vault): falls back to a plain status update.
    _start_newly_synced = False
    if is_in_vault:
        _u = message.from_user
        _, _start_newly_synced = await _sync_pre_vault_user(
            user_id, _u.username or "unknown", _u.first_name or "User"
        )
    else:
        update_verification_status(user_id, vault_joined=False)

    # Check if user was EVER verified before (old user detection)
    was_ever_verified = user_data.get('ever_verified', False)
    
    # Verification = Only vault membership (no YT/IG tracking)
    all_verified = is_in_vault
    
    # If not verified (not in vault) AND this is a NEW user (never verified before)
    if not all_verified and not was_ever_verified:
        grace_consumed = user_data.get('grace_consumed', False)
        
        if not grace_consumed:
            # ── FIRST-TIME USER: Grace available ─────────────────────────────
            # Do NOT block them. Let them fall through so they reach the MSA code
            # prompt or tutorial below. The grace will be consumed after valid delivery.
            logger.info(f"First-time non-vault user {user_id} — allowing through to MSA code prompt")
            # Fall through to the verified interface below (skip the vault block)
        else:
            # ── RETURNING NON-VAULT USER: Grace consumed, no vault ───────────
            # Block them with vault join screen
            join_text = f"""
✨ **{user_name}, The System Awaits.**

You have just activated the **MSA NODE Agent**. This isn't just a bot — it's your personal gateway to elite-level automation, blueprints, and strategies.

━━━━━━━━━━━━━━━━━━━━

🌟 **UNLOCK THE FULL ARSENAL FOR FREE:**

📂 **Unlimited Blueprints**: Instant delivery of all premium guides.
🤖 **Powerful AI Tools**: Access secret automation scripts.
💎 **Full Agent Power**: Search codes, dash, and priority updates.

━━━━━━━━━━━━━━━━━━━━

**🔑 How to Get Instant Access:**

1️⃣ Tap **💎 JOIN MSA VAULT** below to enter our exclusive circle.
2️⃣ Follow us on **YouTube & Instagram** for the latest MSA Codes.
3️⃣ Return here → Your agent will automatically unlock all features.

━━━━━━━━━━━━━━━━━━━━

🚀 **Your integration starts now.**
*Join the Vault to turn on full agent power.*
"""
            verification_msg = await msg.edit_text(
                join_text,
                reply_markup=get_verification_keyboard(user_id, user_data),
                parse_mode=ParseMode.MARKDOWN
            )
            # Hide menu keyboard for non-vault users
            await message.answer(
                "🔒 No access to menu and features",
                reply_markup=ReplyKeyboardRemove()
            )
            # Store verification message ID for later deletion
            update_verification_status(user_id, verification_msg_id=verification_msg.message_id)
            return
    
    # If not verified but WAS verified before (old user who left), just tell them to rejoin
    if not all_verified and was_ever_verified:
        # No tracking needed here — bot2_user_tracking is only written on vault join
        await msg.edit_text(
            f"👋 **{user_name}, We've Missed You!**\n\nYour seat in the **MSA NODE Vault** is still reserved, waiting for your return.\n\n💎 **Everything you left behind?** Still yours.\n🎯 **Your community?** Still here for you.\n\n**One tap. Full access restored. Welcome home.**",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
            parse_mode=ParseMode.MARKDOWN
        )
        # Hide menu keyboard for old users too
        await message.answer(
            "🔒 No access to menu and features",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    # ── FIRST-TIME NON-VAULT USER BRANCH ────────────────────────────────────
    # Users who fell through the grace check above (not in vault, grace not consumed,
    # never verified before) should see a welcome + MSA code prompt, NOT the premium
    # verified dashboard. This keeps the experience clean and honest.
    # ⚠️ CRITICAL FIX: vault-first users (joined vault BEFORE ever opening bot) must
    # NOT enter the code-entry state — they are full members and should go straight
    # to the verified welcome interface. Only set code-entry state if NOT in vault.
    if not is_in_vault and not was_ever_verified and not user_data.get('grace_consumed', False):
        await safe_delete_message(msg)
        first_name_ft = message.from_user.first_name or "Agent"
        await message.answer(
            f"⚡ **AGENT ACTIVATED, {first_name_ft}**\n\n"
            f"You've just unlocked access to the MSA NODE system.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔑 **ENTER YOUR MSA CODE**\n\n"
            f"Every piece of premium content — blueprints, AI tools, guides — is unlocked with a unique MSA Code.\n\n"
            f"📸 Find your code on **Instagram** or **YouTube**.\n"
            f"Then type it below to unlock your first blueprint instantly.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"`TYPE YOUR MSA CODE BELOW:`",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(SearchCodeStates.waiting_for_first_code)
        logger.info(f"First-time non-vault user {user_id} — shown MSA code prompt")
        return

    # User is verified - show welcome interface
    # Mark as verified if not already
    if not user_data.get('verified'):
        update_verification_status(user_id, verified=True)

    # Fetch MSA ID to display (allocate if somehow missing)
    user_msa_id = get_user_msa_id(user_id)
    _uname_track = message.from_user.username or "unknown"
    if not user_msa_id:
        user_msa_id = allocate_msa_id(user_id, _uname_track, user_name)
    # bot2_user_tracking is only written at vault join — no call needed here

    # Final: Enhanced premium interface with ONLINE status
    welcome_text = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━
   🤖 **MSA NODE AGENT**
   🟢 **SYSTEM ONLINE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━

Welcome back, **{user_name}** 👋

⭐ **PREMIUM ACCESS GRANTED**
━━━━━━━━━━━━━━━━━━━━━━━━━━━

📍 **YOUR STATUS**

✅ **Verified Member**
🔓 **Full Access Enabled**
💎 **MSA NODE Elite**
🆔 **MSA+ ID**: `{user_msa_id}`

━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 **AGENT SERVICES**

📊 **DASHBOARD**
   Your profile, stats & live announcements

🔍 **SEARCH CODE**
   Unlock content with your MSA CODES

📺 **WATCH TUTORIAL**
   Your exclusive MSA NODE starter video

📖 **AGENT GUIDE**
   Full manual — how to use MSA NODE Agent

📜 **RULES**
   Community code of conduct

━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 **QUICK START:**
• Use the menu buttons below
• Tap links from videos — content auto-delivers
• Enter codes manually via **SEARCH CODE**
• Watch **TUTORIAL** for a full walkthrough
• Check **DASHBOARD** for your live stats

━━━━━━━━━━━━━━━━━━━━━━━━━━━

🛡️ **Secure** • ⚡ **Fast** • 🎯 **Reliable**

📞 Need help? Use **SUPPORT** anytime

_Select a service from the menu ⬇️_
"""
    
    await safe_delete_message(msg)
    await message.answer(
        welcome_text,
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN
    )
    
    # NOTE: Pending deep-link payloads are delivered by handle_vault_join when the user joins the
    # vault channel. We do NOT re-deliver here to avoid duplicates.
    # ── 🎬 STARTER TUTORIAL — Only on plain empty /start (no referral payload) ──
    # Looks up the universal tutorial link stored via bot3 TUTORIAL manager.
    # Delivered as a premium framed message with an inline watch button.
    # If no link stored yet → professional "coming soon" message instead.
    if not payload:
        try:
            pk_tut = db["bot3_tutorials"].find_one({"type": "PK"})
            await asyncio.sleep(ANIM_FAST)
            if pk_tut and pk_tut.get("link"):
                pk_link = pk_tut["link"]
                kb_pk = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="▶️ WATCH MSA NODE AGENT TUTORIAL", url=pk_link)]
                ])
                await message.answer(
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🎬 **YOUR EXCLUSIVE TUTORIAL IS HERE**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "💎 **You're now inside the vault.** This is where it starts.\n\n"
                    "Most people get access and don't know where to begin.\n"
                    "This tutorial removes that confusion — entirely.\n\n"
                    "In one watch, you'll know:\n"
                    "  ✅ Exactly how MSA NODE works\n"
                    "  ✅ How to unlock content with your codes\n"
                    "  ✅ How to get the most from your elite membership\n\n"
                    "⚡ **Don't skip this. It changes everything.**\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "_Your guide is one tap away ⬇️_",
                    reply_markup=kb_pk,
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await message.answer(
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🎬 **MSA NODE AGENT TUTORIAL IS COMING**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "🛠️ **We're putting the final touches on it.**\n\n"
                    "Your exclusive video guide is being prepared — built specifically "
                    "to walk you through every part of MSA NODE AGENT from day one.\n\n"
                    "While you wait, everything is already unlocked for you:\n"
                    "  📊 Check your **Dashboard** for your MSA+ ID\n"
                    "  🔍 Use **Search Code** to unlock content\n"
                    "  📖 Open **Agent Guide** for the full manual\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🔔 _Agent tutorial drops very soon. Stay ready._ 🚀",
                    parse_mode=ParseMode.MARKDOWN
                )
        except Exception as _pk_err:
            logger.warning(f"PK tutorial delivery failed for {user_id}: {_pk_err}")

    # Check for deep link payload (Legacy check or fallback)
    if payload == "80919449_YTCODE":
        # Track user source for bot2 broadcasts
        try:
            # Get or allocate MSA+ ID for user
            username = message.from_user.username or "unknown"
            first_name = message.from_user.first_name or "User"
            msa_id = get_user_msa_id(user_id)
            if not msa_id:
                msa_id = allocate_msa_id(user_id, username, first_name)
            
            # Record first-touch source in col_user_verification only
            _store_initial_source(user_id, "YTCODE")
        except Exception as track_err:
            logger.error(f"⚠️ Bot2 user tracking failed: {track_err}")
        
        # Auto-trigger Search Code prompt
        await asyncio.sleep(ANIM_SLOW)
        await message.answer(
            "🔑 **ENTER MSA CODE**\n\nTo access the Blueprint, please type the unique **MSA CODE** from the video below.\n\n`Example: MSA001`",
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(SearchCodeStates.waiting_for_code)
        logger.info(f"User {user_id} triggered via YTCODE deep link")

    logger.info(f"User {user_id} started with premium interface")
    
    # Auto-deliver vault unlock message for unknown plain /start only if grace already consumed
    # (First-time users: skip vault prompt so they can use the MSA code prompt freely)
    if not payload:
        _is_in_vault = await check_channel_membership(user_id)
        if not _is_in_vault:
            _udata_plain = get_user_verification_status(user_id)
            _grace_consumed_plain = _udata_plain.get('grace_consumed', False)
            if _grace_consumed_plain:
                # Returning non-vault user → show vault lock
                await send_psychological_vault_lock_message(user_id)

# ==========================================
# 🎉 AUTO-WELCOME ON VAULT JOIN
# ==========================================

@dp.chat_member()
async def handle_vault_join(event: ChatMemberUpdated):
    """Detect when user joins vault and auto-send welcome message with source-specific rewards"""
    # Check if this is the vault channel
    if event.chat.id != CHANNEL_ID:
        return
    
    # Check if user joined (status changed from not member to member)
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    
    # Detect join: was not in channel, now in channel
    if old_status in ["left", "kicked"] and new_status in ["member", "administrator", "creator"]:
        user_id = event.from_user.id
        user_name = event.from_user.first_name or "User"
        
        # 🔑 KEY REQUIREMENT: DO NOT ADD OR TRACK USERS WHO NEVER INTERACTED WITH BOT 1
        existing_bot1_user = col_user_verification.find_one({"user_id": user_id})
        if not existing_bot1_user:
            logger.info(f"Ignored vault join for {user_id} - never interacted with Bot 1")
            return
        

        # ==========================================
        # 🛑 MAINTENANCE MODE CHECK (Chat Member)
        # ==========================================
        try:
            settings = col_bot1_settings.find_one({"setting": "maintenance_mode"})
            if settings and settings.get("value", False) and user_id != OWNER_ID:
                # Maintenance is ON — update DB status but skip welcome messages
                update_verification_status(user_id, vault_joined=True, verified=True, ever_verified=True, rejoin_msg_id=None)
                username = event.from_user.username or "unknown"
                _msa_mm = allocate_msa_id(user_id, username, user_name)
                # Read first-touch source and write to bot2_user_tracking at this vault join
                _v_stored_source = col_user_verification.find_one({"user_id": user_id}, {"initial_source": 1}) or {}
                _v_source = _v_stored_source.get("initial_source", "UNKNOWN")
                track_user_source(user_id, _v_source, username, user_name, _msa_mm)
                try:
                    await bot.send_message(
                        user_id,
                        f"👤 **Dear {user_name},**\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "You've successfully joined the vault! 🎉\n\n"
                        "However, the Agent is currently undergoing a **premium infrastructure upgrade**. "
                        "Your membership is saved — just come back once we're online.\n\n"
                        "⏳ **Status:** Coming back online very soon.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Thank you for your patience.\n\n"
                        "_— MSA Node Systems_",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.error(f"Failed to send maintenance message to vault joiner {user_id}: {e}")
                logger.info(f"🛑 Maintenance: Vault join by {user_id} — saved but no welcome sent.")
                return
        except Exception as e:
            logger.error(f"Error checking maintenance mode in vault join handler: {e}")
        
        # Get user data to check for message IDs and source
        user_data = get_user_verification_status(user_id)
        verification_msg_id = user_data.get('verification_msg_id')
        rejoin_msg_id = user_data.get('rejoin_msg_id')
        pending_payload = user_data.get('pending_payload')  # ← KEY: Get any pending payload they clicked
        
        # Update verification status and mark as EVER verified (for old user detection)
        update_verification_status(user_id, vault_joined=True, verified=True, ever_verified=True, rejoin_msg_id=None)
        # Clear any inactive-tracking fields from when they left — they're back now
        col_user_verification.update_one(
            {"user_id": user_id},
            {"$unset": {"vault_left_at": "", "reminder1_sent": "", "reminder2_sent": ""}}
        )
        
        # Allocate MSA+ ID if not already assigned
        username = event.from_user.username or "unknown"
        msa_id = allocate_msa_id(user_id, username, user_name)

        # 📌 WRITE TO BOT2_USER_TRACKING — vault join is the ONLY moment we do this
        # Read the first-touch source that was stored in col_user_verification
        _stored_source = user_data.get("initial_source", "UNKNOWN")
        track_user_source(user_id, _stored_source, username, user_name, msa_id)
        logger.info(f"📊 Vault join: user {user_id} tracked with source '{_stored_source}'")

        # 📊 GRACE-TO-VAULT CONVERSION TRACKING
        # If this user previously consumed their grace pass, record that they converted.
        # This measures the real ROI of the free-pass feature.
        if user_data.get("grace_consumed") and not user_data.get("grace_converted_to_vault"):
            col_user_verification.update_one(
                {"user_id": user_id},
                {"$set": {
                    "grace_converted_to_vault": True,
                    "grace_converted_at": now_local(),
                    "grace_converted_via": user_data.get("grace_consumed_via", "UNKNOWN")
                }}
            )
            logger.info(f"[GRACE→VAULT] User {user_id} converted: grace pass → vault member")

        # 🗑️ DELETE ALL BLOCKING MESSAGES (IN PROPER ORDER)
        messages_to_delete = []
        
        # 1. Verification message (when they first started)
        if verification_msg_id:
            messages_to_delete.append(("verification", verification_msg_id))
        
        # 2. Rejoin message (if they left and came back)
        if rejoin_msg_id:
            messages_to_delete.append(("rejoin", rejoin_msg_id))
        
        # 3. Pending vault-block messages (from when they clicked link but weren't in vault)
        pending_delete_ids = user_data.get('pending_delete_msg_ids', [])
        for _mid in pending_delete_ids:
            messages_to_delete.append(("pending", _mid))
        
        # Execute all deletions (don't fail on single error)
        for msg_type, msg_id in messages_to_delete:
            try:
                await bot.delete_message(user_id, msg_id)
                logger.info(f"✅ Deleted {msg_type} message {msg_id} for user {user_id}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete {msg_type} message {msg_id}: {e}")
        
        # Clear all message IDs from database (no point keeping dead references)
        col_user_verification.update_one(
            {"user_id": user_id},
            {"$unset": {
                "verification_msg_id": "",
                "rejoin_msg_id": "",
                "pending_delete_msg_ids": ""
            }}
        )
        
        # Send welcome message to user's DM
        try:
            msg_success = (
                f"👑 **{user_name}, You're Inside. Access Granted.**\n\n"
                f"The **MSA NODE Agent** has verified your clearance.\n"
                f"You are now part of the most exclusive network in the system.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ **WHAT YOU JUST UNLOCKED (100% FREE):**\n\n"
                f"🆔 **MSA+ ID**: `{msa_id}` — Your permanent agent identity\n"
                f"📂 **Full Blueprint Library**: Every premium guide, strategy & PDF, delivered on demand\n"
                f"🤖 **Elite AI Tools**: Private automation scripts and agent-grade playbooks\n"
                f"📊 **Live Intel Dashboard**: Real-time performance, stats, and insider announcements\n"
                f"🔎 **MSA Code Access**: Search any code and unlock exclusive drops instantly\n"
                f"💬 **Priority Support**: Direct line to the MSA NODE team\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Most people never reach this level. You did.\n"
                f"The agent is online. The system is yours. \u26a1"
            )
            await bot.send_message(user_id, msg_success, parse_mode=ParseMode.MARKDOWN)
            
            # Send menu keyboard immediately
            await bot.send_message(
                user_id,
                "👇 **Select a service from the menu below to begin: Just say a word!**",
                reply_markup=get_user_menu(user_id),
                parse_mode=ParseMode.MARKDOWN
            )
            
            logger.info(f"Auto-welcomed user {user_id} after vault join")
            
            # --- 🚀 AUTO-DELIVER PENDING PAYLOAD (Atomic — no duplicates) ---
            # Use find_one_and_update to atomically claim and clear the pending payload.
            # This guarantees the content is delivered exactly once even if multiple events fire.
            claimed = col_user_verification.find_one_and_update(
                {"user_id": user_id, "pending_payload": {"$exists": True, "$ne": None}},
                {"$unset": {"pending_payload": ""}},
                return_document=False  # Get the document BEFORE the update (has pending_payload)
            )
            pending_payload = claimed.get("pending_payload") if claimed else None
            if pending_payload:
                logger.info(f"⚡ Delivering pending payload '{pending_payload}' to user {user_id} after vault join")
                
                # Build a minimal mock message so we can re-use the cmd_start delivery logic
                class _MockChat:
                    def __init__(self, chat_id): self.id = chat_id
                
                class _MockMessage:
                    """Duck-typed Message that routes .answer() to bot.send_message."""
                    def __init__(self, uid, fname, uname, payload_text):
                        class _FU:
                            def __init__(self, i, fn, un): self.id = i; self.first_name = fn; self.username = un
                        self.from_user = _FU(uid, fname, uname)
                        self.text = f"/start {payload_text}"
                        self.chat = _MockChat(uid)
                        self.message_id = int(time.time())
                    async def answer(self, text, **kwargs):
                        return await bot.send_message(self.from_user.id, text, **kwargs)
                    async def delete(self): pass
                
                _mock = _MockMessage(user_id, user_name, username, pending_payload)
                _key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
                _fsm = FSMContext(storage=dp.storage, key=_key)
                
                # Capture local copies for the closure
                _uid = user_id
                _pp  = pending_payload
                
                async def _deliver_pending():
                    try:
                        user_last_command.pop(_uid, None)   # clear rate-limit
                        clear_user_processing(_uid)          # clear anti-spam lock
                        await cmd_start(_mock, _fsm)
                        logger.info(f"✅ Pending payload '{_pp}' delivered to {_uid}")
                    except Exception as _e:
                        logger.error(f"❌ Pending delivery failed for {_uid}: {_e}\n{traceback.format_exc()}")
                
                asyncio.create_task(_deliver_pending())
                

        except Exception as e:
            logger.error(f"Failed to send welcome message to {user_id}: {e}")
    
    # Detect leave: was in channel, now not in channel
    elif old_status in ["member", "administrator", "creator"] and new_status in ["left", "kicked"]:
        user_id = event.from_user.id
        user_name = event.from_user.first_name or "User"
        
        # Check if user exists in database (not permanently deleted)
        existing_user = col_user_verification.find_one({"user_id": user_id})
        
        if not existing_user:
            # User was permanently deleted - don't send any message or update anything
            logger.info(f"User {user_id} left vault but was permanently deleted - no action taken")
            return
        
        # Update status - user left vault
        update_verification_status(user_id, vault_joined=False, verified=False)
        # Record when they left so inactive_member_monitor can track 30-day window
        col_user_verification.update_one(
            {"user_id": user_id},
            {
                "$set":  {"vault_left_at": now_local()},
                "$unset": {"reminder1_sent": "", "reminder2_sent": ""}
            }
        )
        
        # Get user data for keyboard
        user_data = get_user_verification_status(user_id)
        
        # Send instant rejoin message with button and store message ID
        try:
            # Instantly remove reply keyboard — no /start required
            await bot.send_message(
                user_id,
                "🔒 **ACCESS SUSPENDED**\n\nYour MSA NODE vault membership was removed.",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )
            rejoin_msg = await bot.send_message(
                user_id,
                f"💫 **{user_name}, Your Journey Paused**\n\nWe see you've stepped away from the Vault. Life happens—we understand.\n\n💎 **Here's the thing:** Your spot? Still reserved.\n🎯 **Your community?** Still rooting for you.\n\n**When you're ready to return, we'll be right here.** One click brings you back.\n\n*No pressure. Just opportunity.* ✨",
                reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
                parse_mode=ParseMode.MARKDOWN
            )
            # Store rejoin message ID for deletion when user rejoins
            update_verification_status(user_id, rejoin_msg_id=rejoin_msg.message_id)
            logger.info(f"Sent rejoin message {rejoin_msg.message_id} to user {user_id} who left vault")
        except Exception as e:
            logger.error(f"Failed to send rejoin message to {user_id}: {e}")



# ==========================================
# 📢 ANNOUNCEMENT HELPERS (reads bot2_broadcasts)
# ==========================================

_DASH_CHAR_LIMIT     = 3900  # safe buffer below Telegram's 4096-char cap
_ANN_CAP             = 3     # show only the N most recent broadcasts
_ANN_PAGE_MAX_CHARS  = 800   # max chars for a single announcement's text in the dashboard


def _escape_dashboard_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"]:
        text = text.replace(char, f"\\{char}")
    return text


def _fetch_deduplicated_broadcasts() -> list:
    """
    Fetch the _ANN_CAP most-recent broadcasts from DB, deduplicated by broadcast_id.
    Newest first. Returns a list of at most _ANN_CAP items.
    """
    seen_ids: set = set()
    result = []
    try:
        cursor = (
            col_broadcasts.find(
                {},
                {
                    "broadcast_id": 1,
                    "created_at": 1,
                    "message_text": 1,
                    "media_type": 1,
                    "index": 1,
                },
            )
            .sort("index", -1)
            .limit(_ANN_CAP * 3)
            .max_time_ms(4000)
        )
        for b in cursor:
            bid = b.get("broadcast_id") or str(b.get("_id", ""))
            if bid in seen_ids:
                continue
            seen_ids.add(bid)
            result.append(b)
            if len(result) >= _ANN_CAP:
                break
    except Exception as bc_err:
        logger.error(f"Dashboard broadcast fetch failed: {bc_err}")
    return result



def _build_ann_page(broadcasts: list, page: int) -> str:
    """
    Build dashboard ANNOUNCEMENTS section for a SINGLE page (1 broadcast).
    Reads full text — no preview truncation except hard cap.
    Always reflects live DB data (caller should pass fresh query result).
    """
    if not broadcasts:
        return (
            "📢 **ANNOUNCEMENTS**\n"
            "―――――――――――――――――\n\n"
            "🔔 _No announcements yet._\n"
            "_Stay tuned for exclusive content!_"
        )

    total = len(broadcasts)
    page  = page % total            # wrap around safely
    b     = broadcasts[page]

    created_at = b.get("created_at")
    raw_text_value = b.get("message_text")
    raw_text   = str(raw_text_value).strip() if raw_text_value is not None else ""
    media_type = b.get("media_type", "")
    b_index    = b.get("index", page + 1)

    date_str = "—"
    if isinstance(created_at, datetime):
        date_str = created_at.strftime("%b %d, %Y  ·  %I:%M %p")

    if raw_text:
        if len(raw_text) > _ANN_PAGE_MAX_CHARS:
            raw_text = str(raw_text)[:_ANN_PAGE_MAX_CHARS].rsplit(" ", 1)[0] + "…"
        preview = _escape_dashboard_md(raw_text)
    elif media_type:
        preview = f"📎 _[{media_type.capitalize()} content]_"
    else:
        preview = "_[No preview available]_"

    # Broadcast type badge
    if media_type == "photo":
        type_badge = "📷 Photo"
    elif media_type == "video":
        type_badge = "🎥 Video"
    elif media_type == "animation":
        type_badge = "🎞️ GIF"
    elif media_type == "document":
        type_badge = "📄 Document"
    elif media_type == "audio":
        type_badge = "🎵 Audio"
    elif media_type == "voice":
        type_badge = "🎙️ Voice"
    else:
        type_badge = "📝 Text"

    # NEW badge for broadcasts within last 48 h
    is_new = False
    if isinstance(created_at, datetime):
        try:
            created_local = created_at.astimezone(TZ).replace(tzinfo=None) if created_at.tzinfo else created_at
            age = now_local() - created_local
            is_new = age.total_seconds() < 172800   # 48 h
        except Exception:
            pass

    new_tag = " 🆕" if is_new else ""

    return (
        f"📢 **ANNOUNCEMENTS** _· {page + 1} of {total}_\n"
        f"―――――――――――――――――\n\n"
        f"🗂 **Broadcast #{b_index}**{new_tag}  ·  _{type_badge}_\n\n"
        f"{preview}\n\n"
        f"🕐 _{date_str}_"
    )


def _build_dashboard_text(user_name, display_msa_id, member_since, ann_text) -> str:
    """Assemble the full dashboard message."""
    safe_user_name = _escape_dashboard_md(str(user_name or "User"))
    safe_member_since = _escape_dashboard_md(str(member_since or "Unknown"))
    safe_msa_id = str(display_msa_id or "Not Assigned").replace("`", "'")
    return (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"   📊 **YOUR DASHBOARD**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**PROFILE INFORMATION**\n\n"
        f"👤 **Name:** {safe_user_name}\n"
        f"🆔 **MSA+ ID:** `{safe_msa_id}`\n"
        f"📅 **Member Since:** {safe_member_since}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**ACCOUNT STATUS**\n\n"
        f"✅ **Verification:** Confirmed\n"
        f"🏆 **Membership:** Premium Active\n"
        f"⭐ **Access Level:** Full Access\n"
        f"🌐 **Network:** MSA NODE Elite\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{ann_text}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡 **TIP:** Use **SEARCH CODE** to access\n"
        f"vault content from videos instantly.\n\n"
        f"📞 **Need help?** Open a **SUPPORT** ticket\n\n"
        f"💎 *MSA NODE Agent — Your Exclusive Gateway*"
    )

# Dashboard frame chars without ann_text (pre-computed once)
_DASH_FRAME_CHARS = len(_build_dashboard_text("X", "X", "X", ""))

# Live-sync registry: currently-open dashboard messages
# { chat_id: { "message_id": int, "user_id": int, "page": int,
#              "user_name": str, "member_since": str } }
# Polled every 10 s by broadcast_live_sync() — removed on stale/deleted msgs.
_DASHBOARD_ACTIVE_MSGS: dict = {}


# ==========================================
#  MENU HANDLERS
# ==========================================

@dp.message(F.text == "📊 DASHBOARD")
@rate_limit(3.0)  # 3 second cooldown for dashboard
@anti_spam("dashboard")
async def dashboard(message: types.Message):
    """Handle Dashboard button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "DASHBOARD" in suspend_doc.get("bot1_suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nDashboard access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, ACCESS DENIED**\n\n"
                f"You walked away from the **MSA NODE Vault**.\n"
                f"That means you walked away from your dashboard.\n\n"
                f"The system doesn't reward hesitation.\n"
                f"Every second you're out, you're losing visibility on your progress.\n\n"
                f"**The choice is simple:**\n"
                f"• Stay out \u2192 Stay blind.\n"
                f"• Get back in \u2192 Get back to work.\n\n"
                f"💎 **Rejoin the Vault. Reclaim your access.**",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, the **Dashboard** feature is exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return

    user_id   = message.from_user.id
    user_name = message.from_user.first_name or "User"
    msa_id    = get_user_msa_id(user_id)
    display_msa_id = msa_id.replace("+", "") if msa_id else 'Not Assigned'
    member_since = "Unknown"
    dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, _build_ann_page([], 0))
    ann_kb = None

    # 🎬 DASHBOARD ANIMATION
    msg = await message.answer("⏳ Accessing User Database...")
    await asyncio.sleep(ANIM_FAST)

    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Accessing User Database...")
        await asyncio.sleep(0.1)

    await msg.edit_text("🔐 Verifying Identity...")
    await asyncio.sleep(ANIM_MEDIUM)

    await msg.edit_text("📊 Loading Profile Stats...")
    await asyncio.sleep(ANIM_MEDIUM)

    try:
        msa_record = col_msa_ids.find_one({"user_id": user_id})
        if msa_record and "assigned_at" in msa_record:
            member_since = msa_record["assigned_at"].strftime("%B %Y")
        else:
            user_data = col_user_verification.find_one({"user_id": user_id})
            if user_data and "first_start" in user_data:
                member_since = user_data["first_start"].strftime("%B %Y")

        all_broadcasts = _fetch_deduplicated_broadcasts()
        total_bc = len(all_broadcasts)
        page = 0

        ann_text = _build_ann_page(all_broadcasts, page)
        dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)
        if len(dashboard_text) > _DASH_CHAR_LIMIT:
            excess = len(dashboard_text) - _DASH_CHAR_LIMIT + 5
            ann_text = str(ann_text[:-excess]).rsplit(" ", 1)[0] + "…"
            dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)

        if total_bc > 1:
            next_pg = 1
            prev_pg = total_bc - 1
            ann_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️",                 callback_data=f"ann_pg:{user_id}:{prev_pg}"),
                InlineKeyboardButton(text=f"📢 1/{total_bc}", callback_data="ann_noop"),
                InlineKeyboardButton(text="▶️",                 callback_data=f"ann_pg:{user_id}:{next_pg}"),
            ]])
        elif total_bc == 1:
            ann_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📢 1/1", callback_data="ann_noop"),
            ]])
    except Exception as dash_err:
        logger.exception(f"Dashboard build failed for user {user_id}: {dash_err}")

    _final_dash_msg = None
    try:
        await msg.edit_text(
            dashboard_text,
            reply_markup=ann_kb,
            parse_mode=ParseMode.MARKDOWN
        )
        _final_dash_msg = msg
    except Exception as edit_err:
        logger.warning(f"Dashboard edit_text failed: {edit_err}")
        try:
            _final_dash_msg = await message.answer(
                dashboard_text,
                reply_markup=ann_kb,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as send_err:
            logger.error(f"Dashboard fallback send failed: {send_err}")
            _final_dash_msg = await message.answer(
                dashboard_text,
                reply_markup=ann_kb,
            )
    # Register session for live broadcast sync (bot2 edits/deletes reflect instantly)
    if _final_dash_msg:
        _DASHBOARD_ACTIVE_MSGS[message.chat.id] = {
            "message_id": _final_dash_msg.message_id,
            "user_id":    user_id,
            "page":        0,
            "user_name":   user_name,
            "member_since": member_since,
        }
    logger.info(f"User {message.from_user.id} accessed Dashboard")

# ==========================================
# 📢 ANNOUNCEMENT NAVIGATION CALLBACKS
# ==========================================

@dp.callback_query(F.data.startswith("ann_pg:"))
async def ann_page_callback(callback: types.CallbackQuery):
    """Navigate announcement pages in the dashboard (PREV / NEXT)."""
    try:
        parts = callback.data.split(":")
        uid   = int(parts[1])
        page  = int(parts[2])

        # Only the owner of the dashboard can navigate it
        if callback.from_user.id != uid:
            await callback.answer("🚫 This is not your dashboard.", show_alert=True)
            return

        # Rebuild user profile data live from DB
        msa_id         = get_user_msa_id(uid)
        display_msa_id = msa_id.replace("+", "") if msa_id else "Not Assigned"
        user_name      = callback.from_user.first_name or "User"

        member_since = "Unknown"
        msa_record   = col_msa_ids.find_one({"user_id": uid})
        if msa_record and "assigned_at" in msa_record:
            member_since = msa_record["assigned_at"].strftime("%B %Y")
        else:
            user_data = col_user_verification.find_one({"user_id": uid})
            if user_data and "first_start" in user_data:
                member_since = user_data["first_start"].strftime("%B %Y")

        # Fetch broadcasts live (fresh DB query — picks up bot2 edits/deletes instantly)
        all_broadcasts = _fetch_deduplicated_broadcasts()
        total_bc = len(all_broadcasts)

        if total_bc == 0:
            # All broadcasts deleted via bot2 — remove stale nav buttons and update text live
            ann_text = _build_ann_page([], 0)
            dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)
            try:
                await callback.message.edit_text(
                    dashboard_text,
                    reply_markup=None,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                try:
                    await callback.message.edit_text(
                        dashboard_text,
                        reply_markup=None,
                    )
                except Exception:
                    pass
            await callback.answer("No announcements available.", show_alert=False)
            return

        page     = page % total_bc    # wrap around safely
        prev_pg  = (page - 1) % total_bc
        next_pg  = (page + 1) % total_bc

        ann_text = _build_ann_page(all_broadcasts, page)
        dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)

        # Guard: hard trim if still over limit
        if len(dashboard_text) > _DASH_CHAR_LIMIT:
            excess = len(dashboard_text) - _DASH_CHAR_LIMIT + 5
            ann_text = str(ann_text[:-excess]).rsplit(" ", 1)[0] + "…"
            dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)

        # Rebuild nav keyboard — only arrows when more than 1 broadcast; no duplicates
        if total_bc == 1:
            ann_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📢 1/1", callback_data="ann_noop"),
            ]])
        else:
            ann_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️",                      callback_data=f"ann_pg:{uid}:{prev_pg}"),
                InlineKeyboardButton(text=f"📢 {page + 1}/{total_bc}", callback_data="ann_noop"),
                InlineKeyboardButton(text="▶️",                      callback_data=f"ann_pg:{uid}:{next_pg}"),
            ]])

        try:
            await callback.message.edit_text(
                dashboard_text,
                reply_markup=ann_kb,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            await callback.message.edit_text(
                dashboard_text,
                reply_markup=ann_kb,
            )
        # Keep live-sync session up-to-date with latest page
        _DASHBOARD_ACTIVE_MSGS[callback.message.chat.id] = {
            "message_id":  callback.message.message_id,
            "user_id":     uid,
            "page":         page,
            "user_name":   user_name,
            "member_since": member_since,
        }
        await callback.answer()
    except Exception as e:
        logger.error(f"ann_page_callback error: {e}")
        await callback.answer("Error loading page. Please re-open dashboard.", show_alert=True)


@dp.callback_query(F.data == "ann_noop")
async def ann_noop_callback(callback: types.CallbackQuery):
    """No-op: page indicator button in announcement nav bar."""
    await callback.answer()


# ==========================================
# 🚫 CANCEL SEARCH HANDLER
# ==========================================
@dp.message(
    F.text == "❌ CANCEL",
    ~StateFilter(SearchCodeStates.waiting_for_code, SearchCodeStates.waiting_for_first_code)
)
@rate_limit(1.0)  # 1 second cooldown for cancel
async def cancel_search_handler(message: types.Message, state: FSMContext):
    """Handle cancel button in search flow"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    user_id = message.from_user.id

    # 🔒 STRICT VAULT GATE — Only vault members can cancel and access main menu
    is_in_vault = await check_channel_membership(user_id)
    if not is_in_vault:
        # Non-vault user typed ❌ CANCEL manually — block it silently or remind them
        await state.clear()
        user_data = get_user_verification_status(user_id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔐 **VAULT ACCESS REQUIRED**\n\n"
            f"Hey {user_name}, this feature is exclusive to Vault Members.\n\n"
            f"📌 **Click the join button below** to unlock full access.\n\n"
            f"_Once joined, all features will be available immediately._",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Animation: Aborting operation
    msg = await message.answer("⚠️")
    await asyncio.sleep(ANIM_MEDIUM)
    await msg.edit_text("⚠️ **ABORTING...**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    await msg.edit_text("🔓 **UNLOCKING SESSION...**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    await safe_delete_message(msg)
    
    await state.clear()
    await message.answer(
        "❌ **SEARCH CANCELLED**\n\n`Operation aborted. Returning to main menu...`",
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} cancelled search")

@dp.message(F.text == "🔍 SEARCH CODE")
@rate_limit(2.0)  # 2 second cooldown for search
@anti_spam("search")
async def search(message: types.Message, state: FSMContext):
    """Handle Search button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "SEARCH_CODE" in suspend_doc.get("bot1_suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nSearch Code access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, AGENT ACCESS PAUSED**\n\n"
                f"You tried to access another premium asset, but your complimentary pass has been used.\n\n"
                f"**The MSA Node Agent is an exclusive ecosystem.** By verifying your free membership, you will instantly unlock:\n\n"
                f"📂 **Unlimited Blueprints:** Seamless delivery of all future PDFs.\n"
                f"🤖 **Elite AI Tools:** Access to our private arsenal.\n"
                f"💎 **The Inner Circle:** Strategies reserved strictly for the Vault.\n\n"
                f"To resume search and unlock the entire system at zero cost, simply join the Vault below and return here.\n\n"
                f"*The content is waiting. The choice is yours.*",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, the **Search Code** feature is exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    # 🎬 CYBER LOADING ANIMATION
    msg = await message.answer("📡 Establishing Secure Uplink...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Establishing Secure Uplink...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("🔍 Initializing Code Search Protocol...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await safe_delete_message(msg)
    first_name = message.from_user.first_name
    
    # Add cancel button
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    
    await message.answer(
        f"🔒 **AUTHENTICATION REQUIRED**\n\n{first_name}, the agent is waiting.\nEnter your **MSA CODE** to decrypt the asset.\n\n*Precision is key.*\n\n`ENTER MSA CODE BELOW:`\n\n⚪️ _Reply 'CANCEL' to cancel this operation._",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(SearchCodeStates.waiting_for_code)
    logger.info(f"User {message.from_user.id} initiated Search Code")

@dp.message(SearchCodeStates.waiting_for_code)
@rate_limit(1.5)
@anti_spam("process_search")
async def process_search_code(message: types.Message, state: FSMContext):
    """Process the MSA code input"""
    just_consumed_grace = False
    just_consumed_grace = False
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await state.clear()
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    incoming_text = (message.text or "").strip()
    if not incoming_text:
        await message.answer(
            "⚠️ **INVALID INPUT**\n\nSend a valid MSA CODE or tap **❌ CANCEL**.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Allow clean escape from code-entry state when user taps menu/navigation buttons.
    if incoming_text == "📺 WATCH TUTORIAL":
        await state.clear()
        await _handle_main_tutorial_request(message, state)
        return

    if incoming_text in {"❌ CANCEL", "CANCEL", "🏠 MAIN MENU", "🔙 BACK TO MENU"}:
        _cancel_uid = message.from_user.id
        _cancel_in_vault = await check_channel_membership(_cancel_uid)
        # 🔒 STRICT SECURITY: Only vault members can cancel — non-vault users must enter code
        if not _cancel_in_vault:
            first_name = message.from_user.first_name or "User"
            await message.answer(
                f"⚡ **AGENT ACTIVATED, {first_name}**\n\n"
                f"You've just unlocked access to the **MSA NODE** system.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔑 **ENTER YOUR MSA CODE**\n\n"
                f"Every piece of premium content — blueprints, AI tools, guides — is unlocked with a unique **MSA Code**.\n\n"
                f"📸 Find your code on **Instagram** or **YouTube**.\n"
                f"Then type it below to unlock your first blueprint instantly.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"`TYPE YOUR MSA CODE BELOW:`",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )
            return  # State preserved — user must enter a valid code
        # ✅ Vault member — allow cancel and return to menu
        await state.clear()
        await message.answer(
            "❌ **SEARCH CANCELLED**\n\n`Operation aborted. Returning to main menu...`",
            reply_markup=get_user_menu(_cancel_uid),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    code = incoming_text
    
    # 🎬 CYBER LOADING ANIMATION (Common for all)
    msg = await message.answer("📡 Establishing Secure Uplink...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Establishing Secure Uplink...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("🔍 Verifying MSA CODE...")
    await asyncio.sleep(ANIM_SLOW)
    
    # 🔍 DATABASE QUERY (Case-insensitive, ReDoS-safe)
    # Sanitize user input: strip invisible unicode + escape regex metacharacters
    import re as _re
    _safe_code = _re.escape(code.encode('ascii', 'ignore').decode('ascii').strip())
    pdf_doc = col_pdfs.find_one({"msa_code": {"$regex": f"^{_safe_code}$", "$options": "i"}})
    
    # Check if code exists
    if not pdf_doc:
        # ❌ INVALID CODE HANDLER
        await msg.edit_text("🚫 ACCESS DENIED")
        await asyncio.sleep(ANIM_SLOW)   
        await safe_delete_message(msg)
        
        # Get state data to check context
        state_data = await state.get_data()
        is_yt_flow = state_data.get("is_yt_flow", False)
        
        # Personalize error message
        first_name = message.from_user.first_name
        is_vault_member = await check_channel_membership(message.from_user.id)
        
        # Invalid code — YT for codes, IG for more content
        error_msg = (
            f"⚠️ **INCORRECT MSA CODE**\n\n"
            f"{first_name}, that code doesn't exist in the system.\n"
            f"**Please enter the correct code and try again.**\n\n"
            f"▶️ **MSA Codes are found in YouTube videos only.**\n"
            f"Watch the video, find the code, and enter it here.\n\n"
            f"📸 **On Instagram?** Explore more exclusive blueprints and premium content — no codes needed."
        )
        if is_vault_member:
            error_msg += "\n\n⚪️ _Press 'CANCEL' to abort._"
        retry_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ GET CODE FROM YOUTUBE", url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text="📸 MORE BLUEPRINTS ON INSTAGRAM", url=INSTAGRAM_LINK)]
        ])
        await message.answer(error_msg, reply_markup=retry_kb, parse_mode=ParseMode.MARKDOWN)
        # Keep state active — user can retry or cancel
        return

    # ✅ VALID CODE HANDLER
    await msg.edit_text("🔐 Decrypting Access Code...")
    await asyncio.sleep(ANIM_SLOW)
    
    # 🎁 CONSUME GRACE-PASS if available (user is not in vault but has one free pass)
    user_id = message.from_user.id
    user_data = get_user_verification_status(user_id)
    grace_allowed = user_data.get('grace_allowed', False)
    grace_consumed = user_data.get('grace_consumed', False)
    is_in_vault = await check_channel_membership(user_id)
    
    if not is_in_vault and grace_allowed and not grace_consumed:
        # User has grace and hasn't used it yet - consume it now atomically
        try:
            col_user_verification.update_one(
                {
                    "user_id": user_id,
                    "grace_allowed": True,
                    "grace_consumed": False  # Ensure not already consumed (atomic check)
                },
                {
                    "$set": {
                        "grace_consumed": True,
                        "grace_consumed_at": now_local(),
                        "grace_consumed_via": "UNKNOWN_SEARCH"
                    }
                }
            )
            just_consumed_grace = True
            logger.info(f"✅ Grace-pass consumed for user {user_id} via SEARCH CODE")
        except Exception as grace_err:
            logger.error(f"⚠️ Failed to consume grace for user {user_id}: {grace_err}")
    
    # 📊 TRACK CLICK ANALYTICS for YT Code clicks
    try:
        yt_uid = message.from_user.id
        # Deduplicated YT code click — only count each user once per PDF
        if _is_new_unique_click(yt_uid, pdf_doc["_id"], "yt_code"):
            col_pdfs.update_one(
                {"_id": pdf_doc["_id"]},
                {
                    "$inc": {"yt_code_clicks": 1, "clicks": 1},
                    "$set": {"last_yt_code_click": now_local(), "last_clicked_at": now_local()}
                }
            )
        # Record first-touch source — bot2_user_tracking written only on vault join
        _store_initial_source(yt_uid, "YTCODE")
        logger.info(f"📊 Analytics: User {yt_uid} entered YT code for PDF '{pdf_doc.get('name')}'")
    except Exception as analytics_err:
        logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")
    
    # Personalize the success message
    first_name = message.from_user.first_name
    await msg.edit_text(f"✅ **IDENTITY CONFIRMED: {first_name}**\n\n`Secure Delivery In Progress...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_DELAY) # Slightly longer to let them see their name
    
    # Delete loading message to clean up
    await safe_delete_message(msg)

    # Get state data to check context
    state_data = await state.get_data()
    is_yt_flow = state_data.get("is_yt_flow", False)

    # DYNAMIC CONTENT SELECTION BASED ON CONTEXT
    if is_yt_flow:
        # User came from YT -> Treat as YT Source -> Show IG Titles/Buttons (Cross-pollinate)
        # 1. PDF Title: Standard
        pdf_title_template = CONTENT_PACKS["PDF_TITLES"][secrets.randbelow(len(CONTENT_PACKS["PDF_TITLES"]))]
        
        # 2. Affiliate Title: Standard
        aff_title_text = CONTENT_PACKS["AFFILIATE_TITLES"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_TITLES"]))]
        
        # 3. Network Message: FORCE IG CONTENT
        # Use IG Video Titles (since they are watching on YT, we sell them on IG)
        msa_code_template = CONTENT_PACKS["IG_VIDEO_TITLES"][secrets.randbelow(len(CONTENT_PACKS["IG_VIDEO_TITLES"]))]
        
        # Use IG Buttons (Force them to IG)
        # We need a list of just IG buttons to pick from
        network_btn_text = CONTENT_PACKS["IG_CODES_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["IG_CODES_BUTTONS"]))]
        network_url = INSTAGRAM_LINK
        
    else:
        # Standard Manual Entry -> Randomize or Standard Logic
        # For now, keep existing random logic or define a "Neutral" flow?
        # Let's keep existing random mix for manual entry
        pdf_title_template = CONTENT_PACKS["PDF_TITLES"][secrets.randbelow(len(CONTENT_PACKS["PDF_TITLES"]))]
        aff_title_text = CONTENT_PACKS["AFFILIATE_TITLES"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_TITLES"]))]
        msa_code_template = CONTENT_PACKS["MSACODE"][secrets.randbelow(len(CONTENT_PACKS["MSACODE"]))]
        network_btn_text = None # Will use dual buttons below

    # Format Titles
    try:
        pdf_title_text = pdf_title_template.format(name=first_name)
    except:
        pdf_title_text = str(pdf_title_template)
        
    try:
        msa_code_text = msa_code_template.format(name=first_name)
    except:
        msa_code_text = str(msa_code_template)
    
    # Retrieve Links from DB
    pdf_link = pdf_doc.get("link") or BOT_FALLBACK_LINK
    affiliate_link = pdf_doc.get("affiliate_link") or BOT_FALLBACK_LINK

    # 1️⃣ SEND PDF MESSAGE (Standard)
    # ... (same as before) ...
    pdf_btn_text = CONTENT_PACKS["PDF_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["PDF_BUTTONS"]))]
    pdf_footer_template = CONTENT_PACKS["PDF_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["PDF_FOOTERS"]))]
    try:
        pdf_footer_text = pdf_footer_template.format(name=first_name)
    except:
        pdf_footer_text = pdf_footer_template
        
    pdf_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=pdf_btn_text, url=pdf_link)]])
    await message.answer(
        f"{pdf_title_text}\n\n`{pdf_footer_text}`",
        reply_markup=pdf_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # ⏳ SEQUENCE DOT ANIMATION 1
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 2️⃣ SEND AFFILIATE MESSAGE with Footer
    # Select random footer
    aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
    try:
        aff_footer_text = aff_footer_template.format(name=first_name)
    except:
        aff_footer_text = aff_footer_template
    
    aff_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💰 ACCESS OPPORTUNITY", url=affiliate_link)]])
    await message.answer(
        f"{aff_title_text}\n\n━━━━━━━━━━━━━━━━\n`{aff_footer_text}`",
        reply_markup=aff_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # ⏳ SEQUENCE DOT ANIMATION 2
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 3️⃣ SEND NETWORK MESSAGE (Context-Aware)
    
    if is_yt_flow:
        # YT Flow: Show single button to IG with footer
        network_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=network_btn_text, url=network_url)]
        ])
        # Add random IG Video Footer
        ig_footer_template = CONTENT_PACKS["IG_VIDEO_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["IG_VIDEO_FOOTERS"]))]
        try:
            ig_footer_text = ig_footer_template.format(name=first_name)
        except:
            ig_footer_text = ig_footer_template
        msa_code_text += f"\n\n━━━━━━━━━━━━━━━━\n`{ig_footer_text}`"
    else:
        # Standard Flow: Dual Buttons (YT + IG)
        yt_btn_text_std, ig_btn_text_std = CONTENT_PACKS["MSACODE_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["MSACODE_BUTTONS"]))]
        footer_template_std = CONTENT_PACKS["MSACODE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["MSACODE_FOOTERS"]))]
        try:
             footer_text_std = footer_template_std.format(name=first_name)
        except:
             footer_text_std = footer_template_std

        network_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=yt_btn_text_std, url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text=ig_btn_text_std, url=INSTAGRAM_LINK)]
        ])
        # Append footer for standard flow if needed
        msa_code_text += f"\n\n━━━━━━━━━━━━━━━━\n`{footer_text_std}`"
    
    await message.answer(
        f"{msa_code_text}",
        reply_markup=network_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Log success
    logger.info(f"User {message.from_user.id} accessed content for code {code} (Index: {pdf_doc.get('index')}) | Context: {'YT Flow' if is_yt_flow else 'Manual'}")
    
    # DO NOT clear state - keep loop active
    # Re-prompt for another MSA CODE
    await asyncio.sleep(ANIM_DELAY)  # Brief pause after content delivery
    
    if just_consumed_grace:
        await send_psychological_vault_lock_message(user_id)
    else:
        await message.answer(
            f"🔒 **AUTHENTICATION REQUIRED**\n\n{first_name}, the agent is waiting.\nEnter your **MSA CODE** to decrypt the asset.\n\n*Precision is key.*\n\n`ENTER MSA CODE BELOW:`\n\n⚪️ _Reply 'CANCEL' to cancel this operation._",
            parse_mode=ParseMode.MARKDOWN
        )
    # State remains active - user can enter another code or cancel



@dp.message(SearchCodeStates.waiting_for_first_code)
@rate_limit(1.5)
@anti_spam("process_first_search")
async def process_first_unknown_start_code(message: types.Message, state: FSMContext):
    """Process the MSA code input for first-time unknown start"""
    user_id = message.from_user.id
    incoming_text = (message.text or "").strip()

    # 🔒 STRICT SECURITY: Cancel only works for vault members — non-vault must enter code
    if incoming_text in {"❌ CANCEL", "CANCEL", "🏠 MAIN MENU", "🔙 BACK TO MENU"}:
        _f_cancel_uid = message.from_user.id
        _f_cancel_in_vault = await check_channel_membership(_f_cancel_uid)
        if not _f_cancel_in_vault:
            # Non-vault first-time user — block cancel, keep state active
            first_name = message.from_user.first_name or "User"
            await message.answer(
                f"⚡ **AGENT ACTIVATED, {first_name}**\n\n"
                f"You've just unlocked access to the **MSA NODE** system.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔑 **ENTER YOUR MSA CODE**\n\n"
                f"Every piece of premium content — blueprints, AI tools, guides — is unlocked with a unique **MSA Code**.\n\n"
                f"📸 Find your code on **Instagram** or **YouTube**.\n"
                f"Then type it below to unlock your first blueprint instantly.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"`TYPE YOUR MSA CODE BELOW:`",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode=ParseMode.MARKDOWN
            )
            return  # State preserved — must enter valid code
        # Vault member — allow cancel
        await state.clear()
        await message.answer(
            "❌ **SEARCH CANCELLED**\n\n`Operation aborted. Returning to main menu...`",
            reply_markup=get_user_menu(_f_cancel_uid),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if not incoming_text:
        await message.answer("⚠️ Please type a valid MSA CODE.")
        return

    code = incoming_text

    # 🎬 CYBER LOADING ANIMATION
    msg = await message.answer("📡 Establishing Secure Uplink...")
    await asyncio.sleep(ANIM_MEDIUM)

    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Establishing Secure Uplink...")
        await asyncio.sleep(0.1)

    await msg.edit_text("🔍 Verifying MSA CODE...")
    await asyncio.sleep(ANIM_SLOW)

    # 🔍 DATABASE QUERY (Case-insensitive, ReDoS-safe)
    import re as _re
    _safe_code = _re.escape(code.encode('ascii', 'ignore').decode('ascii').strip())
    pdf_doc = col_pdfs.find_one({"msa_code": {"$regex": f"^{_safe_code}$", "$options": "i"}})

    if not pdf_doc:
        await msg.edit_text("🚫 ACCESS DENIED")
        await asyncio.sleep(ANIM_SLOW)
        await safe_delete_message(msg)

        first_name = message.from_user.first_name
        is_vault_member = await check_channel_membership(message.from_user.id)
        
        # Invalid code — YT for codes, IG for more content
        error_msg = (
            f"⚠️ **INCORRECT MSA CODE**\n\n"
            f"{first_name}, that code doesn't exist in the system.\n"
            f"**Please enter the correct code and try again.**\n\n"
            f"▶️ **MSA Codes are found in YouTube videos only.**\n"
            f"Watch the video, find the code, and enter it here.\n\n"
            f"📸 **On Instagram?** Explore more exclusive blueprints and premium content — no codes needed."
        )
        if is_vault_member:
            error_msg += "\n\n⚪️ _Press 'CANCEL' to abort._"
        retry_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ GET CODE FROM YOUTUBE", url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text="📸 MORE BLUEPRINTS ON INSTAGRAM", url=INSTAGRAM_LINK)]
        ])
        await message.answer(error_msg, reply_markup=retry_kb, parse_mode=ParseMode.MARKDOWN)
        return

    # ✅ VALID CODE — Identity confirmation
    await msg.edit_text("🔐 Decrypting Access Code...")
    await asyncio.sleep(ANIM_SLOW)

    first_name = message.from_user.first_name
    await msg.edit_text(
        f"✅ **IDENTITY CONFIRMED: {first_name}**\n\n`Secure Delivery In Progress...`",
        parse_mode=ParseMode.MARKDOWN
    )
    await asyncio.sleep(ANIM_DELAY)
    await safe_delete_message(msg)

    # 1️⃣ PDF DELIVERY
    pdf_link = pdf_doc.get("link") or BOT_FALLBACK_LINK
    pdf_btn_text = CONTENT_PACKS["PDF_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["PDF_BUTTONS"]))]
    pdf_title_template = CONTENT_PACKS["PDF_TITLES"][secrets.randbelow(len(CONTENT_PACKS["PDF_TITLES"]))]
    pdf_footer_template = CONTENT_PACKS["PDF_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["PDF_FOOTERS"]))]
    try:
        pdf_title_text = pdf_title_template.format(name=first_name)
    except:
        pdf_title_text = str(pdf_title_template)
    try:
        pdf_footer_text = pdf_footer_template.format(name=first_name)
    except:
        pdf_footer_text = str(pdf_footer_template)

    pdf_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=pdf_btn_text, url=pdf_link)]
    ])
    await message.answer(
        f"{pdf_title_text}\n\n`{pdf_footer_text}`",
        reply_markup=pdf_kb,
        parse_mode=ParseMode.MARKDOWN
    )

    # ⏳ DOT ANIMATION 1
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 2️⃣ AFFILIATE DELIVERY
    affiliate_link = pdf_doc.get("affiliate_link") or BOT_FALLBACK_LINK
    aff_title_text = CONTENT_PACKS["AFFILIATE_TITLES"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_TITLES"]))]
    aff_footer_template = CONTENT_PACKS["AFFILIATE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["AFFILIATE_FOOTERS"]))]
    try:
        aff_footer_text = aff_footer_template.format(name=first_name)
    except:
        aff_footer_text = str(aff_footer_template)

    aff_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 ACCESS OPPORTUNITY", url=affiliate_link)]
    ])
    await message.answer(
        f"{aff_title_text}\n\n━━━━━━━━━━━━━━━━\n`{aff_footer_text}`",
        reply_markup=aff_kb,
        parse_mode=ParseMode.MARKDOWN
    )

    # ⏳ DOT ANIMATION 2
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 3️⃣ NETWORK / MSA CODE MESSAGE (dual YT + IG buttons)
    msa_code_template = CONTENT_PACKS["MSACODE"][secrets.randbelow(len(CONTENT_PACKS["MSACODE"]))]
    try:
        msa_code_text = msa_code_template.format(name=first_name)
    except:
        msa_code_text = str(msa_code_template)

    yt_btn_text_std, ig_btn_text_std = CONTENT_PACKS["MSACODE_BUTTONS"][secrets.randbelow(len(CONTENT_PACKS["MSACODE_BUTTONS"]))]
    footer_template_std = CONTENT_PACKS["MSACODE_FOOTERS"][secrets.randbelow(len(CONTENT_PACKS["MSACODE_FOOTERS"]))]
    try:
        footer_text_std = footer_template_std.format(name=first_name)
    except:
        footer_text_std = str(footer_template_std)

    network_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=yt_btn_text_std, url=YOUTUBE_LINK)],
        [InlineKeyboardButton(text=ig_btn_text_std, url=INSTAGRAM_LINK)]
    ])
    await message.answer(
        f"{msa_code_text}\n\n━━━━━━━━━━━━━━━━\n`{footer_text_std}`",
        reply_markup=network_kb,
        parse_mode=ParseMode.MARKDOWN
    )

    logger.info(f"First-time user {user_id} got full content delivery for code '{code}'")

    await asyncio.sleep(ANIM_DELAY)

    # 4. Consume grace atomically
    await state.clear()
    try:
        col_user_verification.update_one(
            {
                "user_id": user_id,
                "grace_allowed": True,
                "grace_consumed": False,
            },
            {
                "$set": {
                    "grace_consumed": True,
                    "grace_consumed_at": now_local(),
                    "grace_consumed_via": "UNKNOWN_SEARCH",
                }
            }
        )
        logger.info(f"✅ Grace-pass consumed for user {user_id} via UNKNOWN_SEARCH")
    except Exception as _g_err:
        logger.error(f"⚠️ Grace consume failed for {user_id}: {_g_err}")

    # 5. Vault unlock message
    await send_psychological_vault_lock_message(user_id)
    update_verification_status(user_id, verification_msg_id=None)



async def _handle_main_tutorial_request(message: types.Message, state: FSMContext):
    """Shared tutorial flow used by both menu and state-escape routing."""
    if await _check_freeze(message):
        return
    if await check_maintenance_mode(message):
        return

    user_id = message.from_user.id
    ban_doc = await check_if_banned(user_id)
    if ban_doc:
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_doc.get("ban_type", "permanent")),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    suspend_doc = col_suspended_features.find_one({"user_id": user_id})
    if suspend_doc and "TUTORIAL" in suspend_doc.get("bot1_suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nTutorial access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    is_vault = await check_channel_membership(user_id)
    if not is_vault:
        user_data = get_user_verification_status(user_id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, TUTORIAL IS VAULT-EXCLUSIVE**\n\n"
                f"The tutorial video is reserved for verified vault members.\n\n"
                f"Rejoin the vault to unlock it instantly.\n\n"
                f"💎 **Rejoin. Watch. Learn.**",
                reply_markup=get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, the **Tutorial** is exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return

    await state.clear()

    msg = await message.answer("📡 Loading agent tutorial...")
    await asyncio.sleep(ANIM_FAST)
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Fetching tutorial link...")
        await asyncio.sleep(0.1)
    await safe_delete_message(msg)

    try:
        tut_doc = db["bot3_tutorials"].find_one({"type": "PK"})
        link = tut_doc.get("link") if tut_doc else None
    except Exception as e:
        logger.warning(f"Main menu tutorial lookup failed for {user_id}: {e}")
        link = None

    if not link:
        await message.answer(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎬 **AGENT TUTORIAL IS COMING**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🛠️ **It's being prepared for you right now.**\n\n"
            "Your exclusive MSA NODE video guide is almost ready.\n"
            "When it drops, you'll find it right here — one tap away.\n\n"
            "In the meantime, your vault is fully unlocked:\n"
            "  📊 **Dashboard** — your MSA+ ID & live stats\n"
            "  🔍 **Search Code** — unlock exclusive content\n"
            "  📖 **Agent Guide** — everything you need to know\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔔 _Check back soon. It drops shortly!_ 🚀",
            reply_markup=get_user_menu(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ WATCH MSA NODE AGENT TUTORIAL", url=link)]
    ])
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎬 **YOUR MSA NODE AGENT TUTORIAL**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💎 **This video was made for you.**\n\n"
        "Everything you need to know about MSA NODE AGENT —\n"
        "how it works, what you have access to, and\n"
        "exactly how to get the most from your membership.\n\n"
        "🎯 **One watch. Zero confusion. Full clarity.**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "_Tap below and start right now ⬇️_",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )
    await message.answer(
        "_Questions? **📞 SUPPORT** is always available 24/7._",
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} accessed TUTORIAL from main menu")

@dp.message(F.text == "📺 WATCH TUTORIAL")
@rate_limit(3.0)
@anti_spam("tutorial")
async def main_tutorial_handler(message: types.Message, state: FSMContext):
    """Handle 📺 TUTORIAL button from main menu."""
    await _handle_main_tutorial_request(message, state)

# ──────────────────────────────────────────────────────────────
# 📜 RULES SYSTEM — paginated member rules (3 pages)
# ──────────────────────────────────────────────────────────────

_RULES_PAGES = [
    # Page 1 / 3 — Introduction + Rule 1 (Conduct) + Rule 2 (Content Security)
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📜  **MSA NODE — MEMBER RULES**  ·  1 / 3\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "By accessing MSA NODE Agent you confirm that you have **read, understood, and accepted "
        "every rule below** in full. These rules are binding from the moment you first interact "
        "with this agent. Ignorance of any rule is not a valid defence.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**⚖️  RULE 1 — CONDUCT & RESPECT**\n\n"
        "Every member is held to a high standard of conduct at all times.\n\n"
        "  • Treat every member, admin, and team representative with full respect — no exceptions\n"
        "  • Harassment, threats, hate speech, discrimination, or abusive language = **immediate ban**\n"
        "  • Impersonating MSA NODE admins, staff, or other members — strictly prohibited\n"
        "  • Do not argue against, publicly dispute, or undermine admin decisions — use 📞 SUPPORT\n"
        "  • Unsolicited promotion of other services, bots, or communities is forbidden\n"
        "  • Do not scheme, scam, or manipulate other members in any way\n\n"
        "  🔴 _Zero-tolerance violation — immediate permanent ban, no appeal_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔐  RULE 2 — CONTENT SECURITY & VAULT CONFIDENTIALITY**\n\n"
        "The vault contains exclusive, proprietary content. Its protection is a shared responsibility.\n\n"
        "  • All vault PDFs, blueprints, guides, links, and files are **strictly confidential**\n"
        "  • Do NOT share, forward, upload, or distribute any vault content on any platform\n"
        "     _(Includes Telegram, WhatsApp, Instagram, TikTok, YouTube, Discord, and all others)_\n"
        "  • Do NOT screen-record, screenshot, or re-photograph vault content for redistribution\n"
        "  • Your **MSA+ ID** is personal and non-transferable — sharing it is a security violation\n"
        "  • Do not resell, re-package, or monetise any vault material in any form\n"
        "  • Sharing a direct link to vault content externally is treated as a deliberate breach\n\n"
        "  🔴 _Zero-tolerance violation — immediate permanent ban + legal referral if applicable_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 1 of 3_"
    ),
    # Page 2 / 3 — Rule 3 (Account Integrity) + Rule 4 (Agent Usage) + Rule 5 (Support Tickets)
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📜  **MSA NODE — MEMBER RULES**  ·  2 / 3\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🛡  RULE 3 — ACCOUNT INTEGRITY & PRIVACY**\n\n"
        "Your account and personal identity must be used responsibly.\n\n"
        "  • You may only hold **one active MSA NODE account** — duplicate accounts are prohibited\n"
        "  • Creating a new account to bypass a ban, suspension, or restriction is a serious violation\n"
        "  • Do not access, harvest, or attempt to store another member's personal data or MSA+ ID\n"
        "  • Do not share your Telegram account access with others to bypass access controls\n"
        "  • If your account has been compromised or misused, open a 📞 SUPPORT ticket immediately\n"
        "  • Report scam accounts, impersonation attempts, or suspicious links to the admin team at once\n\n"
        "  ⚠️ _Violation result: Account suspension or permanent ban depending on severity_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🤖  RULE 4 — AGENT USAGE & SYSTEM INTEGRITY**\n\n"
        "MSA NODE Agent is a precision-engineered system — use it exactly as intended.\n\n"
        "  • Do not spam buttons, commands, or messages — rate limits are enforced and violations are logged\n"
        "  • Do not attempt to probe, reverse-engineer, stress-test, or exploit any part of this agent\n"
        "  • Automated scripts, macros, bots, or third-party tools interacting with this agent are **forbidden**\n"
        "  • One action at a time — rapid repeated presses will trigger automatic suspension\n"
        "  • Do not inject commands, payloads, or manipulated input into any agent field\n"
        "  • Attempting to access admin features or restricted content without authorisation is a violation\n"
        "  • All interactions with this agent are logged for security, moderation, and audit purposes\n\n"
        "  🔴 _Violation result: Immediate feature suspension or permanent ban_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🎫  RULE 5 — SUPPORT TICKETS**\n\n"
        "The support system is a resource — use it properly and professionally.\n\n"
        "  • Submit only genuine, clearly described issues — vague or spam tickets will be closed\n"
        "  • **One active ticket at a time** — duplicate submissions delay the queue for everyone\n"
        "  • Your ticket must include enough detail for the admin to act without back-and-forth questions\n"
        "  • ✅ Accepted: Text description · One photo with caption · One video (max 3 min, max 50 MB)\n"
        "  • ❌ Not accepted: Voice notes · Documents · GIFs · Stickers · Audio files\n"
        "  • Do not reopen a closed ticket for the same issue without new, relevant information\n"
        "  • Abusing or misusing the support system (e.g. false reports) is itself a rule violation\n\n"
        "  ⚠️ _Violation result: Ticket access suspended_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 2 of 3_"
    ),
    # Page 3 / 3 — Violations + Zero-Tolerance + Appeals + Update Notice
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📜  **MSA NODE — MEMBER RULES**  ·  3 / 3\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🚨  VIOLATIONS & ENFORCEMENT**\n\n"
        "MSA NODE operates a structured, logged, and consistently applied enforcement system.\n\n"
        "  ⚡  **STRIKE 1 — FORMAL WARNING**\n"
        "     A formal notice is issued. The relevant feature may be temporarily restricted.\n"
        "     The member is expected to course-correct immediately.\n"
        "     A repeat of the same violation escalates directly to Strike 2.\n\n"
        "  ⛔  **STRIKE 2 — FEATURE SUSPENSION**\n"
        "     All or selected features are suspended for a period set by the admin team.\n"
        "     The member retains access to 📞 SUPPORT only.\n"
        "     Suspension duration is non-negotiable once issued.\n\n"
        "  🔴  **STRIKE 3 — PERMANENT BAN**\n"
        "     Full removal from MSA NODE Agent with no reinstatement.\n"
        "     The member's MSA+ ID is flagged and all associated accounts are blocked.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔴  ZERO-TOLERANCE VIOLATIONS — IMMEDIATE PERMANENT BAN**\n"
        "_(No prior warning issued under any circumstance)_\n\n"
        "  › Scamming, defrauding, or manipulating vault members\n"
        "  › Redistributing, leaking, or monetising vault content\n"
        "  › Impersonating MSA NODE Agent, admins, or staff\n"
        "  › Hacking, exploiting, or compromising the bot or its infrastructure\n"
        "  › Creating duplicate accounts after a permanent ban\n"
        "  › Providing deliberately false information in an appeal\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📩  APPEALS PROCESS**\n\n"
        "If you believe an action on your account was issued in error:\n\n"
        "  ① Tap **📞 SUPPORT** in the main menu\n"
        "  ② Select the most relevant support category\n"
        "  ③ Tap **🎫 RAISE A TICKET**\n"
        "  ④ Title your ticket: _APPEAL — [Your MSA+ ID]_\n"
        "  ⑤ State clearly and honestly why you believe the action was incorrect\n"
        "  ⑥ Wait for an admin to review — do not open duplicate appeal tickets\n\n"
        "  ❌ _Appeals are rejected if:_\n"
        "  _› False information is provided_\n"
        "  _› The violation was zero-tolerance_\n"
        "  _› The same appeal was previously submitted and closed_\n"
        "  _› No MSA+ ID is included_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📌  RULES UPDATE NOTICE**\n\n"
        "These rules are subject to update at any time without prior personal notice.\n"
        "Continued use of MSA NODE Agent constitutes full acceptance of the current version.\n"
        "Rule updates are announced in the vault channel.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent_\n"
        "_Every interaction confirms your acceptance of these rules._\n"
        "_Enforced: 24 / 7_"
    ),
]

def _rules_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for MSA NODE Rules — PREV / NEXT / HOME."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="🏠 MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


@dp.message(F.text == "📜 RULES")
@rate_limit(3.0)  # 3 second cooldown for rules
@anti_spam("rules")
async def rules_regulations(message: types.Message, state: FSMContext):
    """Handle Rules button — opens paginated rules starting at page 1."""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "RULES" in suspend_doc.get("bot1_suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nRules access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, RULES ARE VAULT-ONLY**\n\n"
                f"The rules aren't public. They're protected.\n"
                f"Only vault members see the blueprint.\n\n"
                f"**You want the rules?**\n"
                f"Earn them. Join the vault.\n\n"
                f"💎 **Rejoin. See the system.**",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, the **Rules** are exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    # 🎬 RULES ANIMATION
    msg = await message.answer("⚖️ Accessing Protocol Database...")
    await asyncio.sleep(ANIM_FAST)
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Accessing Protocol Database...")
        await asyncio.sleep(0.1)
    await msg.edit_text("📜 Loading Member Rules...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)

    page = 1
    await state.set_state(RulesStates.viewing_rules)
    await state.update_data(rules_page=page)
    await message.answer(
        _RULES_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_rules_kb(page, len(_RULES_PAGES)),
    )
    logger.info(f"User {message.from_user.id} opened Rules page 1")




@dp.message(RulesStates.viewing_rules, F.text == "NEXT ➡️")
async def rules_next(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = min(data.get("rules_page", 1) + 1, len(_RULES_PAGES))
    await state.update_data(rules_page=page)
    msg = await message.answer("⏩ Loading next page...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📜 Page {page} / {len(_RULES_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _RULES_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_rules_kb(page, len(_RULES_PAGES)),
    )

@dp.message(RulesStates.viewing_rules, F.text == "⬅️ PREV")
async def rules_prev(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(data.get("rules_page", 1) - 1, 1)
    await state.update_data(rules_page=page)
    msg = await message.answer("⏪ Going back...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📜 Page {page} / {len(_RULES_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _RULES_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_rules_kb(page, len(_RULES_PAGES)),
    )

# ──────────────────────────────────────────────────────────────
# 📚 GUIDE SYSTEM — two-choice selector + paginated user guide
# ──────────────────────────────────────────────────────────────

_AGENT_GUIDE_PAGES = [
    # Page 1 / 5 — WHAT IS MSA NODE AGENT + VERIFICATION FLOW
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  1 / 5\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Welcome to **MSA NODE Agent** — your private gateway to the MSA NODE vault.\n\n"
        "This guide covers every feature in full detail so you always know exactly what to do. "
        "Read it once. Reference it anytime.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔑  WHAT IS MSA NODE AGENT?**\n\n"
        "MSA NODE Agent is a private Telegram bot that acts as your personal vault key.\n"
        "It controls your access to exclusive content — blueprints, guides, resources, and tools — "
        "not available anywhere publicly.\n\n"
        "  • Only **verified vault members** have full access to all features\n"
        "  • Every feature is tied to your Telegram account and your unique **MSA+ ID**\n"
        "  • Access is real-time: join the vault → instant unlock\n"
        "  • Leave the vault → features restrict automatically until you rejoin\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**✅  VERIFICATION — STEP BY STEP**\n\n"
        "Before most features are active, you must be a verified vault member.\n\n"
        "  **STEP 1**  Join the Vault Channel\n"
        "     Tap the Join button when you first open the agent.\n"
        "     This is the official, private MSA NODE channel.\n\n"
        "  **STEP 2**  Confirm Your Membership\n"
        "     After joining, return to the agent and press Confirm Membership.\n"
        "     The system verifies your Telegram account in real-time.\n\n"
        "  **STEP 3**  Receive Your MSA+ ID\n"
        "     Once verified, you are issued a unique MSA+ ID.\n"
        "     This is your permanent vault identity — keep it private.\n\n"
        "  **STEP 4**  Full Access Unlocked\n"
        "     Dashboard, Search Code, Guide, Rules, and Support are now fully active.\n\n"
        "  ⚠️ _Leaving the vault channel automatically restricts your access_\n"
        "  _until you rejoin and send_ /start _to re-verify._\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 1 of 5_"
    ),
    # Page 2 / 5 — DASHBOARD — every element explained
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  2 / 5\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📊  DASHBOARD — YOUR VAULT HUB**\n\n"
        "The Dashboard is your personal control panel inside MSA NODE Agent.\n"
        "Every time you tap 📊 DASHBOARD you receive a **live snapshot** of your account.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🪪  IDENTITY**\n"
        "  MSA+ ID ............. Your unique vault identifier\n"
        "  Account Name ........ Your Telegram display name at time of join\n"
        "  Member Since ........ The exact date you were first verified\n\n"
        "**✅  VAULT STATUS**\n"
        "  Shows whether you are actively inside the vault channel right now.\n"
        "  • _VERIFIED & ACTIVE_ — You're in. Full access enabled.\n"
        "  • _NOT IN VAULT_ — You've left. Rejoin and send /start to restore.\n\n"
        "**📢  ANNOUNCEMENTS**\n"
        "  Live updates from the MSA NODE team are displayed here.\n"
        "  Check this section regularly — it may contain important notices,\n"
        "  content drops, system updates, or maintenance alerts.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📌  HOW TO USE THE DASHBOARD**\n\n"
        "  ① Tap **📊 DASHBOARD** in the main menu\n"
        "  ② Your live profile loads immediately\n"
        "  ③ If status shows _NOT IN VAULT_ — rejoin the channel, then send /start\n\n"
        "**💡  TIPS:**\n"
        "  • Your **MSA+ ID never changes** — save it somewhere secure\n"
        "  • Your name reflects your Telegram display name at the time you first joined\n"
        "  • The Dashboard is the fastest way to confirm your membership is active\n"
        "  • Always check Announcements before opening a support ticket —\n"
        "    your issue may already be addressed there\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 2 of 5_"
    ),
    # Page 3 / 5 — SEARCH CODE — both methods, errors, what codes unlock
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  3 / 5\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔍  SEARCH CODE — UNLOCK EXCLUSIVE CONTENT**\n\n"
        "MSA CODES are unique identifiers linked to specific pieces of vault content.\n"
        "When you enter a valid code, the agent delivers the linked content directly to you.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📲  METHOD A — DIRECT LINK** _(Recommended — Fastest)_\n\n"
        "No manual typing required. The code is passed automatically.\n\n"
        "  ① Find an MSA NODE video on YouTube or Instagram\n"
        "  ② Tap the **MSA NODE Agent** link in the video description\n"
        "  ③ Telegram opens the agent automatically\n"
        "  ④ The code is passed in the background — content delivered instantly\n\n"
        "  ✅ _This method is error-free. Always prefer it when a link is available._\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**⌨️  METHOD B — MANUAL CODE ENTRY**\n\n"
        "Use this when you have a code but no direct link.\n\n"
        "  ① Tap **🔍 SEARCH CODE** in the main menu\n"
        "  ② The agent prompts: _Send the MSA CODE_\n"
        "  ③ Type or paste your code exactly as shown (e.g. `MSA001`)\n"
        "  ④ Tap send — your content arrives within seconds\n\n"
        "  ⚠️ **Common mistakes to avoid:**\n"
        "  • Codes are **case-sensitive** — `MSA001` ≠ `msa001`\n"
        "  • Do not add spaces before or after the code\n"
        "  • Do not include `#` or other symbols unless they are part of the code\n"
        "  • _Code not found_ = the code may be expired or contain a typo\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📦  WHAT CAN A CODE UNLOCK?**\n\n"
        "  • PDF blueprints and downloadable strategy guides\n"
        "  • Exclusive video links and private walkthroughs\n"
        "  • Templates, frameworks, and premium tools\n"
        "  • Bonus material not available anywhere publicly\n\n"
        "  Each code unlocks one specific piece of content.\n"
        "  You can use as many codes as you find — there is no per-member limit.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 3 of 5_"
    ),
    # Page 4 / 5 — TUTORIAL + RULES + SUPPORT detailed walkthroughs
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  4 / 5\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📺  WATCH TUTORIAL — THE OFFICIAL WALKTHROUGH**\n\n"
        "If you are new to MSA NODE Agent, the tutorial is your first stop.\n\n"
        "  • Walks through every feature step-by-step with real examples\n"
        "  • Shows exactly how to use MSA CODES from both direct links and manual entry\n"
        "  • Explains how to get maximum value from your vault membership\n"
        "  • ✅ **Recommended:** Watch the full tutorial before using any other feature\n\n"
        "  ① Tap **📺 WATCH TUTORIAL** in the main menu\n"
        "  ② The agent delivers the official tutorial video directly\n"
        "  ③ Watch it once in full — it covers everything in this guide visually\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📜  RULES — WHAT YOU NEED TO KNOW**\n\n"
        "The vault operates under a strict, enforced code of conduct.\n\n"
        "  • Tap **📜 RULES** to read all rules in full — 3 pages with PREV / NEXT navigation\n"
        "  • Reading the rules takes under 3 minutes — do it as soon as you join\n"
        "  • Every member is accountable regardless of whether they have read the rules\n"
        "  • Covers: Conduct · Content Security · Account Integrity · Agent Usage ·\n"
        "    Support · Violations · Zero-Tolerance Bans · Appeals\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📞  SUPPORT — HOW TO GET HELP**\n\n"
        "The support system connects you directly to the MSA NODE admin team.\n\n"
        "  ① Tap **📞 SUPPORT** in the main menu\n"
        "  ② Select the most relevant support category\n"
        "  ③ Tap **🎫 RAISE A TICKET** to open your issue\n"
        "  ④ In your message, include:\n"
        "       • Your **MSA+ ID**\n"
        "       • Which feature is affected\n"
        "       • Exactly what happened (include screenshots if useful)\n"
        "       • What you have already tried\n"
        "  ⑤ Attach one photo or one short video if relevant (max 3 min · max 50 MB)\n"
        "  ⑥ Submit — an admin will respond via direct message\n\n"
        "  ✅ _Accepted: Text · 1 Photo with caption · 1 Video (≤ 3 min, ≤ 50 MB)_\n"
        "  ❌ _Not accepted: Voice notes · Documents · GIFs · Stickers · Audio_\n\n"
        "  One active ticket at a time. Wait for a response before opening another.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Page 4 of 5_"
    ),
    # Page 5 / 5 — TROUBLESHOOTING + PRO TIPS + QUICK REFERENCE
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  5 / 5\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🛠  TROUBLESHOOTING — COMMON ISSUES & EXACT FIXES**\n\n"
        "  ❓ _\"Code not found\" after entering a valid code_\n"
        "     → Check exact spelling — codes are **case-sensitive**\n"
        "     → Remove any spaces before or after the code\n"
        "     → Code may be expired — raise a 📞 support ticket if you believe it is valid\n\n"
        "  🔒 _\"Access denied\" or feature is locked_\n"
        "     → You are no longer in the vault channel\n"
        "     → Rejoin the vault, then send /start to trigger re-verification\n\n"
        "  ⏳ _Agent not responding / button does nothing_\n"
        "     → Wait a few seconds — the system may be processing a prior request\n"
        "     → Do NOT press the same button repeatedly — anti-spam adds a cooldown\n"
        "     → If unresponsive for 30+ seconds, send /start to reset your session\n\n"
        "  🔴 _\"System under maintenance\" message_\n"
        "     → No action needed — the admin team is performing an upgrade\n"
        "     → The bot will return automatically — monitor the vault channel for updates\n"
        "     → Do not raise a support ticket for maintenance — it resolves on its own\n\n"
        "  🎫 _Your ticket was closed without resolution_\n"
        "     → Reopen with your **MSA+ ID**, specific details, and any relevant screenshots\n"
        "     → Ensure you have no other open ticket\n"
        "     → Confirm your media type is accepted (no voice notes or documents)\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**💡  PRO TIPS FOR MEMBERS**\n\n"
        "  • Always use **Method A (direct link)** for codes — faster and error-free\n"
        "  • Save your **MSA+ ID** somewhere secure — you need it for support tickets\n"
        "  • Check **📢 Announcements** in the Dashboard before raising a ticket\n"
        "  • Keep Telegram notifications **on** for this agent — never miss a reply\n"
        "  • Bookmark the official MSA NODE Agent link — never use third-party links\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📌  QUICK REFERENCE**\n\n"
        "  /start .............. Reset or restart the agent at any time\n"
        "  📊 DASHBOARD ........ View your profile, MSA+ ID, and vault status\n"
        "  🔍 SEARCH CODE ...... Unlock exclusive content with an MSA CODE\n"
        "  📺 WATCH TUTORIAL ... Official onboarding and feature walkthrough\n"
        "  📖 AGENT GUIDE ...... This complete usage manual (5 pages)\n"
        "  📜 RULES ............ Full member code of conduct (3 pages)\n"
        "  📞 SUPPORT .......... Raise a ticket or request assistance\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 _MSA NODE Agent  |  Your Exclusive Gateway_\n"
        "_If you are here, you are already ahead._"
    ),
]

def _agent_guide_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for MSA NODE Agent Guide — PREV / NEXT / HOME only."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="🏠 MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(F.text == "📖 AGENT GUIDE")
@rate_limit(3.0)
@anti_spam("guide")
async def guide(message: types.Message, state: FSMContext):
    """Open MSA NODE Agent Guide — goes straight to page 1 for users."""
    if await _check_freeze(message): return
    if await check_maintenance_mode(message):
        return

    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "GUIDE" in suspend_doc.get("bot1_suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nGuide access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, GUIDE IS LOCKED**\n\n"
                f"The **Guide** is vault-exclusive.\n\n"
                f"💎 **Rejoin to unlock it.**",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, the **Guide** is exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return

    # 🎬 GUIDE BOOT ANIMATION
    msg = await message.answer("📡 Accessing Agent Manual...")
    await asyncio.sleep(ANIM_FAST)
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Decrypting Agent Manual...")
        await asyncio.sleep(0.07)
    await msg.edit_text("📖 Loading Page 1...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)

    page = 1
    await state.set_state(GuideStates.viewing_bot1)
    await state.update_data(guide_page=page)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )
    logger.info(f"User {message.from_user.id} opened Agent Guide page 1")

@dp.message(GuideStates.viewing_bot1, F.text == "NEXT ➡️")
async def guide_bot1_next(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = min(data.get("guide_page", 1) + 1, len(_AGENT_GUIDE_PAGES))
    await state.update_data(guide_page=page)
    msg = await message.answer("⏩ Loading next page...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📖 Page {page} / {len(_AGENT_GUIDE_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot1, F.text == "⬅️ PREV")
async def guide_bot1_prev(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(data.get("guide_page", 1) - 1, 1)
    await state.update_data(guide_page=page)
    msg = await message.answer("⏪ Going back...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📖 Page {page} / {len(_AGENT_GUIDE_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )
@dp.message(F.text == "📚 GUIDE MENU")
async def guide_legacy_menu_btn(message: types.Message, state: FSMContext):
    """Legacy GUIDE MENU button — redirects safely to the canonical Agent Guide handler."""
    await guide(message, state)

@dp.message(
    F.text == "🏠 MAIN MENU",
    ~StateFilter(SearchCodeStates.waiting_for_code, SearchCodeStates.waiting_for_first_code)
)
async def guide_back_to_main_bot1(message: types.Message, state: FSMContext):
    """Return to main menu, clearing any guide state (bot1)."""
    await state.clear()
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Member"

    # 🔒 STRICT VAULT GATE — non-vault users cannot access main menu
    if await _require_vault_check(message):
        return

    msg = await message.answer("🔄 Returning to main menu...")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)

    await message.answer(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📋  **MSA NODE AGENT — MAIN MENU**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Welcome back, **{first_name}**! 👋\n\n"
        f"🚀 **All your services are live and ready.**\n\n"
        f"  📊 **DASHBOARD** — Your vault stats & MSA+ ID\n"
        f"  🔍 **SEARCH CODE** — Unlock exclusive content\n"
        f"  📺 **WATCH TUTORIAL** — Your starter guide video\n"
        f"  📖 **AGENT GUIDE** — Full bot manual\n"
        f"  📜 **RULES** — Community code of conduct\n"
        f"  📞 **SUPPORT** — Open a ticket anytime\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 _MSA NODE Agent  |  Your Exclusive Gateway_",
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN,
    )
    logger.info(f"User {user_id} returned to main menu from guide")



@dp.message(Command("checkvault"))
async def cmd_checkvault(message: types.Message):
    """Owner-only: Show vault and MSA statistics"""
    if message.from_user.id != OWNER_ID:
        return  # silently ignore non-owners
    try:
        total_members   = col_msa_ids.count_documents({})
        total_banned    = col_banned_users.count_documents({})
        perm_banned     = col_banned_users.count_documents({"ban_type": "permanent"})
        temp_banned     = col_banned_users.count_documents({"ban_type": "temporary"})
        total_suspended = col_suspended_features.count_documents({})
        _tracking = db["bot2_user_tracking"]
        total_tracked   = _tracking.count_documents({})

        yt_count      = _tracking.count_documents({"source": "YT"})
        ig_count      = _tracking.count_documents({"source": "IG"})
        igcc_count    = _tracking.count_documents({"source": "IGCC"})
        ytcode_count  = _tracking.count_documents({"source": "YTCODE"})
        unknown_count = _tracking.count_documents({"source": "UNKNOWN"})

        # 9-digit MSA pool: 100000000–999999999 = 900,000,000 possible
        TOTAL_POOL = 900_000_000
        available   = TOTAL_POOL - total_members
        utilization = (total_members / TOTAL_POOL * 100)

        report = (
            "🔐 **VAULT STATS — /checkvault**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 **MSA Members:** {total_members:,}\n"
            f"🔴 **Banned:** {total_banned} (Perm: {perm_banned}, Temp: {temp_banned})\n"
            f"⏸️ **Suspended users:** {total_suspended}\n"
            f"📊 **Total tracked:** {total_tracked:,}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📍 **Traffic Sources:**\n"
            f"  📺 YT: {yt_count}   📸 IG: {ig_count}\n"
            f"  📎 IGCC: {igcc_count}   🔗 YTCODE: {ytcode_count}\n"
            f"  👤 UNKNOWN: {unknown_count}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🆔 **MSA Code Pool (9-digit):**\n"
            f"  🎯 Total Possible: {TOTAL_POOL:,}\n"
            f"  ✅ Allocated: {total_members:,}\n"
            f"  🟢 Available: {available:,}\n"
            f"  📈 Used: {utilization:.6f}%\n\n"
            f"🕒 {now_local().strftime('%b %d, %Y  %I:%M:%S %p')}"
        )
        await message.answer(report, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ checkvault error: {str(e)[:150]}", parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("traffic"))
async def cmd_traffic(message: types.Message):
    """Owner-only: Real-time traffic source analytics (Bot 1, no duplicates, no cross-bot data)"""
    if message.from_user.id != OWNER_ID:
        return
    try:
        _tracking = db["bot2_user_tracking"]

        # ── Core counts (strictly Bot 1 first-touch sources) ──────────────────
        total_tracked   = _tracking.count_documents({})
        active_tracked  = _tracking.count_documents({"is_archived": {"$ne": True}})
        archived_count  = _tracking.count_documents({"is_archived": True})

        # Each doc has unique user_id index — no duplicates possible by design
        yt_count      = _tracking.count_documents({"source": "YT"})
        ig_count      = _tracking.count_documents({"source": "IG"})
        igcc_count    = _tracking.count_documents({"source": "IGCC"})
        ytcode_count  = _tracking.count_documents({"source": "YTCODE"})
        unknown_count = _tracking.count_documents({"source": "UNKNOWN"})
        known_count   = yt_count + ig_count + igcc_count + ytcode_count

        def pct(n: int) -> str:
            return f"{(n / total_tracked * 100):.1f}%" if total_tracked else "0.0%"

        # ── Last 7 days new sign-ups ───────────────────────────────────────────
        from datetime import timedelta
        week_ago  = now_local() - timedelta(days=7)
        new_7d    = _tracking.count_documents({"joined_at": {"$gte": week_ago}})

        # ── Build report ──────────────────────────────────────────────────────
        bar_len = 20
        def mini_bar(n: int) -> str:
            if not total_tracked:
                return "░" * bar_len
            filled = round(n / total_tracked * bar_len)
            return "█" * filled + "░" * (bar_len - filled)

        report = (
            "📊 **BOT 1 — REAL-TIME TRAFFIC ANALYTICS** _(/traffic)_\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 **Total Tracked:** {total_tracked:,}\n"
            f"   ✅ Active: {active_tracked:,}  |  🗄 Archived: {archived_count:,}\n"
            f"   🆕 Last 7 days: +{new_7d:,}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📍 **Traffic Sources (first-touch, no duplicates):**\n\n"
            f"📺 **YT**       {yt_count:>5,}  {pct(yt_count):>7}  `{mini_bar(yt_count)}`\n"
            f"📸 **IG**       {ig_count:>5,}  {pct(ig_count):>7}  `{mini_bar(ig_count)}`\n"
            f"📎 **IGCC**     {igcc_count:>5,}  {pct(igcc_count):>7}  `{mini_bar(igcc_count)}`\n"
            f"🔗 **YTCODE**   {ytcode_count:>5,}  {pct(ytcode_count):>7}  `{mini_bar(ytcode_count)}`\n"
            f"👤 **UNKNOWN**  {unknown_count:>5,}  {pct(unknown_count):>7}  `{mini_bar(unknown_count)}`\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 **Known source rate:** {pct(known_count)} ({known_count:,} / {total_tracked:,})\n"
            f"🔒 **Dedup status:** ✅ Unique by user_id (no duplicates)\n"
            f"🗂 **Scope:** `bot2_user_tracking` — Bot 1 only, isolated from Bot 2 & Bot 3\n\n"
            f"🕒 {now_local().strftime('%b %d, %Y  %I:%M:%S %p')}"
        )
        await message.answer(report, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ /traffic error: {str(e)[:200]}", parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("menu"))
@rate_limit(2.0)  # 2 second cooldown for menu command
async def cmd_menu(message: types.Message):
    """Show the main menu"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned from using MSA NODE Agent.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # 🔒 STRICT VAULT GATE — non-vault users cannot use /menu
    if await _require_vault_check(message):
        return
    
    first_name = message.from_user.first_name or "Member"
    await message.answer(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📋  **MSA NODE AGENT — MAIN MENU**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Hey **{first_name}**! 👋\n\n"
        f"🚀 **All your services are live and ready.**\n\n"
        f"  📊 **DASHBOARD** — Your vault stats & MSA+ ID\n"
        f"  🔍 **SEARCH CODE** — Unlock exclusive content\n"
        f"  📺 **WATCH TUTORIAL** — Your starter guide video\n"
        f"  📖 **AGENT GUIDE** — Full bot manual\n"
        f"  📜 **RULES** — Community code of conduct\n"
        f"  📞 **SUPPORT** — Open a ticket anytime\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 _MSA NODE Agent  |  Your Exclusive Gateway_",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )

# ==========================================
# 📞 SUPPORT SYSTEM
# ==========================================

@dp.message(F.text == "📞 SUPPORT")
@rate_limit(2.0)  # 2 second cooldown
@anti_spam("support")
async def support_menu(message: types.Message, state: FSMContext):
    """Handle Support button - show support options"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check if user is banned - allow support access for banned users
    ban_doc = await check_if_banned(message.from_user.id)
    is_banned = ban_doc is not None
    
    # Check vault access (skip for banned users)
    if not is_banned:
        is_in_vault = await check_channel_membership(message.from_user.id)
    else:
        is_in_vault = True  # Allow banned users to bypass vault check for support
    
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        
        if was_ever_verified:
            await message.answer(
                f"🔒 **{user_name}, SUPPORT IS VAULT-ONLY**\n\n"
                f"Support is for **verified members only**.\n"
                f"You need access to get help.\n\n"
                f"**Join the vault first.**\n\n"
                f"💎 **Rejoin. Get Support.**",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer(
                f"🔐 **VAULT ACCESS REQUIRED**\n\n"
                f"Hey {user_name}, **Support** is exclusive to Vault Members.\n\n"
                f"📌 **Click the join button below** to unlock full access.\n\n"
                f"_Once joined, all features will be available immediately._",
                reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    # Clear any existing state
    await state.clear()
    
    # 🎬 SUPPORT ANIMATION
    msg = await message.answer("🔌 Connecting to Support...")
    await asyncio.sleep(ANIM_FAST)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Connecting to Support...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("📞 Opening Support Center...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    first_name = message.from_user.first_name or "Member"
    
    support_text = f"""
📞 **SUPPORT CENTER**
━━━━━━━━━━━━━━━━━━━━━

Welcome, **{first_name}**! 👋

**Select your issue category:**

📄 **PDF/LINK ISSUES**
   Problems with PDFs, links, codes

🔧 **TROUBLESHOOTING**
   Bot performance, errors, bugs

❓ **OTHER ISSUES**
   General questions & help

🎫 **RAISE A TICKET**
   Submit issue to admin team

🔙 **BACK TO MENU**
   Return to main menu

━━━━━━━━━━━━━━━━━━━━━

💡 **Tip:** Check categories first for instant solutions!
"""
    
    await message.answer(
        support_text,
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} opened Support Center")

@dp.message(F.text == "📄 PDF/LINK ISSUES")
@rate_limit(2.0)
@anti_spam("pdf_issues")
async def pdf_link_issues_handler(message: types.Message):
    """Handle PDF/Link Issues category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("🔎 Analyzing your issue...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"📄 **Loading PDF/Link Solutions for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Preparing Solutions...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **S olutions Ready!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
📄 **PDF & LINK ISSUES**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, I'm here to help with your PDF/Link issues.**

**🔍 COMMON PROBLEMS & SOLUTIONS:**

**Problem 1: Link Not Working**
`Solution:`
• Verify you are in the vault channel
• Don't modify or edit the link
• Wait 2-3 seconds and try again
• Clear Telegram cache and retry

**Problem 2: PDF Not Opening**
`Solution:`
• Check your internet connection
• Update Telegram app to latest version
• Try opening in external browser
• Download and open in PDF reader

**Problem 3: MSA CODE Invalid**
`Solution:`
• Check spelling carefully (case sensitive)
• Ensure you copied the full code
• Code must match video/post source
• Try manual entry instead of paste

**Problem 4: Content Not Delivered**
`Solution:`
• Wait 5-10 seconds (processing time)
• Check if bot sent multiple messages
• Don't spam the button
• Use /start to reset bot

**Problem 5: Google Drive Access Denied**
`Solution:`
• Link opens automatically in Drive
• Make sure you're logged into Google
• Try incognito/private mode
• Request access if prompted

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, did any of these solutions work for you?**

✅ If your issue is resolved, click **RESOLVED**
🔍 Need to check other categories? Click **CHECK OTHER**
🎫 Still need help? Click **RAISE A TICKET** to reach admin

*I'm here to help!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed PDF/Link Issues")

@dp.message(F.text == "🔧 TROUBLESHOOTING")
@rate_limit(2.0)
@anti_spam("troubleshooting")
async def troubleshooting_handler(message: types.Message):
    """Handle Troubleshooting category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("⚙️ Running diagnostics...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"🔧 **Analyzing Bot Performance for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Scanning System...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **Diagnostics Complete!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
🔧 **TROUBLESHOOTING GUIDE**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, let's fix your technical issues together.**

**⚡ PERFORMANCE ISSUES:**

**Issue: Bot is Slow/Laggy**
`Solution:`
• Wait 2-3 seconds between commands
• Don't spam buttons repeatedly
• Check your network connection
• Restart Telegram app
• Clear Telegram cache

**Issue: Commands Not Working**
`Solution:`
• Use /start to reset the agent
• Check vault membership status
• Wait for animations to complete
• Don't send multiple commands at once

**Issue: Stuck in Search Mode**
`Solution:`
• Click ❌ CANCEL button
• Send /start command
• Wait 10 seconds before retrying

**Issue: Menu Buttons Missing**
`Solution:`
• Send /menu command
• Restart Telegram app
• Use /start to reload interface

━━━━━━━━━━━━━━━━━━━━━━━━━━

**🚨 ERROR MESSAGES:**

**"Access Denied"**
• Join the vault channel first
• Verify membership status
• Wait 10 seconds after joining

**"Invalid Code"**
• Check code spelling
• Ensure exact match from source
• Try uppercase/lowercase variants

**"Rate Limited"**
• You clicked too fast
• Wait 2-3 seconds
• Prevents Telegram ban

━━━━━━━━━━━━━━━━━━━━━━━━━━

**💡 BEST PRACTICES:**

✅ Wait for bot responses
✅ Follow on-screen instructions
✅ One command at a time
✅ Keep Telegram updated
✅ Stable internet connection

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, were you able to fix the issue?**

✅ Problem solved? Click **RESOLVED**
🔍 Want to explore other solutions? Click **CHECK OTHER**
🎫 Need direct admin support? Click **RAISE A TICKET**

*We're committed to getting you back on track!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed Troubleshooting")

@dp.message(F.text == "❓ OTHER ISSUES")
@rate_limit(2.0)
@anti_spam("other_issues")
async def other_issues_handler(message: types.Message):
    """Handle Other Issues category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("📚 Accessing knowledge base...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"❓ **Finding Answers for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Searching Database...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **Information Retrieved!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
❓ **OTHER QUESTIONS & HELP**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, I have answers to your general questions.**

**📚 GENERAL INFORMATION:**

**Q: How do I access content?**
`A:` Click links from videos or use SEARCH CODE with MSA CODES.

**Q: Where do I find MSA CODES?**
`A:` MSA CODES are shown in YouTube videos **Only**.

**Q: How to use SEARCH CODE?**
`A:` Click 🔍 SEARCH CODE → Enter MSA CODE → Receive content

**Q: What is MSA+ ID?**
`A:` Your unique member identification number. View in DASHBOARD.

**Q: Can I share content?**
`A:` No. All vault content is exclusive for members only.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**🔐 ACCOUNT & ACCESS:**

**Q: I left vault, what happens?**
`A:` Access revoked immediately. Rejoin to restore full access.

**Q: Can I rejoin after leaving?**
`A:` Yes. Rejoin vault channel to restore access instantly.

**Q: How to check my status?**
`A:` Use 📊 DASHBOARD to view your profile and membership info.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**📱 PLATFORM SUPPORT:**

**Q: Does bot work on mobile?**
`A:` Yes. Fully optimized for mobile and desktop.

**Q: Which Telegram version?**
`A:` Works on all: Official app, Web, Desktop.

**Q: Need special permissions?**
`A:` Only vault channel membership required.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**📖 RESOURCES:**

• Check 📚 GUIDE for complete manual
• Review 📜 RULES for community guidelines
• Visit vault for announcements

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, did you find what you were looking for?**

✅ Got your answer? Click **RESOLVED**
🔍 Need to check other sections? Click **CHECK OTHER**
🎫 Have a specific question for admin? Click **RAISE A TICKET**

*Always happy to help!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed Other Issues")

# ---------------------------------------------------------------------------
# 🔐 VAULT ACCESS GUARD — reusable helper for all support handlers
# Returns True  → user is NOT in vault (caller should return early)
# Returns False → user IS in vault (caller should continue)
# ---------------------------------------------------------------------------
async def _require_vault_check(
    message: types.Message,
    state: FSMContext | None = None
) -> bool:
    """
    Check that the user is a vault (channel) member before allowing
    access to support features.  If they're not a member, send a
    'join first' prompt and return True so the caller can early-return.
    Optionally clears FSM state to avoid stuck flows.
    """
    is_member = await check_channel_membership(message.from_user.id)
    if not is_member:
        if state:
            await state.clear()
        first_name = message.from_user.first_name or "Member"
        user_id = message.from_user.id
        user_data = get_user_verification_status(user_id)
        was_ever_verified = user_data.get('ever_verified', False)
        
        await message.answer(
            f"🔐 **VAULT ACCESS REQUIRED**\n\n"
            f"Hey {first_name}, this feature is exclusive to Vault Members.\n\n"
            f"📌 **Click the join button below** to unlock full support access.\n\n"
            f"_Once joined, all features will be available immediately._",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    return False


@dp.message(F.text == "✅ RESOLVED")
@rate_limit(cooldown=1.0)
@anti_spam("resolved")
async def resolved_handler(message: types.Message):
    
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Vault check
    if await _require_vault_check(message):
        return

    first_name = message.from_user.first_name or "Member"
    
    # 🎬 SUCCESS ANIMATION
    msg = await message.answer("✨")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text("✨ ✨")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text("✨ ✨ ✨")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    await message.answer(
        f"✅ **EXCELLENT, {first_name}!**\n\n"
        f"I'm glad we could resolve your issue together!\n\n"
        f"💎 **You're all set now.**\n\n"
        f"If you ever need support again, I'm here 24/7.\n"
        f"Just click **📞 SUPPORT** anytime.\n\n"
        f"`Returning to main menu...`",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} marked issue as resolved")

@dp.message(F.text == "🔍 CHECK OTHER")
@rate_limit(1.5)
@anti_spam("check_other")
async def check_other_handler(message: types.Message):
    """Handle Check Other button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Vault check
    if await _require_vault_check(message):
        return

    first_name = message.from_user.first_name or "Member"
    
    # 🎬 TRANSITION ANIMATION
    msg = await message.answer("🔄 Switching categories...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    await message.answer(
        f"🔍 **BROWSE OTHER SOLUTIONS, {first_name}**\n\n"
        f"Let's explore other support categories to find what you need.\n\n"
        f"**Select another category below:**",
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} checking other categories")

@dp.message(F.text == "🎫 RAISE A TICKET")
@rate_limit(2.0)
@anti_spam("raise_ticket")
async def raise_ticket_handler(message: types.Message, state: FSMContext):
    """Handle Raise a Ticket button - check for existing ticket first"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    if await _require_vault_check(message):
        return
    
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Member"

    # ── Security lock gate — block BEFORE showing the form ────────────────────
    # Lock is stored in MongoDB, persists across bot restarts and vault leave/rejoin
    lock_remaining = _get_support_lock_remaining(user_id)
    if lock_remaining > 0:
        await message.answer(
            _build_support_security_notice(
                first_name,
                "Temporary lock due to repeated unsafe/spam submissions",
                warning_count=0,
                lock_remaining=lock_remaining,
            ),
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info(f"User {user_id} blocked at RAISE A TICKET entry — support lock active ({lock_remaining}s remaining)")
        return
    # ──────────────────────────────────────────────────────────────────────────

    # Check if user has an open ticket
    existing_ticket = col_support_tickets.find_one({
        "user_id": user_id,
        "status": "open"
    })
    
    if existing_ticket:
        # User already has an open ticket - show lock message
        first_name = message.from_user.first_name or "Member"
        ticket_date = existing_ticket.get('created_at', now_local())
        date_str = ticket_date.strftime("%B %d, %Y at %I:%M %p")
        
        # 🎬 LOCK ANIMATION
        msg = await message.answer("🔒 Checking ticket status...")
        await asyncio.sleep(ANIM_MEDIUM)
        await safe_delete_message(msg)
        
        await message.answer(
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒  **ACTIVE TICKET IN PROGRESS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**{first_name}**, you already have an open support request currently being reviewed by our team.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋  **CURRENT TICKET STATUS**\n\n"
            f"   📅  Submitted:   {date_str}\n"
            f"   🔄  Status:      ⏳ Awaiting Admin Review\n"
            f"   ⏰  Response:   Within 24–48 hours\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️  One active ticket is allowed at a time.\n"
            f"   You can submit a new ticket only after this one is resolved.\n\n"
            f"💡  Our admin team will contact you directly via DM.\n"
            f"   _Please allow up to 24–48 hours for a response._",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"User {user_id} tried to submit ticket while one is open")
        return

    # Cooldown gate — one ticket every 24h, with live auto-refresh preview
    rate_ok, rate_msg = check_ticket_rate_limit(user_id, first_name)
    if not rate_ok:
        cooldown_msg = await message.answer(rate_msg, parse_mode=ParseMode.MARKDOWN)
        await _live_refresh_ticket_cooldown(cooldown_msg, user_id, first_name, seconds=20)
        return

    
    # 🎬 TICKET PREPARATION ANIMATION
    msg = await message.answer("🎫 Preparing ticket form...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"📝 **Setting up for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    # Add cancel button
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🎫  **SUPPORT TICKET FORM**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤  **{first_name}**, our admin team is ready to review your request.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝  **DESCRIBE YOUR ISSUE**\n\n"
        f"Please include:\n"
        f"   ›  What the problem is\n"
        f"   ›  When it started\n"
        f"   ›  What you tried before contacting us\n"
        f"   ›  Any error messages or reference codes\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📎  **ACCEPTED FORMATS**\n\n"
        f"   📷  Photo — 1 image, caption required\n"
        f"   🎥  Video — max 3 minutes · max 50 MB, caption required\n"
        f"   📄  Text only — {MIN_TICKET_LENGTH}–{MAX_TICKET_LENGTH} characters\n\n"
        f"⚠️  _One media file per ticket only._\n"
        f"_Documents, voice notes, GIFs, and stickers are not accepted._\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✍️  **{first_name}**, type your message or send media below.\n\n"
        f"_Tap_ **❌ CANCEL** _to exit at any time._",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Set state to wait for issue description
    await state.set_state(SupportStates.waiting_for_issue)
    logger.info(f"User {message.from_user.id} started ticket submission")

@dp.message(SupportStates.waiting_for_issue)
@rate_limit(1.5)
@anti_spam("submit_ticket")
async def process_ticket_submission(message: types.Message, state: FSMContext):
    """Process the ticket submission with text/photo/video and comprehensive validation"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Vault check (clears state if user left vault mid-flow)
    if await _require_vault_check(message):
        await state.clear()
        return

    # Get user info for personalization
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Member"

    # Concurrency guard: if a ticket is already open, block immediately
    open_ticket = col_support_tickets.find_one({"user_id": user_id, "status": "open"})
    if open_ticket:
        created_at = open_ticket.get("created_at", now_local())
        created_text = created_at.strftime("%B %d, %Y at %I:%M %p") if hasattr(created_at, "strftime") else str(created_at)
        await state.clear()
        await message.answer(
            f"🔒 **ACTIVE TICKET ALREADY OPEN**\n\n"
            f"{user_name}, you already have an active support ticket.\n"
            f"📅 Submitted: {created_text}\n\n"
            f"Please wait for admin response before opening another ticket.",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Security lock gate (triggered by repeated abusive submissions)
    lock_remaining = _get_support_lock_remaining(user_id)
    if lock_remaining > 0:
        await message.answer(
            _build_support_security_notice(
                user_name,
                "Temporary lock due to repeated unsafe/spam submissions",
                warning_count=0,
                lock_remaining=lock_remaining,
            ),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Determine content type and extract text
    has_photo  = message.photo is not None
    has_video  = message.video is not None
    # Detect unsupported media types (documents, voice, stickers, GIFs, etc.)
    has_unsupported = any([
        message.voice       is not None,
        message.audio       is not None,
        message.document    is not None,
        message.sticker     is not None,
        message.animation   is not None,
        message.video_note  is not None,
    ])
    issue_text = (message.caption or message.text or "").strip()

    # Check if user canceled
    if issue_text.upper() == "CANCEL" or issue_text == "❌ CANCEL":
        await state.clear()
        await message.answer(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"❌  **TICKET CANCELLED**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{user_name}, your ticket request has been cancelled.\n\n"
            f"_You can raise a new ticket any time you need help._",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Reject unsupported media types ───────────────────────────────────────
    if has_unsupported:
        warn_count, lock_remaining, _ = _register_support_violation(user_id)
        await message.answer(
            f"⚠️  **UNSUPPORTED FILE TYPE**\n\n"
            f"**{user_name}**, only the following are accepted in a ticket:\n\n"
            f"   📷  Photo (1 image with caption)\n"
            f"   🎥  Video (max 3 min · 50 MB, with caption)\n"
            f"   📄  Text description\n\n"
            f"❌  Documents, voice notes, GIFs, stickers, and audio are not accepted.\n\n"
            f"_Please resend using a supported format, or tap_ **❌ CANCEL** _to exit._",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer(
            _build_support_security_notice(
                user_name,
                "Unsupported media submitted (documents/voice/GIF/stickers/audio)",
                warning_count=warn_count,
                lock_remaining=lock_remaining,
            ),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Reject album / media group — only exactly 1 photo or 1 video per ticket ─
    if message.media_group_id:
        warn_count, lock_remaining, _ = _register_support_violation(user_id)
        await message.answer(
            f"⚠️  **ALBUM NOT ALLOWED**\n\n"
            f"**{user_name}**, you sent multiple files (an album).\n\n"
            f"📋  **Only 1 media file is accepted per ticket:**\n"
            f"   📷  1 photo — with a caption describing your issue\n"
            f"   🎥  1 video — max 3 min · 50 MB, with a caption\n\n"
            f"❌  Albums and multiple attachments are strictly denied.\n\n"
            f"_Please resend with a single image or video. Tap_ **❌ CANCEL** _to exit._",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer(
            _build_support_security_notice(
                user_name,
                "Multiple media files/album submitted (only one attachment allowed)",
                warning_count=warn_count,
                lock_remaining=lock_remaining,
            ),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Reject combined photo + video (one media per ticket only) ────────────
    if has_photo and has_video:
        warn_count, lock_remaining, _ = _register_support_violation(user_id)
        await message.answer(
            f"⚠️  **ONE MEDIA FILE ONLY**\n\n"
            f"**{user_name}**, please send either a **photo** or a **video** — not both at once.\n\n"
            f"_Resend with a single attachment. Tap_ **❌ CANCEL** _to exit._",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer(
            _build_support_security_notice(
                user_name,
                "Multiple media types submitted in one ticket",
                warning_count=warn_count,
                lock_remaining=lock_remaining,
            ),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── NSFW / explicit indicator check for media captions ───────────────────
    if (has_photo or has_video) and issue_text:
        nsfw_hits = _detect_nsfw_caption_terms(issue_text)
        if nsfw_hits:
            warn_count, lock_remaining, _ = _register_support_violation(user_id)
            await message.answer(
                f"🚫 **SENSITIVE MEDIA BLOCKED**\n\n"
                f"**{user_name}**, your media caption appears to contain adult/explicit terms.\n"
                f"This support channel does not allow nude/sexual content.\n\n"
                f"Please resend with a professional issue description only.",
                parse_mode=ParseMode.MARKDOWN
            )
            await message.answer(
                _build_support_security_notice(
                    user_name,
                    "Potential NSFW/adult media context detected",
                    warning_count=warn_count,
                    lock_remaining=lock_remaining,
                ),
                parse_mode=ParseMode.MARKDOWN
            )
            return

    # ── Video-specific restrictions ───────────────────────────────────────────
    if has_video:
        vid = message.video
        MAX_VIDEO_DURATION = 180   # 3 minutes
        MAX_VIDEO_SIZE_MB  = 50
        if vid.duration and vid.duration > MAX_VIDEO_DURATION:
            mins = vid.duration // 60
            secs = vid.duration % 60
            await message.answer(
                f"⚠️  **VIDEO TOO LONG**\n\n"
                f"**{user_name}**, your video is **{mins}m {secs}s** long.\n\n"
                f"📋  Limit: **3 minutes (180 seconds)**\n\n"
                f"_Please trim your video or describe the issue in text.\n"
                f"Tap_ **❌ CANCEL** _to exit._",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if vid.file_size and vid.file_size > MAX_VIDEO_SIZE_MB * 1024 * 1024:
            size_mb = round(vid.file_size / (1024 * 1024), 1)
            await message.answer(
                f"⚠️  **VIDEO TOO LARGE**\n\n"
                f"**{user_name}**, your video is **{size_mb} MB**.\n\n"
                f"📋  Limit: **{MAX_VIDEO_SIZE_MB} MB**\n\n"
                f"_Please compress or shorten your video.\n"
                f"Tap_ **❌ CANCEL** _to exit._",
                parse_mode=ParseMode.MARKDOWN
            )
            return

    # ── Validate that there is actual content ─────────────────────────────────
    if not issue_text and not has_photo and not has_video:
        await message.answer(
            f"⚠️  **NO CONTENT DETECTED**\n\n"
            f"**{user_name}**, please send one of the following:\n\n"
            f"   📷  A screenshot with a caption\n"
            f"   🎥  A short video with a caption\n"
            f"   📄  A text description of your issue\n\n"
            f"_Try again or tap_ **❌ CANCEL** _to exit._",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Caption required when media is sent ──────────────────────────────────
    if (has_photo or has_video) and len(issue_text) == 0:
        media_label = "photo" if has_photo else "video"
        await message.answer(
            f"⚠️  **CAPTION REQUIRED**\n\n"
            f"**{user_name}**, please add a description to your {media_label}.\n\n"
            f"📝  **How to add a caption:**\n"
            f"   1.  Long-press the {media_label}\n"
            f"   2.  Tap ✏️ Add a caption\n"
            f"   3.  Describe your issue, then send\n\n"
            f"_Try again or tap_ **❌ CANCEL** _to exit._",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Comprehensive validation on text content
    is_valid, error_msg = validate_ticket_content(issue_text, user_name)
    if not is_valid:
        await message.answer(error_msg, parse_mode=ParseMode.MARKDOWN)
        if "INAPPROPRIATE CONTENT DETECTED" in error_msg or "MESSAGE REJECTED" in error_msg:
            warn_count, lock_remaining, _ = _register_support_violation(user_id)
            reason = "Offensive, vulnerable, or spam-like text detected"
            await message.answer(
                _build_support_security_notice(
                    user_name,
                    reason,
                    warning_count=warn_count,
                    lock_remaining=lock_remaining,
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        return

    # Rate limit check — prevent ticket flooding
    rate_ok, rate_msg = check_ticket_rate_limit(user_id, user_name)
    if not rate_ok:
        cooldown_msg = await message.answer(rate_msg, parse_mode=ParseMode.MARKDOWN)
        await _live_refresh_ticket_cooldown(cooldown_msg, user_id, user_name, seconds=20)
        return

    # Duplicate content check — reject same issue_text within 7 days
    if issue_text:
        _dup = col_support_tickets.find_one({
            "user_id": user_id,
            "issue_text": issue_text,
            "created_at": {"$gte": now_local() - timedelta(days=7)}
        })
        if _dup:
            _dup_date = _dup.get("created_at", now_local()).strftime("%B %d, %Y at %I:%M %p")
            await message.answer(
                f"⚠️  **DUPLICATE SUBMISSION DETECTED**\n\n"
                f"**{user_name}**, we already have a ticket with this exact message submitted on **{_dup_date}**.\n\n"
                f"🔒  _Your ticket is already on record and being reviewed. Please do not re-submit the same issue._\n\n"
                f"_Please rephrase your issue if it is different, or tap_ **❌ CANCEL** _to exit._",
                parse_mode=ParseMode.MARKDOWN
            )
            return

    # 🎬 SUBMISSION ANIMATION
    msg = await message.answer("📡 Submitting Ticket...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Submitting Ticket...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ Ticket Submitted Successfully!")
    await asyncio.sleep(ANIM_SLOW)
    await safe_delete_message(msg)
    
    # Get additional user info
    username = f"@{message.from_user.username}" if message.from_user.username else "No Username"
    
    # Get MSA+ ID
    msa_id = get_user_msa_id(user_id)
    display_msa_id = msa_id.replace("+", "") if msa_id else "Not Assigned"
    
    # Get current date/time in 12-hour format
    now = now_local()
    date_str = now.strftime("%B %d, %Y")  # e.g., "February 12, 2026"
    time_str = now.strftime("%I:%M %p")   # e.g., "03:45 PM"
    
    # Determine ticket type
    ticket_type = "Text Only"
    if has_photo and has_video:
        ticket_type = "Text + Photo + Video"
    elif has_photo:
        ticket_type = "Text + Photo 📷"
    elif has_video:
        ticket_type = "Text + Video 🎥"

    # Sanitise user text before embedding in Telegram message:
    # - Escape Markdown v1 special chars so they don't break parse_mode=MARKDOWN
    # - Cap at 3,400 chars to stay well under Telegram's 4,096-char hard limit
    _MAX_CHAN_ISSUE = 3400
    safe_issue = (
        issue_text
        .replace('*', '\\*')
        .replace('_', '\\_')
        .replace('`', '\\`')
        .replace('[', '\\[')
    )
    if len(safe_issue) > _MAX_CHAN_ISSUE:
        safe_issue = str(safe_issue)[:_MAX_CHAN_ISSUE] + "\n_… (message truncated — full text stored in database)_"

    # Create ticket message for admin channel
    ticket_msg = f"""
🎫 **NEW SUPPORT TICKET**
━━━━━━━━━━━━━━━━━━━━━━━━

📅 **Date:** {date_str}
🕐 **Time:** {time_str}
📋 **Type:** {ticket_type}

👤 **USER INFORMATION**
━━━━━━━━━━━━━━━━━━━━━━━━

**Name:** {user_name}
**Username:** {username}
**User ID:** `{user_id}`
**MSA+ ID:** `{display_msa_id}`

🔍 **ISSUE DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━

{safe_issue}

━━━━━━━━━━━━━━━━━━━━━━━━

⚡ **STATUS:** Open
🤖 **Source:** MSA NODE Bot
✅ **Validated:** Passed all filters

💡 **Admin Actions:**
• Reply directly to user: [Contact User](tg://user?id={user_id})
• Mark as resolved: `/resolve {user_id}`
"""
    
    # Send ticket to admin channel with media if present
    try:
        # Send media first if present, then text ticket message
        if has_photo:
            # Get the largest photo
            photo = message.photo[-1]
            await bot.send_photo(
                REVIEW_LOG_CHANNEL,
                photo.file_id,
                caption=(
                    f"📷  **TICKET ATTACHMENT — PHOTO**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤  {user_name}  ·  `{user_id}`\n"
                    f"📅  {date_str}  ·  {time_str}\n\n"
                    f"_Full ticket details follow below._"
                ),
                parse_mode=ParseMode.MARKDOWN
            )

        if has_video:
            await bot.send_video(
                REVIEW_LOG_CHANNEL,
                message.video.file_id,
                caption=(
                    f"🎥  **TICKET ATTACHMENT — VIDEO**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤  {user_name}  ·  `{user_id}`\n"
                    f"📅  {date_str}  ·  {time_str}\n\n"
                    f"_Full ticket details follow below._"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Send main ticket message and store message_id
        channel_msg = await bot.send_message(
            REVIEW_LOG_CHANNEL,
            ticket_msg,
            parse_mode=ParseMode.MARKDOWN
        )
        channel_message_id = channel_msg.message_id
        logger.info(f"✅ Ticket submitted by user {user_id} to channel {REVIEW_LOG_CHANNEL} (Type: {ticket_type}, Msg ID: {channel_message_id})")
    except Exception as e:
        logger.error(f"❌ Failed to send ticket to admin channel: {e}")
        await message.answer(
            "❌ **SUBMISSION FAILED**\n\n"
            "Could not submit your ticket. Please try again later.",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        await state.clear()
        return
    
    # Count previous tickets for this user
    previous_tickets_count = col_support_tickets.count_documents({"user_id": user_id})
    support_count = previous_tickets_count + 1  # Current ticket number
    
    # Store ticket in database (LOCK SYSTEM)
    ticket_record = {
        "user_id": user_id,
        "user_name": user_name,
        "username": message.from_user.username or "none",
        "msa_id": display_msa_id,
        "issue_text": issue_text,
        "has_photo": has_photo,
        "has_video": has_video,
        "ticket_type": ticket_type,
        "status": "open",  # open, resolved
        "created_at": now,
        "resolved_at": None,
        "channel_message_id": channel_message_id,  # Store for editing later
        "support_count": support_count  # Track ticket number for this user
    }
    try:
        col_support_tickets.insert_one(ticket_record)
        logger.info(f"Ticket record created for user {user_id} in database (Support #{support_count})")
    except DuplicateKeyError:
        # Race-safe handling when another concurrent request already opened one ticket
        logger.warning(f"Duplicate open ticket blocked for user {user_id} (race condition handled)")
        await state.clear()
        await message.answer(
            "⚠️ **REQUEST ALREADY IN QUEUE**\n\n"
            "A support ticket is already open for your account.\n"
            "Please wait for admin response before submitting again.",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Record submission for rate limiting
    record_ticket_submission(user_id)
    
    # Clear state
    await state.clear()
    
    # 🎬 SUCCESS CONFIRMATION ANIMATION
    success_msg = await message.answer("✨")
    await asyncio.sleep(ANIM_FAST)
    await success_msg.edit_text("✨ ✅")
    await asyncio.sleep(ANIM_FAST)
    await success_msg.edit_text("✨ ✅ ✨")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(success_msg)
    
    # Build media attachment line for confirmation
    media_lines = ""
    if has_photo:
        media_lines += "   📷  Attachment:   Photo included\n"
    if has_video:
        media_lines += "   🎥  Attachment:   Video included\n"

    # Premium success confirmation to user
    await message.answer(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅  **TICKET SUBMITTED**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**{user_name}**, your support request has been securely forwarded to our admin team.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋  **TICKET SUMMARY**\n\n"
        f"   📅  Date:       {date_str}\n"
        f"   🕐  Time:       {time_str}\n"
        f"   🏷️  Type:       {ticket_type}\n"
        f"{media_lines}"
        f"   📊  Length:     {len(issue_text):,} / {MAX_TICKET_LENGTH:,} chars\n"
        f"   📌  Priority:   Normal\n"
        f"   🔄  Status:     ⏳ Awaiting Admin Review\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔔  **WHAT HAPPENS NEXT**\n\n"
        f"   ①  Your ticket is queued for admin review\n"
        f"   ②  Admin will respond to you directly via DM\n"
        f"   ③  Estimated response time: **24–48 hours**\n"
        f"   ④  You will be notified as soon as there is an update\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔒  One ticket at a time — submit a new one only after this is resolved.\n\n"
        f"💬  Thank you for reaching out, **{user_name}**.\n"
        f"   _We are committed to resolving your issue promptly._\n\n"
        f"`Returning to support menu...`",
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

    logger.info(f"User {user_id} ticket confirmed")


# ==========================================
# 📋 MY TICKET — STATUS + HISTORY + PAGINATION
# ==========================================

@dp.message(F.text == "📋 MY TICKET")
@rate_limit(2.0)
@anti_spam("my_ticket")
async def my_ticket_handler(message: types.Message):
    """Show active ticket status (with cancel button) or full ticket history."""
    if await _check_freeze(message): return
    if await check_maintenance_mode(message):
        return

    # Vault check
    if await _require_vault_check(message):
        return

    user_id    = message.from_user.id
    first_name = message.from_user.first_name or "Member"

    msg = await message.answer("📋 Checking your tickets...")
    await asyncio.sleep(ANIM_FAST)

    # ── Active ticket? ──────────────────────────────────────────────
    open_ticket = col_support_tickets.find_one({"user_id": user_id, "status": "open"})

    if open_ticket:
        created_at     = open_ticket.get("created_at", now_local())
        date_str       = created_at.strftime("%B %d, %Y at %I:%M %p")
        ticket_type    = open_ticket.get("ticket_type", "Text Only")
        char_count     = open_ticket.get("character_count", 0)
        issue_raw      = (open_ticket.get("issue_text") or "")
        issue_preview  = issue_raw[:200] + ("…" if len(issue_raw) > 200 else "")

        await safe_delete_message(msg)
        cancel_kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="❌ CANCEL MY TICKET")],
            [KeyboardButton(text="🔙 BACK TO SUPPORT")]
        ], resize_keyboard=True)
        await message.answer(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎫  **YOUR ACTIVE TICKET**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📋  **TICKET DETAILS**\n\n"
            f"   🔄  Status:     ⏳ Awaiting Admin Review\n"
            f"   📅  Submitted:  {date_str}\n"
            f"   🏷️  Type:       {ticket_type}\n"
            f"   📊  Length:     {char_count:,} characters\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝  **YOUR SUBMITTED MESSAGE**\n\n"
            f"_{issue_preview}_\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⏰  Expected response within **24–48 hours**.\n"
            f"🔒  You may not submit new tickets while this one is open.\n\n"
            f"_Tap_ **❌ CANCEL MY TICKET** _to permanently withdraw this request._",
            reply_markup=cancel_kb,
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"User {user_id} viewed active ticket status")
        return

    # ── No open ticket → show submitted ticket history only ──────────────────
    # Strictly: only real submitted tickets (open/resolved/archived)
    # Excludes: security_lock docs, cancelled tickets (already hard-deleted on cancel)
    all_tickets = list(
        col_support_tickets
        .find({
            "user_id": user_id,
            "status": {"$in": ["open", "resolved", "archived"]},
            "type":   {"$ne": "security_lock"},   # exclude support lock records
        })
        .sort("created_at", -1)
        .limit(5)
    )
    total = len(all_tickets)

    if total == 0:
        await safe_delete_message(msg)
        await message.answer(
            f"📋 **NO TICKET HISTORY**\n\n"
            f"{first_name}, you haven't submitted any support tickets yet.\n\n"
            f"Tap **🎫 RAISE A TICKET** whenever you need help!",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    await safe_delete_message(msg)
    await _send_ticket_history_page(message, user_id, all_tickets, 0, first_name)
    logger.info(f"User {user_id} viewed ticket history ({total} submitted tickets)")


# ─── Helpers ────────────────────────────────────────────────────────

def _esc_md(text: str) -> str:
    """Escape markdown special characters to prevent format breaking."""
    if not text:
        return ""
    # Only escaping standard Markdown (not MarkdownV2 which requires all chars)
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in escape_chars:
        text = text.replace(char, f"\\{char}")
    return text

def _build_ticket_history_page(tickets: list, page: int, first_name: str) -> str:
    """Build a single page of ticket history (1 ticket per page)."""
    total = len(tickets)
    page  = page % total
    t     = tickets[page]

    created_at  = t.get("created_at", now_local())
    resolved_at = t.get("resolved_at")
    status      = t.get("status", "open")
    ticket_type = t.get("ticket_type", "Text Only")
    char_count  = t.get("character_count", 0)
    support_num = t.get("support_count", page + 1)
    issue_text  = (t.get("issue_text") or "")
    preview     = _esc_md(issue_text)

    date_str = created_at.strftime("%B %d, %Y at %I:%M %p")
    status_badge = {
        "open":     "⏳ Awaiting Review",
        "resolved": "✅ Resolved",
        "archived": "🗄️ Archived",
    }.get(status, f"❓ {status.capitalize()}")

    resolved_line = ""
    if resolved_at:
        resolved_line = f"**Resolved:** {resolved_at.strftime('%B %d, %Y at %I:%M %p')}\n"

    return (
        f"📋 **TICKET HISTORY** _· {page + 1} of {total}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎫 **Ticket #{support_num}**\n\n"
        f"**Status:** {status_badge}\n"
        f"**Submitted:** {date_str}\n"
        f"{resolved_line}"
        f"**Type:** {ticket_type}\n"
        f"**Characters:** {char_count}\n\n"
        f"📝 **Your Message:**\n"
        f"_{preview}_\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━"
    )


async def _send_ticket_history_page(message_or_cb, user_id: int, tickets: list, page: int, first_name: str):
    """Send (new message) or edit (callback) a ticket history page with PREV/NEXT nav."""
    total = len(tickets)
    page  = page % total
    text  = _build_ticket_history_page(tickets, page, first_name)

    if total > 1:
        prev_pg = (page - 1) % total
        next_pg = (page + 1) % total
        nav_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️",                callback_data=f"tkt_pg:{user_id}:{prev_pg}"),
            InlineKeyboardButton(text=f"🎫 {page + 1}/{total}", callback_data="tkt_noop"),
            InlineKeyboardButton(text="▶️",                callback_data=f"tkt_pg:{user_id}:{next_pg}"),
        ]])
    else:
        nav_kb = None

    if isinstance(message_or_cb, types.Message):
        await message_or_cb.answer(text, reply_markup=nav_kb, parse_mode=ParseMode.MARKDOWN)
    else:
        # CallbackQuery — edit existing message
        await message_or_cb.message.edit_text(text, reply_markup=nav_kb, parse_mode=ParseMode.MARKDOWN)


@dp.callback_query(F.data.startswith("tkt_pg:"))
async def ticket_history_page_callback(callback: types.CallbackQuery):
    """Navigate ticket history pages (PREV / NEXT)."""
    try:
        parts      = callback.data.split(":")
        uid        = int(parts[1])
        page       = int(parts[2])
        first_name = callback.from_user.first_name or "Member"

        # Always fetch only the 3 most recent — same limit as MY TICKET view
        all_tickets = list(
            col_support_tickets
            .find({"user_id": uid})
            .sort("created_at", -1)
            .limit(3)
        )
        if not all_tickets:
            await callback.answer("No tickets found.", show_alert=False)
            return

        await _send_ticket_history_page(callback, uid, all_tickets, page, first_name)
        await callback.answer()
    except Exception as e:
        logger.error(f"ticket_history_page_callback error: {e}")
        await callback.answer("Error loading page.", show_alert=True)


@dp.callback_query(F.data == "tkt_noop")
async def ticket_noop_callback(callback: types.CallbackQuery):
    """No-op: page indicator button in ticket history nav bar."""
    await callback.answer()


@dp.message(F.text == "❌ CANCEL MY TICKET")
@rate_limit(3.0)
async def cancel_ticket_handler(message: types.Message):
    """Allow a user to permanently delete their open support ticket from DB + review channel."""
    if await _check_freeze(message): return

    # Vault check
    if await _require_vault_check(message):
        return

    uid        = message.from_user.id
    first_name = message.from_user.first_name or "Member"
    ticket     = col_support_tickets.find_one({"user_id": uid, "status": "open"})

    if not ticket:
        await message.answer(
            "ℹ️ **NO OPEN TICKET**\n\nYou don't have an active ticket to cancel.",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── 1. Delete from review channel ──────────────────────────────
    channel_msg_id = ticket.get("channel_message_id")
    if channel_msg_id and REVIEW_LOG_CHANNEL:
        try:
            await bot.delete_message(REVIEW_LOG_CHANNEL, channel_msg_id)
            logger.info(f"Deleted ticket channel msg {channel_msg_id} for user {uid}")
        except Exception as e:
            logger.warning(f"Could not delete channel msg {channel_msg_id}: {e}")

    # ── 2. Permanently delete from database ────────────────────────
    col_support_tickets.delete_one({"_id": ticket["_id"]})
    logger.info(f"User {uid} cancelled + permanently deleted open ticket from DB")

    await message.answer(
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"❌  **TICKET WITHDRAWN**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**{first_name}**, your support request has been permanently cancelled.\n\n"
        f"   ✅  Removed from database\n"
        f"   ✅  Removed from admin review queue\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡  You may raise a new ticket at any time.\n"
        f"   _Tap_ **🎫 RAISE A TICKET** _whenever you need help._",
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )


@dp.message(F.text == "🔙 BACK TO SUPPORT")
async def back_to_support_handler(message: types.Message):
    """Return to support menu from ticket view."""
    if await _require_vault_check(message):
        return
    await message.answer("↩️ Support Menu", reply_markup=get_support_menu())


@dp.message(Command("resolve"))
@rate_limit(10.0)  # Strict 10 second cooldown for admin command
async def cmd_resolve_ticket(message: types.Message):
    """Resolve a user's ticket (Admin only command - strict rate limit)"""  
    # Only owner/admin can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Parse command: /resolve <user_id>
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "**Usage:** `/resolve <user_id>`\n\nExample: `/resolve 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        target_user_id = int(parts[1])
        
        # Find and update ticket
        result = col_support_tickets.update_one(
            {"user_id": target_user_id, "status": "open"},
            {"$set": {"status": "resolved", "resolved_at": now_local()}}
        )
        
        if result.modified_count > 0:
            await message.answer(
                f"✅ **Ticket Resolved**\n\n"
                f"**User ID:** `{target_user_id}`\n\n"
                f"User can now submit new tickets.",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Admin {message.from_user.id} resolved ticket for user {target_user_id}")
            
            # Notify user their ticket is resolved
            try:
                await bot.send_message(
                    target_user_id,
                    "✅ **TICKET RESOLVED**\n\n"
                    "Your support ticket has been reviewed and resolved by admin.\n\n"
                    "You can now submit new tickets if needed.\n\n"
                    "Thank you for your patience!",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass  # User might have blocked bot
        else:
            await message.answer(
                f"❌ **No Open Ticket**\n\n"
                f"User `{target_user_id}` has no open tickets.",
                parse_mode=ParseMode.MARKDOWN
            )
    except ValueError:
        await message.answer(
            "❌ **Invalid User ID**\n\nProvide a valid numeric user ID.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error resolving ticket: {e}")

@dp.message(
    F.text == "🔙 BACK TO MENU",
    ~StateFilter(SearchCodeStates.waiting_for_code, SearchCodeStates.waiting_for_first_code)
)
@rate_limit(1.5)
@anti_spam("back_menu")
async def back_to_menu_handler(message: types.Message, state: FSMContext):
    """Handle Back to Menu button"""
    # Check Maintenance Mode - still allow nav back to menu during maintenance
    # but show maintenance screen (user can't do anything anyway)
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Clear any state
    await state.clear()
    
    # Check if user is banned
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **BANNED USER**\n\nYou are banned from using bot features.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # 🔒 STRICT VAULT GATE — Non-vault users cannot access the main menu
    if await _require_vault_check(message):
        return
    
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 TRANSITION ANIMATION
    msg = await message.answer("🔄 Returning to main menu...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    await message.answer(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📋  **MSA NODE AGENT — MAIN MENU**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Welcome back, **{first_name}**! 👋\n\n"
        f"🚀 **All your services are live and ready.**\n\n"
        f"  📊 **DASHBOARD** — Your vault stats & MSA+ ID\n"
        f"  🔍 **SEARCH CODE** — Unlock exclusive content\n"
        f"  📺 **WATCH TUTORIAL** — Your starter guide video\n"
        f"  📖 **AGENT GUIDE** — Full bot manual\n"
        f"  📜 **RULES** — Community code of conduct\n"
        f"  📞 **SUPPORT** — Open a ticket anytime\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 _MSA NODE Agent  |  Your Exclusive Gateway_",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} returned to main menu")

@dp.message(Command("delete"))
@rate_limit(5.0)  # 5 second cooldown for delete command (admin only)
async def cmd_delete_user(message: types.Message):
    """Delete user verification data (Owner only - for testing)"""
    # Only owner can use this command
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ This command is only for the owner.", parse_mode=ParseMode.MARKDOWN)
        return
    
    # Get user ID from command
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "**Usage:** `/delete <user_id>`\n\nExample: `/delete 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        target_user_id = int(parts[1])
        
        # Get MSA+ ID before deletion (for confirmation message)
        msa_record = col_msa_ids.find_one({"user_id": target_user_id})
        deleted_msa_id = msa_record['msa_id'] if msa_record else None
        
        # Delete from both collections
        result_verification = col_user_verification.delete_one({"user_id": target_user_id})
        result_msa = col_msa_ids.delete_one({"user_id": target_user_id})
        
        if result_verification.deleted_count > 0 or result_msa.deleted_count > 0:
            msa_info = f"\n🆔 **MSA+ ID Deleted**: `{deleted_msa_id}`" if deleted_msa_id else ""
            await message.answer(
                f"✅ **User Deleted**\n\n**User ID:** `{target_user_id}`{msa_info}\n\nVerification data has been removed from database.\n\nThis user will be treated as a new user on next /start.\n\n🔄 **Note**: The MSA+ ID `{deleted_msa_id}` is now available for reassignment.",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Owner {message.from_user.id} deleted user {target_user_id} (MSA+ ID: {deleted_msa_id}) from database")
        else:
            await message.answer(
                f"❌ **User Not Found**\n\n**User ID:** `{target_user_id}`\n\nNo verification data found in database.",
                parse_mode=ParseMode.MARKDOWN
            )
    except ValueError:
        await message.answer(
            "❌ **Invalid User ID**\n\nPlease provide a valid numeric user ID.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in delete command: {e}")

# NOTE: /resolve is defined earlier (once). Duplicate removed.

@dp.message(Command("ticket_stats"))
@rate_limit(5.0)
async def cmd_ticket_stats(message: types.Message):
    """Display ticket statistics (Admin only command)"""
    # Only owner/admin can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Count tickets by status
        open_count = col_support_tickets.count_documents({"status": "open"})
        resolved_count = col_support_tickets.count_documents({"status": "resolved"})
        archived_count = col_support_tickets.count_documents({"status": "archived"})
        total_count = open_count + resolved_count + archived_count
        
        # Get recent tickets (last 24 hours)
        yesterday = now_local() - timedelta(days=1)
        recent_count = col_support_tickets.count_documents({
            "created_at": {"$gte": yesterday}
        })
        
        # Get tickets to be archived soon (resolved > 6 days ago)
        expire_soon_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS - 1)
        expire_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS)
        expire_soon_count = col_support_tickets.count_documents({
            "status": "resolved",
            "resolved_at": {"$gte": expire_date, "$lt": expire_soon_date}
        })
        
        await message.answer(
            f"📊 **SUPPORT TICKET STATISTICS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**📋 Overall Status:**\n"
            f"• Total Tickets: `{total_count}`\n"
            f"• 🔴 Open: `{open_count}`\n"
            f"• 🟢 Resolved: `{resolved_count}`\n"
            f"• 🗄️ Archived: `{archived_count}`\n\n"
            f"**📅 Recent Activity:**\n"
            f"• Last 24 Hours: `{recent_count}` new tickets\n\n"
            f"**🗑️ Auto-Archive System:**\n"
            f"• Archive After: `{TICKET_EXPIRE_DAYS} days`\n"
            f"• Expiring Soon: `{expire_soon_count}` tickets\n"
            f"• Status: ✅ Active\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Auto-cleanup runs every 24 hours_",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Admin {message.from_user.id} viewed ticket statistics")
        
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in ticket stats: {e}")

@dp.message(Command("health"))
@rate_limit(5.0)
async def cmd_bot_health(message: types.Message):
    """Display bot health status (Owner only command)"""
    # Only owner can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Calculate uptime
        uptime = now_local() - health_stats["bot_start_time"]
        days = int(uptime.total_seconds() // 86400)
        hours = int((uptime.total_seconds() % 86400) // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)
        
        # Check database status
        db_status = "❌ OFFLINE"
        try:
            client.admin.command('ping')
            db_status = "✅ ONLINE"
        except:
            pass
        
        # Check bot status
        bot_status = "❌ ERROR"
        try:
            me = await bot.get_me()
            bot_status = f"✅ ONLINE (@{me.username})"
        except:
            pass
        
        # Last error info
        last_error_info = "None"
        if health_stats["last_error"]:
            time_since = now_local() - health_stats["last_error"]
            mins_ago = int(time_since.total_seconds() // 60)
            last_error_info = f"{mins_ago} minutes ago"
        
        # Calculate success rate
        total_errors = health_stats["errors_caught"]
        healed = health_stats["auto_healed"]
        success_rate = (healed / total_errors * 100) if total_errors > 0 else 100
        
        await message.answer(
            f"🏥 **BOT HEALTH STATUS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**⚡ System Status:**\n"
            f"• Bot: {bot_status}\n"
            f"• Database: {db_status}\n"
            f"• Health Monitor: ✅ Active\n"
            f"• Auto-Healer: ✅ Active\n\n"
            f"**⏱️ Uptime:**\n"
            f"• Running: {days}d {hours}h {minutes}m\n"
            f"• Started: {health_stats['bot_start_time'].strftime('%b %d, %I:%M %p')}\n\n"
            f"**📊 Error Statistics:**\n"
            f"• Total Caught: `{total_errors}`\n"
            f"• Auto-Healed: `{healed}`\n"
            f"• Manual Fixes: `{total_errors - healed}`\n"
            f"• Success Rate: `{success_rate:.1f}%`\n"
            f"• Owner Alerts: `{health_stats['owner_notified']}`\n\n"
            f"**🕐 Last Error:**\n"
            f"• {last_error_info}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Health checks run automatically every hour_",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Owner {message.from_user.id} checked bot health")
        
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in health command: {e}")

# ==========================================
# � DEAD USER STATS — OWNER ONLY
# ==========================================

@dp.message(Command("dead_users"))
@rate_limit(5.0)
async def cmd_dead_users(message: types.Message):
    """Owner-only: show dead / ghost / inactive user pipeline statistics."""
    if message.from_user.id != OWNER_ID:
        return
    try:
        now = now_local()

        # Active vault members
        active = col_user_verification.count_documents({"vault_joined": True})

        # Phase 1 — left vault, MSA ID still held (0–30 days out)
        phase1 = col_user_verification.count_documents({
            "vault_joined": False,
            "vault_left_at": {"$exists": True}
        })

        # Phase 2 — MSA ID deleted, user_verification record pending cleanup (30–90 days)
        phase2 = col_user_verification.count_documents({
            "msa_cleared_at": {"$exists": True}
        })
        # Breakdown: how many are already past DEAD_USER_CLEANUP_DAYS
        dead_cutoff = now - timedelta(days=DEAD_USER_CLEANUP_DAYS)
        phase2_overdue = col_user_verification.count_documents({
            "msa_cleared_at": {"$exists": True, "$lt": dead_cutoff}
        })

        # Ghost users — /started but never joined vault
        ghost_total = col_user_verification.count_documents({
            "ever_verified": False,
            "vault_joined":  False,
            "vault_left_at":  {"$exists": False},
            "msa_cleared_at": {"$exists": False},
        })
        ghost_cutoff = now - timedelta(days=GHOST_USER_CLEANUP_DAYS)
        ghost_overdue = col_user_verification.count_documents({
            "ever_verified": False,
            "vault_joined":  False,
            "vault_left_at":  {"$exists": False},
            "msa_cleared_at": {"$exists": False},
            "first_start":    {"$lt": ghost_cutoff},
        })

        total_docs = col_user_verification.count_documents({})
        # Exclude retired MSA IDs (from RESET USER DATA) — only count active members
        total_msa  = col_msa_ids.count_documents({"retired": {"$ne": True}})

        await message.answer(
            f"💬 **DEAD USER PIPELINE — /dead_users**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📄 **user_verification docs:** `{total_docs}`\n"
            f"🆔 **Active MSA IDs:**  `{total_msa}`\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ **Active vault members:** `{active}`\n\n"
            f"⌚ **Phase 1 — Left vault, MSA ID held** _(0–30 days)_\n"
            f"   `{phase1}` users pending reminders / ID release\n\n"
            f"🗑️ **Phase 2 — MSA ID released, record pending purge** _(30–90 days)_\n"
            f"   `{phase2}` total  ·  `{phase2_overdue}` overdue (\u2265{DEAD_USER_CLEANUP_DAYS}d, next run clears them)\n\n"
            f"👻 **Ghost users** _(registered, never joined vault)_\n"
            f"   `{ghost_total}` total  ·  `{ghost_overdue}` overdue (≥{GHOST_USER_CLEANUP_DAYS}d, next run clears them)\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Phase-1 cleanup: **30 days** after vault leave\n"
            f"⏱ Phase-2 purge:    **{DEAD_USER_CLEANUP_DAYS} days** after MSA-ID release\n"
            f"⏱ Ghost purge:      **{GHOST_USER_CLEANUP_DAYS} days** after first /start\n"
            f"_(monitor runs every 6 hours automatically)_",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Owner {message.from_user.id} checked dead user stats")
    except Exception as e:
        await message.answer(f"\u274c dead_users error: `{str(e)[:200]}`", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"cmd_dead_users error: {e}")


# ==========================================
# �🗑️ RESET BOT DATA — OWNER ONLY (double-confirm)
# Scope: All bot data lives in single MSANodeDB database.
#         Bot 1  → user data collections only (no backups, no bot3 content)
#         Bot 2 → bot2_user_tracking + bot2_broadcasts only
#                   (MSANodeDB reset must be done via Bot 2 admin panel)
# ==========================================

@dp.message(Command("resetdata"))
@rate_limit(10.0)
async def cmd_resetdata(message: types.Message, state: FSMContext):
    """OWNER-ONLY: Full data reset for Bot 1, Bot 2 or Bot 3 — double-confirm required."""
    if message.from_user.id != OWNER_ID:
        return
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 RESET BOT 1 DATA"), KeyboardButton(text="🤖 RESET BOT 2 DATA")],
            [KeyboardButton(text="🤖 RESET BOT 3 DATA")],
            [KeyboardButton(text="❌ CANCEL RESET")]
        ],
        resize_keyboard=True
    )
    await state.set_state(ResetDataStates.selecting_reset_target)
    await message.answer(
        "⚠️ **RESET BOT DATA — OWNER ONLY**\n\n"
        "This will **permanently delete ALL data** for the selected bot.\n"
        "Backup records are always preserved and not affected.\n\n"
        "Select which bot's data to reset:",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(ResetDataStates.selecting_reset_target)
async def reset_select_target(message: types.Message, state: FSMContext):
    """Step 2 — Store target, display scope, request first CONFIRM."""
    if message.from_user.id != OWNER_ID:
        await state.clear()
        return

    text = message.text

    if text == "❌ CANCEL RESET":
        await state.clear()
        await message.answer(
            "✅ Reset cancelled.",
            reply_markup=get_user_menu(message.from_user.id),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if text == "🤖 RESET BOT 1 DATA":
        target = "bot1"
        label  = "Bot 1"
        scope  = (
            "• `bot1_user_verification`\n"
            "• `bot1_msa_ids`\n"
            "• `bot1_support_tickets`\n"
            "• `bot1_banned_users`\n"
            "• `bot1_suspended_features`\n"
            "• `bot1_settings`\n"
            "• `bot1_permanently_banned_msa`\n"
            "• `bot1_offline_log`\n"
            "• `bot1_state_persistence`\n"
        )
    elif text == "🤖 RESET BOT 2 DATA":
        target = "bot2"
        label  = "Bot 2"
        scope  = (
            "• `bot2_user_tracking`\n"
            "• `bot2_broadcasts`\n"
            "• `bot2_cleanup_logs`\n"
            "• `bot2_access_attempts`\n"
            "• `bot2_live_terminal_logs`\n"
            "• `bot2_admins`\n\n"
        )
    elif text == "🤖 RESET BOT 3 DATA":
        target = "bot3"
        label  = "Bot 3"
        scope  = (
            "• `bot3_pdfs`\n"
            "• `bot3_ig_content`\n"
            "• `bot3_logs`\n"
            "• `bot3_settings`\n"
            "• `bot3_admins`\n"
            "• `bot3_banned_users`\n"
            "• `bot3_user_activity`\n"
            "• `bot3_state`\n\n"
        )
    else:
        await message.answer(
            "❌ Invalid choice. Select **BOT 1**, **BOT 2**, **BOT 3** or press **CANCEL RESET**.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    await state.update_data(reset_target=target)
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL RESET")]],
        resize_keyboard=True
    )
    await state.set_state(ResetDataStates.waiting_for_confirm1)
    await message.answer(
        f"⚠️ **CONFIRM RESET — STEP 1 of 2**\n\n"
        f"You are about to permanently delete ALL **{label}** data:\n\n"
        f"{scope}\n"
        f"✅ Backups are **NOT** included and remain intact.\n\n"
        f"🔴 This action **cannot be undone**.\n\n"
        f"Type `CONFIRM` to continue, or press ❌ CANCEL RESET:",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(ResetDataStates.waiting_for_confirm1)
async def reset_confirm1(message: types.Message, state: FSMContext):
    """Step 3 — Validate CONFIRM then show final DELETE prompt."""
    if message.from_user.id != OWNER_ID:
        await state.clear()
        return

    if message.text == "❌ CANCEL RESET":
        await state.clear()
        await message.answer(
            "✅ Reset cancelled.",
            reply_markup=get_user_menu(message.from_user.id),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if message.text != "CONFIRM":
        await message.answer(
            "❌ You must type exactly `CONFIRM` (all caps) to proceed, "
            "or press ❌ CANCEL RESET.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    data  = await state.get_data()
    target = data.get("reset_target")
    label = "Bot 1" if target == "bot1" else ("Bot 2" if target == "bot2" else "Bot 3")
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL RESET")]],
        resize_keyboard=True
    )
    await state.set_state(ResetDataStates.waiting_for_confirm2)
    await message.answer(
        f"🔴 **FINAL WARNING — STEP 2 of 2**\n\n"
        f"You are about to permanently erase ALL **{label}** data.\n\n"
        f"⛔ **THIS CANNOT BE UNDONE.** Every {label} record will be deleted.\n\n"
        f"Backups remain intact. Only {label} data is affected.\n\n"
        f"Type `DELETE` to execute the full {label} data wipe, "
        f"or press ❌ CANCEL RESET:",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(ResetDataStates.waiting_for_confirm2)
async def reset_confirm2(message: types.Message, state: FSMContext):
    """Step 4 — Validate DELETE then execute targeted delete_many on ONLY the chosen collections."""
    if message.from_user.id != OWNER_ID:
        await state.clear()
        return

    if message.text == "❌ CANCEL RESET":
        await state.clear()
        await message.answer(
            "✅ Reset cancelled.",
            reply_markup=get_user_menu(message.from_user.id),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if message.text != "DELETE":
        await message.answer(
            "❌ You must type exactly `DELETE` (all caps) to execute, "
            "or press ❌ CANCEL RESET.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    data   = await state.get_data()
    target = data.get("reset_target")
    label  = "Bot 1" if target == "bot1" else ("Bot 2" if target == "bot2" else "Bot 3")

    try:
        results: dict[str, int] = {}

        if target == "bot1":
            results["bot1_user_verification"] = col_user_verification.delete_many({}).deleted_count
            results["bot1_msa_ids"] = col_msa_ids.delete_many({}).deleted_count
            results["bot1_support_tickets"] = col_support_tickets.delete_many({}).deleted_count
            results["bot1_banned_users"] = col_banned_users.delete_many({}).deleted_count
            results["bot1_suspended_features"] = col_suspended_features.delete_many({}).deleted_count
            results["bot1_settings"] = col_bot1_settings.delete_many({}).deleted_count
            results["bot1_permanently_banned_msa"] = db["bot1_permanently_banned_msa"].delete_many({}).deleted_count
            results["bot1_offline_log"] = db["bot1_offline_log"].delete_many({}).deleted_count
            results["bot1_state_persistence"] = db["bot1_state_persistence"].delete_many({}).deleted_count

        elif target == "bot2":
            results["bot2_user_tracking"] = db["bot2_user_tracking"].delete_many({}).deleted_count
            results["bot2_broadcasts"] = col_broadcasts.delete_many({}).deleted_count
            results["bot2_cleanup_logs"] = db["bot2_cleanup_logs"].delete_many({}).deleted_count
            results["bot2_access_attempts"] = db["bot2_access_attempts"].delete_many({}).deleted_count
            results["bot2_live_terminal_logs"] = db["bot2_live_terminal_logs"].delete_many({}).deleted_count
            results["bot2_admins"] = db["bot2_admins"].delete_many({}).deleted_count

        elif target == "bot3":
            results["bot3_pdfs"] = db["bot3_pdfs"].delete_many({}).deleted_count
            results["bot3_ig_content"] = db["bot3_ig_content"].delete_many({}).deleted_count
            results["bot3_logs"] = db["bot3_logs"].delete_many({}).deleted_count
            results["bot3_settings"] = db["bot3_settings"].delete_many({}).deleted_count
            results["bot3_admins"] = db["bot3_admins"].delete_many({}).deleted_count
            results["bot3_banned_users"] = db["bot3_banned_users"].delete_many({}).deleted_count
            results["bot3_user_activity"] = db["bot3_user_activity"].delete_many({}).deleted_count
            results["bot3_state"] = db["bot3_state"].delete_many({}).deleted_count

        total     = sum(results.values())
        breakdown = "\n".join(f"  • `{k}`: {v:,}" for k, v in results.items())

        await state.clear()
        await message.answer(
            f"✅ **{label.upper()} DATA RESET COMPLETE**\n\n"
            f"🗑️ Total records deleted: **{total:,}**\n\n"
            f"**Breakdown:**\n{breakdown}\n\n"
            f"✅ Backups remain intact.\n"
            f"✅ Only {label} data was affected.",
            reply_markup=get_user_menu(message.from_user.id),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"OWNER {message.from_user.id} executed full {label} data reset — {total} records deleted.")

    except Exception as e:
        await state.clear()
        await message.answer(
            f"❌ **RESET FAILED**\n\n{str(e)}\n\nPartial deletion may have occurred.",
            reply_markup=get_user_menu(message.from_user.id),
            parse_mode=ParseMode.MARKDOWN
        )


# ==========================================
# 🏥 ENTERPRISE AUTO-HEALER & HEALTH SYSTEM
# ==========================================

# NOTE: health_stats is defined early at top of file.

# Exponential backoff: wait 1s, 2s, 4s, 8s, 16s (max)
_BACKOFF_BASE = 1
_BACKOFF_MAX = 16
_MAX_HEAL_RETRIES = 5

# Per-alert cooldown tracker to prevent notification spam:
# Format: {"{severity}:{error_type}": last_sent_datetime}
_last_owner_alert: dict = {}
# Cooldown seconds per severity level
_NOTIFY_COOLDOWNS = {"WARNING": 1800, "ERROR": 600, "CRITICAL": 120}

async def notify_owner(error_type: str, error_msg: str, severity: str = "CRITICAL", auto_healed: bool = False):
    """Instantly notify owner of errors via Telegram with full context.

    Severity levels: WARNING | ERROR | CRITICAL
    Duplicate alerts of the same type+severity are suppressed within the cooldown window.
    """
    try:
        # --- Cooldown / deduplication ---
        cooldown = _NOTIFY_COOLDOWNS.get(severity, 600)
        alert_key = f"{severity}:{error_type}"
        last_sent = _last_owner_alert.get(alert_key)
        if last_sent:
            elapsed = (datetime.now(TZ) - last_sent).total_seconds()
            if elapsed < cooldown:
                logger.debug(f"[notify_owner] Suppressing {severity} alert '{error_type}' ({cooldown - elapsed:.0f}s left)")
                return
        _last_owner_alert[alert_key] = datetime.now(TZ)
        # --- end cooldown ---

        health_stats["owner_notified"] += 1

        emoji_map = {"CRITICAL": "🔴", "ERROR": "🟠", "WARNING": "🟡"}
        emoji = emoji_map.get(severity, "🟡")
        heal_status = "✅ AUTO-HEALED" if auto_healed else "❌ MANUAL FIX NEEDED"

        now_tz = datetime.now(TZ)
        uptime = now_tz - health_stats["bot_start_time"]
        hours = int(uptime.total_seconds() // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)

        # Truncate error for Telegram (4096 char limit)
        safe_error = str(error_msg)[:600].replace("`", "'")

        notification = (
            f"{emoji} **BOT 1 — HEALTH ALERT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Severity:** `{severity}`\n"
            f"**Type:** `{error_type}`\n"
            f"**Status:** {heal_status}\n\n"
            f"**Error Details:**\n"
            f"```\n{safe_error}\n```\n\n"
            f"**Bot Statistics:**\n"
            f"• Uptime: {hours}h {minutes}m\n"
            f"• Errors Caught: {health_stats['errors_caught']}\n"
            f"• Auto-Healed: {health_stats['auto_healed']}\n"
            f"• Owner Alerts: {health_stats['owner_notified']}\n"
            f"• DB Reconnects: {health_stats['db_reconnects']}\n\n"
            f"**Timestamp:** {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 MSA NODE Bot 1 — Health Monitor"
        )

        await bot.send_message(OWNER_ID, notification, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"📢 Owner notified of {severity}: {error_type}")

    except TelegramRetryAfter as e:
        logger.info(f"[notify_owner] Flood control ({e.retry_after}s) — alert '{error_type}' skipped (cooldown will retry)")
    except Exception as e:
        logger.error(f"❌ Failed to notify owner: {e}")


async def auto_heal(error_type: str, error: Exception, context: dict[str, object] | None = None) -> bool:
    """Attempt automatic healing with exponential backoff retry.

    Returns True if healing succeeded.
    """
    error_str = str(error).lower()
    tb = traceback.format_exc()

    for attempt in range(1, _MAX_HEAL_RETRIES + 1):
        wait = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
        try:
            logger.warning(f"🏥 Auto-heal attempt {attempt}/{_MAX_HEAL_RETRIES} for: {error_type}")

            # ── Database / MongoDB ──────────────────────────────────
            if any(k in error_str for k in ("mongo", "database", "pymongo", "serverselection")):
                logger.info("🔌 Attempting database reconnection...")
                client.admin.command('ping')
                logger.info("✅ Database connection restored!")
                health_stats["auto_healed"] += 1
                health_stats["db_reconnects"] += 1
                return True

            # ── Telegram FloodWait / RetryAfter ─────────────────────
            if isinstance(error, TelegramRetryAfter):
                retry_after = getattr(error, 'retry_after', 30) + 1
                logger.info(f"⏳ Telegram FloodWait: sleeping {retry_after}s")
                await asyncio.sleep(retry_after)
                health_stats["auto_healed"] += 1
                return True

            # ── Generic timeout / network ───────────────────────────
            if any(k in error_str for k in ("timeout", "timed out", "read timeout")):
                logger.info(f"⏱️ Timeout — waiting {wait}s before retry")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            if any(k in error_str for k in ("connection", "network", "socket", "eof", "ssl")):
                logger.info(f"🔄 Network error — waiting {wait}s")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            # ── Rate-limit (non-Telegram) ──────────────────────────
            if "rate limit" in error_str:
                logger.info(f"🚦 Rate limit — waiting {wait}s")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            # ── Unknown ─────────────────────────────────────────────
            logger.warning(f"❓ Unknown error type — cannot auto-heal: {error_type}")
            return False

        except Exception as heal_err:
            logger.error(f"❌ Healing attempt {attempt} failed: {heal_err}")
            if attempt < _MAX_HEAL_RETRIES:
                await asyncio.sleep(wait)

    logger.error(f"💀 All {_MAX_HEAL_RETRIES} healing attempts exhausted for: {error_type}")
    return False


async def health_monitor():
    """Background task: ping DB + bot every hour, alert owner on failure."""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour

            # ── DB ping ──────────────────────────────────────────────
            try:
                client.admin.command('ping')
                logger.info("✅ Hourly health check: Database OK")
            except Exception as e:
                logger.error(f"❌ Hourly health check: DB FAILED — {e}")
                healed = await auto_heal("DB Health Check", e)
                await notify_owner("Database Health Check", str(e), "ERROR" if healed else "CRITICAL", healed)

            # ── Bot API ping ──────────────────────────────────────────
            try:
                me = await bot.get_me()
                logger.info(f"✅ Hourly health check: Bot OK (@{me.username})")
            except Exception as e:
                logger.error(f"❌ Hourly health check: Bot API FAILED — {e}")
                healed = await auto_heal("Bot API Check", e)
                await notify_owner("Bot API Connection", str(e), "CRITICAL", healed)

        except Exception as e:
            logger.error(f"❌ Health monitor loop error: {e}")


async def global_error_handler(update: types.Update, exception: Exception):
    """Catch ALL unhandled errors from dispatcher and attempt auto-healing."""
    try:
        health_stats["errors_caught"] += 1
        health_stats["last_error"] = datetime.now(TZ)
        health_stats["last_error_msg"] = str(exception)[:200]

        error_type = type(exception).__name__
        error_msg = str(exception)
        tb = traceback.format_exc()

        logger.error(f"❌ Unhandled {error_type}: {error_msg}\n{str(tb)[:800]}")

        # Skip logging of harmless Telegram errors
        if isinstance(exception, TelegramAPIError):
            if "message is not modified" in error_msg.lower():
                return True  # Harmless, don't alert owner
            if "message to delete not found" in error_msg.lower():
                return True

        # Attempt healing
        healed = await auto_heal(error_type, exception, {"update": update})

        # Severity determination
        if isinstance(exception, TelegramRetryAfter):
            severity = "WARNING"
        elif "critical" in error_msg.lower() or "fatal" in error_msg.lower():
            severity = "CRITICAL"
        elif healed:
            severity = "WARNING"
        else:
            severity = "ERROR"

        # Always alert owner (even for auto-healed errors) unless WARNING
        if severity != "WARNING" or not healed:
            await notify_owner(error_type, f"{error_msg}\n\nTraceback:\n{str(tb)[:400]}", severity, healed)

        logger.info(f"🏥 Error handled — Auto-healed: {healed}")
        return True

    except Exception as e:
        logger.critical(f"💥 Error handler itself crashed: {e}")
        try:
            await bot.send_message(
                OWNER_ID,
                f"🔴🔴🔴 **CRITICAL — ERROR HANDLER CRASHED**\n\n```{str(e)[:300]}```",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return False


# ==========================================
# 🗑️ AUTO-EXPIRE TICKETS SYSTEM
# ==========================================

TICKET_EXPIRE_DAYS = int(os.getenv("TICKET_EXPIRE_DAYS", 365))  # Days after resolution to auto-archive (status change only, never deleted)

async def auto_expire_tickets():
    """Background task: archive old resolved tickets every 24 hours."""
    while True:
        try:
            await asyncio.sleep(86400)  # 24 hours

            expire_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS)
            old_tickets = list(col_support_tickets.find({
                "status": "resolved",
                "resolved_at": {"$lt": expire_date}
            }))

            archived_count = 0
            for ticket in old_tickets:
                col_support_tickets.update_one(
                    {"_id": ticket["_id"]},
                    {"$set": {"status": "archived", "archived_at": now_local()}}
                )
                archived_count += 1

            if archived_count > 0:
                logger.info(f"🗑️ Auto-archived {archived_count} resolved tickets (>{TICKET_EXPIRE_DAYS} days old)")
            else:
                logger.info("✅ Ticket cleanup: nothing to archive")

        except Exception as e:
            logger.error(f"❌ Auto-expire tickets error: {e}")


# ==========================================
# 📊 TWICE-DAILY REPORT SYSTEM (8:40 AM & PM)
# ==========================================

async def _build_daily_report(period: str) -> str:
    """Build a comprehensive report string for owner."""
    now_tz = datetime.now(TZ)
    uptime = now_tz - health_stats["bot_start_time"]
    days = int(uptime.total_seconds() // 86400)
    hours = int((uptime.total_seconds() % 86400) // 3600)
    minutes = int((uptime.total_seconds() % 3600) // 60)

    # ── DB Stats (run in executor to avoid blocking) ─────────────
    loop = asyncio.get_running_loop()

    def _get_stats(*args):
        total_users = col_user_verification.count_documents({})
        verified_users = col_user_verification.count_documents({"verified": True})
        total_msa_ids = col_msa_ids.count_documents({})
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        archived_tickets = col_support_tickets.count_documents({"status": "archived"})
        banned_users = col_banned_users.count_documents({})
        total_pdfs = col_pdfs.count_documents({})
        total_ig_content = col_ig_content.count_documents({})

        # New users today
        today_start = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
        new_today = col_user_verification.count_documents({
            "first_start": {"$gte": today_start}
        })

        # Clicks today
        total_clicks_today = (
            col_pdfs.aggregate([
                {"$group": {"_id": None, "total": {"$sum": "$clicks"}}}
            ])
        )
        clicks_sum = 0
        for c in total_clicks_today:
            clicks_sum = c.get("total", 0)

        # DB ping + storage size
        try:
            client.admin.command('ping')
            db_status_str = "✅ ONLINE"
        except Exception:
            db_status_str = "❌ OFFLINE"

        # Atlas M0 storage monitoring (512 MB free tier cap)
        try:
            db_stats = db.command("dbStats", scale=1024 * 1024)  # returns MB
            db_size_mb = round(db_stats.get("dataSize", 0), 1)
            storage_mb = round(db_stats.get("storageSize", 0), 1)
            atlas_pct = round(db_size_mb / 512 * 100, 1)
            db_size_str = f"{db_size_mb} MB data / {storage_mb} MB storage ({atlas_pct}% of 512 MB free tier)"
            if atlas_pct >= 80:
                db_size_str = "⚠️ " + db_size_str + " — NEARING LIMIT"
        except Exception:
            db_size_str = "N/A"

        # 7-day churn: users who left vault in the last 7 days
        seven_days_ago = now_tz - timedelta(days=7)
        left_7d = col_user_verification.count_documents({
            "vault_joined": False,
            "vault_left_at": {"$gte": seven_days_ago}
        })
        # 7-day new verified: joined vault in last 7 days (ever_verified set in that window)
        new_verified_7d = col_user_verification.count_documents({
            "verified": True,
            "vault_left_at": {"$exists": False},  # currently active
        })
        # Grace conversion
        grace_consumed_total = col_user_verification.count_documents({"grace_consumed": True})
        grace_converted_total = col_user_verification.count_documents({"grace_converted_to_vault": True})

        return {
            "total_users": total_users,
            "verified_users": verified_users,
            "total_msa_ids": total_msa_ids,
            "open_tickets": open_tickets,
            "resolved_tickets": resolved_tickets,
            "archived_tickets": archived_tickets,
            "banned_users": banned_users,
            "total_pdfs": total_pdfs,
            "total_ig_content": total_ig_content,
            "new_today": new_today,
            "clicks_sum": clicks_sum,
            "db_status": db_status_str,
            "db_size_str": db_size_str,
            "left_7d": left_7d,
            "grace_consumed_total": grace_consumed_total,
            "grace_converted_total": grace_converted_total,
        }

    stats = await loop.run_in_executor(None, _get_stats)

    # ── Build success-rate ──────────────────────────────────────
    total_errors = health_stats["errors_caught"]
    healed = health_stats["auto_healed"]
    success_rate = (healed / total_errors * 100) if total_errors > 0 else 100.0

    report = (
        f"📊 **BOT 1 — {period} REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🕐 **Time:** {now_tz.strftime('%I:%M %p')} {REPORT_TIMEZONE}\n"
        f"📅 **Date:** {now_tz.strftime('%B %d, %Y')}\n\n"
        f"━━ ⏱️ UPTIME ━━\n"
        f"• Running since: {health_stats['bot_start_time'].strftime('%b %d, %I:%M %p')}\n"
        f"• Total uptime: {days}d {hours}h {minutes}m\n\n"
        f"━━ 👥 USERS ━━\n"
        f"• Total registered: `{stats['total_users']}`\n"
        f"• Verified (vault): `{stats['verified_users']}`\n"
        f"• MSA+ IDs assigned: `{stats['total_msa_ids']}`\n"
        f"• New today: `{stats['new_today']}`\n"
        f"• Banned: `{stats['banned_users']}`\n"
        f"• Left vault (last 7d): `{stats['left_7d']}`\n\n"
        f"━━ 🎁 GRACE PASS FUNNEL ━━\n"
        f"• Grace consumed (all time): `{stats['grace_consumed_total']}`\n"
        f"• Converted to vault: `{stats['grace_converted_total']}` "
        f"({'%.1f' % (stats['grace_converted_total']/stats['grace_consumed_total']*100 if stats['grace_consumed_total'] else 0)}%)\n\n"
        f"━━ 📦 CONTENT ━━\n"
        f"• PDFs in DB: `{stats['total_pdfs']}`\n"
        f"• IG Content: `{stats['total_ig_content']}`\n"
        f"• Total content clicks: `{stats['clicks_sum']}`\n\n"
        f"━━ 🎫 SUPPORT TICKETS ━━\n"
        f"• Open: `{stats['open_tickets']}`\n"
        f"• Resolved: `{stats['resolved_tickets']}`\n"
        f"• Archived: `{stats['archived_tickets']}`\n\n"
        f"━━ 🏥 HEALTH ━━\n"
        f"• Database: {stats['db_status']}\n"
        f"• Atlas Storage: `{stats['db_size_str']}`\n"
        f"• Errors caught: `{total_errors}`\n"
        f"• Auto-healed: `{healed}`\n"
        f"• Heal success rate: `{success_rate:.1f}%`\n"
        f"• DB reconnects: `{health_stats['db_reconnects']}`\n"
        f"• Owner alerts sent: `{health_stats['owner_notified']}`\n"
        f"• Reports sent: `{health_stats['reports_sent']}`\n"
        f"• Last error: {health_stats['last_error'].strftime('%I:%M %p') if health_stats['last_error'] else 'None'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 MSA NODE Bot 1 — Auto-Report"
    )
    return report


async def send_daily_report(period: str):
    """Send a report to owner. period = 'MORNING' or 'EVENING'."""
    try:
        report = await _build_daily_report(period)
        await bot.send_message(OWNER_ID, report, parse_mode=ParseMode.MARKDOWN)
        health_stats["reports_sent"] += 1
        logger.info(f"📊 {period} report sent to owner")
    except TelegramRetryAfter as e:
        logger.info(f"Daily report skipped — flood control ({e.retry_after}s). Will retry next scheduled run.")
    except Exception as e:
        logger.error(f"❌ Failed to send {period} report: {e}")


async def daily_report_scheduler():
    """Background task: fire reports at 8:40 AM and 8:40 PM (owner's timezone)."""
    logger.info(f"📅 Daily report scheduler started (timezone: {REPORT_TIMEZONE})")
    report_times = [
        (REPORT_HOUR_AM, REPORT_MIN_AM, "MORNING (8:40 AM)"),
        (REPORT_HOUR_PM, REPORT_MIN_PM, "EVENING (8:40 PM)"),
    ]

    while True:
        try:
            now = datetime.now(TZ)
            # Find next report time
            next_fire: datetime | None = None
            next_label: str | None = None
            for hour, minute, label in report_times:
                candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                if next_fire is None or candidate < next_fire:
                    next_fire = candidate
                    next_label = label

            wait_secs = (next_fire - datetime.now(TZ)).total_seconds() if next_fire is not None else 0.0
            logger.info(f"📅 Next report '{next_label}' in {int(wait_secs // 3600)}h {int((wait_secs % 3600) // 60)}m")

            await asyncio.sleep(max(wait_secs, 1))
            await send_daily_report(next_label if next_label is not None else "Daily")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Daily report scheduler error: {e}")
            await asyncio.sleep(60)  # Back off 1 min on error


# ==========================================
# 💾 STATE PERSISTENCE — Remember on restart
# ==========================================

def save_bot_state(key: str, value: dict):
    """Persist a key-value state to MongoDB so it survives restarts."""
    try:
        db["bot1_state_persistence"].update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "updated_at": now_local()}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"❌ Failed to save bot state '{key}': {e}")


def load_bot_state(key: str) -> dict:
    """Load a persisted state from MongoDB. Returns {} if not found."""
    try:
        doc = db["bot1_state_persistence"].find_one({"key": key})
        if doc:
            return doc.get("value", {})
    except Exception as e:
        logger.error(f"❌ Failed to load bot state '{key}': {e}")
    return {}


def restore_health_stats_from_db():
    """Restore cumulative health_stats counters from last run."""
    saved = load_bot_state("health_stats_cumulative")
    if saved:
        # Restore cumulative counters (but reset session-specific ones)
        health_stats["errors_caught"] = saved.get("errors_caught", 0)
        health_stats["auto_healed"] = saved.get("auto_healed", 0)
        health_stats["owner_notified"] = saved.get("owner_notified", 0)
        health_stats["db_reconnects"] = saved.get("db_reconnects", 0)
        health_stats["reports_sent"] = saved.get("reports_sent", 0)
        logger.info(f"💾 Health stats restored from DB (errors: {health_stats['errors_caught']}, healed: {health_stats['auto_healed']})")


# ==========================================
# 💾 AUTO-BACKUP SYSTEM — Bot 1 (every 12 hours)
# ==========================================
_BOT1_LAST_BACKUP_KEY = "bot1_last_auto_backup"

async def auto_backup_bot1():
    """Run a full Bot 1 data backup every 12 hours into bot1_backups collection."""
    while True:
        try:
            now = now_local()

            # AM / PM label for display
            period = "AM" if now.hour < 12 else "PM"
            timestamp_label = now.strftime(f"%B %d, %Y — %I:%M {period}")  # e.g. February 18, 2026 — 08:40 AM
            timestamp_key  = now.strftime("%Y-%m-%d_%I-%M-%S_") + period   # file-safe key
            window_key     = now.strftime("%Y-%m-%d_") + period             # e.g. "2026-02-19_AM"

            # ✅ Dedup: skip if a backup for this 12 h window already exists
            if col_bot1_backups.count_documents({"window_key": window_key}) > 0:
                logger.info(f"⚠️  Bot1 auto-backup SKIPPED — window {window_key} already stored")
                await asyncio.sleep(12 * 3600)
                continue

            logger.info(f"💾 BOT 1 AUTO-BACKUP STARTING — {timestamp_label}")

            # ── User data collections only (content libraries excluded — static, huge) ──
            collections_to_backup = [
                ("bot1_user_verification",  col_user_verification),
                ("bot1_msa_ids",            col_msa_ids),
                ("bot1_support_tickets",    col_support_tickets),
                ("bot1_banned_users",       col_banned_users),
                ("bot1_suspended_features", col_suspended_features),
            ]

            collection_counts: dict[str, int] = {}
            collections_data  = {}
            total_records: int = 0
            BATCH_SIZE = 5000

            start_time = now_local()
            for col_name, collection in collections_to_backup:
                try:
                    records = []
                    cursor = collection.find({}).batch_size(BATCH_SIZE)
                    for doc in cursor:
                        if "_id" in doc:
                            doc["_id"] = str(doc["_id"])
                        records.append(doc)
                    collection_counts[col_name] = len(records)
                    collections_data[col_name]  = records
                    total_records = int(total_records) + len(records)
                except Exception as ce:
                    logger.warning(f"⚠️ Bot1 backup — could not back up {col_name}: {ce}")
                    collection_counts[col_name] = 0
                    collections_data[col_name]  = []

            processing_time = (now_local() - start_time).total_seconds()

            backup_summary = {
                "bot":              "bot1",
                "backup_date":     now,
                "backup_type":     "automatic_12h",
                "timestamp":       timestamp_key,
                "timestamp_label": timestamp_label,
                "window_key":      now.strftime("%Y-%m-%d_") + str(period),  # e.g. "2026-02-19_AM"
                "period":          period,              # "AM" or "PM"
                "year":            now.year,
                "month":           now.strftime("%B"),  # e.g. "February"
                "day":             now.day,
                "hour_12":         now.strftime("%I").lstrip("0") or "12",  # 12-h no leading zero
                "minute":          now.strftime("%M"),
                "total_records":   total_records,
                "collection_counts": collection_counts,
                "processing_time": processing_time,
            }

            upsert_res = col_bot1_backups.update_one(
                {"bot": "bot1", "window_key": window_key},
                {"$setOnInsert": backup_summary},
                upsert=True,
            )
            if upsert_res.upserted_id is None:
                logger.info(f"⚠️ Bot1 backup dedup hit — summary already exists for window {window_key}")

            # ── Save full restorable snapshot (single always-replaced doc) ──────────
            # Full data in col_bot1_restore_data; backup history in col_bot1_backups (counts only)
            try:
                col_bot1_restore_data.replace_one(
                    {"_id": "bot1_latest"},
                    {
                        "_id":               "bot1_latest",
                        "backup_date":       now,
                        "timestamp":         timestamp_key,
                        "timestamp_label":   timestamp_label,
                        "total_records":     total_records,
                        "collection_counts": collection_counts,
                        "collections":       collections_data,
                    },
                    upsert=True,
                )
                logger.info(f"✅ Bot1 restore snapshot updated — {total_records:,} records restorable")
            except Exception as snap_err:
                logger.warning(f"⚠️ Bot1 restore snapshot warning: {snap_err}")

            # Keep last 60 backup summaries (30 days × 2/day)
            backup_count = col_bot1_backups.count_documents({})
            if backup_count > 60:
                old = list(col_bot1_backups.find({}).sort("backup_date", 1).limit(backup_count - 60))
                col_bot1_backups.delete_many({"_id": {"$in": [b["_id"] for b in old]}})

            logger.info(
                f"✅ Bot 1 auto-backup done — {total_records:,} records | "
                f"{processing_time:.2f}s | Period: {period} | Kept ≤60 backup summaries"
            )

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Bot 1 auto-backup error: {e}")

        # Sleep exactly 12 hours then run again
        await asyncio.sleep(12 * 3600)


# ==========================================
# � MONTHLY JSON BACKUP DELIVERY — Bot 1
# Runs on the 1st of every month, 09:00–11:00 AM local time
# Delivers each collection as a separate gzip-compressed JSON file to the owner
# JSON format: {collection, exported_at, total_records, restore_unique_key, records:[...]}
# Re-importable with zero duplicates via the restore system
# ==========================================

_BOT1_MONTHLY_EXPORT = [
    # (collection_name,          restore_unique_key)
    ("bot1_user_verification",        "user_id"),
    ("bot1_msa_ids",                  "user_id"),
    ("bot1_support_tickets",          "user_id"),
    ("bot1_banned_users",             "user_id"),
    ("bot1_suspended_features",       "user_id"),
    ("bot1_permanently_banned_msa",   "msa_id"),
    ("bot1_offline_log",         "_id"),
    ("bot1_state_persistence",   "key"),
]


def _mongo_json_encoder(obj):
    """Serialize MongoDB-specific types (ObjectId, datetime, bytes) for json.dumps."""
    import datetime as _dt
    try:
        from bson import ObjectId
        if isinstance(obj, ObjectId):
            return str(obj)
    except ImportError:
        pass
    if isinstance(obj, (_dt.datetime, _dt.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)


async def _send_col_json(col_name: str, unique_key: str, now, dest_id: int) -> tuple[int, float]:
    """Dump one collection to gzip JSON and send to dest_id. Returns (record_count, bytes_total)."""
    import json, gzip, io
    from aiogram.types import BufferedInputFile

    period    = "AM" if now.hour < 12 else "PM"
    ts_label  = now.strftime(f"%B %d, %Y \u2014 %I:%M {period}")
    month_str = now.strftime("%B_%Y")
    date_str  = now.strftime("%Y-%m-%d_%I%M")

    records = []
    for doc in db[col_name].find({}):
        doc["_id"] = str(doc.get("_id", ""))
        records.append(doc)

    CHUNK  = 50_000  # split >50k records to stay within Telegram's 50 MB file limit
    chunks = [[records[j] for j in range(i, min(i+CHUNK, len(records)))] for i in range(0, len(records), CHUNK)] if records else [[]]
    total_bytes = 0

    for idx, chunk in enumerate(chunks, 1):
        payload = {
            "collection":         col_name,
            "exported_at":        ts_label,
            "month":              now.strftime("%B %Y"),
            "total_records":      len(records),
            "part":               idx,
            "total_parts":        len(chunks),
            "restore_unique_key": unique_key,
            "records":            chunk,
        }
        raw  = json.dumps(payload, default=_mongo_json_encoder, ensure_ascii=False, indent=2).encode()
        buf  = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(raw)
        data   = buf.getvalue()
        suffix = f"_part{idx}of{len(chunks)}" if len(chunks) > 1 else ""
        fname  = f"{col_name}_{month_str}_{date_str}_{period}{suffix}.json.gz"
        cap    = (
            f"\U0001f4e6 <b>{col_name}</b>"
            + (f" [{idx}/{len(chunks)}]" if len(chunks) > 1 else "")
            + f"\n{len(chunk):,} records \u00b7 {len(data)/1024:.1f} KB compressed"
        )
        await bot.send_document(dest_id, BufferedInputFile(data, filename=fname), caption=cap, parse_mode="HTML")
        total_bytes += len(data)
        await asyncio.sleep(0.5)

    return int(len(records)), float(total_bytes)


async def monthly_json_delivery_bot1():
    """Background task: 1st of every month, 09:00\u201311:00 AM \u2014 deliver full JSON exports to owner."""
    while True:
        try:
            now = now_local()
            if now.day == 1 and 9 <= now.hour <= 11:
                month_key = now.strftime("%Y-%m")
                last = load_bot_state("monthly_json_bot1")
                if last.get("month") != month_key:
                    save_bot_state("monthly_json_bot1", {"month": month_key})
                    period   = "AM" if now.hour < 12 else "PM"
                    ts_label = now.strftime(f"%B %d, %Y \u2014 %I:%M {period}")
                    await bot.send_message(
                        OWNER_ID,
                        f"\U0001f4e6 <b>BOT 1 \u2014 MONTHLY JSON BACKUP</b>\n\n"
                        f"\U0001f5d3 <b>{now.strftime('%B %Y')}</b>\n"
                        f"\U0001f558 {ts_label}\n\n"
                        f"Delivering <b>{len(_BOT1_MONTHLY_EXPORT)}</b> collection files.\n"
                        f"Each file is independently restorable \u2014 zero duplicates on re\u2011import.",
                        parse_mode="HTML",
                    )
                    total_records: int = 0
                    total_bytes: float = 0.0
                    errors: list[str] = []
                    for col_name, unique_key in _BOT1_MONTHLY_EXPORT:
                        try:
                            _result = await _send_col_json(col_name, unique_key, now, OWNER_ID)
                            total_records = int(total_records) + int(_result[0])
                            total_bytes = float(total_bytes) + float(_result[1])
                        except Exception as e:
                            errors.append(f"{col_name}: {e}")
                            logger.error(f"\u274c Monthly JSON bot1 \u2014 {col_name}: {e}")
                    summary = (
                        f"\u2705 <b>BOT 1 MONTHLY BACKUP COMPLETE</b>\n\n"
                        f"\U0001f5d3 {now.strftime('%B %Y')}\n"
                        f"\U0001f4ca Total records: <b>{total_records:,}</b>\n"
                        f"\U0001f4be Compressed: <b>{float(total_bytes) / 1024:.1f} KB</b>\n"
                        f"\U0001f4c1 Files: <b>{len(_BOT1_MONTHLY_EXPORT)-len(errors)}/{len(_BOT1_MONTHLY_EXPORT)}</b>"
                    )
                    if errors:
                        summary += "\n\n\u26a0\ufe0f Errors:\n" + "\n".join(f"\u2022 {e}" for e in errors)
                    await bot.send_message(OWNER_ID, summary, parse_mode="HTML")
                    _tb_kb: float = float(total_bytes) / 1024
                    logger.info(f"\u2705 Bot 1 monthly JSON backup done \u2014 {total_records:,} records, {_tb_kb:.1f} KB")
            await asyncio.sleep(1800)   # check every 30 minutes
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"\u274c monthly_json_delivery_bot1: {e}")
            await asyncio.sleep(300)


# ==========================================
# �📊 INACTIVE MEMBER MONITOR
# Tracks users who left the vault and follows a 30-day cleanup window:
#   Day 15 — First reminder (DM)
#   Day 29 — Final warning (DM)
#   Day 30+ — Delete MSA ID from col_msa_ids and clear it from user_verification
# If they rejoin at ANY point before deletion, all tracking is cleared.
# ==========================================
async def inactive_member_monitor():
    """
    30/60/90-DAY ABANDONMENT LIFECYCLE MONITOR
    
    Tracks users who left vault and sends 3 reminders:
    - Day 30: First reminder + ask to rejoin
    - Day 60: Second reminder + final warning
    - Day 90: Third reminder + auto-delete MSA ID + auto-delete user tracking + reset user_verification
    
    After day 90, if user returns, they are treated as brand-new member with new MSA ID.
    If user returns before day 90 deadline, all reminders are cleared and they resume normal access.
    """
    while True:
        try:
            await asyncio.sleep(6 * 3600)   # Run every 6 hours
            
            now = now_local()
            
            # Find all users who left vault and have vault_left_at timestamp set
            candidates = list(col_user_verification.find({
                "vault_joined": False,
                "vault_left_at": {"$exists": True, "$ne": None}
            }))
            
            for doc in candidates:
                user_id = doc.get("user_id")
                left_at = doc.get("vault_left_at")
                if not user_id or not left_at:
                    continue
                
                days_out = (now - left_at).days
                first_name = doc.get("first_name") or "Member"
                
                # ── SAFETY CHECK: Is user actually back in vault? ──────────────
                try:
                    live = await bot.get_chat_member(CHANNEL_ID, user_id)
                    if live.status in ("member", "administrator", "creator"):
                        # User rejoined! Clear abandonment tracking + restore any archived state
                        logger.info(f"[30/60/90] User {user_id} rejoined vault — clearing abandonment tracking")
                        col_user_verification.update_one(
                            {"user_id": user_id},
                            {
                                "$set": {"vault_joined": True, "verified": True},
                                "$unset": {
                                    "vault_left_at": "",
                                    "reminder1_sent": "",
                                    "reminder2_sent": "",
                                    "reminder3_sent": "",
                                    "is_archived": "",
                                    "archived_at": ""
                                }
                            }
                        )
                        # Restore retired MSA ID if it was soft-archived
                        col_msa_ids.update_one(
                            {"user_id": user_id},
                            {"$unset": {"retired": "", "archived_at": "", "archived_reason": ""}}
                        )
                        # Clear archived flag in tracking
                        db["bot2_user_tracking"].update_one(
                            {"user_id": user_id},
                            {"$unset": {"is_archived": ""}}
                        )
                        logger.info(f"[30/60/90] Restored archived user {user_id} — MSA ID un-retired, full access resumed")
                        continue  # Skip to next user
                except Exception as e:
                    logger.warning(f"[30/60/90] Failed to check vault status for {user_id}: {e}")
                    pass  # Continue with DB-based logic
                
                # ── DAY 90+: AUTO-DELETE & RESET ──────────────────────────────
                if days_out >= 90:
                    logger.info(f"[30/60/90] User {user_id} at day {days_out} — sending final notice + auto-delete")
                    
                    try:
                        # Send final notice BEFORE archiving
                        if not doc.get("reminder3_sent"):
                            await bot.send_message(
                                user_id,
                                f"📬 **A Note from MSA NODE, {first_name}**\n\n"
                                f"It has been **90 days** since you left the MSA NODE Vault.\n\n"
                                f"We’ve kept your access and records intact throughout this time — "
                                f"because we believe in giving our members every opportunity to return.\n\n"
                                f"As of today, your membership has been **moved to archive status**. "
                                f"Your MSA\u002B ID and history are preserved internally.\n\n"
                                f"🔐 **To restore full access instantly**, simply rejoin the MSA NODE Vault below. "
                                f"Everything will resume exactly where it left off.\n\n"
                                f"_The door is always open. We’d love to have you back._ ⚡",
                                reply_markup=get_verification_keyboard(user_id, doc, show_all=False),
                                parse_mode=ParseMode.MARKDOWN
                            )
                            col_user_verification.update_one(
                                {"user_id": user_id},
                                {"$set": {"reminder3_sent": True}}
                            )
                            logger.info(f"[30/60/90] Day-90 archive notice sent to user {user_id}")
                    except Exception as e:
                        logger.warning(f"[30/60/90] Could not send day-90 notice to {user_id}: {e}")

                    # Soft-archive instead of hard-delete — preserve MSA ID and history
                    try:
                        # Mark MSA ID as retired (soft-archive) — NOT deleted
                        # If user rejoins, this flag is cleared and they keep the same ID
                        col_msa_ids.update_one(
                            {"user_id": user_id},
                            {"$set": {
                                "retired": True,
                                "archived_at": now,
                                "archived_reason": "90_day_inactive"
                            }}
                        )
                        logger.info(f"[30/60/90] Soft-archived MSA ID for user {user_id} (retired=True)")

                        # Preserve tracking — keep source attribution, just unset active msa_id ref
                        db["bot2_user_tracking"].update_one(
                            {"user_id": user_id},
                            {"$set": {"is_archived": True}}
                        )
                        logger.info(f"[30/60/90] Marked tracking as archived for user {user_id}")

                        # Reset verification flags but KEEP the skeleton and msa_id reference
                        col_user_verification.update_one(
                            {"user_id": user_id},
                            {
                                "$set": {
                                    "vault_joined": False,
                                    "verified": False,
                                    "is_archived": True,
                                    "archived_at": now,
                                },
                                "$unset": {
                                    "vault_left_at": "",
                                    "reminder1_sent": "",
                                    "reminder2_sent": "",
                                    "reminder3_sent": ""
                                }
                            }
                        )
                        logger.info(
                            f"[30/60/90] SOFT-ARCHIVED user {user_id} after {days_out}d inactive. "
                            f"MSA ID preserved (retired=True). Skeleton kept. User resumes on rejoin."
                        )
                    except Exception as e:
                        logger.error(f"[30/60/90] Failed to soft-archive data for user {user_id}: {e}")

                    continue  # Move to next user
                
                # ── DAY 60: SECOND REMINDER ───────────────────────────────────
                if days_out >= 60 and not doc.get("reminder2_sent"):
                    logger.info(f"[30/60/90] User {user_id} at day {days_out} — sending 2nd reminder")
                    try:
                        await bot.send_message(
                            user_id,
                            f"⚠️ **A Reminder from MSA NODE, {first_name}**\n\n"
                            f"It’s been **60 days** since you stepped away from the MSA NODE Vault.\n\n"
                            f"Your membership, MSA\u002B ID, and all access are still preserved. "
                            f"However, inactive memberships are archived after **90 days** to keep the "
                            f"community active and the data clean for all members.\n\n"
                            f"🔓 **You have 30 days remaining.** Rejoin anytime before the deadline "
                            f"to instantly restore your full agent access.\n\n"
                            f"_We’re still holding your spot. Come back when you’re ready._ 🔐",
                            reply_markup=get_verification_keyboard(user_id, doc, show_all=False),
                            parse_mode=ParseMode.MARKDOWN
                        )
                        col_user_verification.update_one(
                            {"user_id": user_id},
                            {"$set": {"reminder2_sent": True}}
                        )
                        logger.info(f"[30/60/90] Day-60 reminder sent to user {user_id}")
                    except Exception as e:
                        logger.warning(f"[30/60/90] Could not send day-60 reminder to {user_id}: {e}")
                    continue
                
                # ── DAY 30: FIRST REMINDER ────────────────────────────────────
                if days_out >= 30 and not doc.get("reminder1_sent"):
                    logger.info(f"[30/60/90] User {user_id} at day {days_out} — sending 1st reminder")
                    try:
                        await bot.send_message(
                            user_id,
                            f"📬 **We Miss You, {first_name}!**\n\n"
                            f"It’s been **30 days** since you stepped away from the MSA NODE Vault.\n\n"
                            f"Your MSA\u002B ID, access, and full membership are all still intact. "
                            f"Rejoining takes just one tap.\n\n"
                            f"📌 **One thing to know:** Memberships that remain inactive for 90 days "
                            f"are moved to archive status to keep the community active and healthy.\n\n"
                            f"_We’re still holding your spot. Come back anytime._ 💙",
                            reply_markup=get_verification_keyboard(user_id, doc, show_all=False),
                            parse_mode=ParseMode.MARKDOWN
                        )
                        col_user_verification.update_one(
                            {"user_id": user_id},
                            {"$set": {"reminder1_sent": True}}
                        )
                        logger.info(f"[30/60/90] Day-30 reminder sent to user {user_id}")
                    except Exception as e:
                        logger.warning(f"[30/60/90] Could not send day-30 reminder to {user_id}: {e}")
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ 30/60/90 abandonment monitor error: {e}")


async def blueprint_link_checker():
    """
    Weekly background task: verify all blueprint and IG content links are alive.
    Runs every 7 days. Flags dead links in DB and sends owner a single batched alert.
    Skips Telegram file IDs (no URL to check). Safe to run — never modifies content.
    """
    # Wait 30 min after startup before first check (avoid hitting rate limits on cold start)
    await asyncio.sleep(1800)
    while True:
        try:
            broken_items = []

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "MSANode-LinkChecker/1.0"}
            ) as session:

                # ── Check PDF blueprints ──────────────────────────────────────
                pdfs = list(col_pdfs.find({}, {
                    "_id": 1, "msa_code": 1, "title": 1,
                    "pdf_url": 1, "file_id": 1
                }))
                for pdf in pdfs:
                    url = pdf.get("pdf_url") or pdf.get("url", "")
                    if not url or not url.startswith("http"):
                        continue  # Skip Telegram file IDs and empty fields
                    try:
                        async with session.head(url, allow_redirects=True) as r:
                            if r.status >= 400:
                                broken_items.append({
                                    "type": "PDF",
                                    "code": pdf.get("msa_code", "?"),
                                    "title": pdf.get("title", "Untitled"),
                                    "status": r.status
                                })
                                col_pdfs.update_one(
                                    {"_id": pdf["_id"]},
                                    {"$set": {
                                        "link_status": "broken",
                                        "link_last_checked": now_local(),
                                        "link_http_status": r.status
                                    }}
                                )
                            else:
                                col_pdfs.update_one(
                                    {"_id": pdf["_id"]},
                                    {"$set": {
                                        "link_status": "ok",
                                        "link_last_checked": now_local()
                                    }}
                                )
                    except Exception:
                        broken_items.append({
                            "type": "PDF",
                            "code": pdf.get("msa_code", "?"),
                            "title": pdf.get("title", "Untitled"),
                            "status": "TIMEOUT"
                        })
                        col_pdfs.update_one(
                            {"_id": pdf["_id"]},
                            {"$set": {"link_status": "timeout", "link_last_checked": now_local()}}
                        )
                    await asyncio.sleep(0.5)  # gentle rate-limit between requests

                # ── Check IG content ──────────────────────────────────────────
                ig_items = list(col_ig_content.find({}, {
                    "_id": 1, "cc_code": 1, "start_code": 1, "caption": 1,
                    "content_url": 1, "link": 1
                }))
                for item in ig_items:
                    url = item.get("content_url") or item.get("link", "")
                    if not url or not url.startswith("http"):
                        continue
                    try:
                        async with session.head(url, allow_redirects=True) as r:
                            if r.status >= 400:
                                broken_items.append({
                                    "type": "IG",
                                    "code": item.get("cc_code") or item.get("start_code", "?"),
                                    "title": str(item.get("caption", "IG Content"))[:40],
                                    "status": r.status
                                })
                                col_ig_content.update_one(
                                    {"_id": item["_id"]},
                                    {"$set": {"link_status": "broken", "link_last_checked": now_local()}}
                                )
                            else:
                                col_ig_content.update_one(
                                    {"_id": item["_id"]},
                                    {"$set": {"link_status": "ok", "link_last_checked": now_local()}}
                                )
                    except Exception:
                        broken_items.append({
                            "type": "IG",
                            "code": item.get("cc_code") or item.get("start_code", "?"),
                            "title": str(item.get("caption", "IG Content"))[:40],
                            "status": "TIMEOUT"
                        })
                        col_ig_content.update_one(
                            {"_id": item["_id"]},
                            {"$set": {"link_status": "timeout", "link_last_checked": now_local()}}
                        )
                    await asyncio.sleep(0.5)

            # ── Send owner alert if any broken links found ────────────────────
            if broken_items:
                alert_lines = [f"⚠️ **BLUEPRINT LINK HEALTH ALERT**\n{len(broken_items)} link(s) appear broken:\n"]
                for b in broken_items[:20]:  # cap at 20 to avoid message overflow
                    alert_lines.append(f"• [{b['type']}] `{b['code']}` — {b['title'][:35]} (HTTP {b['status']})")
                if len(broken_items) > 20:
                    alert_lines.append(f"_...and {len(broken_items) - 20} more. Check Atlas for full list._")
                alert_lines.append("\n_Review and update the affected content in your content library._")
                try:
                    await bot.send_message(OWNER_ID, "\n".join(alert_lines), parse_mode=ParseMode.MARKDOWN)
                    logger.info(f"[LinkChecker] Alert sent: {len(broken_items)} broken link(s)")
                except Exception as e:
                    logger.error(f"[LinkChecker] Could not send owner alert: {e}")
            else:
                logger.info(f"[LinkChecker] All blueprint links healthy ✅")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[LinkChecker] Weekly check error: {e}")

        # Run once per week
        await asyncio.sleep(7 * 24 * 3600)


async def broadcast_live_sync():
    """
    Background task: poll bot2_broadcasts every 10 s for changes.
    When a change is detected (broadcast added / edited / deleted via bot2),
    instantly refresh every open dashboard message in bot1 — no user action needed.
    """
    last_fp: str = ""
    while True:
        try:
            await asyncio.sleep(10)

            # Build a quick fingerprint of current broadcasts (id + index + text head)
            items = list(col_broadcasts.find(
                {}, {"broadcast_id": 1, "index": 1, "message_text": 1}
            ).sort("index", -1).limit(30))
            fp = "|".join(
                f"{b.get('broadcast_id','')}/{b.get('index','')}/{(b.get('message_text','')[:60])}"
                for b in items
            )

            if fp == last_fp:
                continue       # Nothing changed — skip expensive work
            last_fp = fp

            if not _DASHBOARD_ACTIVE_MSGS:
                continue       # No active dashboards open

            all_broadcasts = _fetch_deduplicated_broadcasts()
            total_bc       = len(all_broadcasts)

            for chat_id, sess in list(_DASHBOARD_ACTIVE_MSGS.items()):
                try:
                    uid          = sess["user_id"]
                    page         = (sess["page"] % total_bc) if total_bc else 0
                    user_name    = sess.get("user_name", "User")
                    member_since = sess.get("member_since", "Unknown")

                    msa_id         = get_user_msa_id(uid)
                    display_msa_id = msa_id.replace("+", "") if msa_id else "Not Assigned"

                    ann_text       = _build_ann_page(all_broadcasts, page)
                    dashboard_text = _build_dashboard_text(
                        user_name, display_msa_id, member_since, ann_text
                    )
                    if len(dashboard_text) > _DASH_CHAR_LIMIT:
                        excess = len(dashboard_text) - _DASH_CHAR_LIMIT + 5
                        ann_text = ann_text[:-excess].rsplit(" ", 1)[0] + "\u2026"
                        dashboard_text = _build_dashboard_text(
                            user_name, display_msa_id, member_since, ann_text
                        )

                    if total_bc == 0:
                        ann_kb = None
                    elif total_bc == 1:
                        ann_kb = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="\U0001f4e2 1/1", callback_data="ann_noop"),
                        ]])
                    else:
                        prev_pg = (page - 1) % total_bc
                        next_pg = (page + 1) % total_bc
                        ann_kb  = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="\u25c0\ufe0f",                       callback_data=f"ann_pg:{uid}:{prev_pg}"),
                            InlineKeyboardButton(text=f"\U0001f4e2 {page+1}/{total_bc}",    callback_data="ann_noop"),
                            InlineKeyboardButton(text="\u25b6\ufe0f",                       callback_data=f"ann_pg:{uid}:{next_pg}"),
                        ]])

                    await bot.edit_message_text(
                        chat_id      = chat_id,
                        message_id   = sess["message_id"],
                        text         = dashboard_text,
                        reply_markup = ann_kb,
                        parse_mode   = ParseMode.MARKDOWN,
                    )
                    _DASHBOARD_ACTIVE_MSGS[chat_id]["page"] = page   # keep page in sync

                except Exception as upd_err:
                    err_str = str(upd_err).lower()
                    if "message is not modified" in err_str:
                        pass   # already current — silently skip
                    elif any(k in err_str for k in (
                        "message to edit not found", "bot was kicked",
                        "chat not found", "user is deactivated",
                    )):
                        _DASHBOARD_ACTIVE_MSGS.pop(chat_id, None)  # stale — remove
                    # other transient errors: keep session alive

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"broadcast_live_sync error: {e}")


async def periodic_state_saver():
    """Save health_stats to DB every 5 minutes so restarts don't lose counts."""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            save_bot_state("health_stats_cumulative", {
                "errors_caught": health_stats["errors_caught"],
                "auto_healed": health_stats["auto_healed"],
                "owner_notified": health_stats["owner_notified"],
                "db_reconnects": health_stats["db_reconnects"],
                "reports_sent": health_stats["reports_sent"],
                "last_saved": now_local().isoformat(),
            })
            logger.debug("💾 Health stats auto-saved to DB")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ State saver error: {e}")


# ==========================================
# 🌐 RENDER HEALTH CHECK WEB SERVER
# Render requires a web service to respond on $PORT — this lightweight
# aiohttp server satisfies that requirement alongside the bot polling.
# ==========================================

async def _health_handler(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """Health check endpoint — Render pings this to confirm the service is alive."""
    uptime = datetime.now(TZ) - health_stats["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    return aiohttp_web.json_response({
        "status": "ok",
        "bot": "MSA NODE Agent",
        "uptime": f"{h}h {m}m",
        "errors_caught": health_stats["errors_caught"],
        "auto_healed": health_stats["auto_healed"],
    })


async def start_health_server():
    """Start the lightweight aiohttp web server for Render health checks + webhook."""
    if "PORT" not in os.environ:
        logger.info("🌐 Health server skipped (PORT not set — local dev mode)")
        return None
    app = aiohttp_web.Application()
    app.router.add_get("/health", _health_handler)
    app.router.add_get("/", _health_handler)  # Render also checks root

    if _WEBHOOK_URL:
        # Register Telegram webhook route onto the same aiohttp app
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=_WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        logger.info(f"✅ Webhook route registered: {_WEBHOOK_PATH}")

    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Web server running on port {PORT}")
    return runner


# ==========================================
# 🚀 MAIN FUNCTION — Enterprise Launch
# ==========================================

async def main():
    """Start Bot 1 with all enterprise background tasks."""
    tasks = []
    health_runner = None

    try:
        logger.info("🚀 MSA NODE AGENT Bot 1 — Enterprise startup...")

        # ── Restore persisted state ──────────────────────────────
        health_stats["bot_start_time"] = datetime.now(TZ)
        restore_health_stats_from_db()

        # ── Register global error handler ────────────────────────
        dp.errors.register(global_error_handler)
        logger.info("🏥 Global error handler + auto-healer registered")

        # ── Register permanent-ban gate (FIRST — outermost middleware) ──
        # Drops ALL messages and callback queries from permanently banned users.
        # Temp-banned users are passed through to their per-handler SUPPORT-only flow.
        _ban_gate = BanGateMiddleware()
        dp.message.middleware(_ban_gate)
        dp.callback_query.middleware(_ban_gate)
        logger.info("🚫 BanGateMiddleware registered (message + callback_query)")

        # ── Register live terminal middleware ────────────────────
        dp.message.middleware(Bot1TerminalMiddleware())
        log_to_terminal("STARTUP", 0, "Bot 1 online — live terminal active")
        logger.info("🖥️ Live terminal middleware registered (logs visible in Bot 2)")

        # ─── Fail fast if BOT_1_TOKEN is invalid/revoked ──────────
        try:
            me = await bot.get_me()
            logger.info(f"🤖 Telegram auth OK: @{me.username or me.id}")
        except TelegramUnauthorizedError:
            logger.critical(
                "❌ BOT_1_TOKEN is unauthorized. Update BOT_1_TOKEN in bot1.env and restart."
            )
            raise

        # ── Start Render health check web server ─────────────────
        health_runner = await start_health_server()

        # ── Start background tasks ───────────────────────────────
        tasks = [
            asyncio.create_task(health_monitor(),          name="health_monitor"),
            asyncio.create_task(auto_expire_tickets(),     name="ticket_archiver"),
            asyncio.create_task(daily_report_scheduler(),  name="daily_reports"),
            asyncio.create_task(periodic_state_saver(),    name="state_saver"),
            asyncio.create_task(inactive_member_monitor(),    name="inactive_member_monitor"),
            asyncio.create_task(broadcast_live_sync(),        name="broadcast_live_sync"),
            asyncio.create_task(blueprint_link_checker(),     name="blueprint_link_checker"),
        ]
        
        # ── NEW: Unified weekly backup (reads PROD → writes to BACKUP cluster) ──
        _b1_backup_uri = BACKUP_MONGO_URI or MONGO_URI
        _b1_backup_db  = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
        if not BACKUP_MONGO_URI:
            logger.warning("⚠️ BACKUP_MONGO_URI not set — bot1 weekly backup falling back to PROD cluster!")
        if weekly_backup_scheduler:
            tasks.append(asyncio.create_task(
                weekly_backup_scheduler(
                    bot_instance=bot,
                    bot_name="bot1",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=_b1_backup_uri,
                    backup_db_name=_b1_backup_db
                ),
                name="weekly_backup_bot1"
            ))
        
        # ── NEW: Month-end auto-export (last day of month → GDrive upload) ──
        if monthly_export_scheduler:
            tasks.append(asyncio.create_task(
                monthly_export_scheduler(
                    bot_instance=bot,
                    bot_name="bot1",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=_b1_backup_uri,
                    backup_db_name=_b1_backup_db
                ),
                name="monthly_export_bot1"
            ))
        
        # ⚠️ DEPRECATED: Old 12h backups (auto_backup_bot1, monthly_json_delivery_bot1) are disabled
        # They are replaced by the unified weekly+monthly-end system above
        
        logger.info(f"✅ {len(tasks)} background tasks started: {[t.get_name() for t in tasks]}")

        # ── Startup notification to owner ────────────────────────
        try:
            now_tz = datetime.now(TZ)
            saved = load_bot_state("health_stats_cumulative")
            continued_from = saved.get("last_saved", "N/A")
            await bot.send_message(
                OWNER_ID,
                f"✅ <b>BOT 1 — ONLINE &amp; READY</b>\n\n"
                f"🏥 Auto-Healer: ✅ Active\n"
                f"💊 Health Monitor: ✅ Running (hourly)\n"
                f"📊 Daily Reports: ✅ Scheduled (8:40 AM &amp; PM {REPORT_TIMEZONE})\n"
                f"🗑️ Ticket Archiver: ✅ Active\n"
                f"💾 State Persistence: ✅ Enabled\n"
                f"🗄️ Auto-Backup: ✅ Every 12h — bot1_backups\n\n"
                f"<b>Started:</b> {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n"
                f"<b>Continued from save:</b> {continued_from}\n\n"
                f"<i>All systems operational — Scaling ready</i>",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not send startup notification: {e}")

        # ── Start polling or webhook ──────────────────────────────────────
        if _WEBHOOK_URL:
            # ── WEBHOOK MODE (production) ───────────────────────────────────
            logger.info("🔄 Starting in WEBHOOK mode...")
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(_WEBHOOK_URL)
            logger.info(f"✅ Webhook set: {_WEBHOOK_URL}")
            # Webhook handler is registered in start_health_server()
            # Just keep alive — aiohttp serves incoming Telegram updates
            await asyncio.Event().wait()
        else:
            # ── POLLING MODE (local dev fallback) ──────────────────────────
            logger.info("ℹ️ No RENDER_EXTERNAL_URL — using polling (local dev mode)")
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except TelegramUnauthorizedError as e:
        logger.critical(f"💥 Fatal startup error: {e}")
        raise

    except Exception as e:
        logger.critical(f"💥 Fatal startup error: {e}\n{traceback.format_exc()}")
        try:
            await notify_owner("Bot 1 Startup FATAL", f"{e}\n{str(traceback.format_exc())[:500]}", "CRITICAL", False)
        except Exception:
            pass
        raise

    finally:
        # ── Save final state before exit ─────────────────────────
        try:
            save_bot_state("health_stats_cumulative", {
                "errors_caught": health_stats["errors_caught"],
                "auto_healed": health_stats["auto_healed"],
                "owner_notified": health_stats["owner_notified"],
                "db_reconnects": health_stats["db_reconnects"],
                "reports_sent": health_stats["reports_sent"],
                "last_saved": now_local().isoformat(),
            })
            logger.info("💾 Final state saved to DB")
        except Exception:
            pass

        # ── Cancel all background tasks ──────────────────────────
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"🛑 Task '{task.get_name()}' stopped")

        # ── Shutdown notification ────────────────────────────────
        try:
            now_tz = datetime.now(TZ)
            uptime = now_tz - health_stats["bot_start_time"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)
            await bot.send_message(
                OWNER_ID,
                f"🛑 **BOT 1 — SHUTDOWN**\n\n"
                f"**Uptime this session:** {h}h {m}m\n"
                f"**Errors Caught:** {health_stats['errors_caught']}\n"
                f"**Auto-Healed:** {health_stats['auto_healed']}\n"
                f"**Owner Alerts:** {health_stats['owner_notified']}\n"
                f"**Reports Sent:** {health_stats['reports_sent']}\n\n"
                f"**Shutdown at:** {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n\n"
                f"_State persisted. Will resume counts on restart._",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

        try:
            await bot.session.close()
        except Exception:
            pass

        # ── Stop health check web server ─────────────────────────
        if health_runner and hasattr(health_runner, "cleanup") and callable(getattr(health_runner, "cleanup", None)):
            try:
                _cleanup = getattr(health_runner, "cleanup")
                await _cleanup()
                logger.info("🌐 Health check server stopped")
            except Exception:
                pass

        logger.info("✅ Bot 1 shutdown complete")


# ==========================================
# 🏁 ENTRY POINT — With auto-restart wrapper
# ==========================================

if __name__ == "__main__":
    _restart_delay = 5  # seconds between restarts
    _script_path = os.path.abspath(__file__)
    while True:
        try:
            asyncio.run(main())
            # main() only returns on clean shutdown → don't restart
            logger.info("✅ Clean shutdown. Exiting.")
            break
        except KeyboardInterrupt:
            logger.info("⚠️ Bot stopped by user (Ctrl+C)")
            break
        except SystemExit:
            logger.info("⚠️ SystemExit received. Stopping.")
            break
        except TelegramUnauthorizedError:
            logger.critical(
                "🛑 Restart disabled: Telegram Unauthorized. Fix BOT_1_TOKEN in bot1.env, then start again."
            )
            break
        except Exception as e:
            logger.critical(f"💥 Unhandled top-level crash: {e}\n{traceback.format_exc()}")
            logger.info(f"♻️ Auto-restarting in {_restart_delay} seconds...")
            time.sleep(_restart_delay)
            _restart_delay = min(_restart_delay * 2, 60)
            # Replace the entire process to get a clean event loop
            os.execv(sys.executable, [sys.executable, _script_path])

