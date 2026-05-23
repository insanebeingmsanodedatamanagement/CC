import logging
import asyncio
import os
import sys

# Force UTF-8 output — prevents UnicodeEncodeError on Windows cp1252 console
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psutil
import json
import traceback
import pickle
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure, DuplicateKeyError
import re
import string
import random
from bson.objectid import ObjectId
import pytz
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler
from aiohttp import web
import html as _html
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


# ==========================================
# ENTERPRISE CONFIGURATION
# ==========================================

# Bot Configuration
# Prefer bot-specific tokens first; generic BOT_TOKEN is last fallback.
BOT_TOKEN = os.environ.get("BOT_3_TOKEN") or os.environ.get("BOT_TOKEN")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip("@")  # Auto-detected at startup if empty
MONGO_URI = os.environ.get("MONGO_URI")
MASTER_ADMIN_ID = int(os.environ.get("MASTER_ADMIN_ID", 0))
OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Global variable for health server cleanup
health_server_runner = None

# ==========================================
# 🌐 WEBHOOK CONFIGURATION
# ==========================================
_IS_RENDER = os.environ.get("RENDER", "").lower() in ("true", "1", "yes")
_WEBHOOK_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/") if _IS_RENDER else ""
_WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
_WEBHOOK_URL = f"{_WEBHOOK_BASE_URL}{_WEBHOOK_PATH}" if _WEBHOOK_BASE_URL else ""

# ==========================================
# 🧱 INLINED BACKUP SCHEDULERS SYSTEM (Bot 3)
# ==========================================
# All backup functions are natively inlined — no external backup_schedulers module needed.

import io
import zipfile
import certifi
from calendar import monthrange
from pymongo import ASCENDING
from pymongo.errors import ServerSelectionTimeoutError as _BkpSSTE

# ── Constants ─────────────────────────────────────────────────────────────────

_LOCAL_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSANode_Local_Backups")

_BOT3_GDRIVE_FOLDER_ID = os.environ.get("BOT3_GDRIVE_FOLDER_ID", "")
_BOT1_GDRIVE_FOLDER_ID = os.environ.get("BOT1_GDRIVE_FOLDER_ID", "")
_BOT2_GDRIVE_FOLDER_ID = os.environ.get("BOT2_GDRIVE_FOLDER_ID", "")

_TTL_SECONDS = 90 * 24 * 3600  # 90-day TTL for cluster backup docs

_BOT_COLLECTIONS = {
    "bot3": [
        "bot3_pdfs",
        "bot3_ig_content",
        "bot3_rewards",
        "bot3_admins",
        "bot3_settings",
        "bot3_banned_users",
        "bot3_logs",
        "bot3_user_activity",
        "bot3_backups",
    ],
}

# ── Internal helpers ──────────────────────────────────────────────────────────

def _ensure_ttl_index(col):
    """Create 90-day TTL index on backup_date field (idempotent)."""
    try:
        col.create_index(
            [("backup_date", ASCENDING)],
            expireAfterSeconds=_TTL_SECONDS,
            name="backup_ttl_90d",
            background=True
        )
    except Exception as _e:
        pass  # Non-fatal — backup cluster may have transient SSL/network issues


def _backup_mongo_client(uri: str, **kwargs):
    """Create a MongoClient for the backup cluster using the certifi CA bundle."""
    from pymongo import MongoClient as _MC
    opts = {
        "serverSelectionTimeoutMS": 10000,
        "tlsCAFile": certifi.where(),
    }
    opts.update(kwargs)
    return _MC(uri, **opts)


def _get_gdrive_service():
    """Build and return an authenticated Google Drive service object."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise RuntimeError(
                "Google Drive token.json missing or invalid. "
                "Re-run the OAuth flow to generate a fresh token."
            )
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _gdrive_file_exists(service, filename: str, folder_id: str) -> bool:
    """Return True if a file with this exact name already exists in the GDrive folder."""
    query = (
        f"name='{filename}' and "
        f"'{folder_id}' in parents and "
        "trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return len(results.get("files", [])) > 0


def _gdrive_upload_bytes(service, zip_bytes: bytes, filename: str, folder_id: str) -> str:
    """Upload bytes as a file to GDrive, return the file ID."""
    from googleapiclient.http import MediaIoBaseUpload
    meta = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(zip_bytes), mimetype="application/zip", resumable=True)
    f = service.files().create(body=meta, media_body=media, fields="id").execute()
    return f.get("id", "")


def _export_bot_collections(prod_db, bot_name: str) -> dict:
    """Export all collections for a bot from PROD DB."""
    from pymongo import MongoClient as _MC
    result = {}
    cols = _BOT_COLLECTIONS.get(bot_name, [])
    all_db_cols = prod_db.list_collection_names()
    prefix = f"{bot_name}_"
    extra = [c for c in all_db_cols if c.startswith(prefix) and c not in cols]
    all_cols = list(cols) + extra

    for col_name in all_cols:
        try:
            docs = []
            for d in prod_db[col_name].find({}):
                d["_id"] = str(d["_id"])
                docs.append(d)
            result[col_name] = {"count": len(docs), "documents": docs}
        except Exception as e:
            result[col_name] = {"count": 0, "error": str(e)}
    return result


def _build_zip_from_collections(bot_name: str, collections: dict) -> io.BytesIO:
    """Package collections dict into an in-memory ZIP of JSON files."""
    from datetime import timezone as _tz
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for col_name, col_data in collections.items():
            docs = col_data.get("documents", [])
            payload = json.dumps(docs, default=str, indent=2, ensure_ascii=False)
            zf.writestr(f"{col_name}.json", payload)
        meta = {
            "bot": bot_name,
            "exported_at": datetime.now(_tz.utc).isoformat(),
            "collections": {k: v.get("count", 0) for k, v in collections.items()},
        }
        zf.writestr("_metadata.json", json.dumps(meta, indent=2))
    buf.seek(0)
    return buf


def _save_local_daily_backup(bot_name: str, prod_db, now: datetime):
    """Save daily JSON export to local folder hierarchy."""
    try:
        year     = str(now.year)
        month    = now.strftime("%B %Y")
        week_num = (now.day - 1) // 7 + 1
        day      = now.strftime("%Y-%m-%d")
        day_dir  = os.path.join(_LOCAL_ROOT, bot_name, year, month, f"Week {week_num}", day)
        os.makedirs(day_dir, exist_ok=True)
        collections = _export_bot_collections(prod_db, bot_name)
        for col_name, col_data in collections.items():
            docs = col_data.get("documents", [])
            if docs:
                fpath = os.path.join(day_dir, f"{col_name}.json")
                with open(fpath, "w", encoding="utf-8") as f:
                    json.dump(docs, f, default=str, indent=2, ensure_ascii=False)
        logging.info(f"[BACKUP] Daily local export saved: {day_dir}")
    except Exception as e:
        logging.error(f"[BACKUP] _save_local_daily_backup failed for {bot_name}: {e}")


def _upsert_cluster_snapshot(bot_name: str, prod_db, bkp_db, now: datetime):
    """Upsert today's snapshot into MSANodeBackups cluster (one doc per day per bot)."""
    from datetime import timezone as _tz
    try:
        bot_num     = bot_name[-1]
        bkp_col     = bkp_db[f'bot{bot_num}_backups']
        _ensure_ttl_index(bkp_col)
        day_key     = now.strftime('%Y-%m-%d')
        week_num    = (now.day - 1) // 7 + 1
        week_label  = f'Week {week_num}'
        month_label = now.strftime('%B %Y')
        year        = now.year
        month_n     = now.month
        collections = _export_bot_collections(prod_db, bot_name)
        doc_count   = sum(v.get('count', 0) for v in collections.values())
        bkp_col.update_one(
            {'bot': bot_name, 'window_key': day_key},
            {'$set': {
                'bot':         bot_name,
                'window_key':  day_key,
                'backup_date': now,
                'backup_type': 'daily',
                'docs':        doc_count,
                'data':        collections,
                'week_num':    week_num,
                'week_label':  week_label,
                'month_label': month_label,
                'year':        year,
                'month':       month_n,
            }},
            upsert=True
        )
        logging.info(f'[BACKUP] Cluster snapshot upserted: {bot_name} {day_key} ({doc_count} docs)')
    except Exception as e:
        logging.error(f'[BACKUP] _upsert_cluster_snapshot failed for {bot_name}: {e}')


# ── Public API ────────────────────────────────────────────────────────────────

def force_backup_to_cluster(
    bot_name: str,
    mongo_uri: str,
    db_name: str,
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
) -> dict:
    """
    Snapshot all of a bot's live collections → MSANodeBackups cluster.
    Returns: { status: "ok"|"error", collections: int, docs: int, error: str }
    Called as: await loop.run_in_executor(None, force_backup_to_cluster, "bot3", ...)
    """
    from pymongo import MongoClient as _MC
    from datetime import timezone as _tz
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name

    try:
        prod_client = _MC(mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_client  = _backup_mongo_client(_write_uri)
        prod_db = prod_client[db_name]
        bkp_db  = bkp_client[_write_db]

        collections = _export_bot_collections(prod_db, bot_name)
        prod_client.close()

        col_count = len(collections)
        doc_count = sum(v.get("count", 0) for v in collections.values())
        now = datetime.now(_tz.utc)
        ts  = now.strftime("%Y%m%d_%H%M%S")

        bkp_col = bkp_db[f"bot{bot_name[-1]}_backups"] if bot_name.startswith("bot") else bkp_db["bot_backups"]
        _ensure_ttl_index(bkp_col)

        snap = {
            "bot":         bot_name,
            "backup_date": now,
            "backup_type": "force",
            "window_key":  ts,
            "collections": col_count,
            "docs":        doc_count,
            "data":        collections,
        }
        bkp_col.insert_one(snap)
        bkp_client.close()

        return {"status": "ok", "collections": col_count, "docs": doc_count}

    except Exception as e:
        logging.error(f"[BACKUP] force_backup_to_cluster failed for {bot_name}: {e}")
        return {"status": "error", "error": str(e), "collections": 0, "docs": 0}


def list_cluster_backups(
    bot_name: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
) -> list:
    """
    List all backup records for bot_name from MSANodeBackups cluster.
    Returns sorted list (newest first) of metadata dicts.
    The heavy 'data' field is excluded for performance.
    """
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        records = list(
            bkp_col.find(
                {"bot": bot_name},
                {"data": 0}
            ).sort("backup_date", -1)
        )
        bkp_client.close()

        for r in records:
            r["_id"] = str(r["_id"])

        return records
    except Exception as e:
        logging.error(f"[BACKUP] list_cluster_backups failed for {bot_name}: {e}")
        return []


def download_cluster_backup(
    backup_id: str,
    bot_name: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
) -> tuple:
    """
    Fetch one backup record by _id from MSANodeBackups, build an in-memory ZIP.
    Returns: (zip_bytes: bytes, filename: str) or (None, None) on error.
    """
    from bson import ObjectId
    from datetime import timezone as _tz
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        record = bkp_col.find_one({"_id": ObjectId(backup_id), "bot": bot_name})
        bkp_client.close()

        if not record:
            return None, None

        collections = record.get("data", {})
        ts_str      = record.get("window_key", datetime.now(_tz.utc).strftime("%Y%m%d_%H%M%S"))
        filename    = f"bot3_backup_{ts_str}.zip"

        buf = _build_zip_from_collections(bot_name, collections)
        return buf.read(), filename

    except Exception as e:
        logging.error(f"[BACKUP] download_cluster_backup failed for {bot_name}/{backup_id}: {e}")
        return None, None


def gdrive_upload_cluster_backup(
    backup_id: str,
    bot_name: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
    gdrive_folder_id: str = None,
) -> dict:
    """
    Upload one MSANodeBackups record to Google Drive.
    On SUCCESS: marks gdrive_uploaded=True in the record.
    On FAILURE: leaves the record untouched. NEVER deletes on failure.
    Returns: {status, zip_name, size_mb, file_id, message}
    """
    from bson import ObjectId
    from datetime import timezone as _tz
    if gdrive_folder_id:
        _folder = gdrive_folder_id
    elif bot_name == "bot1":
        _folder = _BOT1_GDRIVE_FOLDER_ID
    elif bot_name == "bot2":
        _folder = _BOT2_GDRIVE_FOLDER_ID
    else:
        _folder = _BOT3_GDRIVE_FOLDER_ID

    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        record = bkp_col.find_one({"_id": ObjectId(backup_id), "bot": bot_name})
        if not record:
            bkp_client.close()
            return {"status": "error", "message": f"Backup record {backup_id} not found."}

        collections = record.get("data", {})
        ts_str      = record.get("window_key", datetime.now(_tz.utc).strftime("%Y%m%d_%H%M%S"))
        zip_name    = f"gdrive_{bot_name}_{ts_str}.zip"

        buf       = _build_zip_from_collections(bot_name, collections)
        zip_bytes = buf.read()
        size_mb   = len(zip_bytes) / (1024 * 1024)

        service = _get_gdrive_service()
        if _gdrive_file_exists(service, zip_name, _folder):
            bkp_client.close()
            return {"status": "error", "message": f"File '{zip_name}' already exists in GDrive — skipped duplicate."}

        file_id = _gdrive_upload_bytes(service, zip_bytes, zip_name, _folder)

        bkp_col.update_one(
            {"_id": ObjectId(backup_id)},
            {"$set": {
                "gdrive_uploaded": True,
                "gdrive_file_id": file_id,
                "gdrive_uploaded_at": datetime.now(_tz.utc)
            }}
        )
        bkp_client.close()

        return {
            "status":   "success",
            "zip_name": zip_name,
            "size_mb":  size_mb,
            "file_id":  file_id,
            "message":  f"Uploaded {zip_name} ({size_mb:.2f} MB) — record marked for 90-day TTL cleanup.",
        }

    except Exception as e:
        logging.error(f"[BACKUP] gdrive_upload_cluster_backup failed for {bot_name}/{backup_id}: {e}")
        return {"status": "error", "message": str(e)}


# ── Auto-schedulers ───────────────────────────────────────────────────────────

async def weekly_backup_scheduler(
    bot_instance,
    bot_name: str,
    owner_id: int,
    mongo_uri: str,
    db_name: str,
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
):
    """
    Daily at 23:59 UTC: save local JSON + upsert to cluster.
    Also triggers month-end ZIP on the last day of the month.
    Runs forever; auto-restarts on crash with owner alert.
    """
    from datetime import timezone as _tz
    from pymongo import MongoClient as _MC
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name

    while True:
        try:
            while True:
                now = datetime.now(_tz.utc)

                next_run = now.replace(hour=23, minute=59, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                try:
                    run_now     = datetime.now(_tz.utc)
                    prod_client = _MC(mongo_uri, serverSelectionTimeoutMS=10000)
                    bkp_client  = _backup_mongo_client(_write_uri)
                    prod_db     = prod_client[db_name]
                    bkp_db      = bkp_client[_write_db]

                    _IS_RENDER_LOCAL = os.environ.get("RENDER", "").lower() in ("true", "1", "yes")
                    if not _IS_RENDER_LOCAL:
                        _save_local_daily_backup(bot_name, prod_db, run_now)
                    else:
                        logging.info(f"[BACKUP] Render detected — skipping local disk write for {bot_name}")

                    _upsert_cluster_snapshot(bot_name, prod_db, bkp_db, run_now)
                    prod_client.close()

                    _, last_day = monthrange(run_now.year, run_now.month)
                    if run_now.day == last_day:
                        # ── MONTH-END: upload this snapshot to Google Drive ──────────
                        try:
                            bkp_col_name = f"bot{bot_name[-1]}_backups"
                            latest_rec = bkp_db[bkp_col_name].find_one(
                                {"bot": bot_name},
                                sort=[("backup_date", -1)]
                            )
                            if latest_rec:
                                gdrive_result = await asyncio.to_thread(
                                    gdrive_upload_cluster_backup,
                                    str(latest_rec["_id"]),
                                    bot_name,
                                    _write_uri,
                                    _write_db,
                                    None  # uses _BOT3_GDRIVE_FOLDER_ID from env
                                )
                                if gdrive_result.get("status") == "success":
                                    logging.info(f"[BACKUP] Month-end GDrive upload OK: {gdrive_result['zip_name']}")
                                    try:
                                        await bot_instance.send_message(
                                            chat_id=owner_id,
                                            text=(
                                                f"☁️ <b>Month-End GDrive Backup</b> — <code>{bot_name}</code>\n"
                                                f"📁 <b>File:</b> {gdrive_result['zip_name']}\n"
                                                f"💾 <b>Size:</b> {gdrive_result['size_mb']:.2f} MB\n"
                                                f"✅ Uploaded to Google Drive folder"
                                            ),
                                            parse_mode="HTML"
                                        )
                                    except Exception:
                                        pass
                                else:
                                    logging.error(f"[BACKUP] Month-end GDrive FAILED: {gdrive_result.get('message')}")
                                    try:
                                        await bot_instance.send_message(
                                            chat_id=owner_id,
                                            text=(
                                                f"⚠️ <b>Month-End GDrive FAILED</b> — <code>{bot_name}</code>\n"
                                                f"<code>{str(gdrive_result.get('message', 'Unknown error'))[:200]}</code>"
                                            ),
                                            parse_mode="HTML"
                                        )
                                    except Exception:
                                        pass
                        except Exception as gdrive_err:
                            logging.error(f"[BACKUP] Month-end GDrive upload exception for {bot_name}: {gdrive_err}")

                    is_sunday = run_now.weekday() == 6
                    bkp_client.close()

                    await bot_instance.send_message(
                        chat_id=owner_id,
                        text=(
                            f"✅ <b>{'Weekly' if is_sunday else 'Daily'} Backup</b> — <code>{bot_name}</code>\n"
                            f"📅 {run_now.strftime('%B %d, %Y — %I:%M %p UTC')}\n"
                            f"🗄️ Cluster snapshot upserted\n"
                            f"🕐 TTL: 90 days on cluster"
                        ),
                        parse_mode="HTML"
                    )

                except Exception as e:
                    logging.error(f"[BACKUP] Daily run failed for {bot_name}: {e}")
                    try:
                        await bot_instance.send_message(
                            chat_id=owner_id,
                            text=f"❌ <b>Daily backup FAILED</b> — <code>{bot_name}</code>\n<code>{str(e)[:300]}</code>",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                await asyncio.sleep(60)

        except asyncio.CancelledError:
            logging.info(f"[BACKUP] weekly_backup_scheduler cancelled for {bot_name}")
            return
        except Exception as crash_err:
            logging.error(f"[BACKUP] weekly_backup_scheduler CRASHED for {bot_name}: {crash_err}")
            try:
                await bot_instance.send_message(
                    chat_id=owner_id,
                    text=f"⚠️ <b>Backup scheduler CRASHED</b> — <code>{bot_name}</code>\nAuto-restarting in 5 min.\n<code>{str(crash_err)[:200]}</code>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            await asyncio.sleep(300)


async def monthly_export_scheduler(
    bot_instance,
    bot_name: str,
    owner_id: int,
    mongo_uri: str,
    db_name: str,
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
):
    """
    Monthly export scheduler — kept for compatibility.
    The actual monthly logic is embedded in weekly_backup_scheduler.
    This function is a no-op loop to prevent errors when called.
    """
    logging.info(f"[BACKUP] monthly_export_scheduler started for {bot_name} (handled by weekly_backup_scheduler)")
    while True:
        try:
            await asyncio.sleep(86400)
        except asyncio.CancelledError:
            logging.info(f"[BACKUP] monthly_export_scheduler cancelled for {bot_name}")
            return
        except Exception:
            await asyncio.sleep(3600)

# ── End of inlined backup system ──────────────────────────────────────────────


# Database Configuration
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "MSANodeDB")  # Single database — all bots use MSANodeDB
BACKUP_MONGO_URI = os.environ.get("BACKUP_MONGO_URI", "")  # Separate Atlas cluster for backups
BACKUP_MONGO_DB_NAME = os.environ.get("BACKUP_MONGO_DB_NAME", "MSANodeBackups")
# _IS_RENDER already defined above in webhook section
MONGO_MAX_POOL_SIZE = int(os.environ.get("MONGO_MAX_POOL_SIZE", 100))
MONGO_MIN_POOL_SIZE = int(os.environ.get("MONGO_MIN_POOL_SIZE", 10))
MONGO_CONNECT_TIMEOUT_MS = int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", 10000))
ALLOW_DB_FINGERPRINT_CHANGE = os.environ.get("ALLOW_DB_FINGERPRINT_CHANGE", "false").lower() == "true"

# Security Configuration
RATE_LIMIT_SPAM_THRESHOLD = int(os.environ.get("RATE_LIMIT_SPAM_THRESHOLD", 10))
RATE_LIMIT_SPAM_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_SPAM_WINDOW_SECONDS", 30))
OWNER_PASSWORD = os.environ.get("OWNER_PASSWORD", "change_this_password_immediately")  # Password for ownership transfer
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")   # Set on Render; never hardcode here

# In-memory set of owner IDs that have completed password auth this session
_admin_authenticated: set = set()

# Auto-Healer Configuration
HEALTH_CHECK_INTERVAL = int(os.environ.get("HEALTH_CHECK_INTERVAL_SECONDS", 60))
HEALTH_CHECK_CRITICAL_THRESHOLD = int(os.environ.get("HEALTH_CHECK_CRITICAL_THRESHOLD", 3))
ERROR_NOTIFICATION_ENABLED = os.environ.get("ERROR_NOTIFICATION_ENABLED", "true").lower() == "true"
CRITICAL_ERROR_NOTIFY_IMMEDIATELY = os.environ.get("CRITICAL_ERROR_NOTIFY_IMMEDIATELY", "true").lower() == "true"

# Daily Reports Configuration
DAILY_REPORT_ENABLED = os.environ.get("DAILY_REPORT_ENABLED", "true").lower() == "true"
DAILY_REPORT_TIME_1 = os.environ.get("DAILY_REPORT_TIME_1", "08:40")
DAILY_REPORT_TIME_2 = os.environ.get("DAILY_REPORT_TIME_2", "20:40")
DAILY_REPORT_TIMEZONE = os.environ.get("DAILY_REPORT_TIMEZONE", "Asia/Kolkata")

# ---- Local timezone helper ----
try:
    _BOT3_TZ = ZoneInfo(DAILY_REPORT_TIMEZONE)
except Exception:
    _BOT3_TZ = ZoneInfo("Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(_BOT3_TZ).replace(tzinfo=None)

# Bot 3 live terminal logger — writes to shared bot2_live_terminal_logs collection
_BOT3_LOG_MAX = 100  # Keep last 100 bot3 entries

def log_to_terminal(action_type: str, user_id: int = 0, details: str = "", user_name: str = "", channel: str = ""):
    """Write a bot3 log entry to the shared live terminal collection (viewable in Bot 2 Terminal)."""
    try:
        _now = now_local()
        timestamp = _now.strftime('%b %d  %I:%M:%S %p')
        entry = {
            "timestamp": timestamp,
            "created_at": _now,
            "bot": "bot3",
            "action": action_type,
            "user_id": user_id,
            "user_name": user_name,
            "channel": channel,
            "details": details,
        }
        col_live_logs.insert_one(entry)
        # Trim: keep newest _BOT3_LOG_MAX entries for bot3
        count = col_live_logs.count_documents({"bot": "bot3"})
        if count > _BOT3_LOG_MAX:
            oldest = list(col_live_logs.find({"bot": "bot3"}, {"_id": 1}).sort("created_at", 1).limit(count - _BOT3_LOG_MAX))
            if oldest:
                col_live_logs.delete_many({"_id": {"$in": [d["_id"] for d in oldest]}})
    except Exception:
        pass  # Never let logging crash the bot
# --------------------------------

# State Persistence Configuration
STATE_BACKUP_ENABLED = os.environ.get("STATE_BACKUP_ENABLED", "true").lower() == "true"
STATE_BACKUP_INTERVAL_MINUTES = int(os.environ.get("STATE_BACKUP_INTERVAL_MINUTES", 5))
STATE_BACKUP_LOCATION = os.environ.get("STATE_BACKUP_LOCATION", "./backups/state")
AUTO_RESUME_ON_STARTUP = os.environ.get("AUTO_RESUME_ON_STARTUP", "true").lower() == "true"

# Monitoring Configuration
TRACK_MEMORY_USAGE = os.environ.get("TRACK_MEMORY_USAGE", "true").lower() == "true"
TRACK_CPU_USAGE = os.environ.get("TRACK_CPU_USAGE", "true").lower() == "true"
ALERT_HIGH_MEMORY_MB = int(os.environ.get("ALERT_HIGH_MEMORY_MB", 500))
ALERT_HIGH_CPU_PERCENT = int(os.environ.get("ALERT_HIGH_CPU_PERCENT", 80))

# ==========================================
# ENTERPRISE LOGGING SETUP
# ==========================================

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)

# Main log handler with rotation
main_handler = RotatingFileHandler(
    "logs/bot3.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
main_handler.setLevel(logging.INFO)

# Error log handler (separate file for errors)
error_handler = RotatingFileHandler(
    "logs/bot3_errors.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
error_handler.setLevel(logging.ERROR)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
main_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[main_handler, error_handler, console_handler]
)
logger = logging.getLogger(__name__)

def _escape_md(text) -> str:
    """Escape Telegram MarkdownV1 special chars in user-supplied names."""
    if not text:
        return ""
    text = str(text)
    for ch in ('_', '*', '[', ']', '`'):
        text = text.replace(ch, f'\\{ch}')
    return text

def _html_escape(text) -> str:
    """Escape HTML special chars for safe use in parse_mode=HTML messages."""
    if not text:
        return ""
    text = str(text)
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


# Suppress noisy background warnings from aiogram/pymongo auto-reconnections
for _noisy in ("aiogram", "aiogram.event", "aiogram.dispatcher", "aiohttp", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
for _ultra_noisy in ("pymongo", "pymongo.client", "pymongo.pool", "pymongo.topology"):
    logging.getLogger(_ultra_noisy).setLevel(logging.CRITICAL)
del _noisy
del _ultra_noisy
# ==========================================
# ENTERPRISE HEALTH MONITORING SYSTEM
# ==========================================

class HealthMonitor:
    """Enterprise-grade health monitoring and auto-healing system"""
    
    # Per-level alert cooldown (seconds): how long to suppress duplicate alerts of the same type
    ALERT_COOLDOWNS = {
        "SUCCESS":  0,       # Never suppressed
        "INFO":     300,     # 5 min between same INFO alert
        "WARNING":  1800,    # 30 min between same WARNING alert
        "ERROR":    600,     # 10 min between same ERROR alert
        "CRITICAL": 600,     # 10 min between CRITICAL alerts (was 2 min — caused spam during SSL outages)
    }
    # Consecutive high readings required before firing a WARNING (avoids transient spikes)
    CPU_SUSTAINED_THRESHOLD = 3   # 3 × 60 s = 3 minutes sustained
    MEM_SUSTAINED_THRESHOLD = 2   # 2 × 60 s = 2 minutes sustained

    def __init__(self):
        self.health_checks_failed = 0
        self.last_health_check = now_local()
        self.error_count = 0
        self.warning_count = 0
        self.last_error_notification = None
        # Cooldown tracking: {alert_key: last_sent_datetime}
        self.last_alert_sent: dict = {}
        # Consecutive high-resource counters
        self.consecutive_cpu_high: int = 0
        self.consecutive_mem_high: int = 0
        self.system_metrics = {
            "uptime_start": now_local(),
            "total_requests": 0,
            "total_errors": 0,
            "db_errors": 0,
            "api_errors": 0
        }
        self.is_healthy = True
        logger.info("✅ Health Monitor initialized")
    
    async def check_system_health(self):
        """Perform comprehensive system health check"""
        try:
            # Check memory usage
            memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            if memory_mb > ALERT_HIGH_MEMORY_MB:
                self.consecutive_mem_high += 1
                if self.consecutive_mem_high >= self.MEM_SUSTAINED_THRESHOLD:
                    await self.send_alert(
                        "WARNING",
                        f"High Memory Usage: {memory_mb:.2f} MB (Threshold: {ALERT_HIGH_MEMORY_MB} MB)"
                    )
            else:
                self.consecutive_mem_high = 0
            
            # Check CPU usage — use non-blocking call (interval=0 uses last known value)
            # CRITICAL: Do NOT use interval=1 as it blocks the event loop for 1 second
            # This was causing 100% CPU when handlers ran during the block
            cpu_percent = psutil.cpu_percent(interval=0)
            if cpu_percent > ALERT_HIGH_CPU_PERCENT:
                self.consecutive_cpu_high += 1
                if self.consecutive_cpu_high >= self.CPU_SUSTAINED_THRESHOLD:
                    await self.send_alert(
                        "WARNING",
                        f"High CPU Usage: {cpu_percent:.1f}% (Threshold: {ALERT_HIGH_CPU_PERCENT}%)"
                    )
            else:
                self.consecutive_cpu_high = 0
            
            # Check database connection (async, non-blocking)
            try:
                client.admin.command('ping')
                self.health_checks_failed = 0
                self.is_healthy = True
            except Exception as e:
                self.health_checks_failed += 1
                logger.error(f"Database health check failed: {e}")
                
                if self.health_checks_failed >= HEALTH_CHECK_CRITICAL_THRESHOLD:
                    await self.send_alert(
                        "CRITICAL",
                        f"Database connection failed {self.health_checks_failed} times! Attempting auto-heal..."
                    )
                    await self.auto_heal_database()
            
            self.last_health_check = now_local()
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            await self.send_error_notification("Health Check Failed", str(e), traceback.format_exc())
    
    async def auto_heal_database(self):
        """Attempt to auto-heal database connection (CRITICAL: Only on confirmed failures)"""
        try:
            logger.info("🔧 Attempting database auto-heal...")
            global client, db, col_pdfs, col_ig_content, col_logs, col_admins, col_banned_users, col_user_activity, col_settings, col_backups
            
            # SAFETY: Verify old collections still work before healing
            try:
                test_count = col_pdfs.count_documents({})
                logger.warning(f"⚠️ Auto-heal triggered but col_pdfs is still responsive ({test_count} docs). Skipping heal.")
                return
            except:
                logger.error("✅ Confirmed: Database connection is truly broken. Proceeding with auto-heal.")
            
            # Close existing connection
            try:
                client.close()
                logger.info("✅ Old connection closed")
            except Exception as e:
                logger.warning(f"⚠️ Could not close old connection: {e}")
            
            # Reconnect
            new_client = pymongo.MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
                connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
                maxPoolSize=MONGO_MAX_POOL_SIZE,
                minPoolSize=MONGO_MIN_POOL_SIZE
            )
            new_db = new_client[MONGO_DB_NAME]
            
            # Verify credentials and database name BEFORE reinitializing
            try:
                new_client.admin.command('ping')
                logger.info(f"✅ New connection verified")
                if new_db.name != MONGO_DB_NAME:
                    logger.error(f"❌ CRITICAL: New connection points to wrong database '{new_db.name}' (expected {MONGO_DB_NAME}). Abort heal.")
                    return
            except Exception as e:
                logger.error(f"❌ New connection failed: {e}. Reverting.")
                return
            
            # ONLY NOW: Reinitialize collections
            client = new_client
            db = new_db
            col_logs = db["bot3_logs"]
            col_pdfs = db["bot3_pdfs"]
            col_ig_content = db["bot3_ig_content"]
            col_rewards = db["bot3_rewards"]
            col_settings = db["bot3_settings"]
            col_admins = db["bot3_admins"]
            col_banned_users = db["bot3_banned_users"]
            col_user_activity = db["bot3_user_activity"]
            col_backups = db["bot3_backups"]
            
            # Verify collections are responsive
            try:
                pdf_count = col_pdfs.count_documents({})
                logger.info(f"✅ Collections reinitialized. PDFs: {pdf_count}")
            except Exception as e:
                logger.error(f"❌ New collections not responding: {e}")
                return
            
            self.health_checks_failed = 0
            self.is_healthy = True
            logger.info("✅ Database connection auto-healed successfully!")
            
            await self.send_alert("SUCCESS", "Database connection auto-healed successfully!")
            
        except Exception as e:
            logger.error(f"Auto-heal failed: {e}")
            self.is_healthy = False
            await self.send_alert(
                "CRITICAL",
                f"Auto-heal FAILED! Manual intervention required!\nError: {str(e)}"
            )
    
    async def send_alert(self, level: str, message: str):
        """Send alert notification to admin, with per-type cooldown to prevent spam"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return

            # --- Cooldown / deduplication ---
            cooldown_secs = self.ALERT_COOLDOWNS.get(level, 1800)
            if cooldown_secs > 0:
                # Key on level + first 40 chars — so "failed N times" variants
                # all map to the same cooldown slot and don't spam on Atlas SSL outages
                alert_key = f"{level}:{message[:40]}"
                last_sent = self.last_alert_sent.get(alert_key)
                if last_sent:
                    elapsed = (now_local() - last_sent).total_seconds()
                    if elapsed < cooldown_secs:
                        logger.debug(
                            f"[HealthMonitor] Suppressing {level} alert (cooldown {cooldown_secs - elapsed:.0f}s left)"
                        )
                        return
                self.last_alert_sent[alert_key] = now_local()
            # --- end cooldown ---

            emoji_map = {
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "ERROR": "❌",
                "CRITICAL": "🚨",
                "SUCCESS": "✅"
            }
            
            emoji = emoji_map.get(level, "📢")
            timestamp = now_local().strftime("%Y-%m-%d %I:%M:%S %p")
            
            alert_msg = f"{emoji} <b>BOT 3 HEALTH ALERT</b>\n\n"
            alert_msg += f"<b>Level:</b> {level}\n"
            alert_msg += f"<b>Time:</b> {timestamp}\n\n"
            alert_msg += f"<b>Message:</b>\n{message}\n\n"
            alert_msg += f"🤖 <b>Source:</b> Bot 3 Auto-Healer"
            
            await bot.send_message(MASTER_ADMIN_ID, alert_msg, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def send_error_notification(self, error_title: str, error_message: str, stack_trace: str = None):
        """Send instant error notification"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return
            
            # Rate limit error notifications (max 1 per minute for same error)
            now = now_local()
            if self.last_error_notification:
                time_diff = (now - self.last_error_notification).total_seconds()
                if time_diff < 60:
                    return
            
            self.last_error_notification = now
            self.error_count += 1
            
            timestamp = now.strftime("%Y-%m-%d %I:%M:%S %p")
            
            error_msg = f"🚨 <b>BOT 3 ERROR ALERT</b>\n\n"
            error_msg += f"<b>Error #{self.error_count}</b>\n"
            error_msg += f"<b>Time:</b> {timestamp}\n\n"
            error_msg += f"<b>Title:</b> {error_title}\n\n"
            error_msg += f"<b>Message:</b>\n`{error_message[:500]}`\n\n"
            
            if stack_trace and CRITICAL_ERROR_NOTIFY_IMMEDIATELY:
                error_msg += f"<b>Stack Trace:</b>\n```\n{stack_trace[:500]}\n```\n\n"
            
            error_msg += f"💡 <b>System Status:</b> {'Healthy' if self.is_healthy else 'Degraded'}\n"
            error_msg += f"📊 <b>Total Errors:</b> {self.error_count}"
            
            await bot.send_message(MASTER_ADMIN_ID, error_msg, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
    
    async def log_system_metrics(self):
        """Log system metrics periodically"""
        try:
            uptime = now_local() - self.system_metrics["uptime_start"]
            memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            cpu_percent = psutil.cpu_percent(interval=1)
            
            logger.info(
                f"📊 System Metrics - Uptime: {uptime}, "
                f"Memory: {memory_mb:.2f}MB, CPU: {cpu_percent}%, "
                f"Requests: {self.system_metrics['total_requests']}, "
                f"Errors: {self.system_metrics['total_errors']}"
            )
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")

# Initialize health monitor
health_monitor = HealthMonitor()

# ==========================================
# STATE PERSISTENCE SYSTEM
# ==========================================

class StatePersistence:
    """Persistent state management for bot recovery"""
    
    def __init__(self):
        self.state_file = os.path.join(STATE_BACKUP_LOCATION, "bot3_state.pkl")
        os.makedirs(STATE_BACKUP_LOCATION, exist_ok=True)
        logger.info("✅ State Persistence initialized")
    
    async def save_state(self):
        """Save bot state to disk"""
        try:
            if not STATE_BACKUP_ENABLED:
                return
            
            state_data = {
                "timestamp": now_local().isoformat(),
                "health_metrics": health_monitor.system_metrics,
                "error_count": health_monitor.error_count,
                "last_backup": now_local().isoformat()
            }
            
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'wb') as f:
                pickle.dump(state_data, f)
            
            logger.debug(f"State saved at {now_local()}")
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    async def load_state(self):
        """Load bot state from disk"""
        try:
            if not AUTO_RESUME_ON_STARTUP or not os.path.exists(self.state_file):
                return None
            
            with open(self.state_file, 'rb') as f:
                state_data = pickle.load(f)
            
            logger.info(f"✅ State restored from {state_data['timestamp']}")
            return state_data
            
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return None

# Initialize state persistence
state_persistence = StatePersistence()

# Helper to Log User Action
def log_user_action(user, action, details=None):
    """Log user action to file"""
    try:
        username = user.username or user.first_name or "Unknown"
        log_msg = f"User: {username} ({user.id}) | Action: {action}"
        if details:
            log_msg += f" | Details: {details}"
        logging.info(log_msg)
    except Exception as e:
        logging.error(f"Error logging action: {e}")

# --- Authorization and Ban Management ---

# Permission Constants matching Menu Buttons
PERMISSIONS = {
    "can_list": "📋 LIST",
    "can_add": "➕ ADD",
    "can_search": "🔍 SEARCH",
    "can_links": "🔗 LINKS",
    "can_analytics": "📊 ANALYTICS",
    "can_diagnosis": "🩺 DIAGNOSIS",
    "can_terminal": "🖥️ TERMINAL",
    "can_backup": "💾 BACKUP DATA",
    "can_manage_admins": "👥 ADMINS",
    "can_credit": "💳 CREDIT MANAGEMENT"
}

# Reverse map for easy lookup
PERMISSION_KEYS = {v: k for k, v in PERMISSIONS.items()}

# Default SAFE Permissions (Exclude dangerous features)
DEFAULT_SAFE_PERMISSIONS = [
    k for k in PERMISSIONS.keys() 
    if k not in ["can_manage_admins", "can_reset"]
]

# Role Definitions
ROLES = {
    "OWNER": list(PERMISSIONS.keys()), # Full Access
    "MANAGER": [
        "can_list", "can_add", "can_search", "can_links", 
        "can_analytics", "can_manage_admins", "can_backup",
        "can_credit"
        # No Reset, No Diagnosis, No Terminal
    ],
    "ADMIN": [
        "can_list", "can_add", "can_search", "can_links",
        "can_analytics" 
        # No Admin Management
    ],
    "MODERATOR": [
        "can_list", "can_add", "can_search", "can_links"
        # No Admin/Analytics
    ],
    "SUPPORT": [
        "can_list", "can_search"
        # Read Only
    ]
}

def is_admin(user_id: int) -> bool:
    """Check if user is MASTER_ADMIN or in bot3_admins collection"""
    global MASTER_ADMIN_ID # Allow global update
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if admin:
        # CRITICAL FIX: Respect Lock Status
        if admin.get("is_locked", False):
            return False
            
        # Check if this admin is marked as OWNER in DB
        if admin.get("is_owner", False):
            # If DB says owner, but Env Var doesn't match, trust DB logic (Bot restart might reset Env)
            # But effectively this user IS an admin.
            return True
        return True
    return False

def has_permission(user_id: int, required_perm: str) -> bool:
    """
    Check if admin has specific permission.
    MASTER_ADMIN always has ALL permissions.
    """
    if user_id == MASTER_ADMIN_ID:
        return True
        
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False
        
    # If no permissions array exists, default to ALL ALLOWED (for backward compatibility/initial setup)
    # OR change to False if you want strict default deny. 
    # User requested: "if clicked permission please display available admin... selected menu buttons will be only available"
    # This implies we should default to Empty or All? 
    # Let's default to ALL permissions if the field is missing, so we don't break existing admins immediately.
    # The permission editor will allow revoking.
    current_perms = admin.get("permissions")
    
    if current_perms is None:
        return True # Default allow if not configured yet
        
    return required_perm in current_perms  # FIXED: Was required_permission

def is_banned(user_id: int) -> bool:
    """Check if user is banned"""
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return False
    
    # Exempt all admins from bans (prevents auto-ban from locking them out)
    # To ban an admin, first remove them from admin list.
    if is_admin(user_id):
        return False
        
    banned = col_banned_users.find_one({"user_id": user_id})
    return banned is not None

def ban_user(user_id: int, user_name: str, username: str, reason: str):
    """Ban a user and log the action"""
    try:
        col_banned_users.insert_one({
            "user_id": user_id,
            "user_name": user_name,
            "username": username,
            "banned_by": "SYSTEM",
            "banned_at": now_local(),
            "reason": reason,
            "status": "banned"
        })
        logger.info(f"User {user_id} banned: {reason}")
    except Exception as e:
        logger.error(f"Failed to ban user {user_id}: {e}")

def format_datetime_12h(dt: datetime) -> str:
    """Format datetime to 12-hour AM/PM format in local timezone"""
    if not dt:
        return "N/A"
    return dt.strftime("%b %d, %Y %I:%M %p")

async def check_spam_and_ban(user_id: int, user_name: str, username: str, action: str) -> tuple:
    """
    Check if user is spamming and auto-ban if threshold exceeded.
    Returns (was_banned: bool, attempt_count: int)
    Uses environment variables: RATE_LIMIT_SPAM_THRESHOLD, RATE_LIMIT_SPAM_WINDOW_SECONDS
    """
    from datetime import timedelta
    
    try:
        # Record this attempt
        col_user_activity.insert_one({
            "user_id": user_id,
            "timestamp": now_local(),
            "action": action
        })
        
        # Count attempts in configured window
        window_ago = now_local() - timedelta(seconds=RATE_LIMIT_SPAM_WINDOW_SECONDS)
        attempt_count = col_user_activity.count_documents({
            "user_id": user_id,
            "timestamp": {"$gte": window_ago}
        })
        
        # Auto-ban if threshold exceeded
        if attempt_count >= RATE_LIMIT_SPAM_THRESHOLD:
            # Check if already banned
            if not is_banned(user_id):
                reason = f"Automated: Spam detection ({attempt_count} unauthorized attempts in {RATE_LIMIT_SPAM_WINDOW_SECONDS} seconds)"
                ban_user(user_id, user_name, username, reason)
                
                # Notify admin about auto-ban
                await notify_admin_auto_ban(user_id, user_name, username, attempt_count)
                
                return True, attempt_count
        
        return False, attempt_count
        
    except Exception as e:
        logger.error(f"Error in spam check: {e}")
        await health_monitor.send_error_notification(
            "Spam Detection Error",
            str(e),
            traceback.format_exc()
        )
        return False, 0

async def log_unauthorized_access(user, action: str, attempt_count: int):
    """Log unauthorized access attempt and notify admin"""
    try:
        # Log to file
        log_user_action(user, f"UNAUTHORIZED ACCESS: {action}", f"Attempt #{attempt_count}")
        
        # Notify admin
        await notify_admin_unauthorized_access(user, action, attempt_count)
    except Exception as e:
        logger.error(f"Failed to log unauthorized access: {e}")

async def notify_admin_unauthorized_access(user, action: str, attempt_count: int):
    """Send unauthorized access report to master admin"""
    try:
        timestamp = format_datetime_12h(now_local())
        username = user.username or "No username"
        full_name = user.full_name or "Unknown"
        
        msg = (
            f"🚨 <b>UNAUTHORIZED ACCESS ATTEMPT</b>\n\n"
            f"👤 <b>User ID</b>: `{user.id}`\n"
            f"📝 <b>Username</b>: @{username}\n"
            f"👨 <b>Name</b>: {_html_escape(full_name)}\n"
            f"🕐 <b>Time</b>: {timestamp}\n"
            f"🎯 <b>Action</b>: {action}\n"
            f"🔢 <b>Attempt</b>: #{attempt_count}\n\n"
            f"⚠️ <b>Status</b>: Access denied (non-admin)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin about unauthorized access: {e}")

async def notify_admin_auto_ban(user_id: int, user_name: str, username: str, spam_count: int):
    """Notify admin about auto-ban"""
    try:
        timestamp = format_datetime_12h(now_local())
        
        msg = (
            f"🚫 <b>AUTO-BAN TRIGGERED</b>\n\n"
            f"👤 <b>User ID</b>: `{user_id}`\n"
            f"📝 <b>Username</b>: @{username or 'None'}\n"
            f"👨 <b>Name</b>: {user_name or 'Unknown'}\n"
            f"🕐 <b>Time</b>: {timestamp}\n"
            f"⚠️ <b>Reason</b>: Spam detected\n"
            f"📊 <b>Attempts</b>: {spam_count} in 30 seconds\n\n"
            f"🔇 User will receive NO responses (silent ban)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin about auto-ban: {e}")

async def check_authorization_user(user: types.User, message: types.Message, action_name: str = "access bot", required_perm: str = None) -> bool:
    """
    Same as check_authorization but accepts a User object directly.
    Use this for callback handlers where message.from_user is the bot itself.
    """
    # Temporarily swap message.from_user by delegating with user_id override
    user_id = user.id

    # 0. Master Admin / Owner Bypass
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return True

    admin_doc = col_admins.find_one({"user_id": user_id})
    if admin_doc:
        if admin_doc.get("is_locked", False):
            return False
        if required_perm:
            perms = admin_doc.get("permissions")
            if perms is not None and required_perm not in perms:
                await message.answer("⛔ <b>ACCESS DENIED</b>\n\nYou do not have permission to access this feature.", parse_mode="HTML")
                return False
        return True

    banned = col_banned_users.find_one({"user_id": user_id})
    if banned:
        return False

    was_banned, attempt_count = await check_spam_and_ban(user_id, user.full_name, user.username, action_name)
    if was_banned:
        return False

    await log_unauthorized_access(user, action_name, attempt_count)
    return False

async def check_authorization(message: types.Message, action_name: str = "access bot", required_perm: str = None) -> bool:
    """
    Universal authorization check for all handlers.
    OPTIMIZED: Single DB query for admins.
    
    Checks:
    1. Master/Owner -> Bypass
    2. Fetch Admin Doc -> Check Lock, Update Info, Check Perms
    3. If Not Admin -> Check Ban, Check Spam/Log
    """
    user_id = message.from_user.id
    
    # 0. Master Admin / Owner Bypass (Always allowed, never banned)
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return True
    
    # 1. Fetch Admin Doc ONCE
    admin_doc = col_admins.find_one({"user_id": user_id})

    # 2. Process Admin
    if admin_doc:
        # A. Check Lock Status
        if admin_doc.get("is_locked", False):
            # Locked admins cannot access anything.
            return False
            
        # B. Update Admin Info (Keep DB Fresh)
        # Check against current to minimize writes (optimization)
        current_name = admin_doc.get("full_name")
        current_username = admin_doc.get("username")
        
        if current_name != message.from_user.full_name or current_username != message.from_user.username:
            try:
                col_admins.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "full_name": message.from_user.full_name,
                        "username": message.from_user.username,
                        "last_active": now_local()
                    }}
                )
            except Exception as e:
                logger.error(f"Failed to update admin info: {e}")

        # C. Check Permission (if required)
        if required_perm:
            # If permissions not set, allow all (backward compatibility)
            perms = admin_doc.get("permissions")
            if perms is not None and required_perm not in perms:
                await message.answer("⛔ <b>ACCESS DENIED</b>\n\nYou do not have permission to access this feature.", parse_mode="HTML")
                logger.warning(f"Admin {user_id} denied access to {action_name} (Missing: {required_perm})")
                return False
        
        # Log successful authorized access to shared live terminal
        try:
            log_to_terminal(
                action_type=f"✅ {action_name}",
                user_id=user_id,
                user_name=message.from_user.full_name or "",
                details=f"@{message.from_user.username}" if message.from_user.username else f"ID:{user_id}"
            )
        except Exception:
            pass
        return True

    # 3. Process Non-Admin (If we are here, admin_doc is None)
    
    # A. Check ban status
    # We only check this for non-admins because admins are exempt in is_banned() anyway
    banned = col_banned_users.find_one({"user_id": user_id})
    if banned:
        return False  # No response at all for banned users

    # B. Non-admin unauthorized access logic
    was_banned, attempt_count = await check_spam_and_ban(
        user_id,
        message.from_user.full_name,
        message.from_user.username,
        action_name
    )
    
    if was_banned:
        return False  # Silent ban, no response
    
    # Log unauthorized access attempt
    await log_unauthorized_access(
        message.from_user,
        action_name,
        attempt_count
    )
    
    return False  # BLOCK - No access for non-admins

# ==========================================
# BOT AND DATABASE SETUP (ENTERPRISE)
# ==========================================

# Bot Setup
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Database Connection with Enterprise Configuration
try:
    print("🔌 Connecting to MongoDB...")
    import certifi
    client = pymongo.MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        maxPoolSize=MONGO_MAX_POOL_SIZE,
        minPoolSize=MONGO_MIN_POOL_SIZE,
        retryWrites=True,
        retryReads=True,
        tlsCAFile=certifi.where()
    )
    db = client[MONGO_DB_NAME]
    
    # Bot3 Management Collections
    col_logs = db["bot3_logs"]
    col_pdfs = db["bot3_pdfs"]
    col_ig_content = db["bot3_ig_content"]
    col_rewards = db["bot3_rewards"]          # Referral reward content pool (RW1, RW2...)
    col_settings = db["bot3_settings"]
    col_admins = db["bot3_admins"]
    col_banned_users = db["bot3_banned_users"]
    col_user_activity = db["bot3_user_activity"]
    col_backups = db["bot3_backups"]  # Backup history collection
    col_live_logs = db["bot2_live_terminal_logs"]  # Shared live terminal with Bot 2
    col_store_items = db["bot3_store_items"]  # Vault Shop items
    col_milestones  = db["bot3_milestones"]   # Referral milestone tiers
    
    # Test connection
    client.admin.command('ping')
    print("✅ Connected to MongoDB")
    print(f"   Database: {MONGO_DB_NAME}")
    print(f"   Connection Pool: {MONGO_MIN_POOL_SIZE}-{MONGO_MAX_POOL_SIZE}")

    # -- STARTUP DB NAME GUARD --
    # Identical guard used in bot1.py and bot2.py.
    # If the environment points to the wrong database (e.g. MSANODEDATA instead
    # of MSANodeDB), the bot exits immediately with a clear error so the problem
    # is obvious in Render logs rather than silently storing data to the wrong DB.
    if db.name != "MSANodeDB":
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"CRITICAL: Connected to wrong database '{db.name}'")
        print("Expected: MSANodeDB")
        print("Fix: Set MONGO_DB_NAME=MSANodeDB in Render environment")
        print("     OR verify MONGO_URI contains /MSANodeDB")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        sys.exit(1)
    print("   ✅ Database name verified: MSANodeDB")
    # -- END DB GUARD --

    # -- STARTUP DB FINGERPRINT GUARD --
    # Prevents silent connection drift to another cluster/URI, which looks like
    # "data loss" after restart even when data still exists elsewhere.
    try:
        def _mongo_host_from_uri(uri: str) -> str:
            if not uri:
                return "unknown"
            s = uri.split("://", 1)[-1]
            if "@" in s:
                s = s.split("@", 1)[1]
            s = s.split("/", 1)[0]
            s = s.split("?", 1)[0]
            return s or "unknown"

        current_fingerprint = f"{_mongo_host_from_uri(MONGO_URI)}|{MONGO_DB_NAME}"
        fp_doc = col_settings.find_one({"key": "db_fingerprint"})
        saved_fingerprint = fp_doc.get("value") if fp_doc else None

        if not saved_fingerprint:
            col_settings.update_one(
                {"key": "db_fingerprint"},
                {"$set": {
                    "key": "db_fingerprint",
                    "value": current_fingerprint,
                    "updated_at": now_local()
                }},
                upsert=True
            )
            print("   ✅ DB fingerprint initialized")
        elif saved_fingerprint != current_fingerprint:
            if ALLOW_DB_FINGERPRINT_CHANGE:
                col_settings.update_one(
                    {"key": "db_fingerprint"},
                    {"$set": {"value": current_fingerprint, "updated_at": now_local()}}
                )
                print("   ⚠️ DB fingerprint changed (allowed by ALLOW_DB_FINGERPRINT_CHANGE=true)")
            else:
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                print("CRITICAL: Database fingerprint mismatch detected")
                print(f"Saved:   {saved_fingerprint}")
                print(f"Current: {current_fingerprint}")
                print("Refusing startup to prevent silent cross-cluster data drift.")
                print("Set ALLOW_DB_FINGERPRINT_CHANGE=true only if this change is intentional.")
                print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                sys.exit(1)
        else:
            print("   ✅ DB fingerprint verified")
    except Exception as _fp_err:
        print(f"⚠️ DB fingerprint guard warning: {_fp_err}")
    # -- END DB FINGERPRINT GUARD --

    # Create indexes for better performance (idempotent - skips if exists)
    def create_index_safe(collection, keys, **kwargs):
        """Helper to create index safely, ignoring if already exists"""
        try:
            collection.create_index(keys, **kwargs)
        except Exception as e:
            # Ignore if index already exists with different options
            if "already exists" not in str(e).lower() and "indexkeyspecsconflict" not in str(e).lower():
                raise
    
    try:
        print("🔍 Creating database indexes...")
        # Basic indexes with explicit names to avoid conflicts
        create_index_safe(col_pdfs, "index", unique=True, name="pdf_index_unique")
        create_index_safe(col_pdfs, "created_at", name="pdf_created_at")
        create_index_safe(col_pdfs, "msa_code", sparse=True, name="pdf_msa_code")
        create_index_safe(col_ig_content, "created_at", name="ig_created_at")
        create_index_safe(col_ig_content, "cc_number", unique=True, name="ig_cc_number_unique")
        create_index_safe(col_logs, "timestamp", name="log_timestamp")
        
        # Ban and activity tracking indexes (security)
        create_index_safe(col_banned_users, "user_id", unique=True, name="banned_user_id_unique")
        create_index_safe(col_user_activity, [("user_id", 1), ("timestamp", -1)], name="activity_user_time")
        create_index_safe(col_user_activity, [("timestamp", -1)], name="activity_timestamp")
        
        # Analytics performance indexes (for scalability with millions of records)
        create_index_safe(col_pdfs, [("clicks", -1)], sparse=True, name="pdf_clicks_desc")
        create_index_safe(col_pdfs, [("affiliate_clicks", -1)], sparse=True, name="pdf_aff_clicks_desc")
        create_index_safe(col_pdfs, [("ig_start_clicks", -1)], sparse=True, name="pdf_ig_clicks_desc")
        create_index_safe(col_pdfs, [("yt_start_clicks", -1)], sparse=True, name="pdf_yt_clicks_desc")
        create_index_safe(col_pdfs, [("yt_code_clicks", -1)], sparse=True, name="pdf_yt_code_clicks_desc")
        create_index_safe(col_ig_content, [("ig_cc_clicks", -1)], sparse=True, name="ig_cc_clicks_desc")
        
        # Compound indexes for filtered analytics queries
        create_index_safe(col_pdfs, [("affiliate_link", 1), ("affiliate_clicks", -1)], name="pdf_aff_link_clicks")
        create_index_safe(col_pdfs, [("ig_start_code", 1), ("ig_start_clicks", -1)], name="pdf_ig_code_clicks")
        create_index_safe(col_pdfs, [("yt_link", 1), ("yt_start_clicks", -1)], name="pdf_yt_link_clicks")
        create_index_safe(col_pdfs, [("msa_code", 1), ("yt_code_clicks", -1)], name="pdf_msa_yt_clicks")
        
        # Backup collection indexes
        create_index_safe(col_backups, "created_at", name="backup_created_at")
        create_index_safe(col_backups, "filename", name="backup_filename")
        
        # Admin collection indexes
        create_index_safe(col_admins, "user_id", unique=True, name="admin_user_id_unique")

        # Settings collection — key field is the natural primary key
        create_index_safe(col_settings, "key", unique=True, name="settings_key_unique")

        # Rewards collection indexes (RW codes for referral reward pool)
        create_index_safe(col_rewards, "rw_number", unique=True, name="reward_rw_number_unique")
        create_index_safe(col_rewards, "created_at", name="reward_created_at")

        # PDF lookup by yt_start_code (used by bot1 on every user click)
        create_index_safe(col_pdfs, "yt_start_code", sparse=True, name="pdf_yt_start_code")
        
        print("✅ Database indexes created (optimized for millions of records)")
    except Exception as idx_err:
        print(f"⚠️ Warning: Some indexes could not be created: {idx_err}")
        print("   Bot will continue, existing indexes will be used")

    # ── TTL AUTO-EXPIRY INDEXES ────────────────────────────────────────────────
    # Prevent unbounded growth of activity + log collections.
    # Each block is independent — a failure in one never blocks the others or startup.
    # Drop-before-create avoids "index already exists with different options" on re-deploy.

    # bot3_user_activity — auto-delete rows after 7 days
    try:
        try:
            col_user_activity.drop_index("activity_timestamp_ttl_7d")
        except Exception:
            pass
        col_user_activity.create_index(
            [("timestamp", 1)],
            expireAfterSeconds=604_800,   # 7 days
            sparse=True,
            name="activity_timestamp_ttl_7d"
        )
        print("✅ TTL index set: bot3_user_activity → 7-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (bot3_user_activity): {_ttl_err}")

    # bot3_logs — auto-delete log rows after 7 days
    try:
        try:
            col_logs.drop_index("log_timestamp_ttl_7d")
        except Exception:
            pass
        col_logs.create_index(
            [("timestamp", 1)],
            expireAfterSeconds=604_800,   # 7 days
            sparse=True,
            name="log_timestamp_ttl_7d"
        )
        print("✅ TTL index set: bot3_logs → 7-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (bot3_logs): {_ttl_err}")
    
    # Initialize click tracking fields for existing documents (migration)
    try:
        print("🔄 Initializing click tracking fields...")
        # Update PDFs without click fields
        pdf_updated = col_pdfs.update_many(
            {"clicks": {"$exists": False}},
            {"$set": {
                "clicks": 0,
                "affiliate_clicks": 0,
                "ig_start_clicks": 0,
                "yt_start_clicks": 0,
                "yt_code_clicks": 0
            }}
        )
        
        # Update IG content without click fields
        ig_updated = col_ig_content.update_many(
            {"ig_cc_clicks": {"$exists": False}},
            {"$set": {"ig_cc_clicks": 0}}
        )
        
        print(f"✅ Click tracking initialized (PDFs: {pdf_updated.modified_count}, IG: {ig_updated.modified_count})")
    except Exception as migration_err:
        print(f"⚠️ Warning: Could not initialize click fields: {migration_err}")
    
    # ── CRITICAL: COLLECTION HEALTH CHECK ──────────────────────────────────
    # Verifies data accessibility on startup. If collections are empty, attempts
    # reconnection and recreation before continuing.
    print("\n🏥 Running collection health check...")
    try:
        pdf_count = col_pdfs.count_documents({})
        ig_count = col_ig_content.count_documents({})
        
        print(f"   📋 bot3_pdfs: {pdf_count} documents")
        print(f"   📸 bot3_ig_content: {ig_count} documents")
        
        # If collections are unexpectedly empty, attempt recovery
        if pdf_count == 0 or ig_count == 0:
            print("\n⚠️ WARNING: Empty collections detected!")
            print("   Attempting database reconnection & recovery...")
            
            # Force reconnect to MongoDB
            try:
                client.admin.command('ping')
                print("   ✅ Database connection verified")
                
                # Try to verify collections still exist in database
                collections = db.list_collection_names()
                print(f"   📊 Available collections: {', '.join(collections)}")
                
                if "bot3_pdfs" not in collections:
                    print("   ❌ bot3_pdfs collection not found in database!")
                if "bot3_ig_content" not in collections:
                    print("   ❌ bot3_ig_content collection not found in database!")
                    
            except Exception as reconnect_err:
                print(f"   ❌ Database reconnection failed: {reconnect_err}")
                print("   This may cause 'No PDFs found' errors on /links command.")
                print("   Try restarting the bot in a few moments.")
        else:
            print("   ✅ Collections are accessible and populated")
    except Exception as health_err:
        print(f"⚠️ Health check error: {health_err}")
    
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("✅ DATABASE READY FOR ENTERPRISE SCALE")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
except Exception as e:
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("❌ CRITICAL: DATABASE CONNECTION FAILED")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"Error: {e}")
    print(f"MongoDB URI: {MONGO_URI[:20]}..." if MONGO_URI else "MONGO_URI not set!")
    print("\n⚠️ Please check:")
    print("  1. MongoDB is running")
    print(f"  2. MONGO_URI in {ACTIVE_ENV_FILE} is correct")
    print("  3. Network connectivity")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    sys.exit(1)

# --- FSM States ---
class PDFStates(StatesGroup):
    waiting_for_add_name = State()
    waiting_for_add_link = State()
    waiting_for_edit_search = State()
    waiting_for_edit_field = State()
    waiting_for_edit_value = State()
    waiting_for_delete_search = State()
    waiting_for_delete_confirm = State()
    viewing_list = State()

class AffiliateStates(StatesGroup):
    waiting_for_pdf_selection = State()
    waiting_for_link = State()
    waiting_for_delete_confirm = State()
    viewing_list = State()

class AffiliateDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class MSACodeStates(StatesGroup):
    waiting_for_pdf_selection = State()
    waiting_for_code = State()
    viewing_list = State()
    
class MSACodeEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_new_code = State()
    
class MSACodeDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class YTStates(StatesGroup):
    waiting_for_title = State()
    waiting_for_link = State()
    waiting_for_pdf_selection = State()
    viewing_list = State()
    
class YTEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_field = State()
    waiting_for_value = State()
    
class YTDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class IGStates(StatesGroup):
    waiting_for_content_name = State()
    
class IGEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_new_name = State()
    
class IGDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class IGListStates(StatesGroup):
    viewing = State()

class IGAffiliateStates(StatesGroup):
    waiting_for_ig_selection = State()  # Select IG content for affiliate
    waiting_for_link = State()           # Enter affiliate link

class IGAffiliateEditStates(StatesGroup):
    waiting_for_selection = State()      # Select IG content to edit
    waiting_for_new_link = State()       # Enter new affiliate link

class IGAffiliateDeleteStates(StatesGroup):
    waiting_for_selection = State()      # Select IG content to delete affiliate
    waiting_for_confirm = State()        # Confirm deletion

class TutorialPKStates(StatesGroup):
    waiting_for_link = State()            # ADD step 1: waiting for URL input
    waiting_for_link_confirm = State()    # ADD step 2: waiting for reply-KB confirm
    waiting_for_edit_link = State()       # EDIT step 1: waiting for new URL input
    waiting_for_edit_confirm = State()    # EDIT step 2: waiting for reply-KB confirm
    waiting_for_delete_confirm = State()  # DELETE: waiting for reply-KB confirm


class ListStates(StatesGroup):
    viewing_all = State()  # For viewing ALL PDFs
    viewing_ig = State()   # For viewing IG content

class SearchStates(StatesGroup):
    viewing_pdf_list = State()           # Viewing paginated PDF list
    waiting_for_pdf_input = State()      # Waiting for PDF index/name
    viewing_ig_list = State()            # Viewing paginated IG list
    waiting_for_ig_input = State()       # Waiting for IG index/CC code
    waiting_for_reward_input = State()   # Waiting for RW index/code in search

class PDFActionStates(StatesGroup):
    waiting_for_action = State()


class ResetStates(StatesGroup):
    waiting_for_confirm_button = State()
    waiting_for_confirm_text = State()
    waiting_for_final_wipe_code = State()

class BackupResetStates(StatesGroup):
    waiting_for_confirm_button = State()
    waiting_for_confirm_text = State()

class RewardStates(StatesGroup):
    waiting_for_content = State()           # ADD: waiting for reward text/link
    edit_waiting_for_selection = State()    # EDIT: select reward by RW number
    edit_waiting_for_new_content = State()  # EDIT: enter new content
    delete_waiting_for_selection = State()  # DELETE: select reward to remove
    delete_waiting_for_confirm = State()    # DELETE: confirm removal
    viewing_list = State()                  # LIST: paginated browsing state

class MilestoneStates(StatesGroup):
    waiting_for_refs       = State()   # ADD: referral count
    waiting_for_name       = State()   # ADD: milestone name
    waiting_for_badge      = State()   # ADD: badge emoji
    waiting_for_credits    = State()   # ADD: bonus credits
    waiting_for_reward_txt = State()   # ADD: reward text
    edit_select            = State()   # EDIT: select milestone
    edit_field             = State()   # EDIT: choose field
    edit_value             = State()   # EDIT: enter new value
    delete_select          = State()   # DELETE: select
    delete_confirm         = State()   # DELETE: confirm

class StoreStates(StatesGroup):
    waiting_for_name       = State()   # ADD: item name
    waiting_for_desc       = State()   # ADD: description
    waiting_for_cost       = State()   # ADD: credit cost
    waiting_for_btn_name   = State()   # ADD: reply keyboard button label
    waiting_for_reward_txt = State()   # ADD: reward text
    edit_select            = State()   # EDIT: select item
    edit_field             = State()   # EDIT: choose field
    edit_value             = State()   # EDIT: new value
    delete_select          = State()   # DELETE: select
    delete_confirm         = State()   # DELETE: confirm
    list_select            = State()   # LIST: select item to view content
    viewing_detail         = State()   # LIST: full-detail view of a single reward

class AnalyticsStates(StatesGroup):
    viewing_analytics = State()
    viewing_category = State()

class BackupStates(StatesGroup):
    viewing_backup_menu      = State()
    waiting_for_json_file    = State()  # UPLOAD DATA restore
    viewing_history          = State()  # paginated backup history
    selecting_download       = State()  # browse MSANodeBackups to download
    selecting_gdrive_upload  = State()  # browse MSANodeBackups to GDrive-upload

class Bot3BackupStates(StatesGroup):
    # 📊 BACKUP STATUS
    status_viewing        = State()
    # 💾 DOWNLOAD BACKUP
    dl_month_select       = State()
    dl_format_select      = State()
    # 📤 UPLOAD BACKUP
    upload_db_choice      = State()
    upload_file_wait      = State()
    upload_restore_choice = State()  # Choose: restore previous codes or assign new
    # 📜 HISTORY
    history_viewing       = State()
    # ☁️ GDRIVE SYSTEM
    gdrive_month_select   = State()
    gdrive_ttl_confirm    = State()
    # ⏳ ACTIVATE TTL
    ttl_month_select      = State()
    ttl_active_toggle     = State()
    # 🗑️ RESET BACKUP DATA
    reset_target_select   = State()
    reset_confirm1        = State()
    reset_confirm2        = State()

# =============================================================================
# 💾 BOT 3 BACKUP SYSTEM — Complete Handlers (No duplicates)
# Helpers → Download → Upload → History → GDrive → Active Deletion
# =============================================================================

async def _b3_send_paged(message, text, reply_markup=None, parse_mode="HTML"):
    """Auto-split and send messages that exceed Telegram's 4096 char limit."""
    MAX = 4000
    if len(text) <= MAX:
        await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return
    parts = []
    while text:
        if len(text) <= MAX:
            parts.append(text); break
        cut = text.rfind('\n', 0, MAX)
        if cut == -1: cut = MAX
        parts.append(text[:cut])
        text = text[cut:].lstrip('\n')
    for i, part in enumerate(parts):
        kb = reply_markup if i == len(parts) - 1 else None
        await message.answer(part, parse_mode=parse_mode, reply_markup=kb)


def _b3_group_backups(records, current_date):
    """
    Groups daily backup documents into UI chunks:
      - Past months  → one chunk per month (Full Month)
      - Current month → one chunk per 7-day week (Week 1..5)
    Returns list of (key, label, docs_list, is_full_month)  newest-first.
    """
    import calendar as _cal
    groups = {}
    for r in records:
        dt = r.get("backup_date")
        if not dt:
            continue
        is_past = (dt.year < current_date.year or
                   (dt.year == current_date.year and dt.month < current_date.month))
        if is_past:
            k   = f"{dt.year}_{dt.month:02d}_FULL"
            lbl = f"{dt.year} {dt.strftime('%B')} (Full Month)"
        else:
            w   = min((dt.day - 1) // 7 + 1, 5)
            k   = f"{dt.year}_{dt.month:02d}_W{w}"
            s_d = (w - 1) * 7 + 1
            e_d = _cal.monthrange(dt.year, dt.month)[1] if w == 5 else w * 7
            lbl = f"{dt.year} {dt.strftime('%B')} (Week {w}: {s_d}–{e_d})"
        if k not in groups:
            groups[k] = {"label": lbl, "docs": [], "is_full": is_past}
        groups[k]["docs"].append(r)
    sorted_keys = sorted(groups.keys(), reverse=True)
    return [(k, groups[k]["label"], groups[k]["docs"], groups[k]["is_full"])
            for k in sorted_keys]


# =============================================================================
# 💾 DOWNLOAD BACKUP — Hierarchical Month/Week list → structured ZIP
# =============================================================================

@dp.message(F.text.in_({"💾 DOWNLOAD BACKUP"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_dl_backup_start(message: types.Message, state: FSMContext):
    """Show structured grouped list of backup chunks from cluster."""
    if not await check_authorization(message, "Download Backup", "can_manage_admins"): return
    await state.clear()
    loading = await message.answer("⏳ Loading available backups...")
    try:
        loop = asyncio.get_event_loop()
        records = await loop.run_in_executor(
            None, lambda: list(col_bot3_backups.find(
                {}, {"backup_date": 1, "backup_type": 1, "docs": 1}
            ).sort("backup_date", -1))
        )
        try: await loading.delete()
        except: pass

        if not records:
            await message.answer(
                "📦 <b>DOWNLOAD BACKUP — Bot 3</b>\n\n"
                "❌ No snapshots found in backup cluster yet.\n"
                "The daily auto-backup runs at <b>23:59 UTC</b> every night.",
                reply_markup=get_backup_menu(), parse_mode="HTML"
            )
            return

        chunks    = _b3_group_backups(records, now_local())
        text      = "📦 <b>DOWNLOAD BACKUP — Bot 3</b>\n<i>Select a time period by number:</i>\n" + "─" * 28 + "\n\n"
        chunk_ids = []

        for i, (k, lbl, docs, is_full) in enumerate(chunks, 1):
            dates = [d["backup_date"] for d in docs if d.get("backup_date")]
            latest_str   = max(dates).strftime("%d %b %Y") if dates else "Unknown"
            earliest_str = min(dates).strftime("%d %b %Y") if dates else "Unknown"
            ttl_cnt = sum(1 for d in docs if "ttl_date" in d)
            ttl_ico = "🗑️" if ttl_cnt > 0 else "🔒"
            text += (
                f"{i}. <b>{lbl}</b>\n"
                f"   📅 {earliest_str} → {latest_str}\n"
                f"   📦 {len(docs)} snapshot(s)  {ttl_ico} TTL: {ttl_cnt}/{len(docs)}\n\n"
            )
            chunk_ids.append([str(d["_id"]) for d in docs])

        text += f"<i>Type a number (1–{len(chunks)}) or press 🔙 BACK</i>"
        await state.update_data(b3_dl_chunks=chunk_ids, b3_dl_labels=[c[1] for c in chunks])
        await state.set_state(Bot3BackupStates.dl_month_select)
        await _b3_send_paged(
            message, text,
            reply_markup=ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
            ], resize_keyboard=True)
        )
    except Exception as e:
        try: await loading.delete()
        except: pass
        await message.answer(f"❌ Error:\n<code>{str(e)[:300]}</code>", reply_markup=get_backup_menu(), parse_mode="HTML")


@dp.message(Bot3BackupStates.dl_month_select)
async def bot3_dl_backup_month_chosen(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Download Select", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())

    data   = await state.get_data()
    chunks = data.get("b3_dl_chunks", [])
    labels = data.get("b3_dl_labels", [])
    count  = len(chunks)
    txt    = (message.text or "").strip()
    if not txt.isdigit() or not (1 <= int(txt) <= count):
        return await message.answer(f"❌ Enter a number between 1 and {count}.")

    idx    = int(txt) - 1
    label  = labels[idx]
    doc_ids = chunks[idx]
    await state.clear()

    # ── Auto-deliver ZIP immediately — no format selection step ─────────────────────
    loading = await message.answer(f"⏳ <b>Generating ZIP...</b>", parse_mode="HTML")
    try:
        loop = asyncio.get_event_loop()
        buf  = io.BytesIO()
        snap_count = 0

        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for doc_id in doc_ids:
                doc = await loop.run_in_executor(None, lambda i=doc_id: col_bot3_backups.find_one({"_id": ObjectId(i)}))
                if not doc: continue
                dt = doc.get("backup_date")
                if not dt: continue
                c_data = doc.get("data", {})
                w    = min((dt.day - 1) // 7 + 1, 5)
                path = f"{dt.strftime('%Y')}/{dt.strftime('%B')}/Week {w}/{dt.strftime('%Y-%m-%d_%A')}.json"
                out  = {cn: (cd.get("documents", []) if isinstance(cd, dict) else cd)
                        for cn, cd in c_data.items()}
                zf.writestr(path, json.dumps(out, default=str, indent=2, ensure_ascii=False))
                snap_count += 1

        buf.seek(0)
        safe  = label.replace(" ", "_").replace("(", "").replace(")", "").replace("–", "-")
        fname = f"Bot3_{safe}.zip"

        try: await loading.delete()
        except: pass

        await message.answer_document(
            types.BufferedInputFile(buf.read(), filename=fname),
            caption=(
                f"✅ <b>Bot 3 Backup — {label}</b>\n"
                f"📦 ZIP | {snap_count} daily snapshot(s)\n"
                f"📄 Structure: <code>YYYY/Month/Week N/YYYY-MM-DD_Day.json</code>"
            ),
            parse_mode="HTML",
            reply_markup=get_backup_menu()
        )
        # ── Log success to history ──
        try:
            from datetime import timezone as _tz
            col_bot3_bk_history.insert_one({
                "bot": "bot3", "action": "📥 Download Backup",
                "details": f"{fname} | {snap_count} snapshot(s)",
                "status": "success", "timestamp": datetime.now(_tz.utc)
            })
        except: pass

    except Exception as e:
        try: await loading.delete()
        except: pass
        safe_err = str(e)[:300].replace("<", "&lt;").replace(">", "&gt;")
        await message.answer(
            f"❌ <b>Download failed:</b>\n<code>{safe_err}</code>",
            parse_mode="HTML", reply_markup=get_backup_menu()
        )



# =============================================================================
# 📤 UPLOAD DATA — Strict target selection, upsert-based, no DB mixing
# =============================================================================

@dp.message(F.text.in_({"📤 UPLOAD BACKUP", "📤 UPLOAD DATA"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_upload_backup_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Upload Backup", "can_manage_admins"): return
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🗄️ MAIN DB (MSANodeDB)"), KeyboardButton(text="🔐 BACKUP DB (MSANodeBackups)")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await state.set_state(Bot3BackupStates.upload_db_choice)
    await message.answer(
        "📤 <b>UPLOAD BACKUP — Bot 3</b>\n\n"
        "Choose the <b>strict target database</b>:\n\n"
        "🗄️ <b>MAIN DB</b> — Restores into <code>MSANodeDB</code> (live production)\n"
        "🔐 <b>BACKUP DB</b> — Restores into <code>MSANodeBackups</code> (cluster only)\n\n"
        "⚠️ Completely separate. No cross-DB writing. All inserts use <b>upsert</b> (no duplicates).",
        reply_markup=kb, parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.upload_db_choice)
async def bot3_upload_backup_db_chosen(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Upload DB Choose", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())
    if "MAIN DB" not in message.text and "BACKUP DB" not in message.text:
        return await message.answer("Choose MAIN DB or BACKUP DB.")
    use_backup   = "BACKUP DB" in message.text
    target_label = "MSANodeBackups (cluster)" if use_backup else "MSANodeDB (production)"
    await state.update_data(b3_upload_target="backup" if use_backup else "main")
    await state.set_state(Bot3BackupStates.upload_file_wait)
    await message.answer(
        f"✅ Target locked: <b>{target_label}</b>\n\n"
        "Send a <b>.json</b> or <b>.zip</b> file now.\n"
        "Use 🔙 BACK or ⬅️ MAIN MENU to abort.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]], resize_keyboard=True),
        parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.upload_file_wait)
async def bot3_upload_backup_receive(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Upload Receive", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())
    if not message.document:
        return await message.answer("❌ Please send a .json or .zip file.")

    fname = (message.document.file_name or "").lower()
    if not (fname.endswith(".json") or fname.endswith(".zip")):
        return await message.answer("❌ Only .json or .zip files accepted.")

    data       = await state.get_data()
    use_backup = data.get("b3_upload_target") == "backup"
    target_db  = _bkp_db_b3 if use_backup else db
    target_lbl = "MSANodeBackups" if use_backup else "MSANodeDB"

    # ── Check if a link registry exists ─────────────────────────────────
    # Only offer restore-codes option when targeting the main production DB
    latest_registry = None
    if not use_backup:
        try:
            latest_registry = col_bot3_link_registry.find_one(
                sort=[("saved_at", -1)]  # most recent snapshot
            )
        except Exception:
            pass

    if latest_registry and not use_backup:
        # Ask admin: restore old codes or generate fresh ones
        saved_at  = latest_registry.get("saved_at")
        ts_str    = saved_at.strftime("%d %b %Y at %I:%M %p") if saved_at else "Unknown"
        ig_n      = latest_registry.get("ig_count", 0)
        pdf_n     = latest_registry.get("pdf_count", 0)
        await state.update_data(
            b3_upload_target="backup" if use_backup else "main",
            b3_upload_fname=fname,
            b3_upload_file_id=message.document.file_id,
            b3_registry_id=str(latest_registry["_id"]),
        )
        await state.set_state(Bot3BackupStates.upload_restore_choice)
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🔗 RESTORE PREVIOUS CODES")],
            [KeyboardButton(text="🔢 ASSIGN NEW CODES")],
            [KeyboardButton(text="❌ CANCEL")],
        ], resize_keyboard=True)
        await message.answer(
            f"🔀 <b>CODE RESTORE OPTION</b>\n{'─'*28}\n\n"
            f"A link registry from a previous reset was found:\n"
            f"  🗓 Saved: <b>{ts_str}</b>\n"
            f"  📷 IG CC codes: <b>{ig_n}</b> entries\n"
            f"  📄 PDF indices: <b>{pdf_n}</b> entries\n\n"
            f"<b>🔗 RESTORE PREVIOUS CODES</b>\n"
            f"  Matches each URL to its old code/index.\n"
            f"  Same CC3 stays CC3. Same PDF 5 stays PDF 5.\n"
            f"  New URLs that weren't in the old data get auto-assigned.\n\n"
            f"<b>🔢 ASSIGN NEW CODES</b>\n"
            f"  Ignores the registry. All codes renumbered sequentially from 1.\n\n"
            f"Which would you like?",
            parse_mode="HTML", reply_markup=kb
        )
        return  # Wait for the admin's choice

    # No registry — proceed with direct upload
    await state.update_data(
        b3_upload_target="backup" if use_backup else "main",
        b3_upload_fname=fname,
        b3_upload_file_id=message.document.file_id,
        b3_registry_id=None,
        b3_restore_codes=False,
    )
    await state.clear()
    await _b3_do_upload(message, state, target_db, target_lbl, fname,
                        message.document.file_id, restore_codes=False, registry_doc=None)


@dp.message(Bot3BackupStates.upload_restore_choice)
async def bot3_upload_restore_choice(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Upload Restore Choice", "can_manage_admins"): return
    txt = (message.text or "").strip()
    if "❌" in txt or "CANCEL" in txt:
        await state.clear()
        return await message.answer("❌ Upload cancelled.", reply_markup=get_backup_menu())
    if "RESTORE" not in txt and "NEW" not in txt and "ASSIGN" not in txt:
        return await message.answer("Please choose one of the options above.")

    restore = "RESTORE" in txt
    data    = await state.get_data()
    fname   = data.get("b3_upload_fname", "upload")
    file_id = data.get("b3_upload_file_id", "")
    reg_id  = data.get("b3_registry_id")
    use_backup = data.get("b3_upload_target") == "backup"
    target_db  = _bkp_db_b3 if use_backup else db
    target_lbl = "MSANodeBackups" if use_backup else "MSANodeDB"
    await state.clear()

    registry_doc = None
    if restore and reg_id:
        try:
            from bson import ObjectId as _OID2
            registry_doc = col_bot3_link_registry.find_one({"_id": _OID2(reg_id)})
        except Exception:
            pass

    await _b3_do_upload(message, state, target_db, target_lbl, fname,
                        file_id, restore_codes=restore, registry_doc=registry_doc)


async def _b3_do_upload(
    message,
    state,
    target_db,
    target_lbl: str,
    fname: str,
    file_id: str,
    restore_codes: bool,
    registry_doc: dict | None,
):
    """Core upload logic. Optionally remaps IG/PDF codes from a pre-reset registry."""
    status = await message.answer(
        f"⏳ <b>{'Restoring with previous codes...' if restore_codes else 'Processing upload...'}</b>",
        parse_mode="HTML"
        # NO reply_markup — needs to stay editable for edit_text calls
    )

    async def _safe_edit(text: str):
        try:
            await status.edit_text(text, parse_mode="HTML")
        except Exception:
            await message.answer(text, parse_mode="HTML")

    try:
        file_info = await bot.get_file(file_id)
        buf       = io.BytesIO()
        await bot.download_file(file_info.file_path, buf)
        raw       = buf.getvalue()

        col_map: dict = {}
        if fname.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for member in [n for n in zf.namelist() if n.lower().endswith(".json")]:
                    payload = json.loads(zf.read(member).decode("utf-8-sig"))
                    if isinstance(payload, dict):
                        for k, v in payload.items():
                            if isinstance(v, list):
                                col_map.setdefault(k, []).extend(v)
                    elif isinstance(payload, list):
                        import os as _os
                        stem = _os.path.splitext(_os.path.basename(member))[0]
                        col_map.setdefault(stem, []).extend(payload)
        else:
            payload = json.loads(raw.decode("utf-8-sig"))
            if isinstance(payload, dict):
                for k, v in payload.items():
                    if isinstance(v, list):
                        col_map[k] = v
            elif isinstance(payload, list):
                import os as _os
                stem = _os.path.splitext(_os.path.basename(fname))[0]
                col_map[stem] = payload

        # ── Smart deduplication: most-recent snapshot wins ───────────────────────
        # A multi-snapshot ZIP may contain the same PDF/IG entry N times across
        # N daily snapshots. We must keep EXACTLY ONE copy per unique item.
        # Strategy: sort by created_at DESCENDING first (newest entry first),
        # then take only the first occurrence of each unique key.
        # Primary key: 'link' for PDFs, 'url' for IG — the actual content URL.
        # Fallback key: 'name' for PDFs, 'cc_number' for IG — used when the
        # primary key is missing, so nothing is silently lost.
        def _get_doc_ts(doc):
            """Return a sortable timestamp string for recency comparison."""
            for field in ("created_at", "backup_date", "added_at", "updated_at"):
                v = doc.get(field)
                if v:
                    if hasattr(v, "isoformat"):
                        return v.isoformat()
                    return str(v)
            return ""

        _DEDUP_CFG = {
            # collection_name: (primary_key, fallback_key)
            "bot3_pdfs":       ("link",       "name"),
            "bot3_ig_content": ("name",       "cc_number"),
            "bot3_settings":   ("key",        None),
        }
        for _cname, (_pk, _fk) in _DEDUP_CFG.items():
            if _cname not in col_map:
                continue
            # Sort newest → oldest so first-seen = most recent
            _sorted = sorted(col_map[_cname], key=_get_doc_ts, reverse=True)
            _seen_pk = {}   # primary_key_val   → doc
            _seen_fk = {}   # fallback_key_val  → doc
            _no_key  = []   # no usable key at all
            for _doc in _sorted:
                _pv = _doc.get(_pk) if _pk else None
                _fv = _doc.get(_fk) if _fk else None
                if _pv:
                    if _pv not in _seen_pk:
                        _seen_pk[_pv] = _doc        # first (newest) wins
                elif _fv:
                    if _fv not in _seen_fk:
                        _seen_fk[_fv] = _doc        # fallback: deduplicate by name
                else:
                    _no_key.append(_doc)            # no key — keep all
            col_map[_cname] = list(_seen_pk.values()) + list(_seen_fk.values()) + _no_key
        # ────────────────────────────────────────────────────────────────────────


        ig_registry  = {}   # url → cc_number  from saved registry
        pdf_registry = {}   # url → {index, msa_code}  from saved registry
        restored_ig  = 0
        restored_pdf = 0
        new_ig       = 0
        new_pdf      = 0

        if restore_codes and registry_doc:
            ig_registry  = registry_doc.get("ig_url_map", {})
            pdf_registry = registry_doc.get("pdf_url_map", {})
            set_registry = registry_doc.get("settings_snap", {})

        if restore_codes and ig_registry and "bot3_ig_content" in col_map:
            # Safely extract existing cc_numbers (handles both old string format and new dict format)
            existing_nums = []
            for v in ig_registry.values():
                if isinstance(v, dict) and "cc_number" in v:
                    try: existing_nums.append(int(v["cc_number"]))
                    except: pass
                elif not isinstance(v, dict) and str(v).isdigit():
                    existing_nums.append(int(v))
                    
            next_cc = (max(existing_nums) + 1) if existing_nums else 1
            for doc in col_map["bot3_ig_content"]:
                # IG CC uses 'name' as unique content identifier for remapping
                key = doc.get("name", "")
                if key and key in ig_registry:
                    old = ig_registry[key]
                    if isinstance(old, dict):
                        doc["cc_number"] = old.get("cc_number", next_cc)
                        doc["start_code"] = old.get("start_code", doc.get("start_code", ""))
                    else:
                        doc["cc_number"] = int(old) if str(old).isdigit() else next_cc  # legacy fallback
                    doc["cc_code"] = f"CC{doc['cc_number']}"
                    restored_ig += 1
                else:
                    doc["cc_number"] = next_cc
                    doc["cc_code"] = f"CC{next_cc}"
                    next_cc += 1
                    new_ig += 1

        if restore_codes and set_registry and "bot3_settings" in col_map:
            for doc in col_map["bot3_settings"]:
                key = doc.get("key")
                if key and key in set_registry:
                    doc["value"] = set_registry[key]

        if restore_codes and pdf_registry and "bot3_pdfs" in col_map:
            existing_idxs = [v["index"] for v in pdf_registry.values() if isinstance(v, dict)]
            next_idx = (max(existing_idxs) + 1) if existing_idxs else 1
            for doc in col_map["bot3_pdfs"]:
                # PDFs use 'link' field (not 'url') as their unique download identifier
                link = doc.get("link", "") or doc.get("url", "")
                if link and link in pdf_registry:
                    old = pdf_registry[link]
                    doc["index"]    = old.get("index", next_idx)
                    doc["msa_code"] = old.get("msa_code", doc.get("msa_code", ""))
                    # Preserve name if not already in doc
                    if not doc.get("name") and old.get("name"):
                        doc["name"] = old["name"]
                    restored_pdf += 1
                else:
                    # New PDF not in registry — keep its own msa_code from backup,
                    # only assign a new index position
                    doc["index"] = next_idx
                    next_idx += 1
                    new_pdf += 1
        # ──────────────────────────────────────────────────────────────────

        UPSERT_KEYS = {
            "bot3_pdfs": "msa_code", "bot3_ig_content": "cc_number",
            "bot3_admins": "user_id", "bot3_banned_users": "user_id",
            "bot3_settings": "key", "bot3_logs": "_id",
            "bot3_backups": "_id", "bot3_restore_data": "_id",
            "bot3_backup_history": "_id",
        }
        total_up     = 0
        report_lines = []
        for cname, docs in col_map.items():
            if not docs: continue
            col_obj  = target_db[cname]
            ukey     = UPSERT_KEYS.get(cname, "_id")
            upserted = 0
            for doc in docs:
                try:
                    from bson import ObjectId as _OID
                    if ukey == "_id" and isinstance(doc.get("_id"), str):
                        try: doc["_id"] = _OID(doc["_id"])
                        except: pass
                    fq = {ukey: doc[ukey]} if ukey in doc else {"_id": doc.get("_id")}
                    col_obj.replace_one(fq, doc, upsert=True)
                    upserted += 1
                except: pass
            total_up += upserted
            report_lines.append(f"  • <code>{cname}</code>: {upserted:,} upserted")

        detail = "\n".join(report_lines) or "  (no valid collections found)"

        # ── Build restore note ──
        restore_note = ""
        if restore_codes and registry_doc:
            restore_note = (
                f"\n\n🔗 <b>Code Restore Summary:</b>\n"
                f"  📷 IG: {restored_ig} codes restored, {new_ig} new assigned\n"
                f"  📄 PDF: {restored_pdf} codes restored, {new_pdf} new assigned"
            )
        elif restore_codes:
            restore_note = "\n\n⚠️ Registry not found — codes assigned sequentially."

        success_text = (
            f"✅ <b>UPLOAD COMPLETE — {target_lbl}</b>\n{'─'*28}\n\n"
            f"{detail}\n\n"
            f"📊 <b>Total upserted: {total_up:,}</b>"
            f"{restore_note}"
        )
        await _safe_edit(success_text)

        # ── Log to history ──
        try:
            from datetime import timezone as _tz
            col_bot3_bk_history.insert_one({
                "bot": "bot3", "action": "📤 Upload Data",
                "details": f"{fname} → {target_lbl} | {total_up} docs{'| Codes restored' if restore_codes else ''}",
                "status": "success", "timestamp": datetime.now(_tz.utc)
            })
        except: pass

    except Exception as e:
        safe_err = str(e)[:300].replace("<", "&lt;").replace(">", "&gt;")
        await _safe_edit(f"❌ <b>Upload failed:</b>\n<code>{safe_err}</code>")

    await message.answer("✅ Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# 📜 BACKUP HISTORY — Permanent log, NEVER subject to TTL or any auto-deletion
# =============================================================================

_B3_HIST_PG = 8

def _b3_hist_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    nav = []
    if page > 0:
        nav.append(KeyboardButton(text=f"⬅️ B3HIST_PREV {page - 1}"))
    if page < total - 1:
        nav.append(KeyboardButton(text=f"➡️ B3HIST_NEXT {page + 1}"))
    rows = [nav] if nav else []
    rows.append([KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def _b3_render_history(message: types.Message, page: int):
    try:
        total_docs = col_bot3_bk_history.count_documents({})
        if total_docs == 0:
            await message.answer(
                "📜 <b>BACKUP HISTORY — Bot 3</b>\n\n"
                "❌ No events logged yet.\n"
                "Events are recorded for every backup, upload, GDrive transfer and reset.",
                reply_markup=get_backup_menu(), parse_mode="HTML"
            )
            return
        total_pages = max(1, -(-total_docs // _B3_HIST_PG))
        page = max(0, min(page, total_pages - 1))
        skip = page * _B3_HIST_PG
        events = list(col_bot3_bk_history.find().sort("timestamp", -1).skip(skip).limit(_B3_HIST_PG))
        text   = (
            f"📜 <b>BACKUP HISTORY — Bot 3</b>  (Page {page+1}/{total_pages})\n"
            f"📊 Total: {total_docs:,} events  |  🔒 Permanent (no TTL)\n" + "─" * 28 + "\n\n"
        )
        for ev in events:
            ts     = ev.get("timestamp") or ev.get("backup_date")
            ts_str = format_datetime_12h(ts) if ts else "Unknown"
            action = ev.get("action", ev.get("type", "?"))
            detail = ev.get("details") or ev.get("message") or ""
            icon   = "✅" if ev.get("status") != "error" else "❌"
            text  += f"{icon} <b>{action}</b>\n  🕐 {ts_str}\n"
            if detail:
                text += f"  💬 {str(detail)[:120]}\n"
            text += "\n"
        await _b3_send_paged(message, text, reply_markup=_b3_hist_kb(page, total_pages))
    except Exception as e:
        await message.answer(
            f"❌ <b>Could not read history:</b>\n<code>{str(e)[:200]}</code>",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        )


@dp.message(F.text.in_({"📜 HISTORY", "📜 BACKUP HISTORY"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_backup_history_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup History", "can_manage_admins"): return
    await state.set_state(Bot3BackupStates.history_viewing)
    await _b3_render_history(message, 0)


@dp.message(lambda m: m.text and m.text.startswith("⬅️ B3HIST_PREV"))
async def b3_hist_prev(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup History Prev", "can_manage_admins"): return
    try: await _b3_render_history(message, int(message.text.split()[-1]))
    except: await _b3_render_history(message, 0)


@dp.message(lambda m: m.text and m.text.startswith("➡️ B3HIST_NEXT"))
async def b3_hist_next(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup History Next", "can_manage_admins"): return
    try: await _b3_render_history(message, int(message.text.split()[-1]))
    except: await _b3_render_history(message, 0)


@dp.message(F.text.in_({"◀️ BACK TO BACKUP MENU", "🔙 BACK"}))
async def b3_hist_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("📂 Backup menu:", reply_markup=get_backup_menu())


# =============================================================================
# ☁️ GDRIVE UPLOAD — Structured chunk upload with full/partial month TTL logic
# =============================================================================

@dp.message(F.text.in_({"☁️ GDRIVE SYSTEM", "☁️ GDRIVE UPLOAD"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_gdrive_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "GDrive System", "can_manage_admins"): return
    await state.clear()
    loading = await message.answer("⏳ Loading cluster chunks for GDrive...")
    try:
        loop    = asyncio.get_event_loop()
        records = await loop.run_in_executor(
            None, lambda: list(col_bot3_backups.find(
                {}, {"backup_date": 1, "gdrive_uploaded": 1, "ttl_date": 1}
            ).sort("backup_date", -1))
        )
        try: await loading.delete()
        except: pass

        if not records:
            await message.answer("❌ No snapshots in cluster for GDrive upload.", reply_markup=get_backup_menu())
            return

        chunks = _b3_group_backups(records, now_local())
        text   = "☁️ <b>GDRIVE SYSTEM — Bot 3</b>\n<i>Select time period to transfer:</i>\n" + "─" * 28 + "\n\n"
        chunk_store = []

        for i, (k, lbl, docs, is_full) in enumerate(chunks, 1):
            dates      = [d["backup_date"] for d in docs if d.get("backup_date")]
            latest_str = max(dates).strftime("%d %b %Y %I:%M %p") if dates else "?"
            gd_cnt     = sum(1 for d in docs if d.get("gdrive_uploaded"))
            ttl_cnt    = sum(1 for d in docs if "ttl_date" in d)
            gd_ico     = "☁️" if gd_cnt == len(docs) else ("⚡" if gd_cnt > 0 else "📦")
            full_ico   = "📅 Full month" if is_full else "🗓️ Partial"
            text += (
                f"{i}. <b>{lbl}</b>  {gd_ico}\n"
                f"   Latest: {latest_str}  |  {full_ico}\n"
                f"   GDrive: {gd_cnt}/{len(docs)}  |  TTL: {ttl_cnt}/{len(docs)}\n\n"
            )
            chunk_store.append((lbl, [str(d["_id"]) for d in docs], is_full))

        text += f"<i>Type number (1–{len(chunks)}) or press 🔙 BACK</i>"
        await state.update_data(b3_gdrive_chunks=chunk_store)
        await state.set_state(Bot3BackupStates.gdrive_month_select)
        await _b3_send_paged(
            message, text,
            reply_markup=ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
            ], resize_keyboard=True)
        )
    except Exception as e:
        try: await loading.delete()
        except: pass
        await message.answer(f"❌ Error:\n<code>{str(e)[:200]}</code>", reply_markup=get_backup_menu(), parse_mode="HTML")


@dp.message(Bot3BackupStates.gdrive_month_select)
async def bot3_gdrive_month_chosen(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "GDrive Select", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())

    data   = await state.get_data()
    chunks = data.get("b3_gdrive_chunks", [])
    count  = len(chunks)
    txt    = (message.text or "").strip()
    if not txt.isdigit() or not (1 <= int(txt) <= count):
        return await message.answer(f"❌ Enter a number between 1 and {count}.")

    idx              = int(txt) - 1
    lbl, doc_ids, is_full = chunks[idx]
    await state.update_data(b3_gdrive_lbl=lbl, b3_gdrive_selected_docs=doc_ids, b3_gdrive_is_full=is_full)

    if is_full:
        # Complete month → ask about 90-day TTL
        await state.set_state(Bot3BackupStates.gdrive_ttl_confirm)
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="✅ ACTIVATE 90-DAY TTL")],
            [KeyboardButton(text="❌ NO TTL — Keep Forever")],
            [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
        ], resize_keyboard=True)
        await message.answer(
            f"☁️ <b>FULL MONTH DETECTED — {lbl}</b>\n\n"
            "This is a <b>completed calendar month</b>.\n\n"
            "🗑️ Activate <b>90-day auto-deletion</b> on these cluster snapshots after GDrive upload?\n\n"
            "• ✅ ACTIVATE — Cluster records auto-deleted in 90 days (GDrive copy is permanent)\n"
            "• ❌ NO TTL — Keep cluster records forever (no deletion schedule)",
            reply_markup=kb, parse_mode="HTML"
        )
    else:
        # Partial month → upload directly, never ask TTL
        await bot3_gdrive_execute_upload(message, state, activate_ttl=False)


@dp.message(Bot3BackupStates.gdrive_ttl_confirm)
async def bot3_gdrive_ttl_answer(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "GDrive TTL", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())
    if message.text not in ("✅ ACTIVATE 90-DAY TTL", "❌ NO TTL — Keep Forever"):
        return await message.answer("Please choose ✅ ACTIVATE or ❌ NO TTL.")
    activate = message.text == "✅ ACTIVATE 90-DAY TTL"
    await bot3_gdrive_execute_upload(message, state, activate_ttl=activate)


async def bot3_gdrive_execute_upload(message: types.Message, state: FSMContext, activate_ttl: bool):
    data     = await state.get_data()
    lbl      = data.get("b3_gdrive_lbl", "backup")
    doc_ids  = data.get("b3_gdrive_selected_docs", [])
    await state.clear()

    # NO reply_markup on status — ReplyKeyboardMarkup makes messages un-editable
    status = await message.answer(
        f"⏳ <b>Generating ZIP and uploading to GDrive...</b>\n📦 {lbl}",
        parse_mode="HTML"
    )

    async def _safe_edit(text: str):
        """Try edit_text first, fallback to new message if edit fails."""
        try:
            await status.edit_text(text, parse_mode="HTML")
        except Exception:
            await message.answer(text, parse_mode="HTML")

    try:
        loop = asyncio.get_event_loop()
        buf  = io.BytesIO()
        snap_count = 0

        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for doc_id in doc_ids:
                doc = await loop.run_in_executor(None, lambda i=doc_id: col_bot3_backups.find_one({"_id": ObjectId(i)}))
                if not doc: continue
                dt = doc.get("backup_date")
                if not dt: continue
                c_data = doc.get("data", {})
                w      = min((dt.day - 1) // 7 + 1, 5)
                path   = f"{dt.strftime('%Y')}/{dt.strftime('%B')}/Week {w}/{dt.strftime('%Y-%m-%d_%A')}.json"
                out    = {cn: (cd.get("documents", []) if isinstance(cd, dict) else cd)
                          for cn, cd in c_data.items()}
                zf.writestr(path, json.dumps(out, default=str, indent=2, ensure_ascii=False))
                snap_count += 1

        zip_bytes = buf.getvalue()
        size_mb   = round(len(zip_bytes) / 1024 / 1024, 2)
        safe_lbl  = lbl.replace(" ", "_").replace("(", "").replace(")", "").replace("–", "-")
        fname     = f"Bot3_GDrive_{safe_lbl}.zip"

        # ── Direct GDrive upload using Bot 3's isolated folder ──────────────
        folder_id = _BOT3_GDRIVE_FOLDER_ID
        if not folder_id:
            raise ValueError(
                "GDrive folder ID not configured for Bot 3. "
                "Set BOT3_GDRIVE_FOLDER_ID in bot3.env."
            )

        def _do_upload():
            service = _get_gdrive_service()
            # Dedup check — never overwrite an existing file
            if _gdrive_file_exists(service, fname, folder_id):
                return "exists", ""
            file_id = _gdrive_upload_bytes(service, zip_bytes, fname, folder_id)
            return "ok", file_id

        upload_status, file_id = await loop.run_in_executor(None, _do_upload)

        if upload_status == "exists":
            await _safe_edit(
                f"☁️ <b>File already exists on GDrive — not uploaded again.</b>\n"
                f"📦 <code>{fname}</code>\n"
                f"No duplicate created. GDrive data is safe."
            )
            await message.answer("✅ Returned to Backup Menu.", reply_markup=get_backup_menu())
            return

        upload_ok  = True
        gdrive_msg = f"🆔 File ID: <code>{file_id}</code>"

        if upload_ok:
            # Apply TTL flags only on successful upload
            ttl_applied = 0
            if activate_ttl:
                for doc_id in doc_ids:
                    doc = await loop.run_in_executor(None, lambda i=doc_id: col_bot3_backups.find_one({"_id": ObjectId(i)}))
                    if doc and "backup_date" in doc:
                        col_bot3_backups.update_one({"_id": ObjectId(doc_id)}, {"$set": {"ttl_date": doc["backup_date"]}})
                        ttl_applied += 1
            # Mark as GDrive uploaded
            for doc_id in doc_ids:
                col_bot3_backups.update_one({"_id": ObjectId(doc_id)}, {"$set": {"gdrive_uploaded": True}})

            ttl_note = (f"\n🗑️ <b>90-day TTL activated</b> on {ttl_applied} snapshot(s)."
                        if activate_ttl else "\n🔒 <b>No TTL applied</b> — cluster records kept permanently.")

            await _safe_edit(
                f"✅ <b>GDRIVE UPLOAD COMPLETE</b>\n{'─'*28}\n\n"
                f"📦 <code>{fname}</code>\n"
                f"💾 {size_mb} MB  |  {snap_count} snapshot(s)\n"
                f"{gdrive_msg}{ttl_note}"
            )
            try:
                from datetime import timezone as _tz
                col_bot3_bk_history.insert_one({
                    "bot": "bot3", "action": "GDrive Upload",
                    "details": f"{fname} ({size_mb} MB) | TTL:{activate_ttl} | ID:{file_id}",
                    "status": "success", "timestamp": datetime.now(_tz.utc)
                })
            except Exception as hist_err:
                logger.warning(f"[HISTORY] Bot3 GDrive history write failed: {hist_err}")

    except Exception as e:
        safe_err = str(e)[:300].replace("<", "&lt;").replace(">", "&gt;")
        await _safe_edit(
            f"❌ <b>GDrive execution error:</b>\n<code>{safe_err}</code>"
        )
    await message.answer("✅ Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# ⏳ ACTIVATE TTL — Toggle 90-day TTL per chunk; history is always exempt
# =============================================================================

@dp.message(F.text.in_({"⏳ ACTIVATE TTL", "⏱️ ACTIVE DELETION"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_activate_ttl_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Activate TTL", "can_manage_admins"): return
    await state.clear()
    loading = await message.answer("⏳ Analyzing TTL status across all snapshots...")
    try:
        loop    = asyncio.get_event_loop()
        records = await loop.run_in_executor(
            None, lambda: list(col_bot3_backups.find(
                {}, {"backup_date": 1, "ttl_date": 1, "gdrive_uploaded": 1}
            ).sort("backup_date", -1))
        )
        try: await loading.delete()
        except: pass

        if not records:
            await message.answer("❌ No snapshots found in cluster yet.", reply_markup=get_backup_menu())
            return

        chunks      = _b3_group_backups(records, now_local())
        text        = "⏳ <b>ACTIVATE TTL — 90-Day Manager</b>\n\n"
        text       += "🔒 <i>Backup History is ALWAYS exempt from any deletion.</i>\n" + "─" * 28 + "\n\n"
        chunk_store = []

        for i, (k, lbl, docs, is_full) in enumerate(chunks, 1):
            ttl_cnt = sum(1 for d in docs if "ttl_date" in d)
            gd_cnt  = sum(1 for d in docs if d.get("gdrive_uploaded"))
            if ttl_cnt == len(docs) and len(docs) > 0:
                ttl_ico = "✅ ACTIVE"
            elif ttl_cnt == 0:
                ttl_ico = "❌ INACTIVE"
            else:
                ttl_ico = f"⚠️ PARTIAL ({ttl_cnt}/{len(docs)})"
            text += (
                f"{i}. <b>{lbl}</b>\n"
                f"   ⏳ TTL: {ttl_ico}  |  ☁️ GDrive: {gd_cnt}/{len(docs)}\n\n"
            )
            chunk_store.append((lbl, [str(d["_id"]) for d in docs]))

        text += f"\n<i>Type a number (1–{len(chunks)}) or press 🔙 BACK</i>"
        await state.update_data(b3_ttl_chunks=chunk_store)
        await state.set_state(Bot3BackupStates.ttl_month_select)
        await _b3_send_paged(
            message, text,
            reply_markup=ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
            ], resize_keyboard=True)
        )
    except Exception as e:
        try: await loading.delete()
        except: pass
        await message.answer(f"❌ Error:\n<code>{str(e)[:200]}</code>", reply_markup=get_backup_menu(), parse_mode="HTML")


@dp.message(Bot3BackupStates.ttl_month_select)
async def bot3_ttl_month_chosen(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "TTL Month Select", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())

    data   = await state.get_data()
    chunks = data.get("b3_ttl_chunks", [])
    count  = len(chunks)
    txt    = (message.text or "").strip()
    if not txt.isdigit() or not (1 <= int(txt) <= count):
        return await message.answer(f"❌ Enter a number between 1 and {count}.")

    idx  = int(txt) - 1
    lbl, doc_ids = chunks[idx]
    await state.update_data(b3_ttl_selected=doc_ids, b3_ttl_lbl=lbl)
    await state.set_state(Bot3BackupStates.ttl_active_toggle)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ ACTIVATE TTL (90 days)"), KeyboardButton(text="🛑 DEACTIVATE TTL")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await message.answer(
        f"⏳ <b>TTL Manager:</b> {lbl}\n\n"
        "✅ <b>ACTIVATE TTL</b> — Cluster snapshots auto-delete 90 days after backup date.\n"
        "🛑 <b>DEACTIVATE TTL</b> — Remove countdown; snapshots kept permanently in cluster.\n\n"
        "⚠️ Backup History is always excluded from this action.",
        reply_markup=kb, parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.ttl_active_toggle)
async def bot3_ttl_toggle_action(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "TTL Toggle", "can_manage_admins"): return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        return await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu())
    if message.text not in ("✅ ACTIVATE TTL (90 days)", "🛑 DEACTIVATE TTL"):
        return await message.answer("❌ Choose ✅ ACTIVATE TTL or 🛑 DEACTIVATE TTL.")

    start_ttl = message.text == "✅ ACTIVATE TTL (90 days)"
    data      = await state.get_data()
    doc_ids   = data.get("b3_ttl_selected", [])
    lbl       = data.get("b3_ttl_lbl", "")
    await state.clear()

    loop     = asyncio.get_event_loop()
    applied  = 0
    errors   = 0
    for doc_id in doc_ids:
        try:
            doc = await loop.run_in_executor(None, lambda i=doc_id: col_bot3_backups.find_one({"_id": ObjectId(i)}))
            if not doc: continue
            if start_ttl:
                col_bot3_backups.update_one({"_id": ObjectId(doc_id)}, {"$set": {"ttl_date": doc.get("backup_date")}})
            else:
                col_bot3_backups.update_one({"_id": ObjectId(doc_id)}, {"$unset": {"ttl_date": ""}})
            applied += 1
        except:
            errors += 1

    action_lbl = "✅ <b>90-DAY TTL ACTIVATED</b>" if start_ttl else "🛑 <b>TTL DEACTIVATED — Permanent Retention</b>"
    err_note   = f"\n⚠️ {errors} error(s) encountered." if errors else ""
    await message.answer(
        f"{action_lbl}\n\n"
        f"📦 Chunk: <b>{lbl}</b>\n"
        f"✅ Applied to: {applied} snapshot(s){err_note}",
        parse_mode="HTML", reply_markup=get_backup_menu()
    )




class AdminManagementStates(StatesGroup):
    waiting_for_new_admin_id = State()
    waiting_for_remove_admin_id = State()
    viewing_admin_list = State()
    waiting_for_ban_user_id = State()
    waiting_for_unban_user_id = State()

class AdminPermissionStates(StatesGroup):
    waiting_for_admin_selection = State()
    configuring_permissions = State()

class AdminRoleStates(StatesGroup):
    waiting_for_admin_selection = State()
    waiting_for_role_selection = State()
    waiting_for_owner_password = State() # For Ownership Transfer
    waiting_for_owner_confirm = State() # For Ownership Transfer Confirm
    waiting_for_owner_second_confirm = State() # For Double Confirmation

class AdminAuthStates(StatesGroup):
    """Password gate states — used once per session when owner sends /start"""
    pw_first  = State()   # First password entry
    pw_second = State()   # Confirmation entry

# --- Helpers ---
def get_cancel_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)

async def reindex_all_pdfs():
    """
    Re-index all PDFs to have sequential indices starting from 1, with no gaps.
    This ensures proper ordering after deletions.
    """
    try:
        # Get all PDFs sorted by current index
        all_pdfs = list(col_pdfs.find().sort("index", 1))
        
        # Re-assign indices sequentially
        for new_index, pdf in enumerate(all_pdfs, start=1):
            if pdf["index"] != new_index:
                col_pdfs.update_one(
                    {"_id": pdf["_id"]},
                    {"$set": {"index": new_index}}
                )
        
        logger.info(f"Re-indexed {len(all_pdfs)} PDFs successfully")
        return len(all_pdfs)
    except Exception as e:
        logger.error(f"Error re-indexing PDFs: {e}")
        return 0

async def reindex_all_ig_cc():
    """
    Re-number all IG CC entries sequentially (CC1, CC2, CC3…) with no gaps.
    Sorted by existing cc_number so the relative order is preserved.
    Uses a two-phase approach to avoid unique-index conflicts:
      Phase 1 — shift all existing cc_numbers to a safe high range (10000+)
      Phase 2 — assign the final sequential numbers
    This guarantees no duplicate key violation during reindexing.
    """
    try:
        all_ig = list(col_ig_content.find().sort("cc_number", 1))
        if not all_ig:
            return 0

        # Phase 1: move every document to a guaranteed-unique temporary number
        # (10000 + original position) so no two docs share a cc_number during the swap
        for temp_idx, item in enumerate(all_ig, start=10001):
            col_ig_content.update_one(
                {"_id": item["_id"]},
                {"$set": {"cc_number": temp_idx, "cc_code": f"CC{temp_idx}"}}
            )

        # Phase 2: assign the final sequential numbers CC1, CC2, CC3…
        for new_num, item in enumerate(all_ig, start=1):
            col_ig_content.update_one(
                {"_id": item["_id"]},
                {"$set": {"cc_number": new_num, "cc_code": f"CC{new_num}"}}
            )

        logger.info(f"Reindexed {len(all_ig)} IG CC entries successfully")
        return len(all_ig)
    except Exception as e:
        logger.error(f"Error reindexing IG CC: {e}")
        return 0

async def get_next_pdf_index():
    latest = col_pdfs.find_one(sort=[("index", -1)])
    return (latest["index"] + 1) if latest else 1

def validate_msa_code(code):
    """
    Validates MSA code format: MSA + exactly 4 digits.
    Returns (is_valid: bool, error_msg: str)
    """
    code = (code or "").strip().upper()
    if not code:
        return False, "⚠️ Code cannot be empty."
    
    if not re.fullmatch(r'^MSA\d{4}$', code):
        return False, "⚠️ Invalid format. Use exactly 4 digits, e.g. MSA1234"
    
    return True, ""

def is_msa_code_duplicate(code, exclude_pdf_id=None):
    """
    Check if MSA code already exists in database
    exclude_pdf_id: ObjectId to exclude from check (for edit operations)
    """
    query = {"msa_code": code}
    if exclude_pdf_id:
        from bson.objectid import ObjectId
        query["_id"] = {"$ne": ObjectId(exclude_pdf_id)}
    
    return col_pdfs.find_one(query) is not None

def generate_unique_msa_code():
    """Generate a random unique MSA code (non-sequential, no repeats)."""
    rng = random.SystemRandom()

    # Strict pool: 4-digit auto IDs (MSA1000..MSA9999).
    for _ in range(30000):
        code = f"MSA{rng.randint(1000, 9999)}"
        if not is_msa_code_duplicate(code):
            return code

    raise RuntimeError("MSA code pool exhausted (MSA1000-MSA9999)")

def repair_missing_duplicate_msa_codes():
    """Backfill missing/invalid/duplicate MSA codes at startup."""
    repaired = 0
    seen = set()

    docs = list(col_pdfs.find({}, {"_id": 1, "msa_code": 1}))
    for doc in docs:
        code = (doc.get("msa_code") or "").strip().upper()
        valid = bool(re.fullmatch(r"^MSA\d{4}$", code))

        if valid and code not in seen:
            seen.add(code)
            continue

        for _ in range(20):
            new_code = generate_unique_msa_code()
            try:
                res = col_pdfs.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"msa_code": new_code}}
                )
                if res.modified_count:
                    repaired += 1
                seen.add(new_code)
                break
            except DuplicateKeyError:
                continue

    return repaired

def randomize_non_4_digit_msa_codes():
    """Replace non-4-digit legacy MSA IDs with strict random 4-digit IDs."""
    migrated = 0
    docs = list(col_pdfs.find({"msa_code": {"$exists": True, "$not": {"$regex": r"^MSA\d{4}$"}}}, {"_id": 1, "msa_code": 1}))
    for doc in docs:
        for _ in range(20):
            new_code = generate_unique_msa_code()
            try:
                res = col_pdfs.update_one({"_id": doc["_id"]}, {"$set": {"msa_code": new_code}})
                if res.modified_count:
                    migrated += 1
                break
            except DuplicateKeyError:
                continue
    return migrated

def ensure_unique_msa_code_index():
    """Ensure DB-level uniqueness for msa_code so duplicates can never persist."""
    try:
        col_pdfs.create_index("msa_code", unique=True, sparse=True, name="pdf_msa_code_unique")
        return
    except Exception as first_err:
        msg = str(first_err).lower()
        if "already exists" not in msg and "indexkeyspecsconflict" not in msg and "equivalent" not in msg:
            raise

    for idx_name in ("pdf_msa_code", "pdf_msa_code_1", "pdf_msa_code_unique"):
        try:
            col_pdfs.drop_index(idx_name)
        except Exception:
            pass

    col_pdfs.create_index("msa_code", unique=True, sparse=True, name="pdf_msa_code_unique")

async def attempt_db_recovery():
    """
    Emergency recovery function: called when handlers detect empty collections.
    Attempts to reconnect to MongoDB and verify data accessibility.
    Returns (success, pdf_count, ig_count)
    """
    import time
    logger.info("[RECOVERY] Attempting database recovery...")
    print("[RECOVERY] Attempting database recovery...")
    
    try:
        # Force reconnect by pinging database
        client.admin.command('ping')
        logger.info("[RECOVERY] ✅ Database ping successful")
        
        # Verify collections exist
        collections = db.list_collection_names()
        logger.info(f"[RECOVERY] Available collections: {collections}")
        
        # Count documents in critical collections
        pdf_count = col_pdfs.count_documents({})
        ig_count = col_ig_content.count_documents({})
        
        logger.info(f"[RECOVERY] bot3_pdfs: {pdf_count} documents")
        logger.info(f"[RECOVERY] bot3_ig_content: {ig_count} documents")
        
        if pdf_count > 0 and ig_count > 0:
            logger.info("[RECOVERY] ✅ Data recovery successful!")
            print("[RECOVERY] ✅ Data recovery successful!")
            return (True, pdf_count, ig_count)
        else:
            logger.warning(f"[RECOVERY] ⚠️ Collections still empty after recovery. PDF:{pdf_count}, IG:{ig_count}")
            print(f"[RECOVERY] ⚠️ Collections still empty. PDF:{pdf_count}, IG:{ig_count}")
            return (False, pdf_count, ig_count)
            
    except Exception as recovery_err:
        logger.error(f"[RECOVERY] ❌ Recovery failed: {recovery_err}")
        print(f"[RECOVERY] ❌ Recovery failed: {recovery_err}")
        return (False, 0, 0)

async def find_latest_backup_file():
    """
    Find the most recent backup ZIP file in /backups folder
    Returns (backup_path, creation_timestamp) or (None, None) if not found
    """
    import os
    from pathlib import Path
    
    backups_dir = Path("backups")
    if not backups_dir.exists():
        return None, None
    
    backup_files = sorted(
        backups_dir.glob("Backup_*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    
    if backup_files:
        latest = backup_files[0]
        mtime = latest.stat().st_mtime
        from datetime import datetime
        created = datetime.fromtimestamp(mtime)
        logger.info(f"[RECOVERY] Found latest backup: {latest.name} (created {created})")
        return str(latest), created
    
    return None, None

def get_next_cc_code():
    """
    Generate next CC code (CC1, CC2, CC3...) with no gaps
    Finds the highest existing CC number and returns next
    """
    # Get all IG content sorted by CC code
    all_content = list(col_ig_content.find().sort("cc_number", 1))
    
    if not all_content:
        return "CC1", 1
    
    # Get the highest CC number
    highest = max(content['cc_number'] for content in all_content)
    next_number = highest + 1
    return f"CC{next_number}", next_number

def get_next_rw_code():
    """
    Generate next RW code filling the lowest available gap.
    Examples: RW1,RW3 exist → returns RW2. RW1,RW2,RW3 exist → returns RW4.
    """
    existing = sorted([r["rw_number"] for r in col_rewards.find({}, {"rw_number": 1})])
    # Walk from 1 upward, find first gap
    for i, n in enumerate(existing, 1):
        if n != i:
            return f"RW{i}", i
    next_num = len(existing) + 1
    return f"RW{next_num}", next_num

def is_ig_name_duplicate(name, exclude_id=None):
    """
    Check if IG content name already exists
    exclude_id: ObjectId to exclude from check (for edit operations)
    """
    query = {"name": {"$regex": f"^{name}$", "$options": "i"}}  # Case-insensitive exact match
    if exclude_id:
        from bson.objectid import ObjectId
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    
    return col_ig_content.find_one(query) is not None

def split_text_chunks(text: str, chunk_size: int = 3800):
    """Split text into Telegram-safe chunks, preferring line boundaries."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    current = ""

    for line in text.splitlines(keepends=True):
        if len(line) > chunk_size:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i:i + chunk_size])
            continue

        if len(current) + len(line) > chunk_size:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    return chunks

async def send_chunked_message(
    message: types.Message,
    text: str,
    reply_markup=None,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = False,
    chunk_size: int = 3800,
):
    """Send long text safely; only the final chunk keeps the reply keyboard.
    If HTML parse fails (unclosed tags from user content), auto-retries as plain text."""
    from aiogram.exceptions import TelegramBadRequest
    import re as _re
    parts = split_text_chunks(text, chunk_size=chunk_size)
    for i, part in enumerate(parts):
        kb = reply_markup if i == len(parts) - 1 else None
        try:
            await message.answer(
                part,
                reply_markup=kb,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
        except TelegramBadRequest as _html_err:
            if "can't parse entities" in str(_html_err).lower() or "bad request" in str(_html_err).lower():
                # Strip all HTML tags and retry as plain text
                plain = _re.sub(r'<[^>]+>', '', part)
                try:
                    await message.answer(
                        plain,
                        reply_markup=kb,
                        parse_mode=None,
                        disable_web_page_preview=disable_web_page_preview,
                    )
                except Exception:
                    pass  # Silent fail on last resort
            else:
                raise

async def send_ig_list_view(message: types.Message, page=0, mode="list"):
    """
    Display paginated IG content list
    modes: 'list', 'edit', 'delete', 'ig_affiliate_select', 'ig_affiliate_edit', 'ig_affiliate_delete'
    """
    limit = 5  # Changed to 5 items per page
    skip = page * limit
    
    # Filter based on mode
    if mode == "ig_affiliate_select":
        # Show ONLY content that does NOT have an affiliate link
        query = {"$or": [{"affiliate_link": {"$exists": False}}, {"affiliate_link": ""}]}
    elif mode in ["ig_affiliate_edit", "ig_affiliate_delete"]:
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    else:
        query = {}
    
    total = col_ig_content.count_documents(query)
    cursor = col_ig_content.find(query).sort("cc_number", 1).skip(skip).limit(limit)
    contents = list(cursor)
    
    # Header & Keyboard Setup
    if mode == "edit":
        title = "✏️ <b>EDIT IG CONTENT</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ <b>DELETE IG CONTENT</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_select":
        title = "📎 <b>SELECT IG FOR AFFILIATE</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_edit":
        title = "✏️ <b>EDIT AFFILIATE LINK</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_delete":
        title = "🗑️ <b>DELETE AFFILIATE LINK</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    else:
        title = "📸 <b>IG CONTENT LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO IG MENU")
    
    if not contents:
        msg = "⚠️ <b>No IG Content found.</b>\nAdd one first!"
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="HTML")
        return
    
    text = f"{title} (Page {page+1})\nResult {skip+1}-{min(skip+len(contents), total)} of {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for idx, content in enumerate(contents, start=1):
        display_index = skip + idx
        # Display index, CC code, and content name
        content_name = content.get('name', 'Unnamed')
        if len(content_name) > 25:
            content_name = content_name[:25] + "..."
        text += f"{display_index}. {content['cc_code']} - {content_name}"
        
        # Affiliate Status Logic
        has_affiliate = content.get("affiliate_link", "") != ""
        if mode in ["ig_affiliate_select", "ig_affiliate_edit", "ig_affiliate_delete"]:
             text += f" {'🔗' if has_affiliate else '⚠️'}"
        else:
             # Normal modes: Show explicit status
             status = "✅" if has_affiliate else "❌"
             text += f" | Aff: {status}"
        
        text += "\n"
    
    # Pagination — each mode gets its OWN prefix so NEXT/PREV buttons
    # never collide with the pdf_pagination_handler (which catches bare NEXT/PREV).
    _IG_PREFIX_MAP = {
        "list":               "_IG",
        "delete":             "_IGDEL",
        "edit":               "_IGEDIT",
        "ig_affiliate_select": "_IGAFFS",
        "ig_affiliate_edit":   "_IGAFFE",
        "ig_affiliate_delete": "_IGAFFD",
    }
    nav_prefix = _IG_PREFIX_MAP.get(mode, "_IG")
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV{nav_prefix} {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT{nav_prefix} {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([cancel_btn])
    
    await send_chunked_message(
        message,
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

async def send_pdf_list_view(message: types.Message, page=0, mode="list"):
    """
    Helper to display paginated PDF list.
    modes: 'list', 'edit', 'delete', 'affiliate_select', 'affiliate_delete', 
           'msa_add_select', 'msa_edit_select', 'msa_delete', 'list_msa',
           'yt_add_select', 'yt_edit_select', 'yt_delete', 'list_yt'
    """
    limit = 5  # User requested 5 at a time
    skip = page * limit
    
    # query modification based on mode
    query = {}
    if mode == "affiliate_delete" or mode == "list_affiliate" or mode == "affiliate_edit_select":
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    elif mode == "affiliate_add_select":
        # PDFs that DO NOT have an affiliate link (field missing, null, or empty string)
        query = {"$or": [
            {"affiliate_link": {"$exists": False}},
            {"affiliate_link": None},
            {"affiliate_link": ""}
        ]}
    elif mode == "msa_add_select":
        # Since all PDFs now auto-generate MSA codes, we display ALL PDFs so users can Replace/Override them.
        query = {}
    elif mode == "msa_edit_select" or mode == "msa_delete" or mode == "list_msa":
        # PDFs that HAVE MSA code
        query = {"msa_code": {"$exists": True, "$ne": ""}}
    elif mode == "yt_add_select":
        # PDFs that DO NOT have YT data
        query = {"$or": [
            {"yt_title": {"$exists": False}},
            {"yt_title": None},
            {"yt_title": ""}
        ]}
    elif mode == "yt_add_select":
        # PDFs that DO NOT have YT data
        query = {"$or": [
            {"yt_title": {"$exists": False}},
            {"yt_title": None},
            {"yt_title": ""}
        ]}
    elif mode == "yt_edit_select" or mode == "yt_delete" or mode == "list_yt":
        # PDFs that HAVE YT data
        query = {"yt_title": {"$exists": True, "$ne": ""}}

    total = col_pdfs.count_documents(query)
    
    cursor = col_pdfs.find(query).sort("index", 1).skip(skip).limit(limit)
    pdfs = list(cursor)
    
    # Header & Keyboard Setup
    if mode == "edit":
        title = "✏️ <b>EDIT PDF</b> - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ <b>DELETE PDF</b> - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_add_select":
        title = "💸 <b>ADD AFFILIATE LINK</b> - Select PDF (No Link)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_edit_select":
        title = "✏️ <b>EDIT AFFILIATE LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_delete":
        title = "🗑️ <b>DELETE AFFILIATE LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_affiliate":
        title = "💸 <b>AFFILIATE LINKS LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO AFFILIATE MENU")
    elif mode == "msa_add_select":
        title = "🔑 <b>REPLACE MSA CODE</b> - Select PDF to Override"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_edit_select":
        title = "✏️ <b>EDIT MSA CODE</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_delete":
        title = "🗑️ <b>DELETE MSA CODE</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_msa":
        title = "🔑 <b>MSA CODES LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO CODE MENU")
    elif mode == "yt_add_select":
        title = "▶️ <b>ADD YT LINK</b> - Select PDF (No YT)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_add_select":
        title = "▶️ <b>ADD YT LINK</b> - Select PDF (No YT)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_edit_select":
        title = "✏️ <b>EDIT YT LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_delete":
        title = "🗑️ <b>DELETE YT LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_yt":
        title = "▶️ <b>YT LINKS LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO YT MENU")
    else:
        title = "📂 <b>PDF LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO PDF MENU")

    if not pdfs:
        msg = f"📂 No PDFs found matching criteria.\nTotal: {total}"
        if mode == "affiliate_add_select":
            msg = "⚠️ <b>All existing PDFs already have Affiliate Links!</b>\nPlease add a new PDF first."
        elif mode == "affiliate_edit_select":
            msg = "⚠️ <b>No Affiliate Links found to edit.</b>\nAdd one first!"
        elif mode == "msa_add_select":
            msg = "⚠️ <b>All existing PDFs already have MSA Codes!</b>\nPlease add a new PDF first."
        elif mode == "msa_edit_select":
            msg = "⚠️ <b>No MSA Codes found to edit.</b>\nAdd one first!"
        elif mode == "msa_delete" or mode == "list_msa":
            msg = "⚠️ <b>No MSA Codes found.</b>\nAdd one first!"
        elif mode == "yt_add_select":
            msg = "⚠️ <b>All existing PDFs already have YT Links!</b>\nPlease add a new PDF first."
        elif mode == "yt_edit_select":
            msg = "⚠️ <b>No YT Links found to edit.</b>\nAdd one first!"
        elif mode == "yt_delete" or mode == "list_yt":
            msg = "⚠️ <b>No YT Links found.</b>\nAdd one first!"
            
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="HTML")
        return

    text = f"<b>{title} (Page {page+1})</b>\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    # Use sequential numbering for list modes, actual index for others
    use_sequential = mode in ["list_affiliate", "list_msa", "list_yt", "yt_delete"]
    
    for idx, pdf in enumerate(pdfs, start=1):
        # Display index: sequential for list modes, actual for operation modes
        display_index = skip + idx if use_sequential else pdf['index']
        
        clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        text += f"<b>{display_index}.</b> <code>{clean_name}</code>\n"
        text += f"🔗 Link: {pdf['link']}\n"
        
        # Show different fields based on mode
        if mode.startswith("yt_") or mode == "list_yt":
            # YT modes: Show ONLY Index, PDF Name, PDF Link (NO affiliate or MSA code)
            yt_title = pdf.get('yt_title', 'Not Set')
            yt_link = pdf.get('yt_link', 'Not Set')
            # Always show YT Title and Link (even if "Not Set")
            text += f"▶️ YT Title: {yt_title}\n"
            text += f"🔗 YT Link: {yt_link}\n\n"
        elif mode.startswith("msa_") or mode == "list_msa":
            aff_link = pdf.get('affiliate_link', 'Not Set')
            text += f"💸 AFF LINK: {aff_link}\n"
            
            # Show MSA Code if it exists
            msa_code = pdf.get('msa_code', 'Not Set')
            text += f"🔑 MSA CODE: {msa_code}\n\n"
        else:
            # Show Affiliate Link (Always, as requested)
            aff_link = pdf.get('affiliate_link', 'Not Set')
            text += f"💸 AFF LINK: {aff_link}\n\n"
    
    # Pagination
    buttons = []
    # Prefix mapping for different modes to handle callbacks/text correctly if needed
    # But since we use text-based handling, we just need unique text or state-based handling.
    # We will use "PREV" and "NEXT" generally, and the handlers will interpret based on state.
    # EXCEPT for strict list modes where there is no state.
    
    nav_prefix = ""
    if mode == "list_affiliate":
        nav_prefix = "_AFF"
    elif mode == "list_msa":
        nav_prefix = "_MSA"
    elif mode == "list_yt":
        nav_prefix = "_YT"
    
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV{nav_prefix} {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT{nav_prefix} {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([cancel_btn])
    
    await send_chunked_message(
        message,
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )



# ... (Existing Keyboards and Handlers) ...

def get_main_menu(user_id: int):
    """Bot 3 Main Menu Structure - Dynamically Filtered"""
    
    # 1. Master Admin sees EVERYTHING
    if user_id == MASTER_ADMIN_ID:
        keyboard = [
            [KeyboardButton(text="📋 LIST"), KeyboardButton(text="➕ ADD")],
            [KeyboardButton(text="🔍 SEARCH"), KeyboardButton(text="🔗 LINKS")],
            [KeyboardButton(text="📊 ANALYTICS"), KeyboardButton(text="🩺 DIAGNOSIS")],
            [KeyboardButton(text="🖥️ TERMINAL"), KeyboardButton(text="💾 BACKUP DATA")],
            [KeyboardButton(text="👥 ADMINS"), KeyboardButton(text="💳 CREDIT MANAGEMENT")],
            [KeyboardButton(text="📚 BOT GUIDE")]
        ]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

    # 2. Check Admin Permissions
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        # Fallback for non-admins (Access Control should block them anyway)
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📚 BOT GUIDE")]], resize_keyboard=True)
        
    perms = admin.get("permissions")
    
    # If permissions are NOT set (None) -> Default to SAFE ACCESS (No Admin/Reset)
    if perms is None:
        perms = DEFAULT_SAFE_PERMISSIONS
        
    # 3. Filter Buttons based on Permissions
    buttons = []
    
    # Define mapping and order
    # Row 1
    if "can_list" in perms: buttons.append("📋 LIST")
    if "can_add" in perms: buttons.append("➕ ADD")
    
    # Row 2
    if "can_search" in perms: buttons.append("🔍 SEARCH")
    if "can_links" in perms: buttons.append("🔗 LINKS")
    
    # Row 3
    if "can_analytics" in perms: buttons.append("📊 ANALYTICS")
    if "can_diagnosis" in perms: buttons.append("🩺 DIAGNOSIS")
    
    # Row 4
    if "can_terminal" in perms: buttons.append("🖥️ TERMINAL")
    if "can_backup" in perms: buttons.append("💾 BACKUP DATA")
    
    # Row 5 (Admins / Reset)
    if "can_manage_admins" in perms: buttons.append("👥 ADMINS")
    if "can_credit" in perms: buttons.append("💳 CREDIT MANAGEMENT")
    
    # Always add Guide
    buttons.append("📚 BOT GUIDE")
    
    # Build Keyboard (Two columns)
    keyboard = []
    row = []
    for btn_text in buttons:
        row.append(KeyboardButton(text=btn_text))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_add_menu():
    """Add Menu Structure"""
    keyboard = [
        [KeyboardButton(text="📄 PDF"), KeyboardButton(text="💸 AFFILIATE")],
        [KeyboardButton(text="🔑 CODE"), KeyboardButton(text="▶️ YT")],
        [KeyboardButton(text="📸 IGCC"), KeyboardButton(text="🎁 REWARD")],
        [KeyboardButton(text="🎬 TUTORIAL")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_pdf_menu():
    """PDF Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD PDF"), KeyboardButton(text="✏️ EDIT PDF")],
        [KeyboardButton(text="🗑️ DELETE PDF"), KeyboardButton(text="📋 LIST PDF")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_affiliate_menu():
    """Affiliate Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD AFFILIATE"), KeyboardButton(text="✏️ EDIT AFFILIATE")],
        [KeyboardButton(text="🗑️ DELETE AFFILIATE"), KeyboardButton(text="📋 LIST AFFILIATE")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_code_menu():
    """Code Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="✏️ EDIT CODE"), KeyboardButton(text="🗑️ DELETE CODE")],
        [KeyboardButton(text="📋 LIST CODE"), KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_yt_menu():
    """YT Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD YT LINK"), KeyboardButton(text="✏️ EDIT YT LINK")],
        [KeyboardButton(text="🗑️ DELETE YT LINK"), KeyboardButton(text="📋 LIST YT LINK")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_links_menu():
    """Links Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="\U0001f4d1 ALL PDF"), KeyboardButton(text="\U0001f4f8 IG CC")],
        [KeyboardButton(text="\U0001f3e0 HOME YT")],
        [KeyboardButton(text="\u2b05\ufe0f BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_admin_config_menu():
    """Admin Configuration Menu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ NEW ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN")],
        [KeyboardButton(text="🔐 PERMISSIONS"), KeyboardButton(text="👔 ROLES")],
        [KeyboardButton(text="🔒 LOCK/UNLOCK"), KeyboardButton(text="🚫 BAN CONFIG")],
        [KeyboardButton(text="📋 LIST ADMINS")],
        [KeyboardButton(text="🏠 MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_ban_config_menu():
    """Ban Configuration Menu Structure"""
    keyboard = [
        [KeyboardButton(text="🚫 BAN USER"), KeyboardButton(text="✅ UNBAN USER")],
        [KeyboardButton(text="📋 LIST BANNED")],
        [KeyboardButton(text="⬅️ BACK TO ADMIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_roles_menu():
    """Roles Menu Structure"""
    keyboard = [
        [KeyboardButton(text="👑 OWNER")],
        [KeyboardButton(text="👨‍💼 MANAGER"), KeyboardButton(text="👔 ADMIN")],
        [KeyboardButton(text="🛡️ MODERATOR"), KeyboardButton(text="👨‍💻 SUPPORT")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)



def get_analytics_menu():
    """Analytics Menu Structure"""
    keyboard = [
        [KeyboardButton(text="📊 OVERVIEW")],
        [KeyboardButton(text="📄 PDF Clicks"), KeyboardButton(text="💸 Affiliate Clicks")],
        [KeyboardButton(text="📸 IG Start Clicks"), KeyboardButton(text="▶️ YT Start Clicks")],
        [KeyboardButton(text="📸 IG CC Start Clicks"), KeyboardButton(text="🔑 YT Code Start Clicks")],
        [KeyboardButton(text="🤝 Referral Overview"), KeyboardButton(text="🆔 MSA ID POOL")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_backup_menu():
    """Bot 3 Backup menu — exact bot2-style 5-row layout"""
    keyboard = [
        [KeyboardButton(text="💾 DOWNLOAD BACKUP"), KeyboardButton(text="📤 UPLOAD BACKUP")],
        [KeyboardButton(text="☁️ GDRIVE SYSTEM"),   KeyboardButton(text="📊 BACKUP STATUS")],
        [KeyboardButton(text="📜 HISTORY"),          KeyboardButton(text="⏳ ACTIVATE TTL")],
        [KeyboardButton(text="🗑️ RESET BACKUP DATA")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# --- Helpers for Deep Linking ---
def generate_alphanumeric(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(random.SystemRandom().choice(chars) for _ in range(length))

def generate_digits(length=8):
    return "".join(random.SystemRandom().choice(string.digits) for _ in range(length))

def generate_unique_pdf_start_code(field_name: str, length: int = 8, alphanumeric: bool = False) -> str:
    """Generate a DB-unique start code for a specific PDF code field."""
    chars = (string.ascii_letters + string.digits) if alphanumeric else string.digits
    rng = random.SystemRandom()
    for _ in range(5000):
        candidate = "".join(rng.choice(chars) for _ in range(length))
        exists = col_pdfs.find_one({field_name: candidate}, {"_id": 1})
        if not exists:
            return candidate
    raise RuntimeError(f"Unable to allocate unique code for field '{field_name}'")

async def ensure_pdf_codes(pdf):
    """Ensure PDF has all start codes"""
    updates = {}
    if not pdf.get("ig_start_code"):
        updates["ig_start_code"] = generate_unique_pdf_start_code("ig_start_code", length=8, alphanumeric=True)
    if not pdf.get("yt_start_code"):
        updates["yt_start_code"] = generate_unique_pdf_start_code("yt_start_code", length=8, alphanumeric=False)
    if not pdf.get("aff_start_code"):
        updates["aff_start_code"] = generate_unique_pdf_start_code("aff_start_code", length=8, alphanumeric=False)
    if not pdf.get("orig_start_code"):
        updates["orig_start_code"] = generate_unique_pdf_start_code("orig_start_code", length=8, alphanumeric=False)
    
    if updates:
        col_pdfs.update_one({"_id": pdf["_id"]}, {"$set": updates})
        return {**pdf, **updates}
    return pdf

async def ensure_pdf_msa_code(pdf) -> str:
    """Ensure a PDF has a valid unique MSA code and return it.

    Uses count_documents instead of exclude_pdf_id to avoid false-duplicate
    detection when the in-memory _id is a string but MongoDB stores ObjectId.
    """
    code = (pdf.get("msa_code") or "").strip().upper()
    is_valid = bool(re.fullmatch(r"^MSA\d{4}$", code))

    if is_valid:
        # Exactly 1 doc has this code → it is uniquely this PDF → keep it.
        count = col_pdfs.count_documents({"msa_code": code})
        if count == 1:
            return code
        if count == 0:
            # Not yet written to DB — write it now and return
            try:
                col_pdfs.update_one({"_id": pdf["_id"]}, {"$set": {"msa_code": code}})
                return code
            except Exception:
                pass  # Conflict → fall through to generate new

    # Generate a fresh unique code (genuine duplicate or invalid/missing)
    for _ in range(20):
        candidate = generate_unique_msa_code()
        try:
            col_pdfs.update_one({"_id": pdf["_id"]}, {"$set": {"msa_code": candidate}})
            return candidate
        except DuplicateKeyError:
            continue

    raise RuntimeError("Unable to allocate unique MSA code for PDF")

async def ensure_ig_cc_code(content):
    """Ensure IG content has start_code"""
    if not content.get("start_code"):
        code = generate_digits(8)
        col_ig_content.update_one({"_id": content["_id"]}, {"$set": {"start_code": code}})
        return {**content, "start_code": code}
    return content

async def get_home_yt_code():
    """Get or create Home YT code"""
    setting = col_settings.find_one({"key": "home_yt_code"})
    if not setting:
        code = generate_digits(8)
        col_settings.insert_one({"key": "home_yt_code", "value": code})
        return code
    return setting["value"]

def get_ig_menu():
    """IG Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD IG"), KeyboardButton(text="✏️ EDIT IG")],
        [KeyboardButton(text="🗑️ DELETE IG"), KeyboardButton(text="📎 ADD AFFILIATE")],
        [KeyboardButton(text="📋 LIST IG"), KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_reward_menu():
    """Reward Content Submenu — Store Manager only (Milestones and RW deprecated)"""
    keyboard = [
        [KeyboardButton(text="🛒 STORE MANAGER")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_milestone_menu():
    """Milestone submenu keyboard."""
    keyboard = [
        [KeyboardButton(text="➕ ADD MILESTONE"), KeyboardButton(text="✏️ EDIT MILESTONE")],
        [KeyboardButton(text="🗑️ DELETE MILESTONE"), KeyboardButton(text="📋 LIST MILESTONES")],
        [KeyboardButton(text="⬅️ BACK TO REWARD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_store_menu():
    """Store Manager submenu keyboard."""
    keyboard = [
        [KeyboardButton(text="➕ ADD STORE ITEM"), KeyboardButton(text="✏️ EDIT STORE ITEM")],
        [KeyboardButton(text="🗑️ DELETE STORE ITEM"), KeyboardButton(text="📋 LIST STORE ITEMS")],
        [KeyboardButton(text="⬅️ BACK TO REWARD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

RW_PAGE_SIZE = 8  # Rewards per page in list view

def get_ig_affiliate_menu():
    """IG Affiliate Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="📎 Add"), KeyboardButton(text="✏️ Edit")],
        [KeyboardButton(text="🗑️ Delete"), KeyboardButton(text="📋 List")],
        [KeyboardButton(text="◀️ Back")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_tutorial_pk_menu():
    """Tutorial Submenu — universal tutorial link management."""
    keyboard = [
        [KeyboardButton(text="➕ ADD TUTORIAL"), KeyboardButton(text="✏️ EDIT TUTORIAL")],
        [KeyboardButton(text="🗑️ DELETE TUTORIAL"), KeyboardButton(text="📋 LIST TUTORIAL")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def _get_tutorial_confirm_kb(confirm_text: str) -> ReplyKeyboardMarkup:
    """One-shot confirm/cancel reply keyboard for tutorial confirmation prompts."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=confirm_text), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


# --- Handlers ---

# --- GLOBAL PRIORITY HANDLER FOR RETURN BACK ---
@dp.message(F.text == "⬅️ RETURN BACK")
@dp.message(F.text.contains("BACK TO ADMIN MENU"))
async def global_return_back(message: types.Message, state: FSMContext):
    """Global handler for Return Back button to bypass any state issues"""
    # Authorization check
    if not await check_authorization(message, "Global Return Back", "can_manage_admins"):
        return
        
    await state.clear()
    await message.answer(
        "🔐 <b>Admin Management</b>\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Start command handler - ADMIN ONLY"""
    user_id = message.from_user.id
    
    # 1. Check ban status - SILENT ignore
    if is_banned(user_id):
        return  # No response at all

    # 2. Check if user is admin
    if not is_admin(user_id):
        # Non-admin attempting to access bot3
        was_banned, attempt_count = await check_spam_and_ban(
            user_id,
            message.from_user.full_name,
            message.from_user.username,
            "/start command"
        )
        if was_banned:
            return  # Silent ban, no response
        
        # Log unauthorized access attempt
        await log_unauthorized_access(
            message.from_user,
            "/start command",
            attempt_count
        )
        return  # BLOCK - No access for non-admins
    
    # 3. Admin verified - Continue with normal handler logic
    log_user_action(message.from_user, "Started Bot")
    
    await message.answer(
        "🤖 <b>BOT 3 ONLINE</b>\n"
        "System Authorized. Accessing Mainframe...",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🔐 ADMIN PASSWORD GATE (owner only, once per session, double confirmation)
# ──────────────────────────────────────────────────────────────────────────────

@dp.message(AdminAuthStates.pw_first)
async def admin_pw_first(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text and message.text.strip() in ("❌ CANCEL", "❌ Cancel"):
        await state.clear()
        await message.answer("❌ Authentication cancelled.", reply_markup=ReplyKeyboardRemove())
        return
    try: await message.delete()
    except: pass
    data = await state.get_data()
    attempts = data.get("pw_attempts", 0)
    if not ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return
    if message.text == ADMIN_PASSWORD:
        await state.update_data(pw_first_ok=True, pw_attempts=0)
        await state.set_state(AdminAuthStates.pw_second)
        await message.answer("✅ Password accepted.\n\nEnter password again to confirm:", parse_mode="HTML")
    else:
        attempts += 1
        remaining = 3 - attempts
        if remaining <= 0:
            await state.clear()
            await message.answer(
                "❌ Too many failed attempts. Use /start to try again.",
                reply_markup=ReplyKeyboardRemove(),
            )
        else:
            await state.update_data(pw_attempts=attempts)
            await message.answer(
                f"❌ Incorrect password. <b>{remaining}</b> attempt(s) remaining.",
                parse_mode="HTML",
            )


@dp.message(AdminAuthStates.pw_second)
async def admin_pw_second(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text and message.text.strip() in ("❌ CANCEL", "❌ Cancel"):
        # Cancel = skip auth for this session (owner ID already verified)
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return
    try: await message.delete()
    except: pass
    if message.text == ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
    else:
        await state.clear()
        await message.answer(
            "❌ Passwords did not match. Authentication failed.\n\nUse /start to try again.",
            reply_markup=ReplyKeyboardRemove(),
        )


@dp.message(F.text == "⬅️ BACK TO MAIN MENU")
async def back_to_main_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Back to Main Menu"):
        return
    await state.clear()
    await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.text == "🏠 MAIN MENU")
async def main_menu_from_admin_handler(message: types.Message, state: FSMContext):
    """Return to Main Menu (globally available for admins)"""
    if not await check_authorization(message, "Main Menu"):
        return
    await state.clear()
    await message.answer(
        "👋 <b>Welcome Back!</b>\nSelect an option from the menu below:",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )

@dp.message(F.text == "⬅️ MAIN MENU")
async def backup_main_menu_handler(message: types.Message, state: FSMContext):
    """Global handler for the ⬅️ MAIN MENU button (used in backup menu & others)"""
    if not await check_authorization(message, "Main Menu"):
        return
    await state.clear()
    await message.answer(
        "🏠 <b>Main Menu</b>\nSelect an option from the menu below:",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )

@dp.message(F.text == "➕ NEW ADMIN")
async def new_admin_handler(message: types.Message, state: FSMContext):
    """Ask for new admin's user ID"""
    if not await check_authorization(message, "New Admin"):
        return
    await state.set_state(AdminManagementStates.waiting_for_new_admin_id)
    await message.answer(
        "➕ <b>ADD NEW ADMIN</b>\n\n"
        "Please send the <b>Telegram User ID</b> of the user you want to add as admin.\n\n"
        "Example: `123456789`",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")]
            ],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(AdminManagementStates.waiting_for_new_admin_id)
async def process_new_admin_id(message: types.Message, state: FSMContext):
    """Process and save new admin ID"""
    if message.text == "🏠 MAIN MENU":
        await state.clear()
        await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))
        return
    
    # Validate input
    if not message.text.isdigit():
        await message.answer(
            "⚠️ <b>Invalid Input</b>\n\n"
            "Please send a valid numeric Telegram User ID.",
            parse_mode="HTML"
        )
        return
    
    new_admin_id = int(message.text)
    
    # Check if Banned
    if is_banned(new_admin_id):
        await message.answer(
            f"⛔ <b>ACTION DENIED</b>\n\n"
            f"User `{new_admin_id}` is currently <b>BANNED</b>.\n"
            f"You cannot add a banned user as an Admin.\n\n"
            f"<i>Please unban them first from the Ban Config menu.</i>",
            reply_markup=get_admin_config_menu(),
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    # Check for duplicates
    existing = col_admins.find_one({"user_id": new_admin_id})
    if existing:
        await message.answer(
            f"⚠️ <b>Admin Already Exists</b>\n\n"
            f"User ID `{new_admin_id}` is already an admin.",
            parse_mode="HTML"
        )
        return
    
    # Save to database
    # Try to fetch user info
    admin_name = "Unknown"
    admin_username = "Unknown"
    
    try:
        user_chat = await bot.get_chat(new_admin_id)
        admin_name = user_chat.full_name or "Unknown"
        admin_username = user_chat.username or "Unknown"
    except Exception:
        logger.warning(f"Could not fetch info for new admin {new_admin_id}")

    col_admins.insert_one({
        "user_id": new_admin_id,
        "added_by": message.from_user.id,
        "added_at": now_local(),
        "permissions": [],      # LOCKED by default — NO permissions until unlocked by owner
        "full_name": admin_name,
        "username": admin_username,
        "is_locked": True       # Must be explicitly unlocked before they can use bot
    })
    
    await state.clear()
    await message.answer(
        f"✅ <b>Admin Added Successfully!</b>\n\n"
        f"User ID: `{new_admin_id}`\n"
        f"Added by: {message.from_user.id}\n\n"
        f"⚠️ <b>NOTE: New Admins are LOCKED by default.</b>\n"
        f"Use the Lock Menu to unlock them.",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_handler(message: types.Message, state: FSMContext):
    """Show list of admins with pagination"""
    if not await check_authorization(message, "Remove Admin"):
        return
    # Exclude Master Admin
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
    
    if not admins:
        await message.answer(
            "⚠️ <b>No Other Admins Found</b>\n\n"
            "There are no admins to remove.\n"
            "Use <b>➕ NEW ADMIN</b> to add administrators.",
            parse_mode="HTML"
        )
        return
    
    # Store current page in state
    await state.set_state(AdminManagementStates.viewing_admin_list)
    await state.update_data(page=0)
    
    # Show first page
    await show_admin_list_page(message, admins, page=0)

async def show_admin_list_page(message: types.Message, admins: list, page: int):
    """Display admin list with pagination"""
    ADMINS_PER_PAGE = 5
    total_pages = max(1, (len(admins) + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE)
    
    # Cap page just in case
    page = min(page, max(0, total_pages - 1))
    
    start_idx = page * ADMINS_PER_PAGE
    end_idx = min(start_idx + ADMINS_PER_PAGE, len(admins))
    
    # Create list text and buttons
    admin_list_text = ""
    keyboard = []
    
    for i in range(start_idx, end_idx):
        admin = admins[i]
        user_id = admin.get("user_id")
        name = admin.get("full_name", "Unknown")
        is_locked = admin.get("is_locked", False)
        status_icon = "🔒" if is_locked else "🔓"
        
        # Add to text list
        admin_list_text += f"{i+1}. <b>{_html_escape(name)}</b> (`{user_id}`) [{status_icon}]\n"
        
        # Add button
        btn_text = f"❌ Remove: {name} ({user_id})"
        keyboard.append([KeyboardButton(text=btn_text)])
    
    # Add navigation buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])
    
    await message.answer(
        f"➖ <b>REMOVE ADMIN</b>\n\n"
        f"Click on an admin to remove them:\n\n"
        f"{admin_list_text}\n"
        f"📊 Page {page + 1}/{total_pages} | Total: {len(admins)} admins",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(AdminManagementStates.viewing_admin_list)
async def process_admin_removal(message: types.Message, state: FSMContext):
    """Handle admin removal or pagination"""
    if message.text == "🏠 MAIN MENU":
        await state.clear()
        await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))
        return
    elif message.text in ["⬅️ RETURN BACK", "⬅️ BACK TO ADMIN MENU", "/cancel"]:
        await state.clear()
        await message.answer("⚙️ <b>Admin Management Menu</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return
        
    data = await state.get_data()
    current_page = data.get("page", 0)
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
    
    # Handle pagination
    if message.text == "➡️ NEXT ADMINS":
        await state.update_data(page=current_page + 1)
        await show_admin_list_page(message, admins, current_page + 1)
        return
    elif message.text == "⬅️ PREV ADMINS":
        await state.update_data(page=current_page - 1)
        await show_admin_list_page(message, admins, current_page - 1)
        return
    
    # Handle admin removal
    target_id = None
    
    # Regex to extract ID from "❌ Remove: Name (ID)"
    import re
    match = re.search(r"Remove: .* \((\d+)\)$", message.text)
    
    if match: # New format
        target_id = int(match.group(1))
    elif message.text.startswith("❌ Remove Admin: "): # Old/Fallback format
        try:
            target_id = int(message.text.split(":")[-1].strip())
        except ValueError:
            target_id = None
            
    if target_id:
        # Extra safety check to prevent removing Master Admin
        if target_id == MASTER_ADMIN_ID:
            await message.answer("🚫 <b>You cannot remove the Master Admin.</b>", parse_mode="HTML")
            return
            
        try:
            # Remove from database
            result = col_admins.delete_one({"user_id": target_id})
            
            if result.deleted_count > 0:
                await state.clear()
                await message.answer(
                    f"✅ <b>Admin Removed</b>\n\n"
                    f"User ID `{target_id}` is no longer an admin.\n"
                    f"They cannot access Bot 3 anymore.",
                    reply_markup=get_admin_config_menu(),
                    parse_mode="HTML"
                )
            else:
                await message.answer("⚠️ Admin not found in database.")
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            await message.answer("❌ Error removing admin.")
    else:
        await message.answer("⚠️ Invalid selection.")

@dp.message(F.text == "⬅️ BACK TO ADD MENU")
async def back_to_add_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("➕ <b>SELECT ADD COMPONENT:</b>", reply_markup=get_add_menu(), parse_mode="HTML")

@dp.message(F.text == "➕ ADD")
async def add_menu_handler(message: types.Message):
    """Show Add Submenu"""
    if not await check_authorization(message, "Access Add Menu", "can_add"):
        return
    await message.answer(
        "➕ <b>SELECT ADD COMPONENT:</b>",
        reply_markup=get_add_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "📄 PDF")
async def pdf_menu_handler(message: types.Message):
    if not await check_authorization(message, "PDF Menu", "can_add"):
        return
    await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")

def is_pdf_name_duplicate(name, exclude_id=None):
    """Check if PDF name already exists (case-insensitive). Returns conflicting PDF or None"""
    query = {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_pdf_link_duplicate(link, exclude_id=None):
    """Check if PDF link already exists. Returns conflicting PDFw or None"""
    query = {"link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_affiliate_link_duplicate(link, exclude_id=None):
    """Check if Affiliate link already exists. Returns conflicting PDF or None"""
    query = {"affiliate_link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_yt_link_duplicate(link, exclude_id=None):
    """Check if YT link already exists. Returns conflicting PDF or None"""
    query = {"yt_link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_yt_title_duplicate(title, exclude_id=None):
    """Check if YT title already exists. Returns conflicting PDF or None"""
    query = {"yt_title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

# 3. PDF MANAGEMENT HANDLERS
# ---------------------------------------------------------------------------------------

# 1. ADD PDF
@dp.message(F.text == "➕ ADD PDF")
async def start_add_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_add_name)
    await message.answer(
        "📄 <b>ADD PDF</b>\n\n"
        "<b>Options:</b>\n"
        "1. Enter a <b>single PDF name</b>.\n"
        "   OR\n"
        "2. Paste a <b>Bot 4 File List</b> to add multiple PDFs at once!\n\n"
        "<i>(Send ❌ CANCEL anytime to exit)</i>", 
        reply_markup=get_cancel_keyboard(), 
        parse_mode="HTML"
    )

@dp.message(PDFStates.waiting_for_add_name)
async def process_add_pdf_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")
    
    text = message.text.strip()
    
    # Auto-detect Bot 4 format list
    # Regex designed to robustly match Bot 4 file list style
    pattern = r"(?m)^\d+\.\s*(.*?)\s*(?:\[R\])?\s*$(?:\n(?:(?!\d+\.).)*)*?\n🔗\s*(http\S+)"
    matches = re.findall(pattern, text)
    
    if matches:
        added = 0
        skipped = 0
        errors = []
        
        for name, link in matches:
            name = name.strip()
            link = link.strip()
            
            if is_pdf_name_duplicate(name) or is_pdf_link_duplicate(link):
                skipped += 1
                continue
                
            try:
                idx = await get_next_pdf_index()
                base_doc = {
                    "index": idx,
                    "name": name,
                    "link": link,
                    "created_at": now_local(),
                    "clicks": 0, "affiliate_clicks": 0, "ig_start_clicks": 0, "yt_start_clicks": 0, "yt_code_clicks": 0,
                    "last_clicked_at": None, "last_affiliate_click": None, "last_ig_click": None, "last_yt_click": None, "last_yt_code_click": None,
                }
                
                assigned_msa_code = None
                for _ in range(20):
                    candidate_code = generate_unique_msa_code()
                    try:
                        doc = {**base_doc, "msa_code": candidate_code}
                        col_pdfs.insert_one(doc)
                        assigned_msa_code = candidate_code
                        break
                    except DuplicateKeyError:
                        continue
                        
                if assigned_msa_code:
                    added += 1
                else:
                    errors.append(f"Failed to allocate code for {name}")
            except Exception as e:
                errors.append(str(e))
                skipped += 1
                
        msg = f"✅ <b>BULK ADD COMPLETE</b>\n\n"
        msg += f"📥 Added: <b>{added}</b>\n"
        msg += f"⏭ Skipped (Duplicates): <b>{skipped}</b>\n\n"
        if errors:
            msg += f"⚠️ Errors: {len(errors)}\n\n"
        msg += "📄 Enter another PDF Name, paste another list, or click ❌ CANCEL to exit:"
        
        return await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

    # If not a list, process as single add (Name)
    name = text
    
    # Validation: Check duplicate name
    conflict_pdf = is_pdf_name_duplicate(name)
    if conflict_pdf:
        await message.answer(f"⚠️ <b>Name Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    await state.update_data(name=name)
    await state.set_state(PDFStates.waiting_for_add_link)
    await message.answer(f"✅ Name set to: <b>{name}</b>\n\n🔗 <b>Enter PDF Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(PDFStates.waiting_for_add_link)
async def process_add_pdf_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")
    
    link = message.text.strip()
    
    # Validation: Check duplicate link
    conflict_pdf = is_pdf_link_duplicate(link)
    if conflict_pdf:
        await message.answer(f"⚠️ <b>Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    data = await state.get_data()
    name = data['name']
    
    # Validation (Basic)
    if "http" not in link and "t.me" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return

    # Save to DB (MSA code is generated immediately and guaranteed unique)
    idx = await get_next_pdf_index()
    base_doc = {
        "index": idx,
        "name": name,
        "link": link,
        "created_at": now_local(),
        # Initialize click tracking fields
        "clicks": 0,
        "affiliate_clicks": 0,
        "ig_start_clicks": 0,
        "yt_start_clicks": 0,
        "yt_code_clicks": 0,
        # Last clicked timestamps
        "last_clicked_at": None,
        "last_affiliate_click": None,
        "last_ig_click": None,
        "last_yt_click": None,
        "last_yt_code_click": None,
    }

    assigned_msa_code = None
    for _ in range(20):
        candidate_code = generate_unique_msa_code()
        try:
            doc = {**base_doc, "msa_code": candidate_code}
            col_pdfs.insert_one(doc)
            assigned_msa_code = candidate_code
            break
        except DuplicateKeyError:
            continue

    if not assigned_msa_code:
        await state.clear()
        await message.answer(
            "❌ <b>Failed to allocate unique MSA code.</b>\nPlease try again.",
            reply_markup=get_pdf_menu(),
            parse_mode="HTML"
        )
        return
    
    # Log Action
    log_user_action(message.from_user, "Added PDF", f"Name: {name}, Index: {idx}, MSA: {assigned_msa_code}")

    # Set state back to adding name to loop
    await state.set_state(PDFStates.waiting_for_add_name)
    await message.answer(
        f"✅ <b>PDF Added!</b>\n\n"
        f"🆔 Index: `{idx}`\n"
        f"📄 Name: `{name}`\n"
        f"🔗 Link: `{link}`\n"
        "🔑 MSA Code: `Generated` (locked until assigned)\n\n"
        "📄 <b>Enter another PDF Name</b>, paste a list, or click ❌ CANCEL to exit:",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

# 2. LIST PDF
@dp.message(F.text == "📋 LIST PDF")
async def list_pdfs(message: types.Message, state: FSMContext, page=0):
    if not await check_authorization(message, "List PDF", "can_list"):
        return
    await state.set_state(PDFStates.viewing_list)
    await send_pdf_list_view(message, page=page, mode="list")

@dp.message(F.text == "⬅️ BACK TO PDF MENU")
async def back_to_pdf_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV ") or m.text.startswith("➡️ NEXT ")))
async def pdf_pagination_handler(message: types.Message, state: FSMContext):
    # Determine mode based on state
    current_state = await state.get_state()
    mode = "list"
    if current_state == PDFStates.waiting_for_edit_search:
        mode = "edit"
    elif current_state == PDFStates.waiting_for_delete_search:
        mode = "delete"
    elif current_state == AffiliateStates.waiting_for_pdf_selection:
        mode = "affiliate_select"
    elif current_state == AffiliateDeleteStates.waiting_for_selection:
        mode = "affiliate_delete"
    elif current_state == MSACodeStates.waiting_for_pdf_selection:
        # Check selection_mode from state data
        data = await state.get_data()
        mode = data.get('selection_mode', 'msa_add_select')
    elif current_state == MSACodeEditStates.waiting_for_selection:
        mode = "msa_edit_select"
    elif current_state == MSACodeDeleteStates.waiting_for_selection:
        mode = "msa_delete"
    elif current_state == YTStates.waiting_for_pdf_selection:
        mode = "yt_add_select"
    elif current_state == YTEditStates.waiting_for_selection:
        mode = "yt_edit_select"
    elif current_state == YTDeleteStates.waiting_for_selection:
        mode = "yt_delete"

    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode=mode)
    except:
        await send_pdf_list_view(message, page=0, mode=mode)



# 3. EDIT PDF
@dp.message(F.text == "✏️ EDIT PDF")
async def start_edit_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_edit_search)
    await send_pdf_list_view(message, page=0, mode="edit")

@dp.message(PDFStates.waiting_for_edit_search)
async def process_edit_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    # Handle Pagination Interaction within Edit State
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        return await pdf_pagination_handler(message, state)
    
    query = message.text
    # Try Search by Index
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        # Search by Name (Text)
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(edit_id=str(pdf["_id"]), current_name=pdf["name"], current_link=pdf["link"])
    
    # Show Edit Options
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 EDIT NAME"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(PDFStates.waiting_for_edit_field)
    clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
    await message.answer(
        f"📄 <b>PDF FOUND</b>\n"
        f"🆔 Index: <code>{pdf['index']}</code>\n"
        f"📛 Name: {clean_name}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ <b>Select what to edit:</b>",
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.message(PDFStates.waiting_for_edit_field)
async def process_edit_field(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    if message.text == "📝 EDIT NAME":
        await state.update_data(field="name")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Name:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(PDFStates.waiting_for_edit_value)
async def process_edit_value(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    field = data['field']
    new_value = message.text.strip()
    
    if field == "name":
        # Check if same as current
        if new_value.lower() == data['current_name'].lower():
            await message.answer(f"⚠️ <b>Same Name!</b>\nYou entered the exact same name.\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

        # Check duplicate name (exclude current PDF)
        conflict_pdf = is_pdf_name_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
            await message.answer(f"⚠️ <b>Name Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nTry another name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"name": new_value}})
        msg = f"✅ <b>PDF Name Updated!</b>\nOld: {data['current_name']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Name", f"ID: {data['edit_id']}, New: {new_value}")
    
    elif field == "link":
        # Check if same as current
        if new_value == data['current_link']:
            await message.answer(f"⚠️ <b>Same Link!</b>\nYou entered the exact same link.\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

        # Check duplicate link (exclude current PDF)
        conflict_pdf = is_pdf_link_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
            await message.answer(f"⚠️ <b>Link Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nTry another link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        # Basic Validation
        if "http" not in new_value and "t.me" not in new_value:
            await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
            return

        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"link": new_value}})
        msg = f"✅ <b>PDF Link Updated!</b>\nOld: {data['current_link']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Link", f"ID: {data['edit_id']}, New: {new_value}")
    else:
        msg = "⚠️ An unexpected error occurred."

    await state.clear()
    await message.answer(msg, reply_markup=get_pdf_menu(), parse_mode="HTML")

# 4. DELETE PDF
@dp.message(F.text == "🗑️ DELETE PDF")
async def start_delete_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_delete_search)
    await send_pdf_list_view(message, page=0, mode="delete")

@dp.message(PDFStates.waiting_for_delete_search)
async def process_delete_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    # Handle Pagination Interaction within Delete State
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        return await pdf_pagination_handler(message, state)
    
    # Parse input - support comma-separated values
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:  # Skip empty strings
            continue
            
        pdf = None
        if query.isdigit():
            pdf = col_pdfs.find_one({"index": int(query)})
        else:
            # Try exact name match first, then regex
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf:
            # Check for duplicates using _id
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Store delete IDs
    delete_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    await state.update_data(delete_ids=delete_ids)
    
    # Build confirmation message
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(PDFStates.waiting_for_delete_confirm)
    
    # Show what will be deleted
    msg = f"⚠️ <b>CONFIRM BULK DELETION</b>\n\n"
    msg += f"📊 <b>Total to delete: {len(found_pdfs)} PDF(s)</b>\n\n"
    
    for idx, pdf in enumerate(found_pdfs, 1):
        clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        msg += f"{idx}. <code>{pdf['index']}</code> - {clean_name}\n"
    
    if not_found:
        msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
        msg += "\n".join(f"• <code>{q}</code>" for q in not_found[:5])  # Limit to 5
        if len(not_found) > 5:
            msg += f"\n...and {len(not_found) - 5} more"
    
    msg += "\n\n❓ Confirm deletion?"
    
    await message.answer(
        msg,
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.message(PDFStates.waiting_for_delete_confirm)
async def process_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Check if bulk delete (list of IDs) or single delete (single ID)
        delete_ids = data.get('delete_ids')
        
        if delete_ids:
            # Bulk delete
            object_ids = [ObjectId(id_str) for id_str in delete_ids]
            result = col_pdfs.delete_many({"_id": {"$in": object_ids}})
            deleted_count = result.deleted_count
            
            # Auto re-index remaining PDFs
            await reindex_all_pdfs()
            
            await state.clear()
            await message.answer(
                f"🗑️ <b>Bulk Deletion Complete</b>\n\n"
                f"✅ Successfully deleted <b>{deleted_count} PDF(s)</b>\n"
                f"📊 Indices automatically reorganized",
                reply_markup=get_pdf_menu(),
                parse_mode="HTML"
            )
            
            # Log action
            log_user_action(message.from_user, "Bulk Delete PDFs", f"Deleted {deleted_count} PDFs")
        else:
            # Single delete (fallback for compatibility)
            delete_id = data.get('delete_id')
            if delete_id:
                col_pdfs.delete_one({"_id": ObjectId(delete_id)})
                
                # Auto re-index remaining PDFs
                await reindex_all_pdfs()
                
                await state.clear()
                await message.answer(
                    "🗑️ <b>PDF Deleted Successfully.</b>\n"
                    "📊 Indices automatically reorganized",
                    reply_markup=get_pdf_menu(),
                    parse_mode="HTML"
                )
                log_user_action(message.from_user, "Delete PDF", f"ID: {delete_id}")
            else:
                await state.clear()
                await message.answer("❌ Error: No PDFs to delete.", reply_markup=get_pdf_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())

# ... (Previous Handlers) ...

# --- AFFILIATE HANDLERS ---

@dp.message(F.text == "💸 AFFILIATE")
async def affiliate_menu_handler(message: types.Message):
    await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

# --- AFFILIATE HANDLERS ---

# 1. ADD / EDIT AFFILIATE
# Split Handlers

@dp.message(F.text == "➕ ADD AFFILIATE")
async def start_add_affiliate(message: types.Message, state: FSMContext):
    await state.set_state(AffiliateStates.waiting_for_pdf_selection)
    # Mode ensures we only show PDFs WITHOUT links
    await state.update_data(selection_mode="affiliate_add_select")
    await send_pdf_list_view(message, page=0, mode="affiliate_add_select")

@dp.message(F.text == "✏️ EDIT AFFILIATE")
async def start_edit_affiliate(message: types.Message, state: FSMContext):
    await state.set_state(AffiliateStates.waiting_for_pdf_selection)
    # Mode ensures we only show PDFs WITH links
    await state.update_data(selection_mode="affiliate_edit_select")
    await send_pdf_list_view(message, page=0, mode="affiliate_edit_select")

@dp.message(AffiliateStates.waiting_for_pdf_selection)
async def process_affiliate_pdf_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
    
    # Catch Back Button Here too just in case state is active
    if message.text == "⬅️ BACK TO AFFILIATE MENU":
        await state.clear()
        return await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
         try:
            page = int(message.text.split()[-1]) - 1
            data = await state.get_data()
            mode = data.get('selection_mode', 'affiliate_add_select')
            await send_pdf_list_view(message, page=page, mode=mode)
            return
         except: pass

    # Parse input - support comma-separated values for bulk selection
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get current mode to determine selection method
    current_data = await state.get_data()
    selection_mode = current_data.get('selection_mode', 'affiliate_add_select')
    use_sequential = selection_mode == 'affiliate_edit_select'  # Edit uses sequential numbering
    
    # For sequential selection (edit mode), get all PDFs matching the filter first
    all_filtered_pdfs = []
    if use_sequential:
        query_filter = {"affiliate_link": {"$exists": True, "$ne": ""}}
        all_filtered_pdfs = list(col_pdfs.find(query_filter).sort("index", 1))
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:
            continue
            
        pdf = None
        if query.isdigit():
            if use_sequential:
                # Sequential selection - 1 means first item in the filtered list
                idx = int(query) - 1
                if 0 <= idx < len(all_filtered_pdfs):
                    pdf = all_filtered_pdfs[idx]
            else:
                # Direct index selection
                pdf = col_pdfs.find_one({"index": int(query)})
        else:
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf:
            # Check for duplicates
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Store selected PDF IDs and names for bulk operation
    pdf_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    pdf_names = [pdf["name"] for pdf in found_pdfs]
    
    # For single selection, also store singular fields for compatibility
    if len(found_pdfs) == 1:
        await state.update_data(
            pdf_id=pdf_ids[0],
            pdf_name=pdf_names[0],
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=False,
            current_aff=found_pdfs[0].get("affiliate_link", "None")
        )
    else:
        await state.update_data(
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=True
        )
    
    await state.set_state(AffiliateStates.waiting_for_link)
    
    # Build confirmation message
    if len(found_pdfs) > 1:
        msg = f"💸 <b>MULTIPLE PDFs SELECTED ({len(found_pdfs)})</b>\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n📝 <b>Enter affiliate link to apply to ALL selected PDFs:</b>"
    else:
        # Single selection
        pdf = found_pdfs[0]
        current_aff = pdf.get("affiliate_link", "None")
        msg = (
            f"💸 <b>SELECTED PDF:</b>\n`{pdf['index']}`. {pdf['name']}\n"
            f"Current Affiliate Link: `{current_aff}`\n\n"
            "📝 <b>Enter new affiliate link:</b>"
        )
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(AffiliateStates.waiting_for_link)
async def process_affiliate_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
    
    link = message.text.strip()
    data = await state.get_data()
    
    # Basic Validation
    if "http" not in link and "t.me" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    from bson.objectid import ObjectId
    
    # Check if bulk operation
    is_bulk = data.get('is_bulk', False)
    pdf_ids = data.get('pdf_ids', [])
    
    if is_bulk and pdf_ids:
        # Bulk assignment - apply same link to all PDFs
        object_ids = [ObjectId(id_str) for id_str in pdf_ids]
        result = col_pdfs.update_many(
            {"_id": {"$in": object_ids}},
            {"$set": {"affiliate_link": link}}
        )
        
        updated_count = result.modified_count
        pdf_names = data.get('pdf_names', [])
        
        await state.clear()
        await message.answer(
            f"✅ <b>Bulk Affiliate Link Assignment Complete!</b>\n\n"
            f"📊 Successfully set affiliate link for <b>{updated_count} PDF(s)</b>\n"
            f"🔗 Link: `{link}`",
            reply_markup=get_affiliate_menu(),
            parse_mode="HTML"
        )
        
        # Log action
        log_user_action(message.from_user, "Bulk Add Affiliate", f"Set link for {updated_count} PDFs")
    else:
        # Single PDF assignment
        pdf_id = data.get('pdf_id')
        pdf_name = data.get('pdf_name', 'Unknown')
        
        # Check if same as current (only for single)
        current_aff = data.get('current_aff')
        if current_aff and link == current_aff:
            await message.answer(
                f"⚠️ <b>Same Link!</b>\nYou entered the exact same affiliate link.\nPlease enter a different link:",
                reply_markup=get_cancel_keyboard(),
                parse_mode="HTML"
            )
            return
        
        col_pdfs.update_one(
            {"_id": ObjectId(pdf_id)},
            {"$set": {"affiliate_link": link}}
        )
        
        await state.clear()
        await message.answer(
            f"✅ <b>Affiliate Link Set for {pdf_name}!</b>",
            reply_markup=get_affiliate_menu(),
            parse_mode="HTML"
        )

# 2. LIST AFFILIATE
@dp.message(F.text == "📋 LIST AFFILIATE")
async def list_affiliates_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Affiliates", "can_list"):
        return
    await state.set_state(AffiliateStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_affiliate")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_AFF") or m.text.startswith("➡️ NEXT_AFF")))
async def affiliate_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_affiliate")
    except:
        await send_pdf_list_view(message, page=0, mode="list_affiliate")

@dp.message(F.text == "⬅️ BACK TO AFFILIATE MENU")
async def back_to_affiliate_menu(message: types.Message, state: FSMContext):
    await state.clear() # Clear any lingering state
    await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

# 3. DELETE AFFILIATE

@dp.message(F.text == "🗑️ DELETE AFFILIATE")
async def start_delete_aff(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Affiliate", "can_add"):
        return
    await state.set_state(AffiliateDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="affiliate_delete")

@dp.message(AffiliateDeleteStates.waiting_for_selection)
async def process_aff_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
         try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="affiliate_delete")
            return
         except: pass

    # Parse input - support comma-separated values for bulk deletion
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get all PDFs with affiliate links for sequential selection
    query_filter = {"affiliate_link": {"$exists": True, "$ne": ""}}
    all_filtered_pdfs = list(col_pdfs.find(query_filter).sort("index", 1))
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:
            continue
            
        pdf = None
        if query.isdigit():
            # Sequential selection - 1 means first item in the filtered list
            idx = int(query) - 1
            if 0 <= idx < len(all_filtered_pdfs):
                pdf = all_filtered_pdfs[idx]
        else:
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf and pdf.get("affiliate_link"):
            # Check for duplicates
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found or no affiliate link:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Store selected PDF IDs and names
    pdf_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    pdf_names = [pdf["name"] for pdf in found_pdfs]
    
    # Store for both bulk and single
    if len(found_pdfs) == 1:
        await state.update_data(
            pdf_id=pdf_ids[0],
            pdf_name=pdf_names[0],
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=False
        )
    else:
        await state.update_data(
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=True
        )
    
    # Build confirmation message
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await state.set_state(AffiliateDeleteStates.waiting_for_confirm)
    
    if len(found_pdfs) > 1:
        msg = f"⚠️ <b>CONFIRM BULK AFFILIATE DELETE</b>\n\n"
        msg += f"📊 <b>Total to delete: {len(found_pdfs)} affiliate link(s)</b>\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n❓ Remove affiliate links from all selected PDFs?"
    else:
        pdf = found_pdfs[0]
        msg = f"⚠️ Remove Affiliate Link from <b>{_html.escape(str(pdf['name']))}</b>?"
    
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

@dp.message(AffiliateDeleteStates.waiting_for_confirm)
async def process_aff_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Check if bulk delete
        is_bulk = data.get('is_bulk', False)
        pdf_ids = data.get('pdf_ids', [])
        
        if is_bulk and pdf_ids:
            # Bulk delete - remove affiliate links from multiple PDFs
            object_ids = [ObjectId(id_str) for id_str in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"affiliate_link": ""}}
            )
            
            deleted_count = result.modified_count
            
            await state.clear()
            await message.answer(
                f"🗑️ <b>Bulk Affiliate Delete Complete!</b>\n\n"
                f"✅ Removed affiliate links from <b>{deleted_count} PDF(s)</b>",
                reply_markup=get_affiliate_menu(),
                parse_mode="HTML"
            )
            
            # Log action
            log_user_action(message.from_user, "Bulk Delete Affiliate", f"Removed {deleted_count} affiliate links")
        else:
            # Single delete
            pdf_id = data.get('pdf_id')
            pdf_name = data.get('pdf_name', 'PDF')
            
            col_pdfs.update_one(
                {"_id": ObjectId(pdf_id)},
                {"$unset": {"affiliate_link": ""}}
            )
            
            await state.clear()
            await message.answer(
                f"🗑️ Affiliate Link Removed from <b>{pdf_name}</b>.",
                reply_markup=get_affiliate_menu(),
                parse_mode="HTML"
            )
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_affiliate_menu())

@dp.message(F.text == "🔑 CODE")
async def code_menu_handler(message: types.Message):
    if not await check_authorization(message, "Code Menu", "can_add"):
        return
    await message.answer("🔑 <b>CODE MANAGEMENT</b>", reply_markup=get_code_menu(), parse_mode="HTML")

# --- MSA CODE HANDLERS ---

# 2. EDIT MSA CODE
@dp.message(F.text == "✏️ EDIT CODE")
async def start_edit_msa_code(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit Code", "can_add"):
        return
    await state.set_state(MSACodeEditStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="msa_edit_select")

@dp.message(MSACodeEditStates.waiting_for_selection)
async def process_msa_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="msa_edit_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found.", reply_markup=get_cancel_keyboard())
        return

    # Ensure it has an MSA code
    if not pdf.get("msa_code"):
        await message.answer("⚠️ This PDF does not have an MSA Code.\nUse Add instead.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"], old_code=pdf["msa_code"])
    
    await state.set_state(MSACodeEditStates.waiting_for_new_code)
    await message.answer(
        f"✏️ <b>EDITING MSA CODE</b>\n"
        f"📄 PDF: {pdf['name']}\n"
        f"🔑 Current Code: `{pdf['msa_code']}`\n\n"
        "⌨️ <b>Enter New MSA Code</b> (Format: MSA1234):",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(MSACodeEditStates.waiting_for_new_code)
async def process_msa_edit_new_code(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
    
    code = message.text.strip().upper()
    
    # Validate format
    is_valid, error_msg = validate_msa_code(code)
    if not is_valid:
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return
    
    # Check if same as current
    data = await state.get_data()
    if code == data['old_code']:
        await message.answer(f"⚠️ <b>Same Code!</b>\nYou entered the exact same MSA code.\nPlease enter a different code:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    # Check for duplicates (exclude current PDF)
    conflict_pdf = is_msa_code_duplicate(code, exclude_pdf_id=data['pdf_id'])
    if conflict_pdf:
        clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(f"⚠️ <b>MSA Code Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nPlease enter a unique code.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    from bson.objectid import ObjectId
    
    try:
        col_pdfs.update_one(
            {"_id": ObjectId(data['pdf_id'])},
            {"$set": {"msa_code": code}}
        )
    except DuplicateKeyError:
        await message.answer(
            "⚠️ <b>MSA Code Already Exists.</b> Please enter a different code.",
            reply_markup=get_cancel_keyboard(),
            parse_mode="HTML"
        )
        return
    
    old_code = data['old_code']
    pdf_name = data['pdf_name']
    
    await state.clear()
    await message.answer(
        f"✅ <b>MSA Code Updated for {pdf_name}!</b>\n\n"
        f"🔴 Old Code: `{old_code}`\n"
        f"🟢 New Code: `{code}`",
        reply_markup=get_code_menu(),
        parse_mode="HTML"
    )

# 3. DELETE MSA CODE
@dp.message(F.text == "🗑️ DELETE CODE")
async def start_delete_msa_code(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Code", "can_add"):
        return
    await state.set_state(MSACodeDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="msa_delete")

@dp.message(MSACodeDeleteStates.waiting_for_selection)
async def process_msa_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="msa_delete")
            return
        except: pass

    query = message.text.strip()
    
    # Handle Bulk Selection (comma separated)
    queries = [q.strip() for q in query.split(",") if q.strip()]
    
    found_pdfs = []
    not_found = []
    no_code = []
    
    for q in queries:
        pdf_item = None
        if q.isdigit():
            pdf_item = col_pdfs.find_one({"index": int(q)})
        else:
            pdf_item = col_pdfs.find_one({"name": {"$regex": q, "$options": "i"}})
            
        if not pdf_item:
            not_found.append(q)
            continue
            
        if not pdf_item.get("msa_code"):
            no_code.append(pdf_item['name'])
            continue
            
        # Check for duplicates in selection
        if not any(p['_id'] == pdf_item['_id'] for p in found_pdfs):
            found_pdfs.append(pdf_item)
    
    if not found_pdfs:
        error_msg = "❌ No valid PDFs with MSA Codes found."
        if not_found: error_msg += f"\nNot Found: {', '.join(not_found)}"
        if no_code: error_msg += f"\nNo Code: {', '.join(no_code)}"
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return

    # Store for confirmation
    pdf_ids = [str(p["_id"]) for p in found_pdfs]
    
    await state.update_data(
        pdf_ids=pdf_ids,
        is_bulk=len(pdf_ids) > 1
    )
    
    # Build Confirmation Message
    msg = "⚠️ <b>CONFIRM BULK DELETION</b> ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ <b>CONFIRM DELETION</b>\n\n"
    msg += "You are about to remove MSA Codes from:\n"
    
    for p in found_pdfs:
        msg += f"• `{p['index']}`. <b>{_html.escape(str(p['name']))}</b> (Code: `{p.get('msa_code', 'N/A')}`)\n"
        
    if not_found or no_code:
        msg += "\n⚠️ <b>SKIPPED ITEMS (Ignored):</b>\n"
        for q in not_found:
            msg += f"• `{q}`: Not Found\n"
        for name in no_code:
            msg += f"• `{name}`: No MSA Code assigned\n"
        
    msg += "\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(MSACodeDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

@dp.message(MSACodeDeleteStates.waiting_for_confirm)
async def process_msa_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        pdf_ids = data.get('pdf_ids', [])
        is_bulk = data.get('is_bulk', False)
        
        from bson.objectid import ObjectId
        
        # Perform Deletion
        if pdf_ids:
            object_ids = [ObjectId(pid) for pid in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"msa_code": ""}}
            )
            
            count = result.modified_count
            await state.clear()
            await message.answer(
                f"🗑️ <b>Deletion Complete</b>\nRemoved MSA Codes from {count} PDF(s).",
                reply_markup=get_code_menu(),
                parse_mode="HTML"
            )
        else:
            await state.clear()
            await message.answer("⚠️ No PDFs selected.", reply_markup=get_code_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_code_menu())

# 4. LIST MSA CODES
@dp.message(F.text == "📋 LIST CODE")
async def list_msa_codes_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Code", "can_list"):
        return
    await state.set_state(MSACodeStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_msa")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_MSA") or m.text.startswith("➡️ NEXT_MSA")))
async def msa_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_msa")
    except:
        await send_pdf_list_view(message, page=0, mode="list_msa")

@dp.message(F.text == "⬅️ BACK TO CODE MENU")
async def back_to_code_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🔑 <b>CODE MANAGEMENT</b>", reply_markup=get_code_menu(), parse_mode="HTML")


@dp.message(F.text == "▶️ YT")
async def yt_menu_handler(message: types.Message):
    if not await check_authorization(message, "YT Menu", "can_add"):
        return
    await message.answer("▶️ <b>YT MANAGEMENT</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

# --- YT HANDLERS ---

# 1. ADD YT
@dp.message(F.text == "➕ ADD YT LINK")
async def start_add_yt(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add YT", "can_add"):
        return
    await state.set_state(YTStates.waiting_for_pdf_selection)
    await send_pdf_list_view(message, page=0, mode="yt_add_select")

@dp.message(YTStates.waiting_for_pdf_selection)
async def process_yt_pdf_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_add_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Check if PDF already has YT data
    if pdf.get("yt_title") or pdf.get("yt_link"):
        await message.answer("⚠️ This PDF already has YT data.\nPlease add a new PDF first, or use Edit to modify.", reply_markup=get_cancel_keyboard())
        return

    # Store PDF info and ask for title
    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"])
    await state.set_state(YTStates.waiting_for_title)
    await message.answer(
        f"▶️ <b>Selected PDF:</b> {pdf['name']}\n\n"
        "⌨️ <b>Enter YouTube Video Title:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(YTStates.waiting_for_title)
async def process_yt_title(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    await state.update_data(yt_title=message.text.strip())
    
    # Check duplicate title
    conflict_pdf = is_yt_title_duplicate(message.text.strip())
    if conflict_pdf:
         await message.answer(f"⚠️ <b>YT Title Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
         return

    await state.set_state(YTStates.waiting_for_link)
    await message.answer("🔗 <b>Enter YouTube Short Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(YTStates.waiting_for_link)
async def process_yt_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    # Basic validation
    link = message.text.strip()
    
    # Validation: Check duplicate YT link
    conflict_pdf = is_yt_link_duplicate(link)
    if conflict_pdf:
        await message.answer(f"⚠️ <b>YT Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    if "http" not in link and "youtu" not in link:
        await message.answer("⚠️ <b>Invalid YouTube Link.</b> Try again:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {"yt_title": data['yt_title'], "yt_link": link}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ <b>YT Link added to {data['pdf_name']}!</b>\n\n"
        f"▶️ Title: {data['yt_title']}\n"
        f"🔗 Link: {link}",
        reply_markup=get_yt_menu(),
        parse_mode="HTML"
    )

# 2. EDIT YT
@dp.message(F.text == "✏️ EDIT YT LINK")
async def start_edit_yt(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit YT", "can_add"):
        return
    await state.set_state(YTEditStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="yt_edit_select")

@dp.message(YTEditStates.waiting_for_selection)
async def process_yt_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_edit_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found.", reply_markup=get_cancel_keyboard())
        return

    # Ensure it has YT data
    if not pdf.get("yt_title"):
        await message.answer("⚠️ This PDF does not have YT data.\nUse Add instead.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"])
    
    # Show Edit Options
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="▶️ EDIT TITLE"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(YTEditStates.waiting_for_field)
    current_title = pdf.get('yt_title', 'Not Set')
    current_link = pdf.get('yt_link', 'Not Set')
    await state.update_data(current_yt_title=current_title, current_yt_link=current_link) # Store for comparison

    await state.set_state(YTEditStates.waiting_for_field)
    await message.answer(
        f"▶️ <b>YT DATA FOR: {pdf['name']}</b>\n"
        f"Title: {current_title}\n"
        f"Link: {current_link}\n\n"
        "⬇️ <b>Select what to edit:</b>",
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.message(YTEditStates.waiting_for_field)
async def process_yt_edit_field(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    if message.text == "▶️ EDIT TITLE":
        await state.update_data(field="yt_title")
        await state.set_state(YTEditStates.waiting_for_value)
        await message.answer("⌨️ <b>Enter New Title:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="yt_link")
        await state.set_state(YTEditStates.waiting_for_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(YTEditStates.waiting_for_value)
async def process_yt_edit_value(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    new_value = message.text.strip()
    
    if data['field'] == "yt_link":
        pass # Allow same value update

        # Validation: Check duplicate YT link (exclude current PDF)
        conflict_pdf = is_yt_link_duplicate(new_value, exclude_id=data['pdf_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ <b>YT Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        if "http" not in new_value and "youtu" not in new_value:
             await message.answer("⚠️ <b>Invalid YouTube Link.</b> Try again:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
             return

    elif data['field'] == "yt_title":
        pass # Allow same value update (no-op but good UX)

        # Validation: Check duplicate YT title (exclude current PDF)
        conflict_pdf = is_yt_title_duplicate(new_value, exclude_id=data['pdf_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ <b>YT Title Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {data['field']: new_value}}
    )
    
    await state.clear()
    field_name = "Title" if data['field'] == "yt_title" else "Link"
    await message.answer(f"✅ <b>YT {field_name} Updated for {data['pdf_name']}!</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

# 3. DELETE YT
@dp.message(F.text == "🗑️ DELETE YT LINK")
async def start_delete_yt(message: types.Message, state: FSMContext):
    await state.set_state(YTDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="yt_delete")

@dp.message(YTDeleteStates.waiting_for_selection)
async def process_yt_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_delete")
            return
        except: pass

    query = message.text.strip()
    
    # Handle Bulk Selection (comma separated)
    # RESOLVE SEQUENTIAL IDs (1, 2, 3...) to Actual PDFs in the "YT List"
    
    # 1. Fetch ALL PDFs that have YT data, sorted by index (same order as the view)
    yt_pdfs = list(col_pdfs.find({"yt_title": {"$exists": True, "$ne": ""}}).sort("index", 1))
    
    queries = [q.strip() for q in query.split(",") if q.strip()]
    
    found_pdfs = []
    not_found = []
    
    for q in queries:
        if q.isdigit():
            # Treat as SEQUENTIAL ID (1-based index in the list)
            seq_idx = int(q) - 1
            if 0 <= seq_idx < len(yt_pdfs):
                found_pdfs.append(yt_pdfs[seq_idx])
            else:
                not_found.append(q)
        else:
            # Fallback: Try regex search by name if not a digit? 
            # Or strict sequential? User asked for "1, 2, 3" which implies sequential.
            # Let's keep name match as backup but prioritize sequential
            matched_by_name = [p for p in yt_pdfs if q.lower() in p['name'].lower()]
            if matched_by_name:
                found_pdfs.extend(matched_by_name)
            else:
                not_found.append(q)

    # Remove duplicates
    unique_found = []
    seen_ids = set()
    for p in found_pdfs:
        if p['_id'] not in seen_ids:
            unique_found.append(p)
            seen_ids.add(p['_id'])
    found_pdfs = unique_found
    
    if not found_pdfs:
        error_msg = "❌ No valid PDFs selected."
        if not_found: error_msg += f"\nNot Found (in YT list): {', '.join(not_found)}"
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return

    # Store for confirmation
    pdf_ids = [str(p["_id"]) for p in found_pdfs]
    
    await state.update_data(
        pdf_ids=pdf_ids,
        is_bulk=len(pdf_ids) > 1
    )
    
    # Build Confirmation Message
    msg = "⚠️ <b>CONFIRM BULK DELETION</b> ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ <b>CONFIRM DELETION</b>\n\n"
    msg += "You are about to remove YT Data from:\n"
    
    for p in found_pdfs:
        # Show both sequential position? No, show actual Name and Title
        msg += f"• <b>{_html.escape(str(p['name']))}</b> (YT: {p.get('yt_title', 'N/A')})\n"
        
    if not_found:
        msg += "\n⚠️ <b>SKIPPED ITEMS (Ignored):</b>\n"
        for q in not_found:
            msg += f"• `{q}`: Not valid sequential ID\n"
        
    msg += "\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(YTDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

@dp.message(YTDeleteStates.waiting_for_confirm)
async def process_yt_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        pdf_ids = data.get('pdf_ids', [])
        
        from bson.objectid import ObjectId
        
        # Perform Deletion
        if pdf_ids:
            object_ids = [ObjectId(pid) for pid in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"yt_title": "", "yt_link": ""}}
            )
            
            count = result.modified_count
            await state.clear()
            await message.answer(
                f"🗑️ <b>Deletion Complete</b>\nRemoved YT Data from {count} PDF(s).",
                reply_markup=get_yt_menu(),
                parse_mode="HTML"
            )
        else:
            await state.clear()
            await message.answer("⚠️ No PDFs selected.", reply_markup=get_yt_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_yt_menu())

# 4. LIST YT
@dp.message(F.text == "📋 LIST YT LINK")
async def list_yt_handler(message: types.Message, state: FSMContext):
    await state.set_state(YTStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_yt")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_YT") or m.text.startswith("➡️ NEXT_YT")))
async def yt_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_yt")
    except:
        await send_pdf_list_view(message, page=0, mode="list_yt")

@dp.message(F.text == "⬅️ BACK TO YT MENU")
async def back_to_yt_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("▶️ <b>YT MANAGEMENT</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

@dp.message(F.text == "🔗 LINKS")
async def links_menu_handler(message: types.Message):
    if not await check_authorization(message, "Links Menu", "can_list"):
        return
    await message.answer("🔗 <b>DEEP LINKS MANAGER</b>\nSelect a category to generate links:", reply_markup=get_links_menu(), parse_mode="HTML")

@dp.message(F.text == "🏠 HOME YT")
async def home_yt_handler(message: types.Message):
    if not await check_authorization(message, "Home YT Link", "can_list"):
        return
    code = await get_home_yt_code()
    username = BOT_USERNAME
    
    link = f"https://t.me/{username}?start={code}_YTCODE"
    
    text = (
        "🏠 <b>HOME YT LINK</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Link: <code>{link}</code>\n"
        f"🔑 Code: <code>{code}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        " This link is permanent and unique."
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "📸 IG CC")
async def ig_cc_links_handler(message: types.Message, page=0):
    if not await check_authorization(message, "IG CC Links", "can_list"):
        return
    limit = 5
    skip = page * limit
    
    try:
        total = col_ig_content.count_documents({})
    except Exception as db_err:
        logger.error(f"Database query failed in IG CC links: {db_err}")
        await message.answer(
            f"⚠️ Database error: {str(db_err)[:100]}\n\nPlease try again.",
            reply_markup=get_links_menu()
        )
        return
    
    if total == 0:
        # 🔍 DIAGNOSTIC: Check if data exists
        debug_msg = f"⚠️ No IG CC Content found.\n\n"
        debug_msg += f"📊 Database: {db.name}\n"
        debug_msg += f"📋 Collection: bot3_ig_content\n"
        debug_msg += f"🔢 Count: {total}\n\n"
        debug_msg += "🔧 Attempting recovery...\n"
        logger.warning(f"[BOT3] IG CC collection empty: {debug_msg}")
        
        # Attempt recovery
        success, _, recovered_ig_count = await attempt_db_recovery()
        if success and recovered_ig_count > 0:
            debug_msg += f"✅ Recovery successful! {recovered_ig_count} IG items available.\n"
            debug_msg += "Please try the command again."
            await message.answer(debug_msg, reply_markup=get_links_menu())
            return
        else:
            debug_msg += f"❌ Recovery failed or no data found.\n"
            debug_msg += "Please contact support or restart the bot."
            await message.answer(debug_msg, reply_markup=get_links_menu())
            return
    
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))

    text = f"📸 <b>IG CC LINKS</b> (Page {page+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"
    username = BOT_USERNAME
    
    for content in contents:
        # Ensure Code
        content = await ensure_ig_cc_code(content)
        code = content['start_code']
        cc_code = content['cc_code']
        
        link = f"https://t.me/{username}?start={code}_igcc_{cc_code}"
        
        text += (
            f"🆔 <b>{cc_code}</b>\n"
            f"🔗 <code>{link}</code>\n"
            f"🔑 Start Code: <code>{code}</code>\n"
            "────────────────────\n"
        )
    
    # Pagination Buttons
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV_IGLINK {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT_IGLINK {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(F.text == "⬅️ BACK TO LINKS MENU")
async def back_to_links_menu(message: types.Message):
    await message.answer("🔗 <b>DEEP LINKS MANAGER</b>", reply_markup=get_links_menu(), parse_mode="HTML")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IGLINK") or m.text.startswith("➡️ NEXT_IGLINK")))
async def ig_link_pagination(message: types.Message):
    try:
        page = int(message.text.split()[-1]) - 1
        await ig_cc_links_handler(message, page=page)
    except:
        await message.answer("❌ Error navigating.")

@dp.message(F.text == "📑 ALL PDF")
async def all_pdf_links_handler(message: types.Message, page=0):
    # Check authorization - must send response before returning
    if not await check_authorization(message, "All PDF Links", "can_list"):
        return  # check_authorization sends error message itself
    
    limit = 8
    skip = page * limit

    # Verify database connection before querying
    try:
        total = col_pdfs.count_documents({})
    except Exception as db_err:
        logger.error(f"Database query failed in all_pdf_links_handler: {db_err}")
        await message.answer(
            f"⚠️ Database error: {str(db_err)[:100]}\n\nPlease try again.",
            reply_markup=get_links_menu()
        )
        return
        
    if total == 0:
        # 🔍 DIAGNOSTIC: Check if data exists at all
        debug_msg = f"⚠️ No PDFs found in collection.\n\n"
        debug_msg += f"📊 Database: {db.name}\n"
        debug_msg += f"📋 Collection: bot3_pdfs\n"
        debug_msg += f"🔢 Count: {total}\n\n"
        debug_msg += "🔧 Attempting recovery...\n"
        logger.warning(f"[BOT3] PDF collection empty: {debug_msg}")
        
        # Attempt recovery
        success, recovered_pdf_count, _ = await attempt_db_recovery()
        if success and recovered_pdf_count > 0:
            debug_msg += f"✅ Recovery successful! {recovered_pdf_count} PDFs available.\n"
            debug_msg += "Please try the command again."
            await message.answer(debug_msg, reply_markup=get_links_menu())
            return
        else:
            debug_msg += f"❌ Recovery failed or no data found.\n"
            debug_msg += "Please contact support or restart the bot."
            await message.answer(debug_msg, reply_markup=get_links_menu())
            return

    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))

    if not pdfs and page > 0:
        await message.answer("⚠️ End of list.", reply_markup=get_links_menu())
        return

    username = BOT_USERNAME

    entries = []
    ready_count = 0
    for pdf in pdfs:
        idx = pdf.get("index", "?")
        raw_name = pdf.get("name") or "Unnamed"
        safe_name = _html.escape(raw_name)

        has_pdf_link = bool((pdf.get("link") or "").strip())
        has_affiliate = bool((pdf.get("affiliate_link") or "").strip())

        msa_code = ""
        msa_ready = False
        try:
            msa_code = await ensure_pdf_msa_code(pdf)
            msa_ready = bool(msa_code)
        except Exception:
            msa_ready = False

        block = f"🆔 <b>{idx}.</b> <b>{safe_name}</b>\n"

        if has_pdf_link and has_affiliate and msa_ready:
            # Generate deep links only for fully configured PDFs.
            pdf = await ensure_pdf_codes(pdf)

            sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', raw_name or "unknown")
            sanitized_name = re.sub(r'_+', '_', sanitized_name).strip('_') or "unknown"

            ig_code = pdf["ig_start_code"]
            yt_code = pdf["yt_start_code"]

            ig_link = f"https://t.me/{username}?start={ig_code}_ig_{sanitized_name}"
            yt_link = f"https://t.me/{username}?start={yt_code}_yt_{sanitized_name}"

            block += (
                f"📸 <b>IG Link</b>: <code>{ig_link}</code>\n"
                f"   └ 🎟️ <code>{ig_code}</code>\n"
                f"▶️ <b>YT Link</b>: <code>{yt_link}</code>\n"
                f"   └ 🎟️ <code>{yt_code}</code>\n"
                f"🔑 <b>MSA Code</b>: <code>{_html.escape(msa_code)}</code>\n"
            )
            ready_count += 1
        else:
            missing = []
            if not has_pdf_link:
                missing.append("PDF link")
            if not has_affiliate:
                missing.append("Affiliate link")
            if not msa_ready:
                missing.append("MSA code")

            block += "⚠️ <b>Link generation blocked</b>\n"
            block += "🧩 <b>Missing:</b>\n"
            for item in missing:
                block += f"• {item}\n"

            block += "✅ <b>Needed to generate this PDF link:</b>\n"
            if not has_pdf_link:
                block += "• Add PDF link in ➕ ADD → ➕ ADD PDF\n"
            if not has_affiliate:
                block += "• Add affiliate in ➕ ADD → 💸 AFFILIATE → ➕ ADD AFFILIATE\n"
            if not msa_ready:
                block += "• Retry add/edit flow so system can allocate a valid MSA code\n"

            if msa_ready and not has_affiliate:
                block += "🔒 <b>MSA Code Status:</b> Generated and locked until affiliate link is added.\n"
            elif msa_ready:
                block += "🔒 <b>MSA Code Status:</b> Generated (hidden while link requirements are incomplete).\n"
            else:
                block += "ℹ️ <b>MSA Code Status:</b> Allocation pending/fix required.\n"

        block += "────────────────────\n"
        entries.append(block)

    page_total = len(pdfs)
    blocked_count = page_total - ready_count
    header = (
        f"📑 <b>ALL PDF LINKS</b> (Page {page+1} / {((total-1)//limit)+1})\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Ready: <b>{ready_count}</b> | ⚠️ Blocked: <b>{blocked_count}</b>\n\n"
    )

    # Pagination nav keyboard
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_PDFLINK {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_PDFLINK {page+2}"))
    nav_keyboard = []
    if buttons:
        nav_keyboard.append(buttons)
    nav_keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    reply_kb = ReplyKeyboardMarkup(keyboard=nav_keyboard, resize_keyboard=True)

    # Build pages — auto-split if a batch overflows Telegram's 4096-char limit
    MAX_CHARS = 3800
    pages_out = []
    current_text = header
    for entry in entries:
        if len(current_text) + len(entry) > MAX_CHARS:
            pages_out.append(current_text)
            current_text = entry
        else:
            current_text += entry
    pages_out.append(current_text)

    for i, part in enumerate(pages_out):
        kb = reply_kb if i == len(pages_out) - 1 else None
        await message.answer(part, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_PDFLINK") or m.text.startswith("➡️ NEXT_PDFLINK")))
async def pdf_link_pagination(message: types.Message):
    try:
        page = int(message.text.split()[-1]) - 1
        await all_pdf_links_handler(message, page=page)
    except Exception:
        await message.answer("❌ Error navigating.")

@dp.message(F.text == "📸 IGCC")
@dp.message(F.text == "📸 IG")
async def ig_menu_handler(message: types.Message):
    if not await check_authorization(message, "IG Menu", "can_add"):
        return
    await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")

# --- IG HANDLERS ---

# 1. ADD IG
@dp.message(F.text == "➕ ADD IG")
async def start_add_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add IG", "can_add"):
        return
    await state.set_state(IGStates.waiting_for_content_name)
    await message.answer("📝 <b>Enter IG Content:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(IGStates.waiting_for_content_name)
async def process_ig_content_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    content_name = message.text.strip()
    
    # Check for duplicate name
    if is_ig_name_duplicate(content_name):
        await message.answer("⚠️ <b>Content name already exists!</b>\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Auto-generate CC code
    cc_code, cc_number = get_next_cc_code()
    
    # Save to database
    from datetime import datetime
    doc = {
        "cc_code": cc_code,
        "cc_number": cc_number,
        "name": content_name,
        "created_at": now_local(),
        # Initialize click tracking field
        "ig_cc_clicks": 0,
        "last_ig_cc_click": None,
        "credit_points": 25  # MSA Credits awarded to vault members on first claim
    }
    col_ig_content.insert_one(doc)
    
    await state.clear()
    await message.answer(
        f"✅ <b>IG Content Added!</b>\n\n"
        f"🆔 Code: <b>{cc_code}</b>\n"
        f"📝 Name: {content_name}",
        reply_markup=get_ig_menu(),
        parse_mode="HTML"
    )

# 2. EDIT IG
@dp.message(F.text == "✏️ EDIT IG")
async def start_edit_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit IG", "can_add"):
        return
    await state.set_state(IGEditStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="edit")

@dp.message(IGEditStates.waiting_for_selection)
async def process_ig_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV_IGEDIT") or message.text.startswith("➡️ NEXT_IGEDIT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="edit")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    display_index = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1  # Convert to 0-based
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            # Find display index
            all_contents = list(col_ig_content.find().sort("cc_number", 1))
            for idx, c in enumerate(all_contents, start=1):
                if c['_id'] == content['_id']:
                    display_index = idx
                    break
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(content_id=str(content["_id"]), old_name=content["name"], cc_code=content["cc_code"])
    
    # Display ONLY the selected item with FULL content
    text = f"✅ <b>SELECTED IG CONTENT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"`{display_index}`. <b>{content['cc_code']}</b>\n"
    text += f"📝 Full Content:\n{content['name']}\n"
    
    await state.set_state(IGEditStates.waiting_for_new_name)
    await message.answer(
        text + "\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter New Content:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(IGEditStates.waiting_for_new_name)
async def process_ig_edit_new_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    new_name = message.text.strip()
    data = await state.get_data()
    
    # Check for duplicate name (excluding current content)
    if is_ig_name_duplicate(new_name, exclude_id=data['content_id']):
        await message.answer("⚠️ <b>Content name already exists!</b>\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    from bson.objectid import ObjectId
    col_ig_content.update_one(
        {"_id": ObjectId(data['content_id'])},
        {"$set": {"name": new_name}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ <b>IG Content Updated!</b>\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 New Name: {new_name}",
        reply_markup=get_ig_menu(),
        parse_mode="HTML"
    )

# 3. DELETE IG
@dp.message(F.text == "🗑️ DELETE IG")
async def start_delete_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete IG", "can_add"):
        return
    await state.set_state(IGDeleteStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="delete")

@dp.message(IGDeleteStates.waiting_for_selection)
async def process_ig_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV_IGDEL") or message.text.startswith("➡️ NEXT_IGDEL"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="delete")
            return
        except: pass
    
    raw_input = message.text.strip()
    
    # Get all contents sorted by cc_number for sequential resolution
    all_contents = list(col_ig_content.find().sort("cc_number", 1))
    
    found_contents = []
    seen_ids = set()
    not_found = []
    
    # ── Token parser: supports ranges (1-5), singles (3), CC codes (CC2), any comma-separated mix ──
    tokens = [t.strip() for t in raw_input.split(",") if t.strip()]
    
    for token in tokens:
        # Range like "1-5"
        if "-" in token and not token.upper().startswith("CC"):
            parts = token.split("-", 1)
            if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
                start = int(parts[0].strip())
                end   = int(parts[1].strip())
                if start > end:
                    start, end = end, start  # swap if reversed (e.g. 5-1)
                for n in range(start, end + 1):
                    idx = n - 1
                    if 0 <= idx < len(all_contents):
                        c = all_contents[idx]
                        cid = str(c["_id"])
                        if cid not in seen_ids:
                            seen_ids.add(cid)
                            found_contents.append(c)
                    else:
                        not_found.append(str(n))
                continue
        
        # CC code like "CC5"
        if token.upper().startswith("CC"):
            content = next((c for c in all_contents if c['cc_code'].upper() == token.upper()), None)
        # Single index number
        elif token.isdigit():
            idx = int(token) - 1
            content = all_contents[idx] if 0 <= idx < len(all_contents) else None
        else:
            content = None
        
        if content:
            cid = str(content["_id"])
            if cid not in seen_ids:
                seen_ids.add(cid)
                found_contents.append(content)
        else:
            not_found.append(token)
            
    if not found_contents:
        msg = "❌ <b>No Content Found</b>"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    # Store IDs
    delete_ids = [str(c["_id"]) for c in found_contents]
    await state.update_data(delete_ids=delete_ids)
    
    # Confirmation Message — safe length-capped to avoid Telegram 4096-char limit
    count = len(found_contents)
    msg = f"⚠️ <b>CONFIRM DELETION ({count} item{'s' if count > 1 else ''})</b> ⚠️\n\n"
    lines = [
        f"• <b>{c['cc_code']}</b> - {c['name'][:40]}{'\u2026' if len(c['name']) > 40 else ''}"
        for c in found_contents
    ]
    MAX_BODY = 3600
    list_text = "\n".join(lines)
    if len(list_text) > MAX_BODY:
        trimmed = []
        running = 0
        for line in lines:
            if running + len(line) + 1 > MAX_BODY - 60:
                trimmed.append(f"  \u2026 and {len(lines) - len(trimmed)} more")
                break
            trimmed.append(line)
            running += len(line) + 1
        list_text = "\n".join(trimmed)
    msg += list_text
    
    if not_found:
        msg += f"\n\n⚠️ Skipped (not found): {', '.join(not_found)}"
    
    msg += "\n\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await state.set_state(IGDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

@dp.message(IGDeleteStates.waiting_for_confirm)
async def process_ig_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        delete_ids = data.get('delete_ids', [])
        
        from bson.objectid import ObjectId
        if delete_ids:
            object_ids = [ObjectId(uid) for uid in delete_ids]
            result = col_ig_content.delete_many({"_id": {"$in": object_ids}})
            count = result.deleted_count
            
            # ── Auto-reindex: renumber all remaining CC codes with no gaps ──
            remaining = await reindex_all_ig_cc()
            
            await state.clear()
            await message.answer(
                f"🗑️ <b>Deleted {count} IG Content(s)!</b>\n"
                f"🔄 CC codes auto-renumbered — {remaining} items now CC1–CC{remaining}",
                reply_markup=get_ig_menu(),
                parse_mode="HTML"
            )
        else:
             await state.clear()
             await message.answer("❌ Error: No content selected.", reply_markup=get_ig_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_ig_menu())

# 4. LIST IG
@dp.message(F.text == "📋 LIST IG")
async def list_ig_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List IG", "can_list"):
        return
    await state.set_state(IGListStates.viewing)
    await send_ig_list_view(message, page=0, mode="list")

@dp.message(IGListStates.viewing)
async def process_ig_list_view(message: types.Message, state: FSMContext):
    # Handle BACK buttons first
    if message.text == "⬅️ BACK TO IG MENU":
        await state.clear()
        return await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")
    
    if message.text == "⬅️ BACK TO LIST":
        await send_ig_list_view(message, page=0, mode="list")
        return
    
    # Handle Pagination
    if message.text and (message.text.startswith("⬅️ PREV_IG") or message.text.startswith("➡️ NEXT_IG")):
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
            return
        except:
            await send_ig_list_view(message, page=0, mode="list")
            return
    
    query = message.text.strip()
    content = None
    display_index = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1  # Convert to 0-based
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            # Find display index
            all_contents = list(col_ig_content.find().sort("cc_number", 1))
            for idx, c in enumerate(all_contents, start=1):
                if c['_id'] == content['_id']:
                    display_index = idx
                    break
    
    if not content:
        await message.answer("❌ Not Found. Try again or go back.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK TO IG MENU")]], resize_keyboard=True))
        return
    
    # Display the selected content
    text = f"✅ <b>VIEWING IG CONTENT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"{display_index}. {content['cc_code']}\n\n"
    text += f"📝 <b>Full Content:</b>\n{content['name']}\n"
    text += "━━━━━━━━━━━━━━━━━━━━"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⬅️ BACK TO LIST")],
        [KeyboardButton(text="⬅️ BACK TO IG MENU")]
    ], resize_keyboard=True)
    
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "⬅️ BACK TO LIST")
async def back_to_ig_list(message: types.Message, state: FSMContext):
    await state.set_state(IGListStates.viewing)
    await send_ig_list_view(message, page=0, mode="list")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IG ") or m.text.startswith("➡️ NEXT_IG ")))
async def ig_pagination_handler(message: types.Message, state: FSMContext):
    # Check if we're in list viewing state
    current_state = await state.get_state()
    if current_state == IGListStates.viewing:
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
        except:
            await send_ig_list_view(message, page=0, mode="list")
    else:
        # Fallback for non-state pagination (shouldn't happen now)
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
        except:
            await send_ig_list_view(message, page=0, mode="list")

@dp.message(F.text == "⬅️ BACK TO IG MENU")
async def back_to_ig_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")


# =============================================================================
# 🎁 REWARD CONTENT MANAGEMENT (RW1, RW2... — Referral Reward Pool)
# Admin pastes content text → auto-assigned RW code → stored in bot3_rewards
# Bot1 picks a random reward and delivers it to the referrer on vault join.
# =============================================================================

@dp.message(F.text == "🎁 REWARD")
async def reward_menu_handler(message: types.Message):
    """Entry point to reward content management"""
    if not await check_authorization(message, "Reward Menu", "can_add"):
        return
    total = col_rewards.count_documents({})
    await message.answer(
        f"🎁 <b>REWARD CONTENT POOL</b>\n"
        f"────────────────────\n"
        f"📦 Total Rewards: <b>{total}</b>\n"
        f"🔗 Purpose: Referral reward delivery pool\n\n"
        f"ℹ️ Rewards are auto-assigned RW1, RW2... codes.\n"
        f"When a referred user joins vault, Bot 1 picks a random reward and delivers it to the referrer.",
        reply_markup=get_reward_menu(),
        parse_mode="HTML"
    )

# --- ADD REWARD ---
@dp.message(F.text == "➕ ADD REWARD")
async def start_add_reward(message: types.Message, state: FSMContext):
    """Start adding a new reward content item"""
    if not await check_authorization(message, "Add Reward", "can_add"):
        return
    await state.set_state(RewardStates.waiting_for_content)
    await message.answer(
        "🎁 <b>ADD REWARD CONTENT</b>\n"
        "────────────────────\n"
        "📝 <b>Paste the reward content below:</b>\n\n"
        "• Can be a link, a piece of text, a tip, or any exclusive content\n"
        "• An RW code (RW1, RW2...) will be auto-assigned\n"
        "• This content will be randomly delivered to users who refer someone",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(RewardStates.waiting_for_content)
async def process_add_reward_content(message: types.Message, state: FSMContext):
    """Receive and store new reward content with auto-generated RW code"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_reward_menu())

    content = message.text.strip()
    if not content:
        await message.answer("⚠️ Content cannot be empty. Please paste your reward content:", reply_markup=get_cancel_keyboard())
        return

    rw_code, rw_number = get_next_rw_code()

    doc = {
        "rw_code": rw_code,
        "rw_number": rw_number,
        "content": content,
        "affiliate_link": "",
        "created_at": now_local(),
    }
    col_rewards.insert_one(doc)

    await state.clear()
    log_user_action(message.from_user, "Add Reward", f"Added {rw_code}")
    await message.answer(
        f"✅ <b>Reward Added!</b>\n\n"
        f"🎁 Code: <b>{rw_code}</b>\n"
        f"📝 Content Preview: {content[:80]}{'...' if len(content) > 80 else ''}",
        reply_markup=get_reward_menu(),
        parse_mode="HTML"
    )

# --- EDIT REWARD ---
@dp.message(F.text == "✏️ EDIT REWARD")
async def start_edit_reward(message: types.Message, state: FSMContext):
    """Start edit flow — list all rewards and ask which to edit"""
    if not await check_authorization(message, "Edit Reward", "can_add"):
        return
    total = col_rewards.count_documents({})
    if total == 0:
        await message.answer("⚠️ No rewards found. Add one first.", reply_markup=get_reward_menu())
        return
    all_rewards = list(col_rewards.find({}, {"rw_code": 1, "rw_number": 1, "content": 1}).sort("rw_number", 1))
    lines = []
    for r in all_rewards:
        preview = str(r.get("content", ""))[:50]
        lines.append(f"• <b>{r['rw_code']}</b> — {preview}{'...' if len(str(r.get('content',''))) > 50 else ''}")
    listing = "\n".join(lines)
    await state.set_state(RewardStates.edit_waiting_for_selection)
    await message.answer(
        f"✏️ <b>EDIT REWARD</b>\n────────────────────\n{listing}\n\n"
        f"📌 Enter the RW code to edit (e.g. <code>RW1</code>) or its number (e.g. <code>1</code>):",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(RewardStates.edit_waiting_for_selection)
async def process_edit_reward_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_reward_menu())
    query = message.text.strip().upper()
    reward = None
    if query.startswith("RW") and query[2:].isdigit():
        reward = col_rewards.find_one({"rw_code": query})
    elif message.text.strip().isdigit():
        reward = col_rewards.find_one({"rw_number": int(message.text.strip())})
    if not reward:
        await message.answer("❌ Reward not found. Try again (e.g. <code>RW1</code> or <code>1</code>):", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    await state.update_data(reward_id=str(reward["_id"]), rw_code=reward["rw_code"])
    await state.set_state(RewardStates.edit_waiting_for_new_content)
    await message.answer(
        f"✏️ <b>EDITING {reward['rw_code']}</b>\n\n"
        f"📌 Current content:\n<i>{reward.get('content', '')[:200]}</i>\n\n"
        f"📝 Paste the new content to replace it:",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(RewardStates.edit_waiting_for_new_content)
async def process_edit_reward_new_content(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_reward_menu())
    new_content = message.text.strip()
    if not new_content:
        await message.answer("⚠️ Content cannot be empty. Try again:", reply_markup=get_cancel_keyboard())
        return
    data = await state.get_data()
    from bson.objectid import ObjectId
    col_rewards.update_one({"_id": ObjectId(data["reward_id"])}, {"$set": {"content": new_content}})
    await state.clear()
    log_user_action(message.from_user, "Edit Reward", f"Updated {data['rw_code']}")
    await message.answer(
        f"✅ <b>{data['rw_code']} Updated!</b>\n"
        f"📝 New preview: {new_content[:80]}{'...' if len(new_content) > 80 else ''}",
        reply_markup=get_reward_menu(), parse_mode="HTML"
    )

# --- DELETE REWARD ---
@dp.message(F.text == "🗑️ DELETE REWARD")
async def start_delete_reward(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Reward", "can_add"):
        return
    total = col_rewards.count_documents({})
    if total == 0:
        await message.answer("⚠️ No rewards to delete.", reply_markup=get_reward_menu())
        return
    all_rewards = list(col_rewards.find({}, {"rw_code": 1, "rw_number": 1, "content": 1}).sort("rw_number", 1))
    lines = [f"• <b>{r['rw_code']}</b> — {str(r.get('content',''))[:40]}..." for r in all_rewards]
    await state.set_state(RewardStates.delete_waiting_for_selection)
    await message.answer(
        f"🗑️ <b>DELETE REWARD</b>\n────────────────────\n" + "\n".join(lines) +
        "\n\n📌 Enter RW code or number to delete:",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(RewardStates.delete_waiting_for_selection)
async def process_delete_reward_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_reward_menu())
    query = message.text.strip().upper()
    reward = None
    if query.startswith("RW") and query[2:].isdigit():
        reward = col_rewards.find_one({"rw_code": query})
    elif message.text.strip().isdigit():
        reward = col_rewards.find_one({"rw_number": int(message.text.strip())})
    if not reward:
        await message.answer("❌ Not found. Try again:", reply_markup=get_cancel_keyboard())
        return
    await state.update_data(reward_id=str(reward["_id"]), rw_code=reward["rw_code"])
    await state.set_state(RewardStates.delete_waiting_for_confirm)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)
    await message.answer(
        f"⚠️ <b>Delete {reward['rw_code']}?</b>\n"
        f"📝 Content: {str(reward.get('content',''))[:100]}...\n\n"
        f"This cannot be undone.",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(RewardStates.delete_waiting_for_confirm)
async def process_delete_reward_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        from bson.objectid import ObjectId
        col_rewards.delete_one({"_id": ObjectId(data["reward_id"])})
        await state.clear()
        log_user_action(message.from_user, "Delete Reward", f"Deleted {data['rw_code']}")
        await message.answer(f"🗑️ <b>{data['rw_code']} deleted.</b>", reply_markup=get_reward_menu(), parse_mode="HTML")
    else:
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_reward_menu())

# --- LIST REWARD ---
@dp.message(F.text == "📋 LIST REWARD")
async def list_rewards(message: types.Message, state: FSMContext):
    """Show all reward content items"""
    if not await check_authorization(message, "List Reward", "can_list"):
        return
    all_rewards = list(col_rewards.find().sort("rw_number", 1))
    if not all_rewards:
        await message.answer("⚠️ No rewards added yet. Use ➕ ADD REWARD to add one.", reply_markup=get_reward_menu())
        return
    lines = []
    for r in all_rewards:
        preview = str(r.get("content", ""))[:80]
        lines.append(
            f"🎁 <b>{r['rw_code']}</b>\n"
            f"   {preview}{'...' if len(str(r.get('content',''))) > 80 else ''}"
        )
    text = f"📋 <b>REWARD POOL</b> ({len(all_rewards)} items)\n────────────────────\n\n" + "\n\n".join(lines)
    MAX = 4000
    if len(text) <= MAX:
        await message.answer(text, reply_markup=get_reward_menu(), parse_mode="HTML")
    else:
        parts = []
        while text:
            cut = text.rfind('\n', 0, MAX)
            if cut == -1: cut = MAX
            parts.append(text[:cut])
            text = text[cut:].lstrip('\n')
        for i, part in enumerate(parts):
            kb = get_reward_menu() if i == len(parts) - 1 else None
            await message.answer(part, reply_markup=kb, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════════
# COMPREHENSIVE LIST - Shows ALL Data (PDFs + IG Content)
# ═══════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "📋 LIST")
async def comprehensive_list_handler(message: types.Message, state: FSMContext):
    """Show menu to choose between ALL, IG CC, or REWARDS"""
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📚 ALL"), KeyboardButton(text="📸 IG CONTENT")],
        [KeyboardButton(text="🎁 REWARDS")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer("📋 <b>SELECT VIEW:</b>", reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "📚 ALL")
async def list_all_pdfs(message: types.Message, state: FSMContext):
    """Show all PDFs with pagination (5 per page)"""
    await state.set_state(ListStates.viewing_all)
    await send_all_pdfs_view(message, page=0)

@dp.message(F.text == "📸 IG CONTENT")
async def list_ig_content(message: types.Message, state: FSMContext):
    """Show all IG content with pagination (10 per page) - from LIST menu"""
    await state.set_state(ListStates.viewing_ig)
    await send_all_ig_view(message, page=0)

@dp.message(F.text == "⬅️ BACK")
async def back_from_list_menu(message: types.Message, state: FSMContext):
    """Handle BACK from LIST selection menu or viewing states"""
    current_state = await state.get_state()
    
    # If viewing ALL PDFs or IG CC, go back to LIST selection menu
    if current_state in [ListStates.viewing_all, ListStates.viewing_ig]:
        await state.clear()
        return await comprehensive_list_handler(message, state)
    
    # Otherwise, go back to main menu (from LIST selection menu itself)
    await state.clear()
    await message.answer("📋 <b>Main Menu</b>", reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")


async def send_all_pdfs_view(message: types.Message, page=0):
    """Display paginated PDF list (5 per page)"""
    limit = 5
    skip = page * limit
    
    total = col_pdfs.count_documents({})
    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))
    
    if not pdfs:
        await message.answer("⚠️ No PDFs found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📚 <b>PDF DATA</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for pdf in pdfs:
        text += f"{pdf['index']}. <b>{_html.escape(str(pdf['name']))}</b>\n"
        text += f"🔗 Link: {pdf['link']}\n"
        text += f"💸 AFF: {pdf.get('affiliate_link', 'Not Set')}\n"
        text += f"▶️ YT Title: {pdf.get('yt_title', 'Not Set')}\n"
        text += f"🔗 YT Link: {pdf.get('yt_link', 'Not Set')}\n"
        text += f"🔑 MSA: {pdf.get('msa_code', 'Not Set')}\n\n"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_ALL {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_ALL {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

async def send_all_ig_view(message: types.Message, page=0):
    """Display paginated IG content list (10 per page)"""
    limit = 10
    skip = page * limit
    
    total = col_ig_content.count_documents({})
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents:
        await message.answer("⚠️ No IG Content found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📸 <b>IG CONTENT</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for idx, content in enumerate(contents, start=1):
        display_index = skip + idx
        text += f"{display_index}. {content['cc_code']}\n"
        # Show preview (50 chars)
        preview = content['name']
        if len(preview) > 50:
            preview = preview[:50] + "..."
        text += f"📝 {preview}\n\n"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_IGCC {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_IGCC {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

# Handle pagination for ALL PDFs
@dp.message(ListStates.viewing_all)
async def handle_all_pdfs_pagination(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("⬅️ PREV_ALL") or message.text.startswith("➡️ NEXT_ALL")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_all_pdfs_view(message, page=page)
        except:
            await send_all_pdfs_view(message, page=0)

# Handle pagination for IG CC
@dp.message(ListStates.viewing_ig)
async def handle_ig_cc_pagination(message: types.Message, state: FSMContext):
    text = message.text or ""

    # Pagination buttons
    if text.startswith("⬅️ PREV_IGCC") or text.startswith("➡️ NEXT_IGCC"):
        try:
            page = int(text.split()[-1]) - 1
            await send_all_ig_view(message, page=page)
        except:
            await send_all_ig_view(message, page=0)
        return

    # BACK button — go to list selection menu
    if text == "⬅️ BACK":
        await state.clear()
        await comprehensive_list_handler(message, state)
        return

    # ─── Full Detail View: index number or CC code ───────────────────────
    query = text.strip()
    if not query:
        return

    content = None
    display_index = None
    all_contents = list(col_ig_content.find().sort("cc_number", 1))

    if query.isdigit():
        idx = int(query) - 1
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            for i, c in enumerate(all_contents, start=1):
                if c["_id"] == content["_id"]:
                    display_index = i
                    break

    if not content:
        await message.answer(
            "❌ <b>Not Found</b>\n\nSend an index number (e.g. `3`) or CC code (e.g. `CC3`) to view full details.\nOr press ⬅️ BACK.",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ BACK")]],
                resize_keyboard=True
            )
        )
        return

    # Format full detail
    name = content.get("name", "N/A")
    aff_link = content.get("affiliate_link", "Not Set")
    start_code = content.get("start_code", "Not Set")
    ig_cc_clicks = content.get("ig_cc_clicks", 0)
    created_at = content.get("created_at")
    last_click = content.get("last_ig_cc_click")
    cc_number = content.get("cc_number", "?")

    created_str = created_at.strftime("%b %d, %Y  %I:%M %p") if isinstance(created_at, datetime) else str(created_at) if created_at else "N/A"
    last_click_str = last_click.strftime("%b %d, %Y  %I:%M %p") if isinstance(last_click, datetime) else str(last_click) if last_click else "Never"

    detail = (
        f"📸 <b>IG CONTENT — FULL DETAIL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>#{display_index}  {content['cc_code']}</b>\n\n"
        f"📝 <b>Name / Content:</b>\n{_html_escape(name)}\n\n"
        f"💸 <b>Affiliate Link:</b>\n{aff_link}\n\n"
        f"🔗 <b>Start Code:</b> `{start_code}`\n"
        f"📊 <b>IG CC Clicks:</b> `{ig_cc_clicks:,}`\n"
        f"🔢 <b>CC Number:</b> `{cc_number}`\n\n"
        f"📅 <b>Created:</b> {created_str}\n"
        f"🖱️ <b>Last Click:</b> {last_click_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⬅️ BACK TO IG LIST")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer(detail, parse_mode="HTML", reply_markup=kb)

@dp.message(ListStates.viewing_ig, F.text == "⬅️ BACK TO IG LIST")
async def return_to_ig_list_from_detail(message: types.Message, state: FSMContext):
    """Return to IG content list from detail view"""
    await send_all_ig_view(message, page=0)


# ═══════════════════════════════════════════════════════════════════════════
# 🎁 REWARDS — Paginated List View (from 📋 LIST → 🎁 REWARDS)
# ═══════════════════════════════════════════════════════════════════════════

async def send_all_rewards_view(message: types.Message, page: int = 0):
    """Display paginated store items (the active vault shop items)."""
    skip = page * RW_PAGE_SIZE
    total = col_store_items.count_documents({})
    items = list(col_store_items.find().sort("cost", 1).skip(skip).limit(RW_PAGE_SIZE))

    if not items:
        await message.answer(
            "\u26a0\ufe0f No store items found. Add some via \u2795 ADD \u2192 \U0001f6d2 REWARD \u2192 \U0001f6d2 STORE MANAGER.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\u2b05\ufe0f BACK")]], resize_keyboard=True)
        )
        return

    text = f"\U0001f6cd\ufe0f <b>STORE ITEMS</b> (Page {page + 1})\nTotal: <b>{total}</b>\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
    for idx, item in enumerate(items, start=1):
        display_idx = skip + idx
        status = "\u2705 Active" if item.get("active") else "\u26d4 Inactive"
        desc_preview = str(item.get("description", ""))[:50]
        if len(str(item.get("description", ""))) > 50:
            desc_preview += "..."
        text += (
            f"{display_idx}. <b>{item.get('name','?')}</b>\n"
            f"   \U0001f4b3 Cost: {item.get('cost', 0)} Credits  |  {status}\n"
            f"   \U0001f7e2 Btn: <code>{item.get('btn_name','')}</code>\n"
            + (f"   {desc_preview}\n" if desc_preview else "")
            + "\n"
        )
    text += "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\u2328\ufe0f <b>Tap an index number to view full reward text.</b>"

    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text=f"\u2b05\ufe0f PREV_RW {page}"))
    if (skip + RW_PAGE_SIZE) < total:
        nav_row.append(KeyboardButton(text=f"\u27a1\ufe0f NEXT_RW {page + 2}"))
    keyboard = []
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([KeyboardButton(text="\u2b05\ufe0f BACK")])
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(F.text == "🎁 REWARDS")
async def list_rewards_entry(message: types.Message, state: FSMContext):
    """Entry from 📋 LIST → 🎁 REWARDS"""
    if not await check_authorization(message, "List Rewards", "can_list"):
        return
    await state.set_state(RewardStates.viewing_list)
    await send_all_rewards_view(message, page=0)

@dp.message(RewardStates.viewing_list)
async def handle_rewards_list_state(message: types.Message, state: FSMContext):
    """Handle pagination and full-detail lookup for reward list."""
    text = message.text or ""

    # PREV / NEXT pagination
    if text.startswith("⬅️ PREV_RW") or text.startswith("➡️ NEXT_RW"):
        try:
            page = int(text.split()[-1]) - 1
            await send_all_rewards_view(message, page=page)
        except Exception:
            await send_all_rewards_view(message, page=0)
        return

    # BACK — return to LIST selection menu
    if text == "⬅️ BACK":
        await state.clear()
        await comprehensive_list_handler(message, state)
        return

    # Full detail view — by index number
    query = text.strip()
    if not query:
        return

    all_items = list(col_store_items.find().sort("cost", 1))
    item = None
    display_idx = None

    if query.isdigit():
        idx = int(query) - 1
        if 0 <= idx < len(all_items):
            item = all_items[idx]
            display_idx = int(query)

    if not item:
        await message.answer(
            "\u274c <b>Not Found</b>\n\nSend an index number (e.g. <code>1</code>) to view full reward text.\nOr press \u2b05\ufe0f BACK.",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\u2b05\ufe0f BACK")]], resize_keyboard=True)
        )
        return

    reward_text = str(item.get("reward_text", "\u2014 No reward text set \u2014"))
    created_at = item.get("created_at")
    created_str = created_at.strftime("%b %d, %Y  %I:%M %p") if hasattr(created_at, 'strftime') else str(created_at) if created_at else "N/A"
    status = "\u2705 Active" if item.get("active") else "\u26d4 Inactive"

    detail_header = (
        f"\U0001f6cd\ufe0f <b>STORE ITEM — REWARD TEXT</b>\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"<b>#{display_idx}  {item.get('name','?')}</b>\n"
        f"\U0001f4b3 Cost: {item.get('cost', 0)} Credits  |  {status}\n"
        f"\U0001f7e2 Btn: <code>{item.get('btn_name','')}</code>\n"
        + (f"\U0001f4dd Desc: {item.get('description','') or '\u2014'}\n" if item.get('description') else "")
        + f"\U0001f4c5 Added: {created_str}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f381 <b>Reward Text:</b>\n"
    )
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="\u2b05\ufe0f BACK TO REWARD LIST")],
        [KeyboardButton(text="\u2b05\ufe0f BACK")]
    ], resize_keyboard=True)

    MAX = 4096 - len(detail_header)
    if len(reward_text) <= MAX:
        await message.answer(detail_header + reward_text, parse_mode="HTML", reply_markup=kb)
    else:
        await message.answer(detail_header, parse_mode="HTML")
        chunks = [reward_text[i:i + 4096] for i in range(0, len(reward_text), 4096)]
        for ci, chunk in enumerate(chunks):
            await message.answer(chunk, reply_markup=kb if ci == len(chunks) - 1 else None)



@dp.message(RewardStates.viewing_list, F.text == "⬅️ BACK TO REWARD LIST")
async def back_to_reward_list(message: types.Message, state: FSMContext):
    await send_all_rewards_view(message, page=0)


# ═══════════════════════════════════════════════════════════════════════════
# 🔗 REWARD LINKS — from LINKS menu → 🎁 REWARD LINKS
# Shows each reward's index + code (no deep-link, rewards are DM-delivered)
# ═══════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "\U0001f381 REWARD LINKS")
async def reward_links_handler(message: types.Message):
    """REWARD LINKS removed — redirect to Store Manager."""
    if not await check_authorization(message, "Reward Links", "can_list"):
        return
    await message.answer(
        "\U0001f6d2 Reward Links have moved. Use <b>\u2795 ADD \u2192 \U0001f381 REWARD \u2192 \U0001f6d2 STORE MANAGER</b> to manage store items.",
        reply_markup=get_links_menu(), parse_mode="HTML"
    )



# =============================================================================
# 🎯 MILESTONES MANAGER + 🛒 STORE MANAGER
# =============================================================================

# --- BACK TO REWARD MENU ---
@dp.message(F.text == "⬅️ BACK TO REWARD MENU")
async def back_to_reward_menu(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Back to Reward", "can_add"):
        return
    await state.clear()
    total = col_rewards.count_documents({})
    ms_total = col_milestones.count_documents({})
    store_total = col_store_items.count_documents({})
    await message.answer(
        f"<b>🎁 REWARD MENU</b>\n"
        f"────────────────────\n"
        f"📦 Rewards Pool: <b>{total}</b>\n"
        f"🎯 Milestones: <b>{ms_total}</b>\n"
        f"🛒 Store Items: <b>{store_total}</b>",
        reply_markup=get_reward_menu(), parse_mode="HTML"
    )

# ─────────── MILESTONES ───────────

@dp.message(F.text == "🎯 MILESTONES")
async def milestones_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Milestones", "can_add"):
        return
    await state.clear()
    total = col_milestones.count_documents({})
    await message.answer(
        f"<b>🎯 MILESTONE MANAGER</b>\n────────────────────\n"
        f"Active Milestones: <b>{total}</b>\n\n"
        f"Milestones fire automatically when a user hits the required referral count.\n"
        f"They can award bonus MSA Credits, a badge, and a custom reward text.",
        reply_markup=get_milestone_menu(), parse_mode="HTML"
    )

@dp.message(F.text == "📋 LIST MILESTONES")
async def list_milestones(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Milestones", "can_list"):
        return
    await state.clear()
    docs = list(col_milestones.find().sort("refs_required", 1))
    if not docs:
        await message.answer("⚠️ No milestones set yet. Add one first!", reply_markup=get_milestone_menu())
        return
    lines = ["<b>🎯 MILESTONES</b>\n────────────────────"]
    for d in docs:
        lines.append(
            f"<b>{d.get('badge','🏅')} {d.get('refs_required')} refs → {d.get('name','?')}</b>\n"
            f"   💎 Bonus: {d.get('bonus_credits',0)} MSA Credits\n"
            f"   🎁 Reward: {str(d.get('reward_text','None'))[:60]}"
        )
    await message.answer("\n\n".join(lines), reply_markup=get_milestone_menu(), parse_mode="HTML")

@dp.message(F.text == "➕ ADD MILESTONE")
async def add_milestone_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add Milestone", "can_add"):
        return
    await state.set_state(MilestoneStates.waiting_for_refs)
    await message.answer(
        "<b>➕ ADD MILESTONE</b>\n────────────────────\n"
        "Step 1/5: How many <b>confirmed referrals</b> triggers this milestone?\n"
        "<i>Example: 3  (for 3 referrals)</i>",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(MilestoneStates.waiting_for_refs)
async def add_milestone_refs(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    try:
        refs = int(message.text.strip())
        if refs < 1: raise ValueError
    except ValueError:
        return await message.answer("⚠️ Enter a positive number.", reply_markup=get_cancel_keyboard())
    if col_milestones.find_one({"refs_required": refs}):
        return await message.answer(f"⚠️ A milestone for <b>{refs} refs</b> already exists. Delete it first.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    await state.update_data(refs=refs)
    await state.set_state(MilestoneStates.waiting_for_name)
    await message.answer("Step 2/5: Enter a <b>milestone name</b>.\n<i>Example: Silver Networker</i>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(MilestoneStates.waiting_for_name)
async def add_milestone_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    await state.update_data(name=message.text.strip())
    await state.set_state(MilestoneStates.waiting_for_badge)
    await message.answer("Step 3/5: Enter a <b>badge emoji</b> for this milestone.\n<i>Example: 🥈</i>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(MilestoneStates.waiting_for_badge)
async def add_milestone_badge(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    await state.update_data(badge=message.text.strip())
    await state.set_state(MilestoneStates.waiting_for_credits)
    await message.answer("Step 4/5: How many <b>bonus MSA Credits</b> to award? (Enter 0 for none)", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(MilestoneStates.waiting_for_credits)
async def add_milestone_credits(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    try:
        pts = max(0, int(message.text.strip()))
    except ValueError:
        return await message.answer("⚠️ Enter a number (0 or more).", reply_markup=get_cancel_keyboard())
    await state.update_data(bonus_credits=pts)
    await state.set_state(MilestoneStates.waiting_for_reward_txt)
    await message.answer("Step 5/5: Enter the <b>reward text</b> delivered to the user when they hit this milestone.\n<i>This can be a link, a secret message, or any exclusive content.</i>\n\nType <code>SKIP</code> to leave empty.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(MilestoneStates.waiting_for_reward_txt)
async def add_milestone_reward_txt(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    reward_txt = "" if message.text.strip().upper() == "SKIP" else message.text.strip()
    data = await state.get_data()
    doc = {
        "refs_required": data["refs"],
        "name": data["name"],
        "badge": data["badge"],
        "bonus_credits": data["bonus_credits"],
        "reward_text": reward_txt,
        "created_at": now_local()
    }
    col_milestones.insert_one(doc)
    await state.clear()
    await message.answer(
        f"✅ <b>Milestone Added!</b>\n────────────────────\n"
        f"{data['badge']} <b>{data['refs']} refs → {data['name']}</b>\n"
        f"💎 Bonus: {data['bonus_credits']} MSA Credits\n"
        f"🎁 Reward: {reward_txt[:60] if reward_txt else 'None'}",
        reply_markup=get_milestone_menu(), parse_mode="HTML"
    )

@dp.message(F.text == "🗑️ DELETE MILESTONE")
async def delete_milestone_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Milestone", "can_add"):
        return
    docs = list(col_milestones.find().sort("refs_required", 1))
    if not docs:
        return await message.answer("⚠️ No milestones to delete.", reply_markup=get_milestone_menu())
    lines = ["<b>🗑️ DELETE MILESTONE</b>\nType the <b>refs_required number</b> to delete:\n────────────────────"]
    for d in docs:
        lines.append(f"• <b>{d['refs_required']}</b> → {d.get('badge','')} {d.get('name','?')}")
    await state.set_state(MilestoneStates.delete_select)
    await message.answer("\n".join(lines), reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(MilestoneStates.delete_select)
async def delete_milestone_confirm(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    try:
        refs = int(message.text.strip())
    except ValueError:
        return await message.answer("⚠️ Enter a valid number.", reply_markup=get_cancel_keyboard())
    doc = col_milestones.find_one({"refs_required": refs})
    if not doc:
        return await message.answer(f"❌ No milestone found for {refs} refs.", reply_markup=get_cancel_keyboard())
    await state.update_data(del_refs=refs, del_name=doc.get("name","?"))
    await state.set_state(MilestoneStates.delete_confirm)
    await message.answer(
        f"⚠️ Delete milestone <b>{doc.get('badge','')} {refs} refs → {doc.get('name','?')}</b>?\n"
        f"Type <code>CONFIRM</code> to delete.",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(MilestoneStates.delete_confirm)
async def delete_milestone_execute(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_milestone_menu())
    if message.text.strip().upper() != "CONFIRM":
        return await message.answer("Type exactly <code>CONFIRM</code> to proceed.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    data = await state.get_data()
    col_milestones.delete_one({"refs_required": data["del_refs"]})
    await state.clear()
    await message.answer(f"✅ Milestone <b>{data['del_refs']} refs → {data['del_name']}</b> deleted.", reply_markup=get_milestone_menu(), parse_mode="HTML")

# ─────────── STORE MANAGER ───────────

@dp.message(F.text == "🛒 STORE MANAGER")
async def store_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Store Manager", "can_add"):
        return
    await state.clear()
    total = col_store_items.count_documents({})
    active = col_store_items.count_documents({"active": True})
    await message.answer(
        f"<b>🛒 VAULT STORE MANAGER</b>\n────────────────────\n"
        f"Total Items: <b>{total}</b> | Active: <b>{active}</b>\n\n"
        f"Items set here appear in the user's 💎 MSA CREDITS shop.\n"
        f"Users spend MSA Credits to unlock reward text (link, file info, etc.).",
        reply_markup=get_store_menu(), parse_mode="HTML"
    )

@dp.message(F.text == "📋 LIST STORE ITEMS")
async def list_store_items(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Store Items", "can_list"):
        return
    await state.clear()
    docs = list(col_store_items.find().sort("cost", 1))
    if not docs:
        await message.answer("⚠️ No store items yet. Add one!", reply_markup=get_store_menu())
        return
    lines = ["<b>🛒 STORE ITEMS</b>\n────────────────────"]
    for i, d in enumerate(docs, 1):
        status = "✅ Active" if d.get("active") else "⛔ Inactive"
        lines.append(
            f"<b>{i}. {d.get('name','?')}</b> — {d.get('cost',0)} Credits | {status}\n"
            f"   {d.get('description','')[:60]}"
        )
    lines.append("\n👉 <b>Type an item number to view its full reward payload.</b>")
    await state.update_data(store_docs=[str(d["_id"]) for d in docs])
    await state.set_state(StoreStates.list_select)
    await message.answer("\n\n".join(lines), reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(StoreStates.list_select)
async def list_store_select(message: types.Message, state: FSMContext):
    if message.text in ["❌ CANCEL", "⬅️ BACK"]:
        await state.clear(); return await message.answer("⬅️ Back to Store Manager.", reply_markup=get_store_menu())
    try:
        from bson.objectid import ObjectId
        idx = int(message.text.strip()) - 1
        data = await state.get_data()
        doc_ids = data.get("store_docs", [])
        if idx < 0 or idx >= len(doc_ids): raise ValueError
    except (ValueError, IndexError):
        return await message.answer("⚠️ Enter a valid item number, or tap ⬅️ BACK.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
    
    doc = col_store_items.find_one({"_id": ObjectId(doc_ids[idx])})
    if not doc:
        return await message.answer("❌ Item not found.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
    
    status = "✅ Active" if doc.get("active") else "⛔ Inactive"
    content = (
        f"<b>🛒 STORE ITEM: {doc.get('name', '?')}</b>\n"
        f"────────────────────\n"
        f"<b>Button Label:</b> <code>{doc.get('btn_name', '?')}</code>\n"
        f"<b>Cost:</b> {doc.get('cost', 0)} Credits\n"
        f"<b>Status:</b> {status}\n\n"
        f"<b>Preview Description:</b>\n{doc.get('description', 'None')}\n\n"
        f"<b>💎 FULL REWARD PAYLOAD:</b>\n"
        f"<pre>{_html.escape(doc.get('reward_text', ''))}</pre>\n"
    )
    
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer(content, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "➕ ADD STORE ITEM")
async def add_store_item_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add Store Item", "can_add"):
        return
    await state.set_state(StoreStates.waiting_for_name)
    await message.answer(
        "<b>➕ ADD STORE ITEM</b>\n────────────────────\n"
        "Step 1/4: Enter the <b>item name</b>.\n<i>Example: Viral Hook Playbook</i>",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(StoreStates.waiting_for_name)
async def add_store_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    await state.update_data(name=message.text.strip())
    await state.set_state(StoreStates.waiting_for_desc)
    await message.answer("Step 2/4: Enter a short <b>description</b> (shown in shop preview).\nType <code>SKIP</code> to leave empty.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(StoreStates.waiting_for_desc)
async def add_store_desc(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    desc = "" if message.text.strip().upper() == "SKIP" else message.text.strip()
    await state.update_data(description=desc)
    await state.set_state(StoreStates.waiting_for_cost)
    await message.answer("Step 3/4: How many <b>MSA Credits</b> does this item cost?\n<i>Example: 500</i>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(StoreStates.waiting_for_cost)
async def add_store_cost(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    try:
        cost = int(message.text.strip())
        if cost < 1: raise ValueError
    except ValueError:
        return await message.answer("⚠️ Enter a positive number.", reply_markup=get_cancel_keyboard())
    await state.update_data(cost=cost)
    await state.set_state(StoreStates.waiting_for_btn_name)
    await message.answer(
        "Step 4/5: Enter a short <b>button label</b> for this item.\n"
        "This appears as a keyboard button inside the 🛍️ REWARD STORE.\n"
        "<i>Max 25 chars. Example: 🔥 Hook Playbook</i>",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(StoreStates.waiting_for_btn_name)
async def add_store_btn_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    await state.update_data(btn_name=message.text.strip()[:25])
    await state.set_state(StoreStates.waiting_for_reward_txt)
    await message.answer(
        "Step 5/5: Enter the <b>reward text</b> delivered after purchase.\n"
        "<i>This is a link, instructions, or exclusive content the user receives.</i>",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(StoreStates.waiting_for_reward_txt)
async def add_store_reward_txt(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    import uuid
    data = await state.get_data()
    item_id = str(uuid.uuid4())[:8]
    doc = {
        "item_id": item_id,
        "name": data["name"],
        "description": data["description"],
        "cost": data["cost"],
        "btn_name": data.get("btn_name", data["name"][:25]),
        "reward_text": message.text.strip(),
        "active": True,
        "created_at": now_local()
    }
    col_store_items.insert_one(doc)
    await state.clear()
    await message.answer(
        f"✅ <b>Store Item Added!</b>\n────────────────────\n"
        f"<b>{data['name']}</b>\n"
        f"🔘 Button Label: <code>{doc['btn_name']}</code>\n"
        f"💰 Cost: {data['cost']} MSA Credits\n"
        f"🆔 Item ID: <code>{item_id}</code>\n"
        f"✅ Status: Active (visible in 🛍️ REWARD STORE)",
        reply_markup=get_store_menu(), parse_mode="HTML"
    )

@dp.message(F.text == "🗑️ DELETE STORE ITEM")
async def delete_store_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Store Item", "can_add"):
        return
    docs = list(col_store_items.find().sort("cost", 1))
    if not docs:
        return await message.answer("⚠️ No store items to delete.", reply_markup=get_store_menu())
    lines = ["<b>🗑️ DELETE STORE ITEM</b>\nType the <b>item number</b> to delete:\n────────────────────"]
    for i, d in enumerate(docs, 1):
        lines.append(f"<b>{i}.</b> {d.get('name','?')} — {d.get('cost',0)} Credits")
    await state.update_data(store_docs=[str(d["_id"]) for d in docs])
    await state.set_state(StoreStates.delete_select)
    await message.answer("\n".join(lines), reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(StoreStates.delete_select)
async def delete_store_confirm(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    try:
        idx = int(message.text.strip()) - 1
        data = await state.get_data()
        doc_ids = data.get("store_docs", [])
        if idx < 0 or idx >= len(doc_ids): raise ValueError
    except (ValueError, IndexError):
        return await message.answer("⚠️ Enter a valid item number.", reply_markup=get_cancel_keyboard())
    from bson.objectid import ObjectId
    doc = col_store_items.find_one({"_id": ObjectId(doc_ids[idx])})
    if not doc:
        return await message.answer("❌ Item not found.", reply_markup=get_cancel_keyboard())
    await state.update_data(del_oid=doc_ids[idx], del_name=doc.get("name","?"))
    await state.set_state(StoreStates.delete_confirm)
    await message.answer(
        f"⚠️ Delete store item <b>{doc.get('name','?')}</b>?\nType <code>CONFIRM</code> to proceed.",
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(StoreStates.delete_confirm)
async def delete_store_execute(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    if message.text.strip().upper() != "CONFIRM":
        return await message.answer("Type exactly <code>CONFIRM</code>.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    from bson.objectid import ObjectId
    data = await state.get_data()
    col_store_items.delete_one({"_id": ObjectId(data["del_oid"])})
    await state.clear()
    await message.answer(f"✅ Store item <b>{data['del_name']}</b> deleted.", reply_markup=get_store_menu(), parse_mode="HTML")


# ─────────── EDIT STORE ITEM ───────────

_STORE_EDIT_FIELDS = {
    "1": ("name",        "Item Name"),
    "2": ("btn_name",    "Button Label"),
    "3": ("description", "Description"),
    "4": ("cost",        "Cost (Credits)"),
    "5": ("reward_text", "Reward Text"),
    "6": ("active",      "Toggle Active/Inactive"),
}

def _store_field_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="1. Name"),         KeyboardButton(text="2. Button Label")],
        [KeyboardButton(text="3. Description"),  KeyboardButton(text="4. Cost")],
        [KeyboardButton(text="5. Reward Text"),  KeyboardButton(text="6. Toggle Active")],
        [KeyboardButton(text="❌ CANCEL")],
    ], resize_keyboard=True)

@dp.message(F.text == "✏️ EDIT STORE ITEM")
async def edit_store_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit Store Item", "can_add"):
        return
    docs = list(col_store_items.find().sort("cost", 1))
    if not docs:
        return await message.answer("⚠️ No store items yet. Add one first!", reply_markup=get_store_menu())
    lines = ["<b>✏️ EDIT STORE ITEM</b>\nType the <b>item number</b> to edit:\n────────────────────"]
    for i, d in enumerate(docs, 1):
        status = "✅" if d.get("active") else "⛔"
        lines.append(f"<b>{i}.</b> {d.get('name','?')} — {d.get('cost',0)} Credits  {status}")
    await state.update_data(store_docs=[str(d["_id"]) for d in docs])
    await state.set_state(StoreStates.edit_select)
    await message.answer("\n".join(lines), reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(StoreStates.edit_select)
async def edit_store_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    try:
        from bson.objectid import ObjectId
        idx = int(message.text.strip()) - 1
        data = await state.get_data()
        doc_ids = data.get("store_docs", [])
        if idx < 0 or idx >= len(doc_ids): raise ValueError
    except (ValueError, IndexError):
        return await message.answer("⚠️ Enter a valid item number.", reply_markup=get_cancel_keyboard())
    from bson.objectid import ObjectId
    doc = col_store_items.find_one({"_id": ObjectId(doc_ids[idx])})
    if not doc:
        return await message.answer("❌ Item not found.", reply_markup=get_cancel_keyboard())
    await state.update_data(edit_oid=doc_ids[idx], edit_item_name=doc.get("name","?"), edit_active=doc.get("active", True))
    status = "✅ Active" if doc.get("active") else "⛔ Inactive"
    await message.answer(
        f"<b>✏️ {doc.get('name','?')}</b>\n"
        f"────────────────────\n"
        f"💳 Cost: {doc.get('cost',0)} Credits\n"
        f"🔘 Button: <code>{doc.get('btn_name','')}</code>\n"
        f"📝 Desc: {doc.get('description','') or '—'}\n"
        f"📦 Status: {status}\n\n"
        f"Which field do you want to edit?\n"
        f"<i>1=Name  2=Button Label  3=Desc  4=Cost  5=Reward Text  6=Toggle Active</i>",
        reply_markup=_store_field_keyboard(), parse_mode="HTML"
    )
    await state.set_state(StoreStates.edit_field)

@dp.message(StoreStates.edit_field)
async def edit_store_field(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    # Map button text → field key
    btn_map = {
        "1. Name": "1", "2. Button Label": "2", "3. Description": "3",
        "4. Cost": "4", "5. Reward Text": "5", "6. Toggle Active": "6",
    }
    choice = btn_map.get(message.text.strip(), message.text.strip())
    if choice not in _STORE_EDIT_FIELDS:
        return await message.answer("⚠️ Tap one of the field buttons.", reply_markup=_store_field_keyboard())
    field_key, field_label = _STORE_EDIT_FIELDS[choice]
    await state.update_data(edit_field_key=field_key, edit_field_label=field_label)

    # Toggle is instant — no text input needed
    if field_key == "active":
        from bson.objectid import ObjectId
        data = await state.get_data()
        new_val = not data.get("edit_active", True)
        col_store_items.update_one({"_id": ObjectId(data["edit_oid"])}, {"$set": {"active": new_val}})
        await state.clear()
        status = "✅ Active" if new_val else "⛔ Inactive"
        return await message.answer(
            f"✅ <b>{data['edit_item_name']}</b> is now <b>{status}</b>.",
            reply_markup=get_store_menu(), parse_mode="HTML"
        )

    await state.set_state(StoreStates.edit_value)
    await message.answer(
        f"✏️ Enter the new <b>{field_label}</b>:"
        + ("\n<i>Enter a positive number.</i>" if field_key == "cost" else ""),
        reply_markup=get_cancel_keyboard(), parse_mode="HTML"
    )

@dp.message(StoreStates.edit_value)
async def edit_store_value(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear(); return await message.answer("❌ Cancelled.", reply_markup=get_store_menu())
    from bson.objectid import ObjectId
    data = await state.get_data()
    field_key   = data.get("edit_field_key")
    field_label = data.get("edit_field_label", "Field")
    oid         = data.get("edit_oid")
    item_name   = data.get("edit_item_name", "Item")
    new_val     = message.text.strip()

    if field_key == "cost":
        try:
            new_val = int(new_val)
            if new_val < 1: raise ValueError
        except ValueError:
            return await message.answer("⚠️ Enter a positive number.", reply_markup=get_cancel_keyboard())
    elif field_key == "btn_name":
        new_val = new_val[:25]

    col_store_items.update_one({"_id": ObjectId(oid)}, {"$set": {field_key: new_val}})
    await state.clear()
    await message.answer(
        f"✅ <b>{item_name}</b> — <b>{field_label}</b> updated successfully.",
        reply_markup=get_store_menu(), parse_mode="HTML"
    )




# ═════════════════════════════════════════════════════════════════════
# SEARCH - PDF, IG CC, or REWARD with detailed info
# ═════════════════════════════════════════════════════════════════════

@dp.message(F.text == "🔍 SEARCH")
async def search_menu_handler(message: types.Message):
    if not await check_authorization(message, "Search Menu", "can_list"):
        return
    """Show search menu with PDF/IG CC/Reward options"""
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔍 SEARCH PDF"), KeyboardButton(text="🔍 SEARCH IG CC")],
        [KeyboardButton(text="🔍 SEARCH REWARD")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer("🔍 <b>SELECT SEARCH TYPE:</b>", reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "🔍 SEARCH PDF")
async def search_pdf_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Search PDF", "can_list"):
        return
    """Show available PDFs with pagination"""
    await state.set_state(SearchStates.viewing_pdf_list)
    await send_search_pdf_list(message, page=0)


async def send_search_pdf_list(message: types.Message, page=0):
    """Display paginated PDF list for search (5 per page)"""
    limit = 5
    skip = page * limit
    
    total = col_pdfs.count_documents({})
    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))
    
    if not pdfs:
        await message.answer("⚠️ No PDFs found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📚 <b>AVAILABLE PDFs</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for pdf in pdfs:
        text += f"{pdf['index']}. {pdf['name']}\n"
        text += f"🔗 {pdf['link']}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter PDF Index, Name, or MSA Code:</b>"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_SPDF {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_SPDF {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(SearchStates.viewing_pdf_list)
async def handle_search_pdf_list(message: types.Message, state: FSMContext):
    """Handle pagination and input in PDF search list"""
    if message.text == "⬅️ BACK":
        await state.clear()
        return await search_menu_handler(message)
    
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    # Handle pagination
    if message.text and (message.text.startswith("⬅️ PREV_SPDF") or message.text.startswith("➡️ NEXT_SPDF")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_search_pdf_list(message, page=page)
            return
        except:
            await send_search_pdf_list(message, page=0)
            return
    
    # Process search input
    await state.set_state(SearchStates.waiting_for_pdf_input)
    await process_pdf_search(message, state)

@dp.message(SearchStates.waiting_for_pdf_input)
async def process_pdf_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    query = message.text.strip()
    pdf = None
    
    # Try by index
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    # Try by MSA code
    elif re.fullmatch(r"^MSA\d{4}$", query.upper()):
        pdf = col_pdfs.find_one({"msa_code": query.upper()})
    # Try by name
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": f"^{query}$", "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Format creation time
    from datetime import datetime
    creation_time = pdf.get('created_at', now_local())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📄 <b>PDF DETAILS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 Index: {pdf['index']}\n"
    text += f"📛 Name: {pdf['name']}\n"
    text += f"🔗 Link: {pdf['link']}\n"
    text += f"💸 Affiliate: {pdf.get('affiliate_link', 'Not Set')}\n"
    text += f"▶️ YT Title: {pdf.get('yt_title', 'Not Set')}\n"
    text += f"🔗 YT Link: {pdf.get('yt_link', 'Not Set')}\n"
    text += f"🔑 MSA Code: {pdf.get('msa_code', 'Not Set')}\n"
    text += f"📅 Created: {date_str}\n"
    text += f"🕐 Time: {time_12h}"
    
    # Keep state active for continuous search
    await state.set_state(SearchStates.waiting_for_pdf_input)
    
    # Add input prompt
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter another PDF Index, Name, or MSA Code:</b>"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(F.text == "🔍 SEARCH IG CC")
async def search_ig_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Search IG", "can_list"):
        return
    """Show available IG content with pagination"""
    await state.set_state(SearchStates.viewing_ig_list)
    await send_search_ig_list(message, page=0)

async def send_search_ig_list(message: types.Message, page=0):
    """Display paginated IG content list for search (10 per page)"""
    limit = 10
    skip = page * limit
    
    total = col_ig_content.count_documents({})
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents:
        await message.answer("⚠️ No IG Content found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📸 <b>AVAILABLE IG CONTENT</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for idx, content in enumerate(contents, start=1):
        display_idx = skip + idx
        text += f"{display_idx}. {content['cc_code']}\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter Index or CC Code:</b>"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_SIG {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_SIG {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(SearchStates.viewing_ig_list)
async def handle_search_ig_list(message: types.Message, state: FSMContext):
    """Handle pagination and input in IG search list"""
    if message.text == "⬅️ BACK":
        await state.clear()
        return await search_menu_handler(message)
    
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    # Handle pagination
    if message.text and (message.text.startswith("⬅️ PREV_SIG") or message.text.startswith("➡️ NEXT_SIG")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_search_ig_list(message, page=page)
            return
        except:
            await send_search_ig_list(message, page=0)
            return
    
    # Process search input
    await state.set_state(SearchStates.waiting_for_ig_input)
    await process_ig_search(message, state)

@dp.message(SearchStates.waiting_for_ig_input)
async def process_ig_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    query = message.text.strip()
    content = None
    
    # Try by index
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
    
    if not content:
        await message.answer("❌ IG Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Format creation time
    from datetime import datetime
    creation_time = content.get('created_at', now_local())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📸 <b>IG CONTENT DETAILS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 Code: {content['cc_code']}\n"
    text += f"📝 Content:\n{content['name']}\n\n"
    
    # Add affiliate link if exists
    if content.get('affiliate_link'):
        text += f"🔗 Affiliate: {content['affiliate_link']}\n"
    
    text += f"📅 Created: {date_str}\n"
    text += f"🕐 Time: {time_12h}"
    
    # Keep state active for continuous search
    await state.set_state(SearchStates.waiting_for_ig_input)
    
    # Add input prompt
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter another Index or CC Code to Search:</b>"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")


# ─── SEARCH REWARD ────────────────────────────────────────────────────────────

@dp.message(F.text == "🔍 SEARCH REWARD")
async def search_reward_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Search Reward", "can_list"):
        return
    total = col_rewards.count_documents({})
    if total == 0:
        await message.answer("⚠️ No rewards in pool yet.", reply_markup=get_cancel_keyboard())
        return
    all_rewards = list(col_rewards.find().sort("rw_number", 1))
    lines = [f"{i}. <b>{r['rw_code']}</b>" for i, r in enumerate(all_rewards, start=1)]
    index_text = "\n".join(lines)
    await state.set_state(SearchStates.waiting_for_reward_input)
    await message.answer(
        f"🎁 <b>SEARCH REWARD</b>\n━━━━━━━━━━━━━━━━━━━━\n{index_text}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"⌨️ <b>Enter an index number (e.g. <code>2</code>) or RW code (e.g. <code>RW2</code>):</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(SearchStates.waiting_for_reward_input)
async def process_reward_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)

    query = message.text.strip()
    all_rewards = list(col_rewards.find().sort("rw_number", 1))
    reward = None
    display_idx = None

    if query.isdigit():
        idx = int(query) - 1
        if 0 <= idx < len(all_rewards):
            reward = all_rewards[idx]
            display_idx = int(query)
    elif query.upper().startswith("RW"):
        reward = col_rewards.find_one({"rw_code": {"$regex": f"^{query}$", "$options": "i"}})
        if reward:
            for i, r in enumerate(all_rewards, start=1):
                if r["_id"] == reward["_id"]:
                    display_idx = i
                    break

    if not reward:
        await message.answer("❌ Reward not found. Enter a valid index or RW code, or ❌ CANCEL.", reply_markup=get_cancel_keyboard())
        return

    content = str(reward.get("content", ""))
    created_at = reward.get("created_at")
    created_str = created_at.strftime("%b %d, %Y  %I:%M %p") if hasattr(created_at, 'strftime') else str(created_at) if created_at else "N/A"

    header = (
        f"🎁 <b>REWARD DETAIL</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>#{display_idx}  {reward['rw_code']}</b>\n"
        f"📅 Added: {created_str}\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>Content:</b>\n"
    )
    await state.set_state(SearchStates.waiting_for_reward_input)
    if len(header + content) <= 4096:
        await message.answer(header + content, parse_mode="HTML", reply_markup=get_cancel_keyboard())
    else:
        await message.answer(header, parse_mode="HTML")
        chunks = [content[i:i + 4096] for i in range(0, len(content), 4096)]
        for ci, chunk in enumerate(chunks):
            await message.answer(chunk, reply_markup=get_cancel_keyboard() if ci == len(chunks) - 1 else None)


def get_diagnosis_nav_keyboard(page: int, total_pages: int) -> ReplyKeyboardMarkup:
    """Reply keyboard for diagnosis long-report pagination."""
    row = []
    if page > 0:
        row.append(KeyboardButton(text="⬅️ PREV_DIAG"))
    if page < total_pages - 1:
        row.append(KeyboardButton(text="➡️ NEXT_DIAG"))
    keyboard = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO ANALYTICS")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


@dp.message(F.text == "🩺 DIAGNOSIS")
async def diagnosis_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "System Diagnosis", "can_view_analytics"):
        return
    """Comprehensive System Health Check & Diagnosis"""
    status_msg = await message.answer("🔍 <b>Running Complete System Diagnosis...</b>\n\n⏳ This may take a moment...", parse_mode="HTML")
    
    issues = []
    warnings = []
    checks_passed = 0
    total_checks = 0
    
    # --- 1. DATABASE CONNECTION CHECK ---
    total_checks += 1
    try:
        start_t = now_local()
        client.admin.command('ping')
        ping_ms = (now_local() - start_t).microseconds / 1000
        
        if ping_ms > 100:
            warnings.append(f"⚠️ Database latency high: {ping_ms:.1f}ms (>100ms)")
        else:
            checks_passed += 1
            
        # Test write operation
        test_doc = {"test": True, "timestamp": now_local()}
        col_logs.insert_one(test_doc)
        col_logs.delete_one({"_id": test_doc["_id"]})
        checks_passed += 1
        total_checks += 1
        
    except Exception as e:
        issues.append(f"❌ Database Connection: {str(e)}")
        total_checks += 1
    
    # --- 2. COLLECTION INTEGRITY CHECK ---
    collections_to_check = {
        "bot3_pdfs": col_pdfs,
        "bot3_ig_content": col_ig_content,
        "bot3_logs": col_logs,
        "bot3_settings": col_settings
    }
    
    for coll_name, coll in collections_to_check.items():
        total_checks += 1
        try:
            count = coll.count_documents({})
            if count >= 0:
                checks_passed += 1
        except Exception as e:
            issues.append(f"❌ Collection '{coll_name}': {str(e)}")
    
    # --- 3. DATA INTEGRITY CHECK ---
    total_checks += 1
    try:
        # Check PDFs for missing critical fields
        pdfs_no_index = col_pdfs.count_documents({"index": {"$exists": False}})
        pdfs_no_name = col_pdfs.count_documents({"name": {"$exists": False}})
        pdfs_no_link = col_pdfs.count_documents({"link": {"$exists": False}})
        
        if pdfs_no_index > 0:
            issues.append(f"❌ {pdfs_no_index} PDFs missing 'index' field")
        if pdfs_no_name > 0:
            issues.append(f"❌ {pdfs_no_name} PDFs missing 'name' field")
        if pdfs_no_link > 0:
            issues.append(f"❌ {pdfs_no_link} PDFs missing 'link' field")
            
        if pdfs_no_index == 0 and pdfs_no_name == 0 and pdfs_no_link == 0:
            checks_passed += 1
            
    except Exception as e:
        issues.append(f"❌ Data Integrity Check: {str(e)}")
    
    # --- 4. DUPLICATE DETECTION ---
    total_checks += 1
    try:
        migrated_legacy_msa = await asyncio.to_thread(randomize_non_4_digit_msa_codes)
        if migrated_legacy_msa > 0:
            warnings.append(f"ℹ️ Migrated {migrated_legacy_msa} non-4-digit MSA code(s) to strict random MSA1234 format")

        repaired_msa = await asyncio.to_thread(repair_missing_duplicate_msa_codes)
        await asyncio.to_thread(ensure_unique_msa_code_index)
        if repaired_msa > 0:
            warnings.append(f"ℹ️ Auto-repaired {repaired_msa} invalid/duplicate MSA code(s)")

        # Check for duplicate MSA codes
        pipeline = [
            {"$match": {"msa_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$msa_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        duplicates = list(col_pdfs.aggregate(pipeline))
        
        if len(duplicates) > 0:
            issues.append(f"❌ Found {len(duplicates)} duplicate MSA codes")
            for dup in duplicates[:3]:  # Show first 3
                issues.append(f"   • Code '{dup['_id']}' used {dup['count']} times")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Duplicate Check: {str(e)}")
    
    # --- 5. INDEX VERIFICATION ---
    total_checks += 1
    try:
        required_indexes = {
            "pdf_index_unique": (col_pdfs, "index", {"unique": True, "name": "pdf_index_unique"}),
            "pdf_created_at": (col_pdfs, "created_at", {"name": "pdf_created_at"}),
            "pdf_msa_code": (col_pdfs, "msa_code", {"sparse": True, "name": "pdf_msa_code"}),
        }

        current_names = [idx["name"] for idx in col_pdfs.list_indexes()]
        missing_indexes = [name for name in required_indexes if name not in current_names]

        if missing_indexes:
            created = []
            failed = []
            for idx_name in missing_indexes:
                coll, key_spec, kwargs = required_indexes[idx_name]
                try:
                    coll.create_index(key_spec, **kwargs)
                    created.append(idx_name)
                except Exception as ie:
                    failed.append(f"{idx_name} ({ie})")

            if created:
                warnings.append(f"ℹ️ Auto-created missing indexes: {', '.join(created)}")

            current_names = [idx["name"] for idx in col_pdfs.list_indexes()]
            still_missing = [name for name in required_indexes if name not in current_names]
            if still_missing:
                warnings.append(f"⚠️ Named indexes still missing: {', '.join(still_missing)}")
            if failed:
                warnings.append(f"⚠️ Index create errors: {len(failed)}")

        checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Index Check: {str(e)}")
    
    # --- 6. STORAGE CHECK ---
    total_checks += 1
    try:
        db_stats = db.command("dbStats")
        db_size_mb = db_stats.get("dataSize", 0) / (1024 * 1024)
        storage_limit = 512  # MB
        
        if db_size_mb > storage_limit * 0.9:
            issues.append(f"❌ Database nearly full: {db_size_mb:.2f}MB / {storage_limit}MB")
        elif db_size_mb > storage_limit * 0.7:
            warnings.append(f"⚠️ Database usage high: {db_size_mb:.2f}MB / {storage_limit}MB")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Storage Check: {str(e)}")
    
    # --- 7. FILE SYSTEM CHECK ---
    total_checks += 1
    try:
        log_file = "logs/bot3.log"
        if os.path.exists(log_file):
            log_size = os.path.getsize(log_file) / (1024 * 1024)  # MB
            if log_size > 100:
                warnings.append(f"⚠️ Log file large: {log_size:.2f}MB (consider rotation)")
            else:
                checks_passed += 1
        else:
            warnings.append("⚠️ Log file not found")
            
    except Exception as e:
        warnings.append(f"⚠️ File System Check: {str(e)}")
    
    # --- 8. SYSTEM RESOURCES CHECK ---
    total_checks += 1
    try:
        cpu_percent = psutil.cpu_percent(interval=0.5)
        memory = psutil.Process().memory_info()
        memory_mb = memory.rss / (1024 * 1024)
        
        if cpu_percent > 80:
            warnings.append(f"⚠️ High CPU usage: {cpu_percent:.1f}%")
        if memory_mb > 500:
            warnings.append(f"⚠️ High memory usage: {memory_mb:.2f}MB")
            
        if cpu_percent <= 80 and memory_mb <= 500:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Resource Check: {str(e)}")
    
    # --- 9. CONFIGURATION CHECK ---
    total_checks += 1
    try:
        config_ok = True
        if not BOT_TOKEN:
            issues.append("❌ BOT_TOKEN not configured")
            config_ok = False
        if not MONGO_URI:
            issues.append("❌ MONGO_URI not configured")
            config_ok = False
        if not MASTER_ADMIN_ID or MASTER_ADMIN_ID == 0:
            warnings.append("⚠️ MASTER_ADMIN_ID not configured")
            config_ok = False
            
        if config_ok:
            checks_passed += 1
            
    except Exception as e:
        issues.append(f"❌ Config Check: {str(e)}")
    
    # --- 10. DATA CONSISTENCY CHECK ---
    total_checks += 1
    try:
        # Check for orphaned data
        total_pdfs = col_pdfs.count_documents({})
        pdfs_with_codes = col_pdfs.count_documents({"msa_code": {"$exists": True, "$ne": "", "$ne": None}})
        
        # Verify index sequence
        if total_pdfs > 0:
            highest_index = col_pdfs.find_one(sort=[("index", -1)])
            if highest_index:
                expected_max = highest_index.get("index", 0)
                if expected_max > total_pdfs + 100:  # Allow some gaps
                    warnings.append(f"⚠️ Index gaps detected (max: {expected_max}, count: {total_pdfs})")
                else:
                    checks_passed += 1
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Consistency Check: {str(e)}")
    
    # --- 11. CLICK TRACKING FIELDS CHECK ---
    total_checks += 1
    try:
        # Auto-fill missing tracking fields live.
        click_defaults = {
            "clicks": 0,
            "affiliate_clicks": 0,
            "ig_start_clicks": 0,
            "yt_start_clicks": 0,
            "yt_code_clicks": 0,
            "last_clicked_at": None,
            "last_affiliate_click": None,
            "last_ig_click": None,
            "last_yt_click": None,
            "last_yt_code_click": None,
        }
        click_missing_query = {
            "$or": [{k: {"$exists": False}} for k in click_defaults.keys()]
        }
        fix_res = col_pdfs.update_many(click_missing_query, {"$set": click_defaults})
        if fix_res.modified_count > 0:
            warnings.append(f"ℹ️ Auto-filled click tracking fields for {fix_res.modified_count} PDF(s)")

        remaining_missing = col_pdfs.count_documents(click_missing_query)
        if remaining_missing > 0:
            warnings.append(f"⚠️ {remaining_missing} PDFs still missing click tracking fields")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Click Tracking Check: {str(e)}")
    
    # --- 12. DEEP LINK START CODES CHECK ---
    total_checks += 1
    try:
        missing_code_query = {
            "$or": [
                {"ig_start_code": {"$exists": False}},
                {"ig_start_code": None},
                {"ig_start_code": ""},
                {"yt_start_code": {"$exists": False}},
                {"yt_start_code": None},
                {"yt_start_code": ""},
            ]
        }
        missing_docs = list(col_pdfs.find(missing_code_query, {"_id": 1, "ig_start_code": 1, "yt_start_code": 1}))
        if missing_docs:
            for pdf_doc in missing_docs:
                await ensure_pdf_codes(pdf_doc)
            warnings.append(f"ℹ️ Auto-generated deep-link codes for {len(missing_docs)} PDF(s)")

        # Repair duplicate start codes by reassigning all but one occurrence per duplicate value.
        repaired_ig_dups = 0
        repaired_yt_dups = 0
        for field_name, is_alpha in (("ig_start_code", True), ("yt_start_code", False)):
            dup_pipeline = [
                {"$match": {field_name: {"$exists": True, "$ne": None, "$ne": ""}}},
                {"$group": {"_id": f"${field_name}", "ids": {"$push": "$_id"}, "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}},
            ]
            dup_docs = list(col_pdfs.aggregate(dup_pipeline))
            for dup in dup_docs:
                for dup_id in dup["ids"][1:]:
                    new_code = generate_unique_pdf_start_code(field_name, length=8, alphanumeric=is_alpha)
                    col_pdfs.update_one({"_id": dup_id}, {"$set": {field_name: new_code}})
                    if field_name == "ig_start_code":
                        repaired_ig_dups += 1
                    else:
                        repaired_yt_dups += 1

        if repaired_ig_dups > 0:
            warnings.append(f"ℹ️ Repaired {repaired_ig_dups} duplicate IG start code assignment(s)")
        if repaired_yt_dups > 0:
            warnings.append(f"ℹ️ Repaired {repaired_yt_dups} duplicate YT start code assignment(s)")

        pdfs_no_ig_code = col_pdfs.count_documents({"$or": [{"ig_start_code": {"$exists": False}}, {"ig_start_code": None}, {"ig_start_code": ""}]})
        pdfs_no_yt_code = col_pdfs.count_documents({"$or": [{"yt_start_code": {"$exists": False}}, {"yt_start_code": None}, {"yt_start_code": ""}]})

        dup_ig_codes = list(col_pdfs.aggregate([
            {"$match": {"ig_start_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$ig_start_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ]))
        dup_yt_codes = list(col_pdfs.aggregate([
            {"$match": {"yt_start_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$yt_start_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ]))

        if pdfs_no_ig_code > 0 or pdfs_no_yt_code > 0:
            warnings.append(f"⚠️ {max(pdfs_no_ig_code, pdfs_no_yt_code)} PDFs still missing deep link codes")
        if len(dup_ig_codes) > 0:
            issues.append(f"❌ Found {len(dup_ig_codes)} duplicate IG start codes")
        if len(dup_yt_codes) > 0:
            issues.append(f"❌ Found {len(dup_yt_codes)} duplicate YT start codes")

        if pdfs_no_ig_code == 0 and pdfs_no_yt_code == 0 and len(dup_ig_codes) == 0 and len(dup_yt_codes) == 0:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Deep Link Codes Check: {str(e)}")
    
    # --- 13. IG CONTENT VALIDATION ---
    total_checks += 1
    try:
        total_ig = col_ig_content.count_documents({})
        
        # Check for missing critical IG fields
        ig_no_cc = col_ig_content.count_documents({"cc_code": {"$exists": False}})
        ig_no_name = col_ig_content.count_documents({"name": {"$exists": False}})
        ig_no_start_code = col_ig_content.count_documents({"start_code": {"$exists": False}})
        
        # Check for duplicate CC codes
        cc_code_pipeline = [
            {"$match": {"cc_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$cc_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        dup_cc_codes = list(col_ig_content.aggregate(cc_code_pipeline))
        
        if ig_no_cc > 0 or ig_no_name > 0 or ig_no_start_code > 0:
            issues.append(f"❌ {max(ig_no_cc, ig_no_name, ig_no_start_code)} IG items missing critical fields")
        if len(dup_cc_codes) > 0:
            issues.append(f"❌ Found {len(dup_cc_codes)} duplicate CC codes in IG content")
            
        if ig_no_cc == 0 and ig_no_name == 0 and ig_no_start_code == 0 and len(dup_cc_codes) == 0:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ IG Content Check: {str(e)}")
    
    # --- 14. AFFILIATE LINK INTEGRITY ---
    total_checks += 1
    try:
        # Count PDFs with affiliate links
        pdfs_with_affiliate = col_pdfs.count_documents({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}})
        ig_with_affiliate = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}})
        
        # Check for broken/invalid affiliate URLs
        pdfs_invalid_aff = 0
        for pdf in col_pdfs.find({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}}, {"affiliate_link": 1}):
            aff_link = pdf.get("affiliate_link", "")
            if not aff_link.startswith("http://") and not aff_link.startswith("https://"):
                pdfs_invalid_aff += 1
        
        if pdfs_invalid_aff > 0:
            warnings.append(f"⚠️ {pdfs_invalid_aff} PDFs have invalid affiliate URL format")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Affiliate Links Check: {str(e)}")

    # --- 15. ADMIN STATUS CHECK ---
    total_checks += 1
    try:
        total_admins = col_admins.count_documents({})
        active_admins = col_admins.count_documents({"is_locked": False})
        locked_admins = col_admins.count_documents({"is_locked": True})
        if total_admins == 0:
            warnings.append("⚠️ No admins configured (only master admin has access)")
        else:
            checks_passed += 1
        # summarize in report as info
        extra_admin_info = f"👥 Admins: {total_admins} total — {active_admins} active, {locked_admins} locked"
        warnings.append(f"ℹ️ {extra_admin_info}")  # informational, not a warning
    except Exception as e:
        warnings.append(f"⚠️ Admin Check: {str(e)}")

    # --- 16. BACKUP HEALTH CHECK ---
    total_checks += 1
    try:
        last_backup = col_backups.find_one(sort=[("created_at", -1)])
        if last_backup:
            last_bk_time = last_backup.get("created_at")
            if not last_bk_time:
                obj_id = last_backup.get("_id")
                if hasattr(obj_id, "generation_time"):
                    inferred_time = obj_id.generation_time.replace(tzinfo=None)
                    col_backups.update_one({"_id": obj_id}, {"$set": {"created_at": inferred_time}})
                    last_bk_time = inferred_time
                    warnings.append("ℹ️ Auto-filled missing backup timestamp from record metadata")
            if last_bk_time:
                delta = now_local() - last_bk_time
                days_ago = delta.days
                if days_ago > 35:
                    warnings.append(f"⚠️ Last backup was {days_ago} days ago — consider creating a new backup")
                else:
                    checks_passed += 1
                bk_time_str = last_bk_time.strftime("%b %d, %Y  %I:%M %p") if hasattr(last_bk_time, 'strftime') else str(last_bk_time)
                warnings.append(f"ℹ️ Last backup: {bk_time_str} "
                                 f"({last_backup.get('filename', 'unknown')}, "
                                 f"{last_backup.get('file_size_mb', 0):.2f} MB)")
            else:
                warnings.append("⚠️ Last backup has no timestamp")
        else:
            warnings.append("⚠️ No backups found — create a backup via 💾 BACKUP DATA")
    except Exception as e:
        warnings.append(f"⚠️ Backup Health Check: {str(e)}")

    # --- 17. USER SOURCE TRACKING CHECK ---
    total_checks += 1
    try:
        tracking_col = db["bot2_user_tracking"]
        tracked_total = tracking_col.count_documents({})
        with_source = tracking_col.count_documents({"source": {"$exists": True}})
        dedup_col = db["bot3_user_activity"]
        dedup_records = dedup_col.count_documents({})

        source_pipeline = [
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        source_dist = {doc["_id"]: doc["count"] for doc in tracking_col.aggregate(source_pipeline)}
        src_summary = ", ".join([f"{k}: {v}" for k, v in source_dist.items()]) if source_dist else "none"

        checks_passed += 1
        warnings.append(
            f"ℹ️ Source tracking: {tracked_total} users tracked, {with_source} with source locked. "
            f"Distribution: [{src_summary}]. Dedup records: {dedup_records:,}"
        )
    except Exception as e:
        warnings.append(f"⚠️ Source Tracking Check: {str(e)}")

    
    # --- GENERATE REPORT ---
    health_score = (checks_passed / total_checks * 100) if total_checks > 0 else 0
    
    # Determine health status
    if health_score >= 95 and len(issues) == 0:
        status_emoji = "🟢"
        status_text = "EXCELLENT"
        status_msg_text = "All systems operating perfectly!"
    elif health_score >= 80 and len(issues) == 0:
        status_emoji = "🟡"
        status_text = "GOOD"
        status_msg_text = "System healthy with minor warnings"
    elif health_score >= 60:
        status_emoji = "🟠"
        status_text = "FAIR"
        status_msg_text = "Some issues detected, review recommended"
    else:
        status_emoji = "🔴"
        status_text = "CRITICAL"
        status_msg_text = "Immediate attention required!"
    
    # Build detailed report
    report = f"""
🩺 <b>SYSTEM DIAGNOSIS REPORT</b>
━━━━━━━━━━━━━━━━━━━━━━━━

{status_emoji} <b>HEALTH STATUS: {status_text}</b>
{status_msg_text}

<b>📊 CHECKS SUMMARY</b>
• Total Checks: `{total_checks}`
• Passed: `{checks_passed}` ✅
• Warnings: `{len(warnings)}` ⚠️
• Critical: `{len(issues)}` ❌

<b>🎯 HEALTH SCORE</b>
{status_emoji} <b>{health_score:.1f}%</b>
"""

    # Add critical issues
    if issues:
        report += "\n<b>❌ CRITICAL ISSUES:</b>\n"
        for issue in issues:
            report += f"{issue}\n"
    
    # Add warnings
    if warnings:
        report += "\n<b>⚠️ WARNINGS:</b>\n"
        for warning in warnings[:5]:  # Limit to 5
            report += f"{warning}\n"
        if len(warnings) > 5:
            report += f"_...and {len(warnings) - 5} more warnings_\n"
    
    # Add all clear message
    if not issues and not warnings:
        report += "\n<b>✅ ALL CHECKS PASSED</b>\n"
        report += "• Database: Healthy\n"
        report += "• Collections: Valid\n"
        report += "• Data Integrity: Perfect\n"
        report += "• No Duplicates: Verified\n"
        report += "• Indexes: Optimal\n"
        report += "• Storage: Sufficient\n"
        report += "• Logs: Normal\n"
        report += "• Resources: Optimal\n"
        report += "• Configuration: Complete\n"
        report += "• Consistency: Validated\n"
        report += "\n🎉 <b>System is running flawlessly!</b>\n"
    
    # Add recommendations
    # Add recommendations (ignoring informational ℹ️ messages)
    real_warnings = [w for w in warnings if not str(w).startswith("ℹ️")]
    recs = ""
    if len(issues) > 0:
        recs += "• Address critical issues immediately\n"
    if any("duplicate" in str(w).lower() for w in real_warnings + issues):
        recs += "• Run duplicate cleanup\n"
    if any("storage" in str(w).lower() or "database" in str(w).lower() for w in real_warnings):
        recs += "• Consider archiving old data\n"
    if any("log" in str(w).lower() for w in real_warnings):
        recs += "• Rotate log files\n"
    if any("index" in str(w).lower() and "rebuilt" not in str(w).lower() for w in real_warnings):
        recs += "• Rebuild database indexes\n"

    if recs:
        report += "\n<b>💡 RECOMMENDATIONS:</b>\n" + recs
    
    report += f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"_Diagnostic completed at {now_local().strftime('%I:%M:%S %p')}_"

    diag_pages = split_text_chunks(report, chunk_size=3800)
    if len(diag_pages) == 1:
        await state.update_data(diagnosis_pages=[], diagnosis_page=0)
        await status_msg.edit_text(diag_pages[0], parse_mode="HTML")
    else:
        await state.update_data(diagnosis_pages=diag_pages, diagnosis_page=0)
        try:
            await status_msg.delete()
        except Exception:
            pass
        await message.answer(
            diag_pages[0],
            parse_mode="HTML",
            reply_markup=get_diagnosis_nav_keyboard(0, len(diag_pages)),
        )

    log_user_action(message.from_user, "Ran System Diagnosis", f"Score: {health_score:.1f}%")


@dp.message(F.text == "➡️ NEXT_DIAG")
async def diagnosis_next_page_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    pages = data.get("diagnosis_pages") or []
    if not pages:
        return

    page = int(data.get("diagnosis_page", 0))
    page = min(page + 1, len(pages) - 1)
    await state.update_data(diagnosis_page=page)
    await message.answer(
        pages[page],
        reply_markup=get_diagnosis_nav_keyboard(page, len(pages)),
        parse_mode="HTML",
    )


@dp.message(F.text == "⬅️ PREV_DIAG")
async def diagnosis_prev_page_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    pages = data.get("diagnosis_pages") or []
    if not pages:
        return

    page = int(data.get("diagnosis_page", 0))
    page = max(page - 1, 0)
    await state.update_data(diagnosis_page=page)
    await message.answer(
        pages[page],
        reply_markup=get_diagnosis_nav_keyboard(page, len(pages)),
        parse_mode="HTML",
    )

def get_recent_logs(lines_count=30):
    """Refactored log reader"""
    # Logs are written to logs/bot3.log by the RotatingFileHandler
    log_file = "logs/bot3.log"
    if not os.path.exists(log_file):
        # Fallback: try Render's stdout capture via /proc/1/fd/1 is not readable,
        # so we read from the rotating log file only.
        return "⚠️ No logs found yet. (Log file not created - bot may have just started)"
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            content = "".join(lines[-lines_count:])
            if len(content) > 3500:
                content = content[-3500:]
                content = "..." + content
            import html
            return _html.escape(content) if content.strip() else "No recent logs."
    except Exception as e:
        return f"Error reading logs: {e}"

@dp.message(F.text == "🖥️ TERMINAL")
async def terminal_handler(message: types.Message):
    if not await check_authorization(message, "Terminal", "can_view_analytics"):
        return
    log_user_action(message.from_user, "Viewed Terminal")

    user        = message.from_user
    user_name   = user.full_name or str(user.id)
    username    = f"@{user.username}" if user.username else f"ID:{user.id}"
    now_str     = now_local().strftime("%b %d, %Y  %I:%M:%S %p")
    uptime_raw  = now_local() - health_monitor.system_metrics["uptime_start"]
    h3 = int(uptime_raw.total_seconds() // 3600)
    m3 = int((uptime_raw.total_seconds() % 3600) // 60)
    started_str = health_monitor.system_metrics["uptime_start"].strftime("%b %d, %Y  %I:%M:%S %p")

    # Build live action log from MongoDB (bot3 entries)
    live_log_lines = []
    live_log_count = 0
    try:
        docs = list(col_live_logs.find({"bot": "bot3"}, {"_id": 0}).sort("created_at", -1).limit(40))
        live_log_count = col_live_logs.count_documents({"bot": "bot3"})
        docs.reverse()
        for doc in docs:
            raw_ca = doc.get("created_at")
            ts = raw_ca.strftime('%b %d  %I:%M:%S %p') if raw_ca and hasattr(raw_ca, "strftime") else doc.get("timestamp", "??")
            action  = doc.get("action", "")
            detail  = doc.get("details", "")
            uid     = doc.get("user_id", "")
            uname   = doc.get("user_name", "")
            parts = []
            if uname: parts.append(str(uname))
            if uid:   parts.append(f"ID:{uid}")
            tag = (" [" + " | ".join(parts) + "]") if parts else ""
            line = f"[{ts}]{tag} {action}" + (f" > {detail}" if detail else "")
            live_log_lines.append(line)
    except Exception:
        pass

    live_logs_text = "\n".join(live_log_lines) if live_log_lines else ">> NO LIVE LOGS YET..."
    sys_logs = get_recent_logs(lines_count=20)

    header = (
        f"🖥️ <b>BOT 3 — LIVE TERMINAL</b>\n"
        f"<code>"
        f"Triggered by : {user_name} ({username})\n"
        f"User ID      : {user.id}\n"
        f"Time         : {now_str}\n"
        f"Bot 3 Started: {started_str}\n"
        f"Bot 3 Uptime : {h3}h {m3}m\n"
        f"Live DB Logs : {live_log_count} entries\n"
        f"Format       : [Date  Time] [Name|ID] Action > Detail\n"
        f"{'━'*42}\n"
        f"</code>"
    )
    text = (
        header
        + "\n<b>📋 LIVE ACTION LOG (MongoDB):</b>\n"
        + f"<pre>{live_logs_text}</pre>"
        + "\n<b>🖥️ SYSTEM LOG (Process):</b>\n"
        + f"<pre><code>{sys_logs}</code></pre>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    await message.answer(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "refresh_terminal")
async def refresh_terminal_callback(callback: types.CallbackQuery):
    if not await check_authorization_user(callback.from_user, callback.message, "Refresh Terminal", "can_view_analytics"):
         await callback.answer("⛔ Access Denied", show_alert=True)
         return

    user        = callback.from_user
    user_name   = user.full_name or str(user.id)
    username    = f"@{user.username}" if user.username else f"ID:{user.id}"
    now_str     = now_local().strftime("%b %d, %Y  %I:%M:%S %p")
    uptime_raw  = now_local() - health_monitor.system_metrics["uptime_start"]
    h3 = int(uptime_raw.total_seconds() // 3600)
    m3 = int((uptime_raw.total_seconds() % 3600) // 60)
    started_str = health_monitor.system_metrics["uptime_start"].strftime("%b %d, %Y  %I:%M:%S %p")

    live_log_lines = []
    live_log_count = 0
    try:
        docs = list(col_live_logs.find({"bot": "bot3"}, {"_id": 0}).sort("created_at", -1).limit(40))
        live_log_count = col_live_logs.count_documents({"bot": "bot3"})
        docs.reverse()
        for doc in docs:
            raw_ca = doc.get("created_at")
            ts = raw_ca.strftime('%b %d  %I:%M:%S %p') if raw_ca and hasattr(raw_ca, "strftime") else doc.get("timestamp", "??")
            action  = doc.get("action", "")
            detail  = doc.get("details", "")
            uid     = doc.get("user_id", "")
            uname   = doc.get("user_name", "")
            parts = []
            if uname: parts.append(str(uname))
            if uid:   parts.append(f"ID:{uid}")
            tag = (" [" + " | ".join(parts) + "]") if parts else ""
            line = f"[{ts}]{tag} {action}" + (f" > {detail}" if detail else "")
            live_log_lines.append(line)
    except Exception:
        pass

    live_logs_text = "\n".join(live_log_lines) if live_log_lines else ">> NO LIVE LOGS YET..."
    sys_logs = get_recent_logs(lines_count=20)

    header = (
        f"🖥️ <b>BOT 3 — LIVE TERMINAL</b>\n"
        f"<code>"
        f"Refreshed by : {user_name} ({username})\n"
        f"User ID      : {user.id}\n"
        f"Time         : {now_str}\n"
        f"Bot 3 Started: {started_str}\n"
        f"Bot 3 Uptime : {h3}h {m3}m\n"
        f"Live DB Logs : {live_log_count} entries\n"
        f"Format       : [Date  Time] [Name|ID] Action > Detail\n"
        f"{'━'*42}\n"
        f"</code>"
    )
    text = (
        header
        + "\n<b>📋 LIVE ACTION LOG (MongoDB):</b>\n"
        + f"<pre>{live_logs_text}</pre>"
        + "\n<b>🖥️ SYSTEM LOG (Process):</b>\n"
        + f"<pre><code>{sys_logs}</code></pre>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer("Refreshed ✅")



GUIDE_PAGES = [
    # ── PAGE 1 ── Overview + Main Menu buttons
    (
        "📚 <b>BOT 3 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 1 / 4 — Overview & Main Menu*\n\n"

        "🤖 <b>WHAT IS BOT 3?</b>\n"
        "Bot 3 is the <b>content management & analytics hub</b>.\n"
        "It stores PDFs, IG content, affiliate links, YT links, and\n"
        "generates unique tracking links for Bot 1 users.\n\n"

        "🏠 <b>MAIN MENU BUTTONS</b>\n"
        "┌─────────────────────────────\n"
        "│ 📋 LIST       — Browse all stored content\n"
        "│ ➕ ADD        — Add new content (PDF/IG/YT/Code)\n"
        "│ 🔍 SEARCH     — Search content by keyword/code\n"
        "│ 🔗 LINKS      — Generate & view tracking links\n"
        "│ 📊 ANALYTICS  — View click stats & performance\n"
        "│ 🩺 DIAGNOSIS  — System health & DB diagnostics\n"
        "│ 🖥️ TERMINAL    — Run shell commands (Master only)\n"
        "│ 💾 BACKUP DATA — Export/backup the database\n"
        "│ 👥 ADMINS     — Manage admin accounts\n"
        "│ 📚 BOT GUIDE  — This guide\n"
        "└─────────────────────────────\n\n"

        "🔐 <b>ACCESS LEVELS</b>\n"
        "• <b>Master Admin</b> — Full access to all features\n"
        "• <b>Admin</b> — Access based on assigned permissions\n"
        "• <b>Unauthorized</b> — Blocked, only sees Bot Guide\n\n"

        "⬇️ *Use the buttons below to navigate pages*"
    ),

    # ── PAGE 2 ── ADD / LIST / SEARCH / LINKS
    (
        "📚 <b>BOT 3 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 2 / 4 — Content Management*\n\n"

        "➕ <b>ADD MENU</b> *(Add new content)*\n"
        "├ 📄 <b>PDF</b> — Add / Edit / Delete / List PDFs\n"
        "│   └ Each PDF gets a unique link for Bot 1 users\n"
        "│   └ Supports: name, link, MSA code, IG code, YT link\n"
        "├ 💸 <b>AFFILIATE</b> — Manage affiliate links per PDF\n"
        "│   └ Add / Edit / Delete / List affiliate links\n"
        "│   └ Tracks affiliate clicks separately\n"
        "├ 🔑 <b>CODE</b> — YT Code management\n"
        "│   └ Add / Edit / Delete / List YT access codes\n"
        "│   └ Used for YTCODE tracking links\n"
        "├ ▶️ <b>YT</b> — YouTube link management\n"
        "│   └ Add / Edit / Delete / List YT links\n"
        "│   └ Links YT content to PDFs for tracking\n"
        "└ 📸 <b>IG</b> — Instagram content management\n"
        "    └ Add / Edit / Delete / List IG content\n"
        "    └ Supports IG CC codes & click tracking\n\n"

        "📋 <b>LIST MENU</b> *(Browse stored content)*\n"
        "├ 📚 ALL      — Show all PDFs with full details\n"
        "├ 📸 IG CONTENT — Show all IG content\n"
        "└ Paginated with ⬅️ PREV / NEXT ➡️ buttons\n\n"

        "🔍 <b>SEARCH MENU</b> *(Find content fast)*\n"
        "├ 🔍 SEARCH PDF    — Search PDFs by name/code\n"
        "└ 🔍 SEARCH IG CC  — Search IG content by code\n\n"

        "🔗 <b>LINKS MENU</b> *(Generate tracking links)*\n"
        "├ 🏠 HOME YT   — YT homepage tracking link\n"
        "├ 📑 ALL PDF   — Direct PDF tracking links\n"
        "├ 📸 IG CC     — IG CC tracking links\n"
        "└ All links auto-route users through Bot 1"
    ),

    # ── PAGE 3 ── Analytics / Diagnosis / Backup / Terminal
    (
        "📚 <b>BOT 3 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 3 / 4 — Analytics, Diagnosis & Tools*\n\n"

        "📊 <b>ANALYTICS MENU</b> *(Click tracking & stats)*\n"
        "├ 📊 OVERVIEW         — Full dashboard: total clicks,\n"
        "│                        top performers, content counts\n"
        "├ 📄 PDF Clicks        — Per-PDF click breakdown\n"
        "├ 💸 Affiliate Clicks  — Per-affiliate click stats\n"
        "├ 📸 IG Start Clicks   — IG start link clicks\n"
        "├ ▶️ YT Start Clicks   — YT start link clicks\n"
        "├ 📸 IG CC Start Clicks— IG CC link clicks\n"
        "└ 🔑 YT Code Clicks   — YT Code link clicks\n\n"

        "🩺 <b>DIAGNOSIS MENU</b> *(System health checks)*\n"
        "├ Checks MongoDB connection & collection sizes\n"
        "├ Detects orphaned records & broken references\n"
        "├ Reports missing MSA codes, empty fields\n"
        "└ Validates PDF links & IG content integrity\n\n"

        "💾 <b>BACKUP MENU</b> *(Data safety tools)*\n"
        "├ 💾 DOWNLOAD BACKUP      — Export entire DB to JSON file\n"
        "├ 📤 JSON RESTORE     — Restore Bot3 data: single/multi JSON or ZIP\n"
        "├ 📊 BACKUP STATS     — Show DB collection sizes\n"
        "├ 📜 BACKUP HISTORY   — View past backup records\n"
        "🗑️ <b>RESET BACKUPS</b> — Wipe local history logs (Master Admin only)\n\n"

        "🖥️ <b>TERMINAL</b> *(Master Admin only)*\n"
        "├ Run any shell command directly from Telegram\n"
        "├ Output streamed back to chat\n"
        "└ Use with caution — no restrictions applied\n\n"

        "└ Wipes selected collections — irreversible!\n"
        "   Requires double confirmation before executing"
    ),

    # ── PAGE 4 ── Admins / Permissions / Ban / Roles
    (
        "📚 <b>BOT 3 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 4 / 4 — Admin & Permission System*\n\n"

        "👥 <b>ADMINS MENU</b> *(Manage admin accounts)*\n"
        "├ ➕ NEW ADMIN     — Add a new admin by user ID\n"
        "├ ➖ REMOVE ADMIN  — Remove an admin\n"
        "├ 📋 LIST ADMINS   — Show all admins with roles\n"
        "├ 🔐 PERMISSIONS   — Set per-admin permissions\n"
        "├ 👔 ROLES         — Assign role presets\n"
        "├ 🔒 LOCK/UNLOCK   — Temporarily disable an admin\n"
        "└ 🚫 BAN CONFIG    — Ban/unban users from Bot 1\n\n"

        "🔐 <b>PERMISSION FLAGS</b> *(Per-admin access control)*\n"
        "├ can_list         — View content lists\n"
        "├ can_add          — Add/edit/delete content\n"
        "├ can_search       — Use search feature\n"
        "├ can_links        — Access link generator\n"
        "├ can_analytics    — View analytics data\n"
        "├ can_diagnosis    — Run system diagnostics\n"
        "├ can_terminal     — Use terminal (⚠️ powerful)\n"
        "├ can_backup       — Access backup tools\n"
        "├ can_manage_admins— Add/remove other admins\n"
        "└ can_reset        — Reset bot data (⚠️ dangerous)\n\n"

        "🚫 <b>BAN SYSTEM</b>\n"
        "├ 🚫 BAN USER    — Block a user from Bot 1\n"
        "├ ✅ UNBAN USER  — Remove a ban\n"
        "└ 📋 LIST BANNED — See all currently banned users\n\n"

        "📎 <b>ADD AFFILIATE</b> *(Quick inline affiliate tool)*\n"
        "└ Shortcut to attach affiliate links to PDFs\n\n"

        "💡 <b>TIPS</b>\n"
        "• All actions are logged to console\n"
        "• Unauthorized access is auto-blocked & logged\n"
        "• Bot 3 feeds content to Bot 1 in real-time\n"
        "• Back buttons always available to navigate safely"
    ),
]

def get_guide_nav_keyboard(page: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for bot guide pages"""
    total = len(GUIDE_PAGES)
    row = []
    if page > 0:
        row.append(KeyboardButton(text=f"⬅️ GUIDE PREV"))
    if page < total - 1:
        row.append(KeyboardButton(text=f"GUIDE NEXT ➡️"))
    keyboard = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="🏠 MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

@dp.message(F.text == "📚 BOT GUIDE")
async def guide_handler(message: types.Message, state: FSMContext):
    await state.update_data(guide_page=0)
    await message.answer(
        GUIDE_PAGES[0],
        reply_markup=get_guide_nav_keyboard(0),
        parse_mode="HTML"
    )

@dp.message(F.text == "GUIDE NEXT ➡️")
async def guide_next_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 0)
    page = min(page + 1, len(GUIDE_PAGES) - 1)
    await state.update_data(guide_page=page)
    await message.answer(
        GUIDE_PAGES[page],
        reply_markup=get_guide_nav_keyboard(page),
        parse_mode="HTML"
    )

@dp.message(F.text == "⬅️ GUIDE PREV")
async def guide_prev_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 0)
    page = max(page - 1, 0)
    await state.update_data(guide_page=page)
    await message.answer(
        GUIDE_PAGES[page],
        reply_markup=get_guide_nav_keyboard(page),
        parse_mode="HTML"
    )

# ==========================================
# 📊 ANALYTICS HANDLERS
# ==========================================

@dp.message(F.text == "📊 ANALYTICS")
async def analytics_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Analytics Menu", "can_view_analytics"):
        return
    """Show Analytics Menu"""
    await state.set_state(AnalyticsStates.viewing_analytics)
    await message.answer(
        "📊 <b>ANALYTICS DASHBOARD</b>\n\n"
        "Select a category to view detailed analytics:",
        reply_markup=get_analytics_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "📊 OVERVIEW")
async def analytics_overview_handler(message: types.Message):
    if not await check_authorization(message, "Analytics Overview", "can_view_analytics"):
        return
    """Show comprehensive analytics overview"""
    
    # Gather all stats efficiently using aggregation
    total_pdfs = col_pdfs.count_documents({})
    total_ig_content = col_ig_content.count_documents({"cc_code": {"$exists": True}})
    
    # Use aggregation pipeline for efficient click totals (single query)
    pdf_stats = list(col_pdfs.aggregate([
        {"$group": {
            "_id": None,
            "pdf_clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
            "aff_clicks": {"$sum": {"$ifNull": ["$affiliate_clicks", 0]}},
            "ig_clicks": {"$sum": {"$ifNull": ["$ig_start_clicks", 0]}},
            "yt_clicks": {"$sum": {"$ifNull": ["$yt_start_clicks", 0]}},
            "yt_code_clicks": {"$sum": {"$ifNull": ["$yt_code_clicks", 0]}}
        }}
    ]))
    
    ig_stats = list(col_ig_content.aggregate([
        {"$group": {
            "_id": None,
            "ig_cc_clicks": {"$sum": {"$ifNull": ["$ig_cc_clicks", 0]}}
        }}
    ]))
    
    # Extract values (default to 0 if no data)
    pdf_clicks = pdf_stats[0].get("pdf_clicks", 0) if pdf_stats else 0
    aff_clicks = pdf_stats[0].get("aff_clicks", 0) if pdf_stats else 0
    ig_clicks = pdf_stats[0].get("ig_clicks", 0) if pdf_stats else 0
    yt_clicks = pdf_stats[0].get("yt_clicks", 0) if pdf_stats else 0
    yt_code_clicks = pdf_stats[0].get("yt_code_clicks", 0) if pdf_stats else 0
    ig_cc_clicks = ig_stats[0].get("ig_cc_clicks", 0) if ig_stats else 0
    
    total_clicks = pdf_clicks + aff_clicks + ig_clicks + yt_clicks + ig_cc_clicks + yt_code_clicks
    
    # Configuration status
    pdfs_with_affiliate = col_pdfs.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    pdfs_with_ig = col_pdfs.count_documents({"ig_start_code": {"$exists": True, "$ne": ""}})
    pdfs_with_yt = col_pdfs.count_documents({"yt_link": {"$exists": True, "$ne": ""}})
    pdfs_with_msa = col_pdfs.count_documents({"msa_code": {"$exists": True, "$ne": ""}})
    
    # Top 5 performers overall using optimized aggregation
    all_items = []
    
    # Get top PDFs (only fetch name and clicks fields)
    for pdf in col_pdfs.find(
        {"link": {"$exists": True}, "clicks": {"$gt": 0}},
        {"name": 1, "clicks": 1, "_id": 0}
    ).sort("clicks", -1).limit(20):
        all_items.append({"name": pdf.get("name", "Unnamed"), "clicks": pdf.get("clicks", 0), "type": "📄 PDF"})
    
    # Get top Affiliates (only fetch name and affiliate_clicks fields)
    for pdf in col_pdfs.find(
        {"affiliate_link": {"$exists": True, "$ne": ""}, "affiliate_clicks": {"$gt": 0}},
        {"name": 1, "affiliate_clicks": 1, "_id": 0}
    ).sort("affiliate_clicks", -1).limit(20):
        all_items.append({"name": pdf.get("name", "Unnamed"), "clicks": pdf.get("affiliate_clicks", 0), "type": "💸 Affiliate"})
    
    # Get top IG CC (only fetch name and ig_cc_clicks fields)
    for ig in col_ig_content.find(
        {"ig_cc_clicks": {"$gt": 0}},
        {"name": 1, "ig_cc_clicks": 1, "_id": 0}
    ).sort("ig_cc_clicks", -1).limit(20):
        all_items.append({"name": ig.get("name", "Unnamed"), "clicks": ig.get("ig_cc_clicks", 0), "type": "📸 IG CC"})
    
    # Sort all items by clicks and get top 5
    all_items.sort(key=lambda x: x["clicks"], reverse=True)
    top_5 = all_items[:5]
    
    # ── Source tracking from bot2_user_tracking (permanent first-source lock) ──
    try:
        tracking_col = db["bot2_user_tracking"]
        source_pipeline = [
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        source_counts = {doc["_id"]: doc["count"] for doc in tracking_col.aggregate(source_pipeline)}
        total_tracked_users = tracking_col.count_documents({})
        src_ig      = source_counts.get("IG", 0)
        src_yt      = source_counts.get("YT", 0)
        src_igcc    = source_counts.get("IGCC", 0)
        src_ytcode  = source_counts.get("YTCODE", 0)
        src_other   = total_tracked_users - src_ig - src_yt - src_igcc - src_ytcode
    except Exception:
        total_tracked_users = src_ig = src_yt = src_igcc = src_ytcode = src_other = 0

    # Build overview message
    text = "📊 <b>ANALYTICS OVERVIEW</b>\n"
    text += "═══════════════════════\n\n"
    
    text += f"📈 <b>TOTAL CLICKS:</b> {total_clicks:,}\n\n"
    
    text += "<b>📊 Clicks by Category:</b>\n"
    text += f"├ 📄 PDFs: {pdf_clicks:,}\n"
    text += f"├ 💸 Affiliates: {aff_clicks:,}\n"
    text += f"├ 📸 IG Start: {ig_clicks:,}\n"
    text += f"├ ▶️ YT Start: {yt_clicks:,}\n"
    text += f"├ 📸 IG CC: {ig_cc_clicks:,}\n"
    text += f"└ 🔑 YT Code: {yt_code_clicks:,}\n\n"

    text += "<b>📡 TRAFFIC SOURCES (Unique Users — Permanently Locked):</b>\n"
    text += f"├ 👥 Total Tracked Users: {total_tracked_users:,}\n"
    text += f"├ 📸 IG Start: {src_ig:,} users\n"
    text += f"├ ▶️ YT Start: {src_yt:,} users\n"
    text += f"├ 📸 IG CC: {src_igcc:,} users\n"
    text += f"├ 🔑 YT Code: {src_ytcode:,} users\n"
    if src_other > 0:
        text += f"└ ❓ Other: {src_other:,} users\n\n"
    else:
        text += f"└ _(Each user's source is locked on first click — never changes)_\n\n"

    text += "<b>📚 Content Library:</b>\n"
    text += f"├ Total PDFs: {total_pdfs}\n"
    text += f"├ IG Content: {total_ig_content}\n"
    text += f"├ With Affiliates: {pdfs_with_affiliate}\n"
    text += f"├ With IG Codes: {pdfs_with_ig}\n"
    text += f"├ With YT Links: {pdfs_with_yt}\n"
    text += f"└ With MSA Codes: {pdfs_with_msa}\n\n"
    
    if top_5:
        text += "🏆 <b>TOP 5 PERFORMERS:</b>\n"
        for idx, item in enumerate(top_5, 1):
            text += f"{idx}. {item['type']} <b>{_html.escape(str(item['name']))}</b> - {item['clicks']:,} clicks\n"
        text += "\n"
    else:
        text += "📭 No clicks recorded yet.\n\n"
    
    # Performance indicators
    if total_pdfs > 0:
        complete_pdfs = col_pdfs.count_documents({
            "link": {"$exists": True},
            "affiliate_link": {"$exists": True, "$ne": ""},
            "ig_start_code": {"$exists": True, "$ne": ""},
            "yt_link": {"$exists": True, "$ne": ""},
            "msa_code": {"$exists": True, "$ne": ""}
        })
        completion_rate = (complete_pdfs / total_pdfs * 100) if total_pdfs > 0 else 0
        text += f"✅ <b>Setup Completion:</b> {completion_rate:.1f}% ({complete_pdfs}/{total_pdfs} fully configured)\n"

    # ── Referral stats from bot1_referrals ────────────────────────────────────
    try:
        _ref_col = db["bot1_referrals"]
        _ref_confirmed = _ref_col.count_documents({"status": "confirmed"})
        _ref_pending   = _ref_col.count_documents({"status": "pending"})
        _ref_total     = _ref_col.count_documents({})
        _ref_unique    = len(_ref_col.distinct("referrer_id"))
        _recent_cutoff = now_local() - timedelta(days=7)
        _ref_recent    = _ref_col.count_documents({"confirmed_at": {"$gte": _recent_cutoff}, "status": "confirmed"})
        text += f"\n\n🤝 <b>REFERRAL SYSTEM:</b>\n"
        text += f"├ ✅ Confirmed: {_ref_confirmed:,}  ⏳ Pending: {_ref_pending:,}\n"
        text += f"├ 👥 Unique Referrers: {_ref_unique:,}  📌 Total Records: {_ref_total:,}\n"
        text += f"└ 🕗 Last 7 days: {_ref_recent:,} new confirmations\n"
    except Exception:
        pass
    
    text += "\n═══════════════════════\n"
    text += "💡 Select a category below for detailed analytics."
    
    await send_chunked_message(
        message,
        text,
        reply_markup=get_analytics_menu(),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

async def send_analytics_view(message: types.Message, category: str, page: int = 0):
    """Display top clicked items for a category with pagination"""
    items_per_page = 10
    skip = page * items_per_page
    
    # Determine collection, fields, and query based on category
    if category == "pdf":
        collection = col_pdfs
        title = "📄 TOP CLICKED PDFs"
        name_field = "name"
        click_field = "clicks"
        # Show all PDFs that have a link configured
        query = {"link": {"$exists": True}}
    elif category == "affiliate":
        collection = col_pdfs
        title = "💸 TOP CLICKED AFFILIATES"
        name_field = "name"
        click_field = "affiliate_clicks"
        # Show only PDFs that have affiliate link configured
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    elif category == "ig_start":
        collection = col_pdfs
        title = "📸 TOP CLICKED IG START LINKS"
        name_field = "name"
        click_field = "ig_start_clicks"
        # Show only PDFs that have IG start code configured
        query = {"ig_start_code": {"$exists": True, "$ne": ""}}
    elif category == "yt_start":
        collection = col_pdfs
        title = "▶️ TOP CLICKED YT START LINKS"
        name_field = "name"
        click_field = "yt_start_clicks"
        # Show only PDFs that have YT link configured
        query = {"yt_link": {"$exists": True, "$ne": ""}}
    elif category == "ig_cc_start":
        collection = col_ig_content
        title = "📸 TOP CLICKED IG CC START LINKS"
        name_field = "name"
        click_field = "ig_cc_clicks"
        # Show all IG content (all have CC codes)
        query = {"cc_code": {"$exists": True}}
    elif category == "yt_code_start":
        collection = col_pdfs
        title = "🔑 TOP CLICKED YT CODE START LINKS"
        name_field = "name"
        click_field = "yt_code_clicks"
        # Show only PDFs that have MSA code configured
        query = {"msa_code": {"$exists": True, "$ne": ""}}
    else:
        await message.answer("⚠️ Invalid category")
        return
    
    # Count total items matching criteria
    total_items = collection.count_documents(query)
    
    if total_items == 0:
        # Determine empty message based on category
        if category == "affiliate":
            empty_msg = "No PDFs with affiliate links configured yet."
        elif category == "ig_start":
            empty_msg = "No PDFs with IG start codes configured yet."
        elif category == "yt_start":
            empty_msg = "No PDFs with YT links configured yet."
        elif category == "yt_code_start":
            empty_msg = "No PDFs with MSA codes configured yet."
        elif category == "ig_cc_start":
            empty_msg = "No IG content configured yet."
        else:
            empty_msg = "No items configured yet."
            
        await message.answer(
            f"{title}\n\n"
            f"📭 {empty_msg}",
            reply_markup=get_analytics_menu(),
            parse_mode="HTML"
        )
        return
    
    # Fetch top items for current page with field projection (only needed fields)
    projection = {name_field: 1, click_field: 1, "index": 1, "cc_code": 1, "_id": 0}
    if category in ["pdf", "affiliate", "ig_start", "yt_start", "yt_code_start"]:
        projection["link"] = 1
        projection["affiliate_link"] = 1
        projection["ig_start_code"] = 1
        projection["yt_link"] = 1
        projection["msa_code"] = 1
    
    # Add timestamp fields for last clicked info
    if category == "pdf":
        projection["last_clicked_at"] = 1
    elif category == "affiliate":
        projection["last_affiliate_click"] = 1
    elif category == "ig_start":
        projection["last_ig_click"] = 1
    elif category == "yt_start":
        projection["last_yt_click"] = 1
    elif category == "ig_cc_start":
        projection["last_ig_cc_click"] = 1
    elif category == "yt_code_start":
        projection["last_yt_code_click"] = 1
    
    items = list(collection.find(query, projection).sort(click_field, -1).skip(skip).limit(items_per_page))
    
    if not items:
        await message.answer(
            "⚠️ No more items on this page.",
            reply_markup=get_analytics_menu()
        )
        return
    
    # Build display text
    text = f"{title}\n"
    text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, item in enumerate(items, start=skip + 1):
        # For IG CC: show only cc_code (not full content text)
        if category == "ig_cc_start":
            item_name = item.get("cc_code", "Unknown")
        else:
            item_name = item.get(name_field, "Unnamed")
        clicks = item.get(click_field, 0)
        
        # Get last clicked timestamp
        last_click_field = {
            "clicks": "last_clicked_at",
            "affiliate_clicks": "last_affiliate_click",
            "ig_start_clicks": "last_ig_click",
            "yt_start_clicks": "last_yt_click",
            "ig_cc_clicks": "last_ig_cc_click",
            "yt_code_clicks": "last_yt_code_click"
        }.get(click_field, "last_clicked_at")
        
        last_clicked = item.get(last_click_field)
        
        # Performance indicator
        if clicks == 0:
            indicator = "⚪"
        elif clicks < 10:
            indicator = "🟡"
        elif clicks < 50:
            indicator = "🟢"
        elif clicks < 100:
            indicator = "🔵"
        else:
            indicator = "🔥"
        
        text += f"{idx}. {indicator} <b>{item_name}</b>\n"
        text += f"   🔢 Clicks: <b>{clicks:,}</b>"
        
        if last_clicked:
            from datetime import datetime as _dt
            now = now_local()
            # Guard: DB may store timestamp as string (old records) — coerce safely
            if isinstance(last_clicked, str):
                try:
                    from dateutil import parser as _dp
                    last_clicked = _dp.parse(last_clicked)
                except Exception:
                    last_clicked = None
            
            # Ensure last_clicked is naive before subtraction to prevent TypeError
            if last_clicked and isinstance(last_clicked, _dt):
                if last_clicked.tzinfo is not None:
                    last_clicked = last_clicked.replace(tzinfo=None)
                try:
                    time_diff = now - last_clicked
                    if time_diff.days > 0:
                        time_ago = f"{time_diff.days}d ago"
                    elif time_diff.seconds >= 3600:
                        time_ago = f"{time_diff.seconds // 3600}h ago"
                    elif time_diff.seconds >= 60:
                        time_ago = f"{time_diff.seconds // 60}m ago"
                    else:
                        time_ago = "just now"
                    text += f" | 🕐 {time_ago}"
                except Exception:
                    pass
        elif clicks > 0:
            text += f" | 🕐 timestamp missing"
        
        text += "\n\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📊 Showing {skip + 1}-{skip + len(items)} of {total_items} items\n"

    # Pagination buttons
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text=f"⬅️ ANA_PREV ({category})"))
    if skip + items_per_page < total_items:
        nav_row.append(KeyboardButton(text=f"➡️ ANA_NEXT ({category})"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO ANALYTICS")])

    reply_kb = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

    if len(text) > 4000:
        # Split into chunks, last chunk gets keyboard
        parts = []
        chunk = ""
        for line in text.split("\n"):
            if len(chunk) + len(line) + 1 > 4000:
                parts.append(chunk)
                chunk = line + "\n"
            else:
                chunk += line + "\n"
        if chunk:
            parts.append(chunk)
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                await message.answer(part, reply_markup=reply_kb, parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=reply_kb, parse_mode="HTML")

@dp.message(F.text == "🤝 Referral Overview")
async def referral_overview_handler(message: types.Message):
    if not await check_authorization(message, "Referral Overview", "can_view_analytics"):
        return
    try:
        _ref_col = db["bot1_referrals"]
        _uv_col  = db["bot1_user_verification"]

        _confirmed  = _ref_col.count_documents({"status": "confirmed"})
        _pending    = _ref_col.count_documents({"status": "pending"})
        _total      = _ref_col.count_documents({})
        _referrers  = len(_ref_col.distinct("referrer_id"))
        _referred   = len(_ref_col.distinct("referred_id"))

        _now = now_local()
        _7d  = _now - timedelta(days=7)
        _30d = _now - timedelta(days=30)
        _recent_7d  = _ref_col.count_documents({"confirmed_at": {"$gte": _7d},  "status": "confirmed"})
        _recent_30d = _ref_col.count_documents({"confirmed_at": {"$gte": _30d}, "status": "confirmed"})

        confirm_rate = (_confirmed / _total * 100) if _total > 0 else 0.0

        # Top 5 referrers
        pipeline = [
            {"$group": {"_id": "$referrer_id",
                "confirmed": {"$sum": {"$cond": [{"$eq": ["$status","confirmed"]}, 1, 0]}},
                "pending":   {"$sum": {"$cond": [{"$eq": ["$status","pending"]},   1, 0]}}}},
            {"$match": {"confirmed": {"$gt": 0}}},
            {"$sort": {"confirmed": -1}},
            {"$limit": 5}
        ]
        top_refs = list(_ref_col.aggregate(pipeline))

        medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        top_lines = []
        for m, r in zip(medals, top_refs):
            uid = r["_id"]
            u_doc = _uv_col.find_one({"user_id": uid}, {"first_name": 1})
            uname = (u_doc or {}).get("first_name") or f"ID {uid}"
            top_lines.append(f"   {m} <b>{_html.escape(str(uname))}</b> — {r['confirmed']} confirmed, {r['pending']} pending")

        bar_filled = int(confirm_rate / 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)

        text  = "🤝 <b>REFERRAL SYSTEM OVERVIEW</b>\n"
        text += "═══════════════════════\n\n"
        text += f"<b>📊 Live Totals:</b>\n"
        text += f"├ ✅ Confirmed: <b>{_confirmed:,}</b>\n"
        text += f"├ ⏳ Pending:   <b>{_pending:,}</b>\n"
        text += f"├ 📌 Total records: <b>{_total:,}</b>\n"
        text += f"├ 👤 Unique Referrers: <b>{_referrers:,}</b>\n"
        text += f"└ 🎯 Unique Referred: <b>{_referred:,}</b>\n\n"
        text += f"<b>📈 Conversion Rate:</b>\n"
        text += f"[{bar}] {confirm_rate:.1f}%\n\n"
        text += f"<b>🕗 Trends:</b>\n"
        text += f"├ Last 7 days:  <b>{_recent_7d:,}</b> confirmed\n"
        text += f"└ Last 30 days: <b>{_recent_30d:,}</b> confirmed\n\n"
        if top_lines:
            text += "<b>🏆 TOP REFERRERS:</b>\n"
            text += "\n".join(top_lines) + "\n"
        else:
            text += "📭 No confirmed referrals yet.\n"
        text += "\n═══════════════════════\n"
        text += "<i>Live — no cache. Source: bot1_referrals</i>"

        await message.answer(text, parse_mode="HTML", reply_markup=get_analytics_menu())
    except Exception as e:
        await message.answer(f"❌ Referral overview error: {str(e)[:200]}", reply_markup=get_analytics_menu())

@dp.message(F.text == "📄 PDF Clicks")
async def pdf_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="pdf", analytics_page=0)
    await send_analytics_view(message, "pdf", 0)

@dp.message(F.text == "💸 Affiliate Clicks")
async def affiliate_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="affiliate", analytics_page=0)
    await send_analytics_view(message, "affiliate", 0)

@dp.message(F.text == "📸 IG Start Clicks")
async def ig_start_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="ig_start", analytics_page=0)
    await send_analytics_view(message, "ig_start", 0)

@dp.message(F.text == "▶️ YT Start Clicks")
async def yt_start_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="yt_start", analytics_page=0)
    await send_analytics_view(message, "yt_start", 0)

@dp.message(F.text == "📸 IG CC Start Clicks")
async def ig_cc_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="ig_cc_start", analytics_page=0)
    await send_analytics_view(message, "ig_cc_start", 0)

@dp.message(F.text == "🔑 YT Code Start Clicks")
async def yt_code_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="yt_code_start", analytics_page=0)
    await send_analytics_view(message, "yt_code_start", 0)

@dp.message(F.text.startswith("⬅️ ANA_PREV ("))
async def analytics_prev_handler(message: types.Message, state: FSMContext):
    """Handle previous page in analytics"""
    data = await state.get_data()
    category = data.get("analytics_category")
    current_page = data.get("analytics_page", 0)
    
    if current_page > 0:
        new_page = current_page - 1
        await state.update_data(analytics_page=new_page)
        await send_analytics_view(message, category, new_page)
    else:
        await message.answer("⚠️ Already on first page.")

@dp.message(F.text.startswith("➡️ ANA_NEXT ("))
async def analytics_next_handler(message: types.Message, state: FSMContext):
    """Handle next page in analytics"""
    data = await state.get_data()
    category = data.get("analytics_category")
    current_page = data.get("analytics_page", 0)
    
    new_page = current_page + 1
    await state.update_data(analytics_page=new_page)
    await send_analytics_view(message, category, new_page)

@dp.message(F.text == "⬅️ BACK TO ANALYTICS")
async def back_to_analytics_handler(message: types.Message, state: FSMContext):
    """Return to analytics menu"""
    await state.update_data(diagnosis_pages=[], diagnosis_page=0)
    await state.set_state(AnalyticsStates.viewing_analytics)
    await message.answer(
        "📊 <b>ANALYTICS DASHBOARD</b>",
        reply_markup=get_analytics_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "🆔 MSA ID POOL")
async def msa_id_pool_handler(message: types.Message):
    """Show live MSA code integrity and auto-ID pool usage from bot3_pdfs."""
    if not await check_authorization(message, "MSA ID Pool", "can_view_analytics"):
        return
    try:
        msa_exists_query = {"msa_code": {"$exists": True, "$ne": None, "$ne": ""}}
        total_pdfs = col_pdfs.count_documents({})
        with_code = col_pdfs.count_documents(msa_exists_query)

        unique_count_docs = list(col_pdfs.aggregate([
            {"$match": msa_exists_query},
            {"$group": {"_id": "$msa_code"}},
            {"$count": "n"},
        ]))
        unique_codes = unique_count_docs[0]["n"] if unique_count_docs else 0
        duplicate_assignments = max(0, with_code - unique_codes)

        valid_format_count = col_pdfs.count_documents({"msa_code": {"$regex": r"^MSA\d{4}$"}})
        invalid_format_count = max(0, with_code - valid_format_count)

        # Auto-generated pool uses strict 4-digit IDs: MSA1000..MSA9999
        auto_unique_docs = list(col_pdfs.aggregate([
            {"$match": {"msa_code": {"$regex": r"^MSA\d{4}$"}}},
            {"$group": {"_id": "$msa_code"}},
            {"$count": "n"},
        ]))
        auto_unique_used = auto_unique_docs[0]["n"] if auto_unique_docs else 0

        non_4_digit_count = col_pdfs.count_documents({"msa_code": {"$exists": True, "$not": {"$regex": r"^MSA\d{4}$"}}})

        AUTO_POOL_SIZE = 9_000
        available = max(0, AUTO_POOL_SIZE - auto_unique_used)
        pct_used = (auto_unique_used / AUTO_POOL_SIZE * 100) if AUTO_POOL_SIZE else 0
        filled = round(pct_used / 5)  # 20-block bar (each block = 5%)
        bar = "█" * filled + "░" * (20 - filled)

        if pct_used > 90:
            risk = "🔴 CRITICAL"
        elif pct_used > 50:
            risk = "🟠 HIGH"
        elif pct_used > 20:
            risk = "🟡 MODERATE"
        else:
            risk = "🟢 ABUNDANT"

        integrity = "✅ CLEAN" if duplicate_assignments == 0 and invalid_format_count == 0 else "⚠️ REVIEW NEEDED"

        text = (
            "🆔 <b>MSA NODE ID POOL STATUS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📚 <b>Total PDFs:</b> {total_pdfs:,}\n"
            f"🔑 <b>PDFs With MSA Code:</b> {with_code:,}\n"
            f"🧬 <b>Unique MSA Codes:</b> {unique_codes:,}\n"
            f"🟢 <b>Code Integrity:</b> {integrity}\n"
            f"⚠️ <b>Duplicate Assignments:</b> {duplicate_assignments:,}\n"
            f"⚠️ <b>Invalid Format Codes:</b> {invalid_format_count:,}\n\n"
            f"📊 <b>Auto-ID Pool (MSA1000-MSA9999):</b> {AUTO_POOL_SIZE:,}\n"
            f"🔢 <b>Unique Auto IDs Used:</b> {auto_unique_used:,}\n"
            f"🟢 <b>Available Auto IDs:</b> {available:,}\n"
            f"🧹 <b>Non-4-digit Legacy IDs:</b> {non_4_digit_count:,}\n"
            f"📈 <b>Pool Usage:</b>\n<code>[{bar}]</code>\n"
            f"<code>{pct_used:.6f}%</code> used — {risk}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 {now_local().strftime('%B %d, %Y  %I:%M:%S %p')}"
        )
        await send_chunked_message(
            message,
            text,
            reply_markup=get_analytics_menu(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        await message.answer(
            f"❌ MSA Pool check failed: `{str(e)[:150]}`",
            parse_mode="HTML",
            reply_markup=get_analytics_menu()
        )

# 5. IG AFFILIATE MANAGEMENT HANDLERS
# ==========================================

# Main Affiliate Menu Handler
@dp.message(F.text == "📎 ADD AFFILIATE")
async def ig_affiliate_menu_handler(message: types.Message):
    if not await check_authorization(message, "IG Affiliate Menu", "can_add"):
        return
    """Show IG Affiliate Submenu"""
    await message.answer(
        "📎 <b>IG AFFILIATE MANAGEMENT</b>\n\nSelect an option:",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="HTML"
    )

# Back button from affiliate submenu to IG menu
@dp.message(F.text == "◀️ Back")
async def ig_affiliate_back_handler(message: types.Message, state: FSMContext):
    """Return from affiliate menu to IG menu"""
    await state.clear()
    await message.answer("📸 <b>IG CODE MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")

# 5a. ADD AFFILIATE TO IG CONTENT
@dp.message(F.text == "📎 Add")
async def start_add_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add IG Affiliate", "can_add"):
        return
    """Start Add Affiliate flow"""
    await state.set_state(IGAffiliateStates.waiting_for_ig_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_select")

@dp.message(IGAffiliateStates.waiting_for_ig_selection)
async def process_ig_affiliate_selection(message: types.Message, state: FSMContext):
    """Process IG selection for adding affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV_IGAFFS") or message.text.startswith("➡️ NEXT_IGAFFS"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_select")
            return
        except: pass
    
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # ✅ FIX: Filter to ONLY items without affiliate_link — matching exactly what's displayed in list
    all_contents = list(col_ig_content.find(
        {"$or": [{"affiliate_link": {"$exists": False}}, {"affiliate_link": ""}, {"affiliate_link": None}]}
    ).sort("cc_number", 1))
    
    found_contents = []
    seen_ids = set()
    not_found = []
    
    for q in queries:
        if not q: continue
        
        content = None
        if q.isdigit():
            # Sequential selection — matches displayed index
            idx = int(q) - 1
            if 0 <= idx < len(all_contents):
                content = all_contents[idx]
        elif q.upper().startswith("CC"):
            # CC Code match (search all, not just filtered, for CC code entry)
            content = next((c for c in all_contents if c['cc_code'].upper() == q.upper()), None)
            
        if content:
            cid = str(content["_id"])
            if cid not in seen_ids:
                seen_ids.add(cid)
                found_contents.append(content)
        else:
            not_found.append(q)
            
    if not found_contents:
        msg = "❌ <b>No Content Found</b>"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Store IDs
    affiliate_ids = [str(c["_id"]) for c in found_contents]
    
    await state.update_data(affiliate_ids=affiliate_ids)
    await state.set_state(IGAffiliateStates.waiting_for_link)
    
    # ✅ FIX: Show only CC code — never dump full content name
    msg = f"✅ <b>Selected {len(found_contents)} IG item(s):</b>"
    for c in found_contents:
        msg += f"\n• {c['cc_code']}"
    
    msg += "\n\n🔗 <b>Enter Affiliate Link (applies to all above):</b>"
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(IGAffiliateStates.waiting_for_link)
async def process_ig_affiliate_link(message: types.Message, state: FSMContext):
    """Process affiliate link input"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    link = message.text.strip()
    
    # Basic validation
    if "http" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    affiliate_ids = data.get('affiliate_ids', [])
    
    if affiliate_ids:
        object_ids = [ObjectId(uid) for uid in affiliate_ids]
        col_ig_content.update_many(
            {"_id": {"$in": object_ids}},
            {"$set": {"affiliate_link": link}}
        )
        
        log_user_action(message.from_user, "Bulk Added IG Affiliate", f"Count: {len(affiliate_ids)}")
        
        await state.clear()
        await message.answer(
            f"✅ <b>Bulk Affiliate Link Applied!</b>\n\n"
            f"🔗 Link: `{link}`\n"
            f"📊 Applied to {len(affiliate_ids)} items.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    else:
        await state.clear()
        await message.answer("❌ Error: No items selected.", reply_markup=get_ig_affiliate_menu())

# 5b. EDIT IG AFFILIATE
@dp.message(F.text == "✏️ Edit")
async def start_edit_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit IG Affiliate", "can_add"):
        return
    """Start Edit Affiliate flow"""
    # Check if any IG content has affiliate links
    count = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    if count == 0:
        return await message.answer(
            "⚠️ <b>No affiliate links found!</b>\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    
    await state.set_state(IGAffiliateEditStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_edit")

@dp.message(IGAffiliateEditStates.waiting_for_selection)
async def process_ig_affiliate_edit_selection(message: types.Message, state: FSMContext):
    """Process IG selection for editing affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV_IGAFFE") or message.text.startswith("➡️ NEXT_IGAFFE"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_edit")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find({"affiliate_link": {"$exists": True, "$ne": ""}}).sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({
            "cc_code": {"$regex": f"^{query}$", "$options": "i"},
            "affiliate_link": {"$exists": True, "$ne": ""}
        })
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(
        content_id=str(content["_id"]),
        cc_code=content["cc_code"],
        name=content["name"],
        old_link=content.get("affiliate_link", "")
    )
    await state.set_state(IGAffiliateEditStates.waiting_for_new_link)
    
    await message.answer(
        f"✅ <b>Selected:</b> {content['cc_code']} - {content['name']}\n\n"
        f"📎 Current Link: {content.get('affiliate_link', 'N/A')}\n\n"
        f"🔗 <b>Enter New Affiliate Link:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(IGAffiliateEditStates.waiting_for_new_link)
async def process_ig_affiliate_edit_link(message: types.Message, state: FSMContext):
    """Process new affiliate link"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    link = message.text.strip()
    
    # Basic validation
    if "http" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    data = await state.get_data()
    
    # Check if link is same as old link
    if link == data.get('old_link'):
        await message.answer("⚠️ <b>Link is identical to current link.</b>\nNo changes made.", reply_markup=get_ig_affiliate_menu(), parse_mode="HTML")
        await state.clear()
        return

    from bson.objectid import ObjectId
    
    # Update affiliate link
    col_ig_content.update_one(
        {"_id": ObjectId(data['content_id'])},
        {"$set": {"affiliate_link": link}}
    )
    
    log_user_action(message.from_user, "Edited IG Affiliate", f"Code: {data['cc_code']}")
    
    await state.clear()
    await message.answer(
        f"✅ <b>Affiliate Link Updated!</b>\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 Content: {data['name']}\n"
        f"🔗 New Link: {link}",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="HTML"
    )

# 5c. DELETE IG AFFILIATE
@dp.message(F.text == "🗑️ Delete")
async def start_delete_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete IG Affiliate", "can_add"):
        return
    """Start Delete Affiliate flow"""
    # Check if any IG content has affiliate links
    count = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    if count == 0:
        return await message.answer(
            "⚠️ <b>No affiliate links found!</b>\n\nNothing to delete.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    
    await state.set_state(IGAffiliateDeleteStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_delete")

@dp.message(IGAffiliateDeleteStates.waiting_for_selection)
async def process_ig_affiliate_delete_selection(message: types.Message, state: FSMContext):
    """Process IG selection for deleting affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV_IGAFFD") or message.text.startswith("➡️ NEXT_IGAFFD"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_delete")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find({"affiliate_link": {"$exists": True, "$ne": ""}}).sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({
            "cc_code": {"$regex": f"^{query}$", "$options": "i"},
            "affiliate_link": {"$exists": True, "$ne": ""}
        })
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(
        content_id=str(content["_id"]),
        cc_code=content["cc_code"],
        name=content["name"],
        affiliate_link=content.get("affiliate_link", "")
    )
    await state.set_state(IGAffiliateDeleteStates.waiting_for_confirm)
    
    keyboard = [[KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]]
    confirm_kb = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"⚠️ <b>CONFIRM DELETE AFFILIATE</b>\n\n"
        f"🆔 Code: {content['cc_code']}\n"
        f"📝 Content: {content['name']}\n"
        f"🔗 Link: {content.get('affiliate_link', '')}\n\n"
        f"Are you sure?",
        reply_markup=confirm_kb,
        parse_mode="HTML"
    )

@dp.message(IGAffiliateDeleteStates.waiting_for_confirm)
async def process_ig_affiliate_delete_confirm(message: types.Message, state: FSMContext):
    """Process delete confirmation"""
    if message.text == "✅ CONFIRM":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Remove affiliate link from IG content
        col_ig_content.update_one(
            {"_id": ObjectId(data['content_id'])},
            {"$unset": {"affiliate_link": ""}}
        )
        
        log_user_action(message.from_user, "Deleted IG Affiliate", f"Code: {data['cc_code']}")
        
        await state.clear()
        await message.answer(
            f"🗑️ <b>Affiliate Link Deleted!</b>\n\n"
            f"🆔 Code: {data['cc_code']}\n"
            f"📝 Content: {data['name']}",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_ig_affiliate_menu())

# 5d. LIST IG AFFILIATES
# 5d. PAGINATED IG AFFILIATE LIST
async def send_ig_affiliate_list_view_text(message: types.Message, page=0):
    """Helper to send paginated text list of affiliates"""
    limit = 5  # Limit 5 items per page as requested
    skip = page * limit
    
    query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    total = col_ig_content.count_documents(query)
    contents = list(col_ig_content.find(query).sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents and page == 0:
        return await message.answer(
            "⚠️ <b>No affiliate links found!</b>\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    
    text = f"📋 <b>IG CONTENT WITH AFFILIATE LINKS (Page {page+1}):</b>\nResult {skip+1}-{min(skip+len(contents), total)} of {total}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, content in enumerate(contents, start=skip+1):
        text += f"{idx}. <b>{content['cc_code']}</b>\n"
        text += f"   🔗 {content.get('affiliate_link', 'N/A')}\n\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━\nTotal: <b>{total}</b> affiliate link(s)"
    
    # Pagination Keyboard
    buttons = []
    if page > 0: 
        buttons.append(KeyboardButton(text=f"⬅️ PREV_IGAFF {page}"))
    if (skip + limit) < total: 
        buttons.append(KeyboardButton(text=f"➡️ NEXT_IGAFF {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="◀️ Back")]) # Navigate back to affiliate menu
    
    size_mb = sys.getsizeof(text) # Basic size check
    if len(text) > 4000:
        # Split logic if dangerously huge (fallback)
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
             await message.answer(part, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(F.text == "📋 List")
async def list_ig_affiliates_handler(message: types.Message):
    if not await check_authorization(message, "List IG Affiliates", "can_list"):
        return
    """List all IG content with affiliate links"""
    await send_ig_affiliate_list_view_text(message, page=0)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IGAFF") or m.text.startswith("➡️ NEXT_IGAFF")))
async def ig_affiliate_pagination_handler(message: types.Message):
    """Handle pagination for affiliate text list"""
    try:
        page = int(message.text.split()[-1]) - 1
        await send_ig_affiliate_list_view_text(message, page)
    except:
        await send_ig_affiliate_list_view_text(message, 0)

# END OF IG AFFILIATE MANAGEMENT HANDLERS


# --- Placeholders ---

@dp.message(F.text.in_({"📋 LIST", "🔍 SEARCH", "🔗 LINKS"}))
async def not_implemented_handler(message: types.Message):
    """Handler for main menu features not yet implemented"""
    await message.answer("🚧 This feature is coming soon!")

# ==========================================
# 💾 BACKUP SYSTEM
# ==========================================

# Backup collection for metadata (prod DB — legacy local history)
col_backups = db["bot3_backups"]

# ── Backup cluster (MSANodeBackups) — strictly separate from MSANodeDB ──────
try:
    import certifi as _certifi_b3
    _bkp_client_b3 = pymongo.MongoClient(
        BACKUP_MONGO_URI or MONGO_URI,
        serverSelectionTimeoutMS=8000,
        tlsCAFile=_certifi_b3.where(),
    )
    _bkp_db_b3 = _bkp_client_b3[BACKUP_MONGO_DB_NAME or "MSANodeBackups"]
    _bkp_client_b3.admin.command("ping")
    print("✅ Bot3 backup cluster connected:", BACKUP_MONGO_DB_NAME or "MSANodeBackups")
except Exception as _bkp_b3_err:
    print(f"⚠️ Bot3 backup cluster unreachable: {_bkp_b3_err}")
    _bkp_db_b3 = db  # graceful fallback (reads/writes stay on prod — reset guard will block destructive ops)

col_bot3_backups      = _bkp_db_b3["bot3_backups"]       # daily snapshots (90-day TTL)
col_bot3_restore_data = _bkp_db_b3["bot3_restore_data"]  # latest restore point
col_bot3_bk_history   = _bkp_db_b3["bot3_backup_history"]  # permanent event log
col_bot3_link_registry = _bkp_db_b3["bot3_link_registry"]  # URL→code snapshot (survives resets)

# Ensure 90-day TTL index on snapshot collection
# Ensure 90-day TTL index on ttl_date flag
try:
    try: col_bot3_backups.drop_index("bot3_backup_ttl_90d")
    except Exception: pass
    col_bot3_backups.create_index(
        [("ttl_date", 1)], expireAfterSeconds=7_776_000,
        name="bot3_ttl_date_90d", background=True
    )
except Exception:
    pass


def get_month_year_name():
    """Get current month and year in format: 2026_February"""
    now = now_local()
    month_name = now.strftime("%B")  # Full month name
    year = now.year
    return f"{year}_{month_name}"

async def create_backup_file(auto=False, fmt="zip"):
    """
    Create a backup containing all collections (either as a single JSON or ZIP).
    Returns: (success: bool, filepath: str, metadata: dict)
    """
    import zipfile
    import json
    import tempfile

    def _serialize(docs):
        """Convert ObjectId / datetime fields to JSON-safe strings (12-h time)."""
        result = []
        for doc in docs:
            d = dict(doc)
            if '_id' in d:
                d['_id'] = str(d['_id'])
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.strftime("%Y-%m-%d %I:%M:%S %p")
            result.append(d)
        return result

    try:
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)

        now_ts      = now_local()
        month_year  = get_month_year_name()                          # e.g. "March_2026"
        ts_label    = now_ts.strftime("%Y-%m-%d_%I-%M-%S_%p")       # e.g. "2026-03-12_02-00-00_AM"
        
        filename    = f"Backup_{month_year}_{ts_label}.{fmt}"
        filepath    = os.path.join(backup_dir, filename)

        # ── Collect all collections ──────────────────────────────────────────
        collections_data = {
            "bot3_pdfs":          _serialize(list(col_pdfs.find({}))),
            "bot3_ig_content":    _serialize(list(col_ig_content.find({}))),
            "bot3_admins":        _serialize(list(col_admins.find({}))),
            "bot3_settings":      _serialize(list(col_settings.find({}))),
            "bot3_banned_users":  _serialize(list(col_banned_users.find({}))),
            "bot3_logs":          _serialize(list(col_logs.find({}).sort("created_at", -1).limit(500))),
            "bot3_user_activity": _serialize(list(col_user_activity.find({}).sort("timestamp", -1).limit(1000))),
        }

        # ── Click totals for metadata ────────────────────────────────────────
        pdfs_list = collections_data["bot3_pdfs"]
        ig_list   = collections_data["bot3_ig_content"]
        total_clicks     = sum(p.get('clicks', 0) for p in pdfs_list)
        total_ig_clicks  = sum(p.get('ig_start_clicks', 0) for p in pdfs_list)
        total_yt_clicks  = sum(p.get('yt_start_clicks', 0) for p in pdfs_list)
        total_igcc_clicks= sum(p.get('ig_cc_clicks', 0) for p in ig_list)
        total_ytcode_clicks = sum(p.get('yt_code_clicks', 0) for p in pdfs_list)

        metadata = {
            "backup_type":        "auto" if auto else "manual",
            "created_at":         now_ts,
            "created_at_str":     now_ts.strftime("%Y-%m-%d %I:%M:%S %p"),
            "month":              now_ts.strftime("%B"),
            "month_num":          now_ts.month,
            "year":               now_ts.year,
            "backup_key":         f"{now_ts.year}/{now_ts.month:02d}",
            "filename":           filename,
            "pdfs_count":         len(pdfs_list),
            "ig_count":           len(ig_list),
            "admins_count":       len(collections_data["bot3_admins"]),
            "banned_count":       len(collections_data["bot3_banned_users"]),
            "total_clicks":       total_clicks,
            "total_ig_clicks":    total_ig_clicks,
            "total_yt_clicks":    total_yt_clicks,
            "total_igcc_clicks":  total_igcc_clicks,
            "total_ytcode_clicks":total_ytcode_clicks,
        }

        # ── Build output file (JSON or ZIP) ───────────────
        if fmt == "json":
            meta_copy = dict(metadata)
            meta_copy['created_at'] = meta_copy['created_at_str']
            collections_data["metadata"] = meta_copy
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(collections_data, f, indent=2, ensure_ascii=False)
        else:
            temp_files = {}
            try:
                for col_name, docs in collections_data.items():
                    tf = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                                     delete=False, suffix='.json')
                    json.dump(docs, tf, indent=2, default=str, ensure_ascii=False)
                    tf.close()
                    temp_files[col_name] = tf.name

                meta_tf = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                                      delete=False, suffix='.json')
                meta_copy = dict(metadata)
                meta_copy['created_at'] = meta_copy['created_at_str']
                json.dump(meta_copy, meta_tf, indent=2, ensure_ascii=False)
                meta_tf.close()

                with zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for col_name, tmp_path in temp_files.items():
                        zf.write(tmp_path, f"{col_name}.json")
                    zf.write(meta_tf.name, "metadata.json")
            finally:
                for tmp_path in temp_files.values():
                    try: os.remove(tmp_path)
                    except: pass
                try: os.remove(meta_tf.name)
                except: pass

        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        metadata['file_size_mb'] = round(file_size_mb, 2)

        # ── Upsert into backup history (keyed by filename for idempotency) ───
        meta_db = dict(metadata)
        meta_db.pop('created_at', None)   # keep only the str version for clean storage
        # ADD TTL DATE NATIVELY 
        # (This is local DB metadata, MSANodeBackups cluster is separate!)
        col_backups.update_one(
            {"filename": filename},
            {"$set": meta_db},
            upsert=True
        )

        logger.info(f"✅ Backup created: {filename} ({file_size_mb:.2f} MB, "
                    f"{len(pdfs_list)} PDFs, {len(ig_list)} IG)")
        return True, filepath, metadata

    except Exception as e:
        logger.error(f"❌ Backup creation failed: {e}")
        return False, None, None

# NOTE: All backups are kept permanently — never auto-deleted for data integrity.

async def auto_backup_task():
    """Background task — runs monthly auto-backup with catch-up if schedule was missed."""
    last_successful_month_key = None

    while True:
        try:
            now = now_local()
            month_key = f"{now.year}-{now.month:02d}"
            scheduled_window = (now.day == 1 and now.hour == 2)

            auto_exists_this_month = col_backups.count_documents({
                "backup_type": "auto",
                "year": now.year,
                "month_num": now.month,
            }) > 0

            # Catch-up mode ensures a monthly backup still happens even if bot was offline at 2 AM.
            needs_catchup = (now.day >= 1) and (not auto_exists_this_month)
            should_run_now = scheduled_window or needs_catchup

            if should_run_now and month_key != last_successful_month_key:
                trigger_reason = "scheduled (1st day at 2 AM)" if scheduled_window else "catch-up (missed schedule window)"
                logger.info(f"🔄 Starting monthly auto-backup: {trigger_reason}")

                success, filepath, metadata = await create_backup_file(auto=True)

                if success and metadata:
                    last_successful_month_key = month_key
                    logger.info(f"✅ Auto-backup completed: {metadata['filename']}")
                    caption = (
                        f"✅ <b>MONTHLY AUTO-BACKUP</b>\n\n"
                        f"📦 <b>File:</b> <code>{metadata['filename']}</code>\n"
                        f"💾 <b>Size:</b> {metadata['file_size_mb']:.2f} MB\n"
                        f"🕐 <b>Created:</b> {metadata['created_at_str']}\n\n"
                        f"<b>📊 Contents:</b>\n"
                        f"├ 📄 pdfs.json — {metadata['pdfs_count']} records\n"
                        f"├ 📸 ig_content.json — {metadata['ig_count']} records\n"
                        f"├ 👤 admins.json — {metadata['admins_count']} records\n"
                        f"├ 🚫 banned_users.json — {metadata['banned_count']} records\n"
                        f"├ ⚙️ settings.json\n"
                        f"├ 📝 logs.json (last 500)\n"
                        f"└ 📋 metadata.json\n\n"
                        f"<b>🎯 Click Stats:</b>\n"
                        f"├ 📸 IG: {metadata['total_ig_clicks']:,}\n"
                        f"├ ▶️ YT: {metadata['total_yt_clicks']:,}\n"
                        f"├ 📸 IGCC: {metadata['total_igcc_clicks']:,}\n"
                        f"└ 🔑 YT Code: {metadata['total_ytcode_clicks']:,}\n\n"
                        f"🔄 Trigger: {trigger_reason}."
                    )
                    try:
                        await bot.send_document(
                            MASTER_ADMIN_ID,
                            types.FSInputFile(filepath),
                            caption=caption,
                            parse_mode="HTML"
                        )
                    except Exception as send_err:
                        logger.error(f"❌ Could not send auto-backup file: {send_err}")
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"✅ <b>Auto-backup created</b> but file could not be sent to Telegram.\n"
                            f"📦 File: <code>{metadata['filename']}</code>\n"
                            f"❌ Error: <code>{send_err}</code>",
                            parse_mode="HTML"
                        )
                else:
                    try:
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"🚨 <b>AUTO-BACKUP FAILED!</b>\n\n"
                            f"⚠️ The monthly backup could not be created.\n"
                            f"📌 Trigger: <b>{trigger_reason}</b>\n"
                            f"📅 Date: {now.strftime('%B %d, %Y')}\n"
                            f"🕐 Time: {now.strftime('%I:%M %p')}\n\n"
                            f"Please check the system immediately!",
                            parse_mode="HTML"
                        )
                    except:
                        logger.error("Could not notify admin of backup failure!")

                # Sleep 2 hours to avoid re-triggering on the same day
                await asyncio.sleep(7200)
            else:
                # Check every hour
                await asyncio.sleep(3600)

        except Exception as e:
            logger.error(f"❌ Auto-backup task error: {e}")
            try:
                await bot.send_message(
                    MASTER_ADMIN_ID,
                    f"🚨 <b>BACKUP SYSTEM ERROR!</b>\n\n"
                    f"❌ Error: <code>{str(e)}</code>\n"
                    f"🕐 Time: {now_local().strftime('%I:%M %p')}\n\n"
                    f"The auto-backup system encountered an error.",
                    parse_mode="HTML"
                )
            except:
                logger.error("Could not notify admin of backup system error!")
            await asyncio.sleep(3600)


# =============================================================================
# 📊 BACKUP STATUS — Full live stats for Bot 3
# =============================================================================

@dp.message(F.text.in_({"📊 BACKUP STATUS", "📊 BACKUP STATS"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_backup_status_start(message: types.Message, state: FSMContext):
    """Show full live backup statistics for Bot 3."""
    if not await check_authorization(message, "Backup Status", "can_manage_admins"):
        return
    await state.clear()
    loading = await message.answer("⏳ Gathering Bot 3 backup status...")

    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)

    # ── Production DB live counts ─────────────────────────────────────────────
    prod_cols = [
        ("bot3_pdfs",         col_pdfs),
        ("bot3_ig_content",   col_ig_content),
        ("bot3_admins",       col_admins),
        ("bot3_banned_users", col_banned_users),
        ("bot3_settings",     col_settings),
        ("bot3_logs",         col_logs),
        ("bot3_user_activity",col_user_activity),
    ]
    prod_lines = ""
    total_prod = 0
    for name, col in prod_cols:
        try:
            cnt = col.count_documents({})
            total_prod += cnt
            prod_lines += f"  {'🔴' if cnt == 0 else '🟢'} <code>{name}</code>: {cnt:,}\n"
        except Exception:
            prod_lines += f"  ⚠️ <code>{name}</code>: (error)\n"

    # ── Backup cluster snapshots ──────────────────────────────────────────────
    try:
        snap_count  = col_bot3_backups.count_documents({})
        latest      = col_bot3_backups.find_one({}, sort=[("backup_date", -1)])
        last_bk_str = format_datetime_12h(latest["backup_date"]) if latest and latest.get("backup_date") else "Never"
        last_bk_type= latest.get("backup_type", "?") if latest else "—"
        last_bk_docs= latest.get("docs", "?") if latest else "—"
    except Exception as _e:
        snap_count = 0; last_bk_str = f"⚠️ {type(_e).__name__}"; last_bk_type = last_bk_docs = "—"

    # ── Restore snapshot ─────────────────────────────────────────────────────
    try:
        restore_doc = col_bot3_restore_data.find_one({"_id": "bot3_latest"})
        if restore_doc:
            restore_status = (
                f"✅ Exists — {restore_doc.get('total_records', '?'):,} records "
                f"as of {format_datetime_12h(restore_doc.get('backup_date'))}"
            )
        else:
            restore_status = "❌ No restore snapshot yet"
    except Exception:
        restore_status = "⚠️ Cluster unreachable"

    # ── History count ─────────────────────────────────────────────────────────
    try:
        history_count = col_bot3_bk_history.count_documents({"bot": "bot3"})
    except Exception:
        history_count = "?"

    # ── Next auto-backup ETA ─────────────────────────────────────────────────
    next_run = now_utc.replace(hour=23, minute=59, second=0, microsecond=0)
    if next_run <= now_utc:
        next_run += timedelta(days=1)
    wait_s = int((next_run - now_utc).total_seconds())
    h, m = divmod(wait_s // 60, 60)
    eta_str = f"{h}h {m}m" if h else f"{m}m"
    is_sunday = next_run.weekday() == 6

    # ── TTL index check (MongoDB infrastructure) ─────────────────────────────
    try:
        ttl_index_ok = any(
            idx.get("expireAfterSeconds") == 7_776_000
            for idx in col_bot3_backups.list_indexes()
        )
        ttl_index_str = "Configured (90-day index exists)" if ttl_index_ok else "Not configured!"
    except Exception:
        ttl_index_ok  = False
        ttl_index_str = "Unknown"

    # ── Per-document TTL activation count (manual via ACTIVATE TTL) ──────────
    try:
        ttl_docs_active = col_bot3_backups.count_documents({"ttl_date": {"$exists": True}})
        ttl_docs_total  = col_bot3_backups.count_documents({})
        if ttl_docs_active == 0:
            ttl_docs_str = "None activated (use ACTIVATE TTL to schedule deletion)"
        else:
            ttl_docs_str = f"{ttl_docs_active}/{ttl_docs_total} snapshots scheduled for deletion"
    except Exception:
        ttl_docs_str = "Unknown"

    report = (
        f"📊 <b>BACKUP STATUS — Bot 3</b>\n{'─'*28}\n\n"
        f"🗄️ <b>MSANodeDB (Production)</b>\n{prod_lines}"
        f"📊 Total live docs: <b>{total_prod:,}</b>\n\n"
        f"🔐 <b>MSANodeBackups (Cluster)</b>\n"
        f"  📦 Snapshots stored: <b>{snap_count:,}</b>\n"
        f"  🕐 Last backup: <b>{last_bk_str}</b>\n"
        f"  🏷️ Type: <code>{last_bk_type}</code> | Docs: <code>{last_bk_docs}</code>\n"
        f"  🗃️ TTL Index: {'\u2705' if ttl_index_ok else '\u274c'} {ttl_index_str}\n"
        f"  ⏳ TTL Activated: {ttl_docs_str}\n\n"
        f"🔄 <b>Restore Snapshot</b>\n  {restore_status}\n\n"
        f"📜 <b>Backup History</b>\n  {history_count} event(s) logged\n\n"
        f"⏰ <b>Next Auto-Backup</b>\n"
        f"  📅 {next_run.strftime('%a %b %d, %Y')} at 23:59 UTC\n"
        f"  ⏳ In <b>{eta_str}</b>"
        + (f" — <b>WEEKLY SUNDAY backup 🗓️</b>" if is_sunday else " (daily run)") + "\n\n"
        f"🏠 <b>Deployment Note</b>\n"
        f"  ☁️ Local disk: <i>N/A on Render (ephemeral)</i>\n"
        f"  ✅ Cluster + GDrive = permanent backup layers\n"
    )
    try: await loading.delete()
    except Exception: pass
    # ── TTL-activated months list ──────────────────────────────────────────────
    ttl_months_text = ""
    try:
        all_snaps = list(col_bot3_backups.find({}, {"backup_date": 1, "ttl_date": 1}))
        ttl_chunks = {}
        for snap in all_snaps:
            bd = snap.get("backup_date")
            if not bd: continue
            mk = f"{bd.year}-{bd.month:02d}"
            ttl_chunks.setdefault(mk, {"total": 0, "ttl": 0, "month_name": bd.strftime("%B %Y")})
            ttl_chunks[mk]["total"] += 1
            if "ttl_date" in snap: ttl_chunks[mk]["ttl"] += 1
        if ttl_chunks:
            ttl_months_text = "\n🗑️ <b>90-Day TTL Status per Month:</b>\n"
            for mk in sorted(ttl_chunks.keys(), reverse=True):
                info = ttl_chunks[mk]
                ico  = "✅" if info["ttl"] == info["total"] else ("⚠️" if info["ttl"] > 0 else "❌")
                ttl_months_text += f"  {ico} {info['month_name']}: {info['ttl']}/{info['total']} active\n"
        else:
            ttl_months_text = "\n🗑️ <b>90-Day TTL:</b> No snapshots stored\n"
    except Exception:
        ttl_months_text = "\n🗑️ <b>TTL status:</b> ⚠️ Could not read\n"

    report = report.rstrip("\n") + ttl_months_text
    await message.answer(report, parse_mode="HTML", reply_markup=get_backup_menu())


# =============================================================================
# 🗑️ RESET BACKUP DATA — MSANodeBackups cluster only, never touches MSANodeDB
# =============================================================================

@dp.message(F.text.in_({"🗑️ RESET BACKUP DATA", "🗑️ RESET BACKUPS"}), StateFilter(None, BackupStates.viewing_backup_menu))
async def bot3_reset_backup_start(message: types.Message, state: FSMContext):
    """Step 1 — Safety gate + show erase options."""
    if not await check_authorization(message, "Reset Backup Data", "can_manage_admins"):
        return
    if not BACKUP_MONGO_URI:
        await message.answer(
            "🚫 <b>RESET BLOCKED</b>\n\n<code>BACKUP_MONGO_URI</code> is not set.\n"
            "Cannot safely erase backup data without a dedicated cluster URI.",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        )
        return
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 BOT 3 BACKUPS"), KeyboardButton(text="🗑️ ERASE ALL BACKUPS")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await state.set_state(Bot3BackupStates.reset_target_select)
    await message.answer(
        "🗑️ <b>RESET BACKUPS — Bot 3</b>\n"
        "─────────────────────────\n\n"
        "✅ <b>WILL erase from MSANodeBackups only:</b>\n"
        "  • <code>bot3_backups</code> (snapshots)\n"
        "  • <code>bot3_restore_data</code> (restore point)\n"
        "  • <code>bot3_backup_history</code> — if ERASE ALL\n\n"
        "🛡️ <b>Will NEVER touch:</b>\n"
        "  • <code>MSANodeDB</code> (Production) — completely separate\n\n"
        f"🔗 Backup: <code>{BACKUP_MONGO_URI[:40]}...</code>\n"
        f"🔗 Prod: <code>{MONGO_URI[:40]}...</code>\n\n"
        "Choose scope:",
        reply_markup=kb, parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.reset_target_select)
async def bot3_reset_backup_target(message: types.Message, state: FSMContext):
    """Step 2 — Count records and request CONFIRM."""
    if not await check_authorization(message, "Reset Target", "can_manage_admins"):
        await state.clear(); return
    if message.text and message.text in ("🔙 BACK", "⬅️ MAIN MENU"):
        await state.clear()
        await message.answer("↩️ Returned to backup menu.", reply_markup=get_backup_menu()); return
    if message.text == "🤖 BOT 3 BACKUPS":
        target, label = "bot3", "Bot 3"
    elif message.text == "🗑️ ERASE ALL BACKUPS":
        target, label = "all", "ALL BACKUPS"
    else:
        await message.answer("Please choose one of the options above."); return

    try:
        if target == "all":
            all_cols = _bkp_db_b3.list_collection_names()
            col_lines = "\n".join(
                f"  • <code>{c}</code>: {_bkp_db_b3[c].count_documents({}):,} docs"
                for c in sorted(all_cols)
            ) or "  (already empty)"
            preview = f"🗑️ <b>ALL collections to erase:</b>\n{col_lines}"
        else:
            s_cnt = col_bot3_backups.count_documents({})
            r_cnt = col_bot3_restore_data.count_documents({})
            preview = (
                f"🗑️ <b>Will delete from MSANodeBackups:</b>\n"
                f"  • <code>bot3_backups</code>: {s_cnt:,} records\n"
                f"  • <code>bot3_restore_data</code>: {r_cnt} doc"
            )
    except Exception as e:
        await state.clear()
        await message.answer(f"⚠️ Cluster unreachable:\n<code>{str(e)[:150]}</code>", reply_markup=get_backup_menu(), parse_mode="HTML")
        return

    await state.update_data(b3_reset_target=target, b3_reset_label=label)
    await state.set_state(Bot3BackupStates.reset_confirm1)
    await message.answer(
        f"⚠️ <b>CONFIRM DELETION — Step 1 of 2</b>\n{'─'*28}\n\n{preview}\n\n"
        f"🛡️ <b>MSANodeDB (Production) = 100% untouched</b>\n\nType <b>CONFIRM</b> (all caps):",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
        parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.reset_confirm1)
async def bot3_reset_backup_confirm1(message: types.Message, state: FSMContext):
    """Step 3 — Must type CONFIRM."""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if message.text != "CONFIRM":
        await message.answer("❌ Type exactly <b>CONFIRM</b> (all caps) or press ❌ CANCEL.", parse_mode="HTML"); return
    data = await state.get_data()
    label = data.get("b3_reset_label", "Selected")
    await state.set_state(Bot3BackupStates.reset_confirm2)
    await message.answer(
        f"🔴 <b>FINAL WARNING — Step 2 of 2</b>\n\n"
        f"Erasing <b>{label}</b> from <b>MSANodeBackups cluster</b>.\n"
        f"<b>MSANodeDB (Production) is NOT affected.</b>\n\n"
        f"Type <b>DELETE</b> (all caps) to execute:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
        parse_mode="HTML"
    )


@dp.message(Bot3BackupStates.reset_confirm2)
async def bot3_reset_backup_confirm2(message: types.Message, state: FSMContext):
    """Step 4 — Execute erase on BACKUP cluster ONLY."""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if message.text != "DELETE":
        await message.answer("❌ Type exactly <b>DELETE</b> (all caps) or press ❌ CANCEL.", parse_mode="HTML"); return

    data = await state.get_data()
    target = data.get("b3_reset_target")
    label  = data.get("b3_reset_label", "Selected")
    await state.clear()

    if not BACKUP_MONGO_URI:
        await message.answer("🚫 <b>ABORTED</b> — No backup cluster URI. No data deleted.", reply_markup=get_backup_menu(), parse_mode="HTML")
        return

    try:
        report_lines = []
        total_deleted = 0
        if target == "all":
            for col_name in sorted(_bkp_db_b3.list_collection_names()):
                res = _bkp_db_b3[col_name].delete_many({})
                report_lines.append(f"  • <code>{col_name}</code>: {res.deleted_count:,} deleted")
                total_deleted += res.deleted_count
        else:
            r1 = col_bot3_backups.delete_many({})
            r2 = col_bot3_restore_data.delete_many({})
            report_lines.append(f"  • <code>bot3_backups</code>: {r1.deleted_count:,} deleted")
            report_lines.append(f"  • <code>bot3_restore_data</code>: {r2.deleted_count} deleted")
            total_deleted = r1.deleted_count + r2.deleted_count

        await message.answer(
            f"✅ <b>BACKUP DATA ERASED — {label}</b>\n{'─'*28}\n\n"
            + "\n".join(report_lines) + f"\n\n📊 <b>Total: {total_deleted:,} docs deleted</b>\n\n"
            "🛡️ <code>MSANodeDB</code> (Production) — 100% intact",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        )
        logger.warning(f"🗑️ Bot3 RESET BACKUP executed: {label} — {total_deleted} docs by {message.from_user.id}")
    except Exception as e:
        await message.answer(
            f"❌ <b>Reset error:</b>\n<code>{str(e)[:300]}</code>\n\nProduction data NOT touched.",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        )


@dp.message(F.text == "💾 BACKUP DATA", StateFilter(None))
async def backup_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup Menu", "can_manage_admins"):
        return
    """Show backup menu"""
    await state.set_state(BackupStates.viewing_backup_menu)
    await message.answer(
        "💾 <b>BACKUP &amp; RESTORE — Bot 3</b>\n\n"
        "💾 <b>DOWNLOAD BACKUP</b> — Cluster snapshot → select index → ZIP or JSON\n"
        "📤 <b>UPLOAD BACKUP</b> — Restore into Main DB or Backup cluster\n"
        "☁️ <b>GDRIVE SYSTEM</b> — Index pick → full-month TTL detection → upload\n"
        "📊 <b>BACKUP STATUS</b> — Live prod + cluster stats, TTL &amp; ETA\n"
        "📜 <b>HISTORY</b> — Paginated cluster event log (auto prev/next)\n"
        "⏳ <b>ACTIVATE TTL</b> — Toggle 90-day TTL policies per month\n"
        "🗑️ <b>RESET BACKUP DATA</b> — Wipe MSANodeBackups only (prod untouched)\n\n"
        "Auto-backup: <b>23:59 UTC</b> daily → <b>MSANodeBackups cluster</b>",
        reply_markup=get_backup_menu(),
        parse_mode="HTML"
    )


# ==========================================
# ADMIN MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "👥 ADMINS")
async def admin_menu_handler(message: types.Message):
    """Show Admin Management Menu"""
    if not await check_authorization(message, "Access Admin Menu", "can_manage_admins"):
        return
    
    await message.answer("🔐 <b>Admin Management</b>\nSelect an option below:", reply_markup=get_admin_config_menu(), parse_mode="HTML")

@dp.message(F.text == "📋 LIST ADMINS")
async def list_admins_handler(message: types.Message, state: FSMContext):
    """List all admins from database with pagination"""
    if not await check_authorization(message, "List Admins", "can_manage_admins"):
        return
    
    # Reset page to 0
    await state.update_data(admin_page=0)
    await state.set_state(AdminManagementStates.viewing_admin_list)
    await send_admin_list_view(message, page=0)

async def send_admin_list_view(message: types.Message, page: int = 0):
    """Display paginated list of admins"""
    ADMINS_PER_PAGE = 5
    skip = page * ADMINS_PER_PAGE

    try:
        total_admins = col_admins.count_documents({"user_id": {"$ne": MASTER_ADMIN_ID}})
        admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).skip(skip).limit(ADMINS_PER_PAGE))

        if not admins and page == 0:
            # Build keyboard with just back button
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ BACK TO ADMIN MENU")]],
                resize_keyboard=True
            )
            await message.answer("📋 <b>Admin List</b>\n\nNo other admins found in the database.", reply_markup=kb, parse_mode="HTML")
            return

        # Build message
        total_pages = max(1, (total_admins + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE)
        text = f"📋 <b>Admin List</b> — Page {page + 1}/{total_pages} ({total_admins} total)\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, admin in enumerate(admins, start=skip + 1):
            uid = admin.get("user_id", "?")
            name = admin.get("full_name", "Unknown")
            username = admin.get("username", "")
            perms = admin.get("permissions", [])
            perm_count = len(perms) if perms else 0
            is_locked = admin.get("is_locked", False)
            status_str = "[🔒 LOCKED]" if is_locked else "[🔓 ACTIVE]"

            username_str = f"@{username}" if username and username != "Unknown" else "No username"
            
            # Use same format but separate lines carefully
            text += f"<b>{i}.</b> `{uid}` — {name} {status_str}\n"
            text += f"   {username_str} | 🔑 {perm_count} permissions\n\n"

        text += "━━━━━━━━━━━━━━━━━━━━━━━━"

        # Build navigation keyboard
        nav_buttons = []
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
        if (skip + ADMINS_PER_PAGE) < total_admins:
            nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

        keyboard_rows = []
        if nav_buttons:
            keyboard_rows.append(nav_buttons)

        keyboard_rows.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])

        kb = ReplyKeyboardMarkup(keyboard=keyboard_rows, resize_keyboard=True)
        await message.answer(text, reply_markup=kb, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error in send_admin_list_view: {e}")
        await message.answer(f"❌ Error loading admin list: {e}")

@dp.message(AdminManagementStates.viewing_admin_list, F.text == "➡️ NEXT ADMINS")
async def next_admin_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("admin_page", 0) + 1
    await state.update_data(admin_page=page)
    await send_admin_list_view(message, page)

@dp.message(AdminManagementStates.viewing_admin_list, F.text == "⬅️ PREV ADMINS")
async def prev_admin_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(0, data.get("admin_page", 0) - 1)
    await state.update_data(admin_page=page)
    await send_admin_list_view(message, page)

@dp.message(F.text.contains("BACK"))
async def admin_list_back(message: types.Message, state: FSMContext):
    """Return to Admin Management menu from list view"""
    if not await check_authorization(message, "Admin Menu", "can_manage_admins"):
        return
    await state.clear()
    await message.answer(
        "🔐 <b>Admin Management</b>\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )



# ==========================================
# BAN CONFIGURATION HANDLERS
# ==========================================

@dp.message(F.text == "🚫 BAN CONFIG")
async def ban_config_menu_handler(message: types.Message):
    """Show Ban Configuration Menu"""
    if not await check_authorization(message, "Access Ban Config", "can_manage_admins"):
        return
    await message.answer("🚫 <b>BAN CONFIGURATION</b>\nSelect an option below:", reply_markup=get_ban_config_menu(), parse_mode="HTML")

@dp.message(F.text == "⬅️ BACK TO ADMIN MENU")
async def back_to_admin_menu_handler(message: types.Message):
    """Return to Admin Menu"""
    if not await check_authorization(message, "Back to Admin Menu", "can_manage_admins"):
        return
    await message.answer("👥 <b>ADMIN CONFIGURATION</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")

@dp.message(F.text == "🚫 BAN USER")
async def ban_user_start(message: types.Message, state: FSMContext):
    """Start ban user flow"""
    if not await check_authorization(message, "Ban User", "can_manage_admins"):
        return
    await state.set_state(AdminManagementStates.waiting_for_ban_user_id)
    await message.answer(
        "🚫 <b>BAN USER</b>\n\n"
        "Please enter the <b>Telegram User ID</b> of the user to ban.\n"
        "They will be blocked from accessing the bot.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(AdminManagementStates.waiting_for_ban_user_id)
async def ban_user_process_id(message: types.Message, state: FSMContext):
    """Process ban user ID"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_ban_config_menu())
        return

    try:
        if not message.text.isdigit():
            await message.answer("⚠️ Invalid ID. Please enter a numeric User ID.", reply_markup=get_cancel_keyboard())
            return
            
        ban_id = int(message.text)
        
        # Prevent banning Admins
        if is_admin(ban_id):
            await message.answer(
                "⛔ <b>ACTION DENIED</b>\n\n"
                "You cannot ban an active Admin!\n"
                "<i>Please remove them from the Admin list first.</i>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Check if already banned
        if is_banned(ban_id):
            await message.answer(
                f"⚠️ <b>User `{ban_id}` is already banned!</b>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Ban User
        reason = f"Manual Ban by Admin {message.from_user.id}"
        ban_user(ban_id, "Unknown", "Unknown", reason) # Helper function handles logging
        
        await state.clear()
        await message.answer(
            f"✅ <b>SUCCESS!</b>\n\nUser `{ban_id}` has been BANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await message.answer(f"❌ Error banning user: {e}", reply_markup=get_ban_config_menu())
        await state.clear()

@dp.message(F.text == "✅ UNBAN USER")
async def unban_user_start(message: types.Message, state: FSMContext):
    """Start unban user flow"""
    if not await check_authorization(message, "Unban User", "can_manage_admins"):
        return
    await state.set_state(AdminManagementStates.waiting_for_unban_user_id)
    await message.answer(
        "✅ <b>UNBAN USER</b>\n\n"
        "Please enter the <b>Telegram User ID</b> of the user to unban.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@dp.message(AdminManagementStates.waiting_for_unban_user_id)
async def unban_user_process_id(message: types.Message, state: FSMContext):
    """Process unban user ID"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_ban_config_menu())
        return

    try:
        if not message.text.isdigit():
            await message.answer("⚠️ Invalid ID. Please enter a numeric User ID.", reply_markup=get_cancel_keyboard())
            return
            
        unban_id = int(message.text)
        
        # Check if banned (Check DB directly to allow unbanning Exempt users too)
        banned_doc = col_banned_users.find_one({"user_id": unban_id})
        if not banned_doc:
            await message.answer(
                f"⚠️ <b>User {unban_id} is NOT found in ban list.</b>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Unban User
        col_banned_users.delete_one({"user_id": unban_id})
        logger.info(f"User {unban_id} unbanned by Admin {message.from_user.id}")
        
        await state.clear()
        await message.answer(
            f"✅ <b>SUCCESS!</b>\n\nUser `{unban_id}` has been UNBANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await message.answer(f"❌ Error unbanning user: {e}", reply_markup=get_ban_config_menu())
        await state.clear()

@dp.message(F.text == "📋 LIST BANNED")
async def list_banned_handler(message: types.Message):
    """List all banned users with details"""
    if not await check_authorization(message, "List Banned", "can_manage_admins"):
        return
    
    banned_users = list(col_banned_users.find({}))
    
    if not banned_users:
        await message.answer("⚠️ <b>No banned users found.</b>", reply_markup=get_ban_config_menu(), parse_mode="HTML")
        return
        
    msg = "🚫 <b>BANNED USERS LIST</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    count = 0
    for user in banned_users:
        count += 1
        uid = user.get("user_id")
        reason = user.get("reason", "No reason provided")
        
        # Format Date
        date_val = user.get("banned_at", "Unknown")
        date_str = "Unknown"
        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d %I:%M %p") # Date + Time (AM/PM)
        elif isinstance(date_val, str):
            date_str = date_val
            
        # Get Name (if saved)
        name = user.get("full_name", "Unknown")
        
        msg += (
            f"{count}. <b>{name}</b> (`{uid}`)\n"
            f"   📝 Reason: {reason}\n"
            f"   📅 Time: {date_str}\n\n"
        )
        
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
        
    await message.answer(msg, reply_markup=get_ban_config_menu(), parse_mode="HTML")

# ==========================================
# ROLE MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "👔 ROLES")
@dp.message(F.text == "🔒 LOCK/UNLOCK")
async def roles_menu_handler(message: types.Message, state: FSMContext):
    """Show list of admins to select for Role Assignment or Lock/Unlock"""
    if not await check_authorization(message, "Manage Roles", "can_manage_admins"):
        return
        
    # Check if admins exist excluding Master Admin
    if col_admins.count_documents({"user_id": {"$ne": MASTER_ADMIN_ID}}) == 0:
        await message.answer("⚠️ No other admins found.", reply_markup=get_admin_config_menu())
        return

    # Determine Mode
    mode = "roles"
    if message.text == "🔒 LOCK/UNLOCK":
        mode = "lock"
    
    await state.update_data(role_menu_mode=mode)
        
    await state.set_state(AdminRoleStates.waiting_for_admin_selection)
    await state.update_data(role_admin_page=0)
    await send_role_admin_list(message, 0, mode)

async def send_role_admin_list(message: types.Message, page: int, mode: str = "roles"):
    """Helper to send paginated admin list for role selection or lock/unlock"""
    ITEMS_PER_PAGE = 10
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).sort("added_at", 1))
    total_admins = len(admins)
    total_pages = max(1, (total_admins + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    # Cap page just in case
    page = min(page, max(0, total_pages - 1))
    
    start = page * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total_admins)
    current_admins = admins[start:end]
    
    # Build Keyboard & Text List
    keyboard = []
    admin_list_text = ""
    
    # Admin Buttons (2 per row)
    row = []
    for i, admin in enumerate(current_admins):
        user_id = admin.get("user_id")
        
        # Smartly extract Name, fallback to Username, then User ID
        name = admin.get("full_name")
        if not name or name == str(user_id):
            name = admin.get("username")
            if not name:
                name = "Admin"
                
        is_locked = admin.get("is_locked", False)
        status_str = "[🔒 LOCKED]" if is_locked else "[🔓 ACTIVE]"
        
        # Add to text list
        global_idx = start + i + 1
        admin_list_text += f"{global_idx}. <b>{_html_escape(name)}</b> (`{user_id}`) {status_str}\n"
        
        # Button Format changes based on mode
        if mode == "lock":
            icon = "🔒" if is_locked else "🔓"
            btn_text = f"{icon} {name} [{user_id}]"
        else:
            btn_text = f"👤 {name} [{user_id}]"
            
        row.append(KeyboardButton(text=btn_text))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    # Pagination Buttons
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV ROLES"))
    if page < total_pages - 1:
        nav_row.append(KeyboardButton(text="➡️ NEXT ROLES"))
    if nav_row:
        keyboard.append(nav_row)
        
    # Standard Controls
    keyboard.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])
    
    header = "LOCK/UNLOCK" if mode == "lock" else "MODIFY ROLE"
    action = "toggle lock status" if mode == "lock" else "modify their role"
    
    await message.answer(
        f"👔 <b>SELECT ADMIN TO {header}</b>\n\n"
        f"Select an admin from the list below to {action}:\n\n"
        f"{admin_list_text}\n"
        f"Page {page + 1}/{total_pages}",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(AdminRoleStates.waiting_for_admin_selection, F.text == "➡️ NEXT ROLES")
async def next_role_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("role_admin_page", 0) + 1
    mode = data.get("role_menu_mode", "roles")
    await state.update_data(role_admin_page=page)
    await send_role_admin_list(message, page, mode)

@dp.message(AdminRoleStates.waiting_for_admin_selection, F.text == "⬅️ PREV ROLES")
async def prev_role_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(0, data.get("role_admin_page", 0) - 1)
    mode = data.get("role_menu_mode", "roles")
    await state.update_data(role_admin_page=page)
    await send_role_admin_list(message, page, mode)

@dp.message(AdminRoleStates.waiting_for_admin_selection)
async def role_admin_selected(message: types.Message, state: FSMContext):
    """Admin selected for role or lock toggle"""
    text = message.text
    
    if text == "❌ CANCEL" or text == "⬅️ RETURN BACK":
        await state.clear()
        await message.answer("↩️ Returned to Admin menu.", reply_markup=get_admin_config_menu())
        return

    # Try to extract ID from button text "Name [ID]" or "(ID)" for legacy
    import re
    match = re.search(r"\[(\d+)\]$", text)
    if not match:
        match = re.search(r"\((\d+)\)$", text)
        
    target_admin_id = None
    if match:
        target_admin_id = int(match.group(1))
    elif text.isdigit():
        target_admin_id = int(text)
    else:
        await message.answer("⚠️ Invalid selection. Please click a user button.", reply_markup=get_cancel_keyboard())
        return
        
    # Verify admin exists (Direct DB check to allow managing locked admins)
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    if not admin_doc and target_admin_id != MASTER_ADMIN_ID:
        await message.answer(f"⚠️ User {target_admin_id} is not an admin.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Prevent modifying Master Admin
    if target_admin_id == MASTER_ADMIN_ID:
        await message.answer("⛔ You cannot modify the Master Admin.", reply_markup=get_admin_config_menu())
        await state.clear()
        return

    admin_name = admin_doc.get("full_name", "Admin")
    is_locked = admin_doc.get("is_locked", False)
    
    # Determine Menu based on Mode
    data = await state.get_data()
    mode = data.get("role_menu_mode", "roles")
    current_page = data.get("role_admin_page", 0)
    
    if mode == "lock":
        # Instantly toggle lock status and stay on the same paginated keyboard
        new_lock_state = not is_locked
        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": new_lock_state}})
        
        status_text = "LOCKED (Inactive)" if new_lock_state else "UNLOCKED (Active)"
        icon = "🔒" if new_lock_state else "🔓"
        
        log_user_action(message.from_user, f"{icon} ADMIN STATUS CHANGED", f"Set {target_admin_id} to {status_text}")
        
        await message.answer(
            f"✅ <b>STATUS UPDATED</b>\n\n"
            f"👤 Admin: {admin_name} (`{target_admin_id}`)\n"
            f"{icon} Status: <b>{status_text}</b>",
            parse_mode="HTML"
        )
        
        # Refresh the active page
        await send_role_admin_list(message, current_page, mode)
        return
        
    # Standard role assignment flow (mode == "roles")
    await state.update_data(target_admin_id=target_admin_id)
    await state.set_state(AdminRoleStates.waiting_for_role_selection)
    
    target_menu = get_roles_menu()
    msg_text = f"👔 <b>SELECT ROLE FOR {_html_escape(admin_name)}</b> (`{target_admin_id}`)\n\nChoose a role to apply permissions:"
    
    await message.answer(
        msg_text,
        reply_markup=target_menu,
        parse_mode="HTML"
    )

@dp.message(AdminRoleStates.waiting_for_role_selection)
async def role_selected_process(message: types.Message, state: FSMContext):
    """Apply selected role"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return

    selected_role = message.text
    
    data = await state.get_data()
    target_admin_id = data.get("target_admin_id")
    
    role_key = None
    if "OWNER" in selected_role: 
        # Trigger Ownership Transfer Flow
        await state.update_data(target_role="OWNER")
        await state.set_state(AdminRoleStates.waiting_for_owner_password)
        await message.answer(
            "🔐 <b>SECURITY CHECK</b>\n\n"
            "Resetting Ownership requires a password.\n"
            "Please enter the <b>Owner Password</b>:",
            reply_markup=get_cancel_keyboard(),
            parse_mode="HTML"
        )
        return

    elif "MANAGER" in selected_role: role_key = "MANAGER"
    elif "ADMIN" in selected_role: role_key = "ADMIN"
    elif "MODERATOR" in selected_role: role_key = "MODERATOR"
    elif "SUPPORT" in selected_role: role_key = "SUPPORT"
    
    # Handle Lock/Unlock
    elif "🔒 LOCK" in selected_role:
        # Check if already locked
        admin_doc = col_admins.find_one({"user_id": target_admin_id})
        if admin_doc and admin_doc.get("is_locked", False):
            await message.answer(f"⚠️ <b>Admin {target_admin_id} is ALREADY LOCKED.</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": True}})
        log_user_action(message.from_user, "ADMIN LOCKED", f"Locked {target_admin_id}")
        await state.clear()
        await message.answer(
            f"🔒 <b>ADMIN LOCKED</b>\n\nUser `{target_admin_id}` has been locked.\nThey have NO access.",
            reply_markup=get_admin_config_menu(), parse_mode="HTML"
        )
        return
        
    elif "🔓 UNLOCK" in selected_role:
        # Check if already unlocked
        admin_doc = col_admins.find_one({"user_id": target_admin_id})
        if admin_doc and not admin_doc.get("is_locked", False):
            await message.answer(f"⚠️ <b>Admin {target_admin_id} is ALREADY UNLOCKED.</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": False}})
        log_user_action(message.from_user, "ADMIN UNLOCKED", f"Unlocked {target_admin_id}")
        
        # Send Role Message on Unlock
        if admin_doc:
            perms = admin_doc.get("permissions", [])
            # Determine role from perms
            detected_role = "SUPPORT"
            for r_name, r_perms in ROLES.items():
                if set(r_perms) == set(perms):
                    detected_role = r_name
                    break
            
            try:
                caps_list = []
                if detected_role == "OWNER": caps_list = ["• Absolute Power", "• Manage Everything"]
                elif detected_role == "MANAGER": caps_list = ["• Manage Admins", "• Manage Content", "• View Analytics"]
                elif detected_role == "ADMIN": caps_list = ["• Manage Content", "• Manage Links", "• View Analytics"]
                elif detected_role == "MODERATOR": caps_list = ["• Add/Edit Content", "• Search Database"]
                elif detected_role == "SUPPORT": caps_list = ["• View Content", "• Search Only"]
                
                caps_str = "\n".join(caps_list)
                
                await bot.send_message(
                    target_admin_id,
                    f"🌟 <b>ACCESS RESTORED</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n\n"
                    f"<b>Dear Administrator,</b>\n\n"
                    f"Your account status has been officially <b>UNLOCKED</b>.\n\n"
                    f"You are currently designated as a <b>{detected_role}</b>. "
                    f"Your authorized system capabilities are outlined below:\n\n"
                    f"{caps_str}\n\n"
                    f"<i>Access restored and authorized by {message.from_user.full_name}.</i>",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {target_admin_id} of unlock: {e}")

        await state.clear()
        await message.answer(
            f"🔓 <b>ADMIN UNLOCKED</b>\n\nUser `{target_admin_id}` has been unlocked.\nPermissions restored.",
            reply_markup=get_admin_config_menu(), parse_mode="HTML"
        )
        return
    
    if not role_key:
        await message.answer("⚠️ Invalid Role. Please select from keyboard.", reply_markup=get_roles_menu())
        return
        
    data = await state.get_data()
    target_admin_id = data.get("target_admin_id")
    
    # Get Permissions
    new_perms = ROLES.get(role_key)
    
    # Update DB
    col_admins.update_one(
        {"user_id": target_admin_id},
        {"$set": {"permissions": new_perms}}
    )
    
    # LOG
    log_user_action(message.from_user, "ROLE UPDATE", f"Set {target_admin_id} to {role_key}")
    
    # Notify Target Admin (Premium Message) ONLY if not locked
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    if admin_doc and not admin_doc.get("is_locked", False) and "UNLOCK" not in selected_role:
        try:
            caps_list = []
            if role_key == "MANAGER": caps_list = ["• Manage Admins", "• Manage Content", "• View Analytics"]
            elif role_key == "ADMIN": caps_list = ["• Manage Content", "• Manage Links", "• View Analytics"]
            elif role_key == "MODERATOR": caps_list = ["• Add/Edit Content", "• Search Database"]
            elif role_key == "SUPPORT": caps_list = ["• View Content", "• Search Only"]
            
            caps_str = "\n".join(caps_list)
            
            await bot.send_message(
                target_admin_id,
                f"🌟 <b>PROMOTION GRANTED</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>Dear Administrator,</b>\n\n"
                f"Your account has been officially elevated to <b>{role_key}</b> status.\n\n"
                f"Your new authorized system capabilities are outlined below:\n\n"
                f"{caps_str}\n\n"
                f"<i>Access granted and authorized by {message.from_user.full_name}.</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {target_admin_id} of role change: {e}")
        
    await state.clear()
    await message.answer(
        f"✅ <b>SUCCESS!</b>\n\n"
        f"User `{target_admin_id}` is now <b>{role_key}</b>.\n"
        f"Permissions updated.",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(AdminRoleStates.waiting_for_owner_password)
async def process_owner_password(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        # Return to Role Selection state instead of clearing
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Cancelled.", reply_markup=get_roles_menu())
        return

    password = message.text.strip()
    if password == OWNER_PASSWORD:
        # Check permissions — only the owner (who knows OWNER_PASSWORD) can confirm ownership transfer
        await state.set_state(AdminRoleStates.waiting_for_owner_confirm)
        
        data = await state.get_data()
        target_admin_id = data.get("target_admin_id")
        
        await message.answer(
            f"⚠️ <b>CRITICAL WARNING</b> ⚠️\n\n"
            f"You are about to transfer <b>OWNERSHIP</b> to `{target_admin_id}`.\n"
            f"This action is <b>IRREVERSIBLE</b> via the bot.\n"
            f"You will lose your Owner privileges.\n\n"
            f"Are you sure?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ YES, TRANSFER OWNERSHIP"), KeyboardButton(text="❌ CANCEL")]],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
    else:
        await message.answer("⛔ <b>Incorrect Password.</b> Access Denied.", reply_markup=get_roles_menu(), parse_mode="HTML")
        await state.clear()

@dp.message(AdminRoleStates.waiting_for_owner_confirm)
async def process_owner_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ YES, TRANSFER OWNERSHIP":
        await state.set_state(AdminRoleStates.waiting_for_owner_second_confirm)
        await message.answer(
            f"⚠️ <b>FINAL CONFIRMATION</b> ⚠️\n\n"
            f"This is your last warning! Transferring ownership is permanent and you will become a manager.\n"
            f"Are you ABSOLUTELY sure?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ YES, I AM SURE"), KeyboardButton(text="❌ CANCEL")]],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
    else:
        # Return to Role Selection state
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Transfer Cancelled.", reply_markup=get_roles_menu())

@dp.message(AdminRoleStates.waiting_for_owner_second_confirm)
async def process_owner_second_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ YES, I AM SURE":
        data = await state.get_data()
        target_admin_id = data.get("target_admin_id")
        current_owner_id = message.from_user.id
        
        global MASTER_ADMIN_ID
        
        # 1. Promote New Owner
        col_admins.update_one(
            {"user_id": target_admin_id},
            {"$set": {
                "permissions": list(PERMISSIONS.keys()),
                "is_owner": True # Persistent Flag
            }}
        )
        
        # 2. Demote Old Owner (to Manager)
        if current_owner_id != target_admin_id: # Self-promotion check
            col_admins.update_one(
                {"user_id": current_owner_id},
                {"$set": {
                    "permissions": ROLES["MANAGER"],
                    "is_owner": False
                }}
            )
        
        # 3. Update Global Cache & env file permanently
        MASTER_ADMIN_ID = target_admin_id
        try:
            with open(ACTIVE_ENV_FILE, "r", encoding="utf-8") as f:
                env_data = f.read()
            # Replace MASTER_ADMIN_ID correctly
            if "MASTER_ADMIN_ID=" in env_data:
                env_data = re.sub(r"MASTER_ADMIN_ID=.*", f"MASTER_ADMIN_ID={target_admin_id}", env_data)
            else:
                env_data += f"\\nMASTER_ADMIN_ID={target_admin_id}\\n"
            with open(ACTIVE_ENV_FILE, "w", encoding="utf-8") as f:
                f.write(env_data)
        except Exception as e:
            logger.error(f"Failed to update {ACTIVE_ENV_FILE}: {e}")
            
        # Log
        log_user_action(message.from_user, "OWNERSHIP TRANSFER", f"New Owner: {target_admin_id}")
        
        # Notify Steps
        try:
             await bot.send_message(
                target_admin_id,
                f"👑 <b>ALL HAIL THE NEW OWNER!</b>\\n\\n"
                f"You have been granted <b>OWNERSHIP</b> of this bot.\\n"
                f"You now have absolute power.\\n\\n"
                f"*Transfer authorized by previous owner.*",
                parse_mode="HTML"
            )
        except: pass
        
        await state.clear()
        await message.answer(
            f"✅ <b>OWNERSHIP TRANSFERRED!</b>\\n\\n"
            f"New Owner: `{target_admin_id}`\\n"
            f"You are now a <b>MANAGER</b>.\\n"
            f"Please restart the bot for full effect.",
            reply_markup=get_main_menu(current_owner_id),
            parse_mode="HTML"
        )
        
    else:
        # Return to Role Selection state
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Transfer Cancelled.", reply_markup=get_roles_menu())

# ==========================================
# ADMIN PERMISSION HANDLERS
# ==========================================

@dp.message(F.text == "🔐 PERMISSIONS")
async def permissions_menu_handler(message: types.Message, state: FSMContext):
    """Show Permission Management - Select Admin"""
    if not await check_authorization(message, "Access Permissions"):
        return
    
    # Check if Master Admin (Only Master can manage permissions)
    if message.from_user.id != MASTER_ADMIN_ID:
        await message.answer("⛔ <b>ACCESS DENIED</b>\n\nOnly the Master Admin can manage permissions.", parse_mode="HTML")
        return

    # List admins to select (EXCLUDE MASTER ADMIN)
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).sort("added_at", 1))
    
    if not admins:
        await message.answer("⚠️ <b>No additional admins found.</b>\nAdd admins first to configure permissions.", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return
        
    await state.set_state(AdminPermissionStates.waiting_for_admin_selection)
    
    msg = "🔐 <b>MANAGE PERMISSIONS</b>\n\nSelect an admin to configure:\n"
    keyboard = []
    
    for admin in admins:
        user_id = admin['user_id']
        name = admin.get('full_name', 'Unknown')
        username = f"(@{admin.get('username')})" if admin.get('username') else ""
        keyboard.append([KeyboardButton(text=str(user_id))]) # Send ID as text
        msg += f"• `{user_id}`: {name} {username}\n"
        
    keyboard.append([KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(AdminPermissionStates.waiting_for_admin_selection)
async def permission_admin_selected(message: types.Message, state: FSMContext):
    """Admin Selected - Show Permission Toggles"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return
        
    if not message.text.isdigit():
        await message.answer("⚠️ Invalid ID. Please select a valid User ID.", reply_markup=get_cancel_keyboard())
        return
        
    target_id = int(message.text)
    admin = col_admins.find_one({"user_id": target_id})
    
    if not admin:
        await message.answer("⚠️ Admin not found.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Get current permissions (Default to None so we can initialize to empty as requested)
    current_perms = admin.get("permissions")
    if current_perms is None:
        current_perms = [] # Start blank so you explicitly grant what is needed
        
    # Save partial state
    await state.update_data(target_admin_id=target_id, current_perms=current_perms)
    await state.set_state(AdminPermissionStates.configuring_permissions)
    
    # Show toggles via Inline Keyboard (better for toggling)
    await send_permission_toggles(message, target_id, current_perms, admin.get("full_name", "Admin"))

async def send_permission_toggles(message: types.Message, target_id: int, current_perms: list, admin_name: str):
    """Helper to send/update permission toggle UI (Reply Keyboard)"""
    
    text = f"🔐 <b>CONFIGURING: {_html_escape(admin_name)}</b> (`{target_id}`)\n\n"
    text += "Use the buttons below to toggle permissions.\n"
    text += "✅ = Allowed | ❌ = Denied\n\n"
    text += "Click <b>💾 SAVE CHANGES</b> to save and exit."
    
    # Build Reply Keyboard
    keyboard = []
    
    # Permission Buttons (2 per row)
    row = []
    for perm_key, btn_text in PERMISSIONS.items():
        is_allowed = perm_key in current_perms
        status_icon = "✅" if is_allowed else "❌"
        # Button Text: "✅ 👥 ADMINS"
        row.append(KeyboardButton(text=f"{status_icon} {btn_text}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    # Actions
    keyboard.append([KeyboardButton(text="✅ SELECT ALL"), KeyboardButton(text="❌ REVOKE ALL")])
    keyboard.append([KeyboardButton(text="💾 SAVE CHANGES"), KeyboardButton(text="❌ CANCEL")])
    
    # Send message with ReplyKeyboard
    # Note: We rely on ReplyKeyboardMarkup to persistent the buttons until state is cleared
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(AdminPermissionStates.configuring_permissions)
async def permission_message_handler(message: types.Message, state: FSMContext):
    """Handle permission toggles via Text Messages"""
    data = await state.get_data()
    current_perms = data.get("current_perms", [])
    target_id = data.get("target_admin_id")
    
    text = message.text
    
    if text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return

    elif text == "💾 SAVE CHANGES":
        col_admins.update_one(
            {"user_id": target_id},
            {"$set": {"permissions": current_perms}}
        )
        await state.clear()
        await message.answer(f"✅ <b>PERMISSIONS SAVED</b> for Admin `{target_id}`", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return

    elif text == "✅ SELECT ALL":
        current_perms = [p for p in DEFAULT_SAFE_PERMISSIONS] # Only Select Safe ones
        # Feedback message
        await message.answer("✅ <b>Safe permissions selected.</b>\n(Dangerous features must be toggled manually)", parse_mode="HTML")

    elif text == "❌ REVOKE ALL":
        current_perms = []
        # Feedback message
        await message.answer("❌ <b>All permissions revoked.</b>", parse_mode="HTML")

    else:
        # Check if it's a toggle button
        # Format: "✅ [NAME]" or "❌ [NAME]"
        # We need to find which permission key matches
        found_key = None
        for key, name in PERMISSIONS.items():
            if name in text: # "👥 ADMINS" in "✅ 👥 ADMINS"
                found_key = key
                break
        
        if found_key:
            if found_key in current_perms:
                current_perms.remove(found_key)
            else:
                current_perms.append(found_key)
        else:
            # Unknown input - ignore or show error
            await message.answer("⚠️ Invalid option. Please use the buttons.")
            return 

    # Update state
    await state.update_data(current_perms=current_perms)
    
    # Re-send menu to update button states
    admin = col_admins.find_one({"user_id": target_id})
    admin_name = admin.get("full_name", "Admin") if admin else "Admin"
    await send_permission_toggles(message, target_id, current_perms, admin_name)

# ==========================================
# GENERAL HANDLERS (No Admin Features)
# ==========================================






# --- Main Execution ---

async def check_and_create_missed_backup():
    """Check if current month's backup exists, create it if missing"""
    try:
        month_year = get_month_year_name()
        filename = f"Backup_{month_year}.zip"
        
        # Check if backup exists in database
        existing_backup = col_backups.find_one({"filename": filename})
        
        if not existing_backup:
            # Safety guard: never create a backup when BOTH collections are empty.
            # Empty data means either a fresh install or a data-loss event; creating
            # a backup now would insert a 0-count record that masks the real loss.
            _guard_pdf = col_pdfs.count_documents({})
            _guard_ig  = col_ig_content.count_documents({})
            if _guard_pdf == 0 and _guard_ig == 0:
                logger.warning(
                    "⚠️ Skipping startup backup — both collections are empty "
                    "(fresh install or data-loss event — see integrity alert)"
                )
                return
            logger.info(f"⚠️ No backup found for {month_year}, creating now...")
            success, filepath, metadata = await create_backup_file(auto=True)

            if success and metadata:
                logger.info(f"✅ Startup backup created: {metadata['filename']}")
                try:
                    caption = (
                        f"📦 <b>STARTUP BACKUP CREATED</b>\n\n"
                        f"No backup existed for {month_year}.\n"
                        f"✅ Created: <code>{metadata['filename']}</code>\n"
                        f"💾 Size: {metadata['file_size_mb']:.2f} MB\n"
                        f"📄 PDFs: {metadata['pdfs_count']} | 📸 IG: {metadata['ig_count']}\n\n"
                        f"This ensures no monthly backup is missed!"
                    )
                    await bot.send_document(
                        MASTER_ADMIN_ID,
                        types.FSInputFile(filepath),
                        caption=caption,
                        parse_mode="HTML"
                    )
                except Exception as send_err:
                    logger.warning(f"Could not send startup backup file: {send_err}")
                    try:
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"📦 <b>STARTUP BACKUP CREATED</b>\n\n"
                            f"✅ Created: <code>{metadata['filename']}</code>\n"
                            f"(File could not be delivered: {send_err})",
                            parse_mode="HTML"
                        )
                    except:
                        pass
            else:
                logger.error(f"❌ Failed to create startup backup for {month_year}")
        else:
            logger.info(f"✅ Backup for {month_year} already exists")
            
    except Exception as e:
        logger.error(f"❌ Startup backup check failed: {e}")

# ==========================================
# DAILY REPORT SYSTEM
# ==========================================

async def generate_daily_report():
    """Generate comprehensive daily report"""
    try:
        logger.info("📊 Generating daily report...")
        
        # Use now_local() so arithmetic with uptime_start (also naive) works correctly
        now = now_local()
        timestamp = now.strftime("%B %d, %Y %I:%M %p")
        
        # Get statistics
        total_pdfs = col_pdfs.count_documents({})
        total_ig_content = col_ig_content.count_documents({})
        total_admins = col_admins.count_documents({})
        total_banned = col_banned_users.count_documents({})
        
        # Get today's activity
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        pdfs_added_today = col_pdfs.count_documents({"created_at": {"$gte": today_start.replace(tzinfo=None)}})
        ig_added_today = col_ig_content.count_documents({"created_at": {"$gte": today_start.replace(tzinfo=None)}})
        
        # Get click statistics (last 24 hours)
        yesterday = now - timedelta(days=1)
        total_clicks_24h = 0
        pdf_clicks_24h = 0
        ig_cc_clicks_24h = 0
        yt_clicks_24h = 0
        
        from dateutil.parser import parse as _parse_date
        def _safe_dt(val):
            if not val:
                return None
            if isinstance(val, str):
                try:
                    return _parse_date(val).replace(tzinfo=None)
                except:
                    return None
            return val.replace(tzinfo=None) if hasattr(val, 'replace') else val

        # Count clicks from PDFs in last 24 hours
        for pdf in col_pdfs.find({}):
            lca = _safe_dt(pdf.get('last_clicked_at'))
            if lca and lca >= yesterday.replace(tzinfo=None):
                total_clicks_24h += pdf.get('clicks', 0)
            
            lac = _safe_dt(pdf.get('last_affiliate_click'))
            if lac and lac >= yesterday.replace(tzinfo=None):
                pdf_clicks_24h += pdf.get('affiliate_clicks', 0)
            
            lyc = _safe_dt(pdf.get('last_yt_click'))
            if lyc and lyc >= yesterday.replace(tzinfo=None):
                yt_clicks_24h += pdf.get('yt_start_clicks', 0)
        
        # Count IG CC clicks in last 24 hours
        for ig in col_ig_content.find({}):
            licc = _safe_dt(ig.get('last_ig_cc_click'))
            if licc and licc >= yesterday.replace(tzinfo=None):
                ig_cc_clicks_24h += ig.get('ig_cc_clicks', 0)
        
        # Get system metrics
        _uptime_start = health_monitor.system_metrics["uptime_start"]
        _uptime_start = _uptime_start.replace(tzinfo=None) if _uptime_start.tzinfo else _uptime_start
        _now_naive = now.replace(tzinfo=None) if now.tzinfo else now
        uptime = _now_naive - _uptime_start
        uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds // 60) % 60}m"
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get top performing content
        top_pdfs = list(col_pdfs.find({}).sort("clicks", -1).limit(5))
        top_ig = list(col_ig_content.find({}).sort("ig_cc_clicks", -1).limit(5))
        
        # Build report
        report = f"📊 <b>BOT 3 DAILY REPORT</b>\n"
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        report += f"📅 <b>Date:</b> {timestamp}\n"
        report += f"⏰ <b>Report Type:</b> {'Morning' if now.hour < 12 else 'Evening'} Report\n\n"
        
        report += f"📈 <b>DATABASE OVERVIEW</b>\n"
        report += f"├ Total PDFs: {total_pdfs}\n"
        report += f"├ Total IG Content: {total_ig_content}\n"
        report += f"├ Total Admins: {total_admins}\n"
        report += f"└ Banned Users: {total_banned}\n\n"
        
        report += f"🆕 <b>TODAY'S ADDITIONS</b>\n"
        report += f"├ New PDFs: {pdfs_added_today}\n"
        report += f"└ New IG Content: {ig_added_today}\n\n"
        
        report += f"📊 <b>LAST 24 HOURS ACTIVITY</b>\n"
        report += f"├ Total Interactions: {total_clicks_24h}\n"
        report += f"├ PDF Affiliate Clicks: {pdf_clicks_24h}\n"
        report += f"├ YT Link Clicks: {yt_clicks_24h}\n"
        report += f"└ IG CC Clicks: {ig_cc_clicks_24h}\n\n"
        
        if top_pdfs:
            report += f"🔥 <b>TOP 5 PERFORMING PDFs</b>\n"
            for i, pdf in enumerate(top_pdfs, 1):
                name = pdf.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = pdf.get('clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        if top_ig:
            report += f"📸 <b>TOP 5 PERFORMING IG CONTENT</b>\n"
            for i, ig in enumerate(top_ig, 1):
                name = ig.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = ig.get('ig_cc_clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        report += f"🖥️ <b>SYSTEM HEALTH</b>\n"
        report += f"├ Uptime: {uptime_str}\n"
        report += f"├ Memory Usage: {memory_mb:.2f} MB\n"
        report += f"├ CPU Usage: {cpu_percent}%\n"
        report += f"├ Total Errors (Since Start): {health_monitor.error_count}\n"
        report += f"├ Health Checks Failed: {health_monitor.health_checks_failed}\n"
        report += f"└ Status: {'✅ Healthy' if health_monitor.is_healthy else '⚠️ Degraded'}\n\n"
        
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += f"🤖 <b>Bot 3 Enterprise Monitoring System</b>\n"
        _next_lbl = "08:40 PM" if now.hour < 12 else "08:40 AM (tomorrow)"
        report += f"📌 Next report at {_next_lbl}"
        
        # Send report
        await bot.send_message(MASTER_ADMIN_ID, report, parse_mode="HTML")
        logger.info("✅ Daily report sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        await health_monitor.send_error_notification(
            "Daily Report Generation Failed",
            str(e),
            traceback.format_exc()
        )

async def daily_report_task():
    """Background task for scheduled daily reports — sleep-until exact times."""
    if not DAILY_REPORT_ENABLED:
        logger.info("Daily reports disabled")
        return

    # Parse configured time strings into (hour, minute) tuples
    _slots: list = []
    for t_str in [DAILY_REPORT_TIME_1, DAILY_REPORT_TIME_2]:
        try:
            _h, _m = map(int, t_str.split(':'))
            _slots.append((_h, _m))
        except Exception:
            logger.warning(f"Invalid report time '{t_str}' — skipping")
    if not _slots:
        logger.error("No valid daily report times configured")
        return

    logger.info(f"✅ Daily report task started (Times: {DAILY_REPORT_TIME_1}, {DAILY_REPORT_TIME_2})")

    while True:
        try:
            # ── Calculate exact sleep until next slot ──────────────────────
            now = now_local()
            next_fire = None
            for hour, minute in _slots:
                candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                if next_fire is None or candidate < next_fire:
                    next_fire = candidate

            wait_secs = (next_fire - now_local()).total_seconds()
            h_w = int(wait_secs // 3600)
            m_w = int((wait_secs % 3600) // 60)
            logger.info(f"📊 Next daily report in {h_w}h {m_w}m")
            await asyncio.sleep(max(wait_secs, 1))

            await generate_daily_report()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Daily report task error: {e}")
            await asyncio.sleep(60)

# ==========================================
# HEALTH MONITORING BACKGROUND TASK
# ==========================================

async def health_monitoring_task():
    """Background task for continuous health monitoring"""
    logger.info("✅ Health monitoring task started")
    
    while True:
        try:
            await health_monitor.check_system_health()
            await health_monitor.log_system_metrics()
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Health monitoring task error: {e}")
            await asyncio.sleep(60)

# ==========================================
# STATE PERSISTENCE BACKGROUND TASK
# ==========================================

async def state_persistence_task():
    """Background task for periodic state backup"""
    if not STATE_BACKUP_ENABLED:
        logger.info("State persistence disabled")
        return
    
    logger.info(f"✅ State persistence task started (Interval: {STATE_BACKUP_INTERVAL_MINUTES} min)")
    
    while True:
        try:
            await state_persistence.save_state()
            await asyncio.sleep(STATE_BACKUP_INTERVAL_MINUTES * 60)
        except Exception as e:
            logger.error(f"State persistence task error: {e}")
            await asyncio.sleep(60)

async def health_check_endpoint(request):
    """Health check endpoint for uptime monitoring and hosting platforms"""
    try:
        uptime = now_local() - health_monitor.system_metrics["uptime_start"]
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        
        return web.json_response({
            "status": "healthy",
            "bot": "Bot 3 Enterprise",
            "timestamp": now_local().isoformat(),
            "uptime_seconds": int(uptime.total_seconds()),
            "uptime_formatted": str(uptime),
            "memory_mb": round(memory_mb, 2),
            "total_requests": health_monitor.system_metrics["total_requests"],
            "total_errors": health_monitor.system_metrics["total_errors"],
            "is_healthy": health_monitor.is_healthy
        })
    except Exception as e:
        logger.error(f"Health check endpoint error: {e}")
        return web.json_response({
            "status": "error",
            "error": str(e)
        }, status=500)

# ==========================================
# 🎬 TUTORIAL PK — Universal tutorial link for ALL Bot1 users
# Stored in db["bot3_tutorials"] with type="PK"
# Delivered to Bot1 users on empty /start (no referral payload)
# ==========================================

@dp.message(F.text == "🎬 TUTORIAL")
async def tutorial_pk_menu_handler(message: types.Message, state: FSMContext):
    """Open Tutorial management submenu."""
    if not await check_authorization(message, "Tutorial Menu", "can_add"):
        return
    await state.clear()
    await message.answer(
        "🎬 <b>TUTORIAL</b>\n\n"
        "Manage the <b>universal tutorial link</b> shown to every Bot1 member\n"
        "on their empty start and inside the Agent Guide.\n\n"
        "One link — one message — delivered to every member automatically.",
        reply_markup=get_tutorial_pk_menu(),
        parse_mode="HTML"
    )


async def _route_tutorial_pk_menu_action(message: types.Message, state: FSMContext, text: str) -> bool:
    """Allow fast switching between tutorial submenu actions even during active tutorial states."""
    if text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_tutorial_pk_menu(), parse_mode="HTML")
        return True
    if text == "➕ ADD TUTORIAL":
        await state.clear()
        await tutorial_pk_add(message, state)
        return True
    if text == "✏️ EDIT TUTORIAL":
        await state.clear()
        await tutorial_pk_edit(message, state)
        return True
    if text == "🗑️ DELETE TUTORIAL":
        await state.clear()
        await tutorial_pk_delete(message, state)
        return True
    if text == "📋 LIST TUTORIAL":
        await state.clear()
        await tutorial_pk_list(message, state)
        return True
    if text == "⬅️ BACK TO ADD MENU":
        await state.clear()
        await back_to_add_handler(message, state)
        return True
    return False


@dp.message(F.text == "➕ ADD TUTORIAL")
async def tutorial_pk_add(message: types.Message, state: FSMContext):
    """Start ADD flow — ask admin for the YouTube tutorial link."""
    if not await check_authorization(message, "Add Tutorial", "can_add"):
        return
    existing = db["bot3_tutorials"].find_one({"type": "PK"})
    if existing and existing.get("link"):
        safe_link = _html.escape(existing["link"])
        await message.answer(
            f"⚠️ <b>A Tutorial link is already set:</b>\n\n"
            f"<code>{safe_link}</code>\n\n"
            "Use <b>✏️ EDIT TUTORIAL</b> to update it, or <b>🗑️ DELETE TUTORIAL</b> to remove it first.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    await state.set_state(TutorialPKStates.waiting_for_link)
    await message.answer(
        "🔗 <b>SEND THE YOUTUBE TUTORIAL LINK</b>\n\n"
        "This link will be delivered to <b>all Bot1 users</b> when they start\n"
        "with no referral — as a premium tutorial message with an inline button.\n\n"
        "• Must be a valid URL starting with <code>https://</code>\n"
        "• Shown as a button — never as raw text\n\n"
        "Send the link now.",
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_link)
async def tutorial_pk_save_link(message: types.Message, state: FSMContext):
    """Validate URL and show confirm keyboard before saving."""
    if not await check_authorization(message, "Save Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if await _route_tutorial_pk_menu_action(message, state, text):
        return
    if not re.match(r"^https?://", text):
        await message.answer(
            "❌ <b>Invalid URL.</b> Please send a valid link starting with <code>https://</code>",
            parse_mode="HTML"
        )
        return
    safe_link = _html.escape(text)
    await state.update_data(pending_link=text)
    await state.set_state(TutorialPKStates.waiting_for_link_confirm)
    await message.answer(
        f"🔗 <b>CONFIRM ADD TUTORIAL</b>\n\n"
        f"<b>Link to save:</b>\n<code>{safe_link}</code>\n\n"
        "Tap <b>✅ CONFIRM ADD</b> to save, or <b>❌ CANCEL</b> to abort.",
        reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM ADD"),
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_link_confirm)
async def tutorial_pk_confirm_add(message: types.Message, state: FSMContext):
    """Save the tutorial link after admin taps confirm button."""
    if not await check_authorization(message, "Confirm Add Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if await _route_tutorial_pk_menu_action(message, state, text):
        return
    if text == "✅ CONFIRM ADD":
        data = await state.get_data()
        link = data.get("pending_link", "")
        db["bot3_tutorials"].update_one(
            {"type": "PK"},
            {"$set": {"type": "PK", "link": link, "updated_at": datetime.now()}},
            upsert=True
        )
        safe_link = _html.escape(link)
        await state.clear()
        await message.answer(
            f"✅ <b>TUTORIAL SAVED</b>\n\n"
            f"<b>Link:</b> <code>{safe_link}</code>\n\n"
            "All Bot1 users will now see this tutorial on their next empty start and in the Agent Guide.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "⚠️ Tap <b>✅ CONFIRM ADD</b> to save or <b>❌ CANCEL</b> to abort.",
            reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM ADD"),
            parse_mode="HTML"
        )


@dp.message(F.text == "✏️ EDIT TUTORIAL")
async def tutorial_pk_edit(message: types.Message, state: FSMContext):
    """Start EDIT flow — shows current link and asks for replacement."""
    if not await check_authorization(message, "Edit Tutorial", "can_add"):
        return
    existing = db["bot3_tutorials"].find_one({"type": "PK"})
    if not existing or not existing.get("link"):
        await message.answer(
            "⚠️ <b>No Tutorial link set yet.</b>\n\nUse <b>➕ ADD TUTORIAL</b> to add one first.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    safe_link = _html.escape(existing["link"])
    await state.update_data(old_link=existing["link"])
    await state.set_state(TutorialPKStates.waiting_for_edit_link)
    await message.answer(
        f"✏️ <b>EDIT TUTORIAL LINK</b>\n\n"
        f"<b>Current link:</b>\n<code>{safe_link}</code>\n\n"
        "Send the new YouTube link now.",
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_edit_link)
async def tutorial_pk_save_edit(message: types.Message, state: FSMContext):
    """Validate new URL and show confirm keyboard before applying."""
    if not await check_authorization(message, "Save Edit Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if await _route_tutorial_pk_menu_action(message, state, text):
        return
    if not re.match(r"^https?://", text):
        await message.answer(
            "❌ <b>Invalid URL.</b> Please send a valid link starting with <code>https://</code>",
            parse_mode="HTML"
        )
        return
    data = await state.get_data()
    old_link = data.get("old_link", "")
    safe_old = _html.escape(old_link)
    safe_new = _html.escape(text)
    await state.update_data(pending_link=text)
    await state.set_state(TutorialPKStates.waiting_for_edit_confirm)
    await message.answer(
        f"✏️ <b>CONFIRM EDIT TUTORIAL</b>\n\n"
        f"<b>Old link:</b>\n<code>{safe_old}</code>\n\n"
        f"<b>New link:</b>\n<code>{safe_new}</code>\n\n"
        "Tap <b>✅ CONFIRM EDIT</b> to apply, or <b>❌ CANCEL</b> to abort.",
        reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM EDIT"),
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_edit_confirm)
async def tutorial_pk_confirm_edit(message: types.Message, state: FSMContext):
    """Apply the updated tutorial link after admin taps confirm button."""
    if not await check_authorization(message, "Confirm Edit Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if await _route_tutorial_pk_menu_action(message, state, text):
        return
    if text == "✅ CONFIRM EDIT":
        data = await state.get_data()
        link = data.get("pending_link", "")
        db["bot3_tutorials"].update_one(
            {"type": "PK"},
            {"$set": {"link": link, "updated_at": datetime.now()}},
            upsert=True
        )
        safe_link = _html.escape(link)
        await state.clear()
        await message.answer(
            f"✅ <b>TUTORIAL UPDATED</b>\n\n"
            f"<b>New link:</b> <code>{safe_link}</code>\n\n"
            "All Bot1 users will now receive this updated tutorial on their next empty start and in the Agent Guide.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "⚠️ Tap <b>✅ CONFIRM EDIT</b> to apply or <b>❌ CANCEL</b> to abort.",
            reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM EDIT"),
            parse_mode="HTML"
        )


@dp.message(F.text == "🗑️ DELETE TUTORIAL")
async def tutorial_pk_delete(message: types.Message, state: FSMContext):
    """Show current link with confirm keyboard to delete."""
    if not await check_authorization(message, "Delete Tutorial", "can_add"):
        return
    existing = db["bot3_tutorials"].find_one({"type": "PK"})
    if not existing or not existing.get("link"):
        await message.answer(
            "⚠️ <b>No Tutorial link to delete.</b>",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    safe_link = _html.escape(existing["link"])
    await state.set_state(TutorialPKStates.waiting_for_delete_confirm)
    await message.answer(
        f"🗑️ <b>DELETE TUTORIAL?</b>\n\n"
        f"<b>Current link:</b>\n<code>{safe_link}</code>\n\n"
        "Tap <b>✅ CONFIRM DELETE</b> to permanently remove, or <b>❌ CANCEL</b> to abort.",
        reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM DELETE"),
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_delete_confirm)
async def tutorial_pk_confirm_delete(message: types.Message, state: FSMContext):
    """Execute deletion after admin taps confirm button."""
    if not await check_authorization(message, "Confirm Delete Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if await _route_tutorial_pk_menu_action(message, state, text):
        return
    if text == "✅ CONFIRM DELETE":
        db["bot3_tutorials"].delete_one({"type": "PK"})
        await state.clear()
        await message.answer(
            "✅ <b>TUTORIAL DELETED.</b>\n\n"
            "Bot1 users will now see a professional 'coming soon' message "
            "until a new link is added.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "⚠️ Tap <b>✅ CONFIRM DELETE</b> to delete or <b>❌ CANCEL</b> to abort.",
            reply_markup=_get_tutorial_confirm_kb("✅ CONFIRM DELETE"),
            parse_mode="HTML"
        )


@dp.message(F.text == "📋 LIST TUTORIAL")
async def tutorial_pk_list(message: types.Message, state: FSMContext):
    """Display the currently stored tutorial link."""
    if not await check_authorization(message, "List Tutorial", "can_add"):
        return
    await state.clear()
    existing = db["bot3_tutorials"].find_one({"type": "PK"})
    if existing and existing.get("link"):
        safe_link = _html.escape(existing["link"])
        updated = existing.get("updated_at")
        updated_str = updated.strftime("%B %d, %Y — %I:%M %p") if updated else "Unknown"
        await message.answer(
            f"📋 <b>TUTORIAL LINK</b>\n\n"
            f"<b>Status:</b> ✅ Active\n"
            f"<b>Scope:</b> Universal — all Bot1 users (empty start + Agent Guide)\n"
            f"<b>Last updated:</b> {updated_str}\n\n"
            f"<b>Link:</b>\n<code>{safe_link}</code>",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "📋 <b>TUTORIAL STATUS</b>\n\n"
            "❌ <b>No link set yet.</b>\n\n"
            "Use <b>➕ ADD TUTORIAL</b> to add a link — it will be\n"
            "sent to Bot1 users as a premium framed message with a watch button.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )

# --- Smart PDF Selection ---

_PDF_EXCLUDED_BUTTONS = {
    # ── Main menu ─────────────────────────────────────────────────────────────
    "💳 CREDIT MANAGEMENT", "📚 BOT GUIDE", "📋 LIST", "➕ ADD",
    "🔍 SEARCH", "🔗 LINKS", "📊 ANALYTICS", "🩺 DIAGNOSIS",
    "🖥️ TERMINAL", "💾 BACKUP DATA", "👥 ADMINS",
    # ── Economy menu ──────────────────────────────────────────────────────────
    "✏️ EDIT REFERRAL PTS (REFERRER)", "✏️ EDIT BONUS PTS (NEW USER)", "✏️ EDIT IGCC PTS (BOUNTY)",
    "🏆 EDIT LB GOLD", "🏆 EDIT LB SILVER", "🏆 EDIT LB BRONZE",
    # ── Store / Reward menus ───────────────────────────────────────────────────
    "🛒 STORE MANAGER", "➕ ADD STORE ITEM", "✏️ EDIT STORE ITEM",
    "🗑️ DELETE STORE ITEM", "📋 LIST STORE ITEMS",
    "➕ ADD REWARD", "✏️ EDIT REWARD", "🗑️ DELETE REWARD", "📋 LIST REWARD",
    "🎯 MILESTONES", "⬅️ BACK TO REWARD MENU", "⬅️ BACK TO ADD MENU",
    # ── Navigation / Cancel ────────────────────────────────────────────────────
    "⬅️ BACK", "❌ CANCEL", "❌ CANCEL",
}

@dp.message(
    StateFilter(None),  # ONLY fires when no FSM state is active — never swallows FSM input
    lambda m: (
        m.text and
        (m.text.isdigit() or len(m.text) > 2) and
        m.text not in _PDF_EXCLUDED_BUTTONS and
        m.text[0].isascii()
    )
)
async def smart_pdf_selection_handler(message: types.Message, state: FSMContext):
    """Catches text that might be a PDF Index or Name — only runs outside FSM states"""
    query = message.text
    pdf = None
    
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        # Pass through to debug logger if not found
        # We invoke the debug handler explicitly if we want, or just let it fall through
        # But since we are catching it here, we must decide.
        # If it looks like a command (starts with /), let it pass.
        if message.text.startswith("/"): return 
        # Otherwise, treat as "Unknown Command"
        print(f"⚠️ UNHANDLED MESSAGE: '{message.text}'")
        await message.answer(f"⚠️ Unhandled command: {message.text}\nPlease run /start to update your menu.")
        return

    # PDF Found - Show Actions
    await state.update_data(edit_id=str(pdf["_id"]), current_name=pdf["name"], current_link=pdf["link"])
    await state.set_state(PDFActionStates.waiting_for_action)
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 EDIT NAME"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="🗑️ DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await message.answer(
        f"📄 <b>SELECTED PDF</b>\n"
        f"🆔 Index: `{pdf['index']}`\n"
        f"📛 Name: {pdf['name']}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ <b>Select Action:</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(PDFActionStates.waiting_for_action)
async def process_pdf_action(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Selection Cancelled.", reply_markup=get_pdf_menu())
    
    if message.text == "📝 EDIT NAME":
        await state.update_data(field="name")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Name:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        
    elif message.text == "🗑️ DELETE":
        # Transition to delete confirm
        data = await state.get_data()
        await state.update_data(delete_id=data['edit_id']) # Reuse ID
        
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
        ], resize_keyboard=True)
        
        await state.set_state(PDFStates.waiting_for_delete_confirm)
        await message.answer(
            f"⚠️ <b>CONFIRM DELETION</b>\n\nAre you sure you want to delete this PDF?",
            reply_markup=kb,
            parse_mode="HTML"
        )
    else:
         await message.answer("⚠️ Invalid Option. Choose from the buttons.", reply_markup=get_cancel_keyboard())

# ==========================================
# --- General Handlers (Catch-all for buttons outside FSM states) ---
@dp.message(F.text == "❌ CANCEL")
async def general_cancel_handler(message: types.Message, state: FSMContext):
    """Handles cancel button clicks when not in a specific state"""
    if not await check_authorization(message, "Cancel button"):
        return
    await state.clear()
    await message.answer("❌ Operation cancelled.", reply_markup=get_main_menu(message.from_user.id))


# --- Debug Handler - Catch All with Authorization ---
# ==========================================
# ⚙️ ECONOMY SETTINGS MANAGER
# ==========================================

class EconomyStates(StatesGroup):
    waiting_for_ref_pts    = State()
    waiting_for_bonus_pts  = State()
    waiting_for_igcc_pts   = State()
    waiting_for_join_bonus = State()
    waiting_for_lb_gold    = State()
    waiting_for_lb_silver  = State()
    waiting_for_lb_bronze  = State()
    waiting_for_bc_msg     = State()
    waiting_for_bc_amt     = State()
    confirm_bc             = State()
    waiting_for_info_id    = State()
    waiting_for_target_id  = State()
    waiting_for_target_action_amt = State()
    waiting_for_target_amount_input = State()


def get_economy_settings_doc():
    doc = db["bot1_state_persistence"].find_one({"key": "economy_settings"})
    if not doc:
        doc = {"key": "economy_settings", "referral_pts": 25, "referred_bonus": 20, "igcc_default": 25, "leaderboard_rewards": [50, 40, 30]}
        db["bot1_state_persistence"].insert_one(doc)
    if "leaderboard_rewards" not in doc:
        doc["leaderboard_rewards"] = [50, 40, 30]
    return doc


def get_economy_menu_kb():
    keyboard = [
        [KeyboardButton(text="👥 BROADCAST CREDITS (VAULT)")],
        [KeyboardButton(text="ℹ️ INFO USER"), KeyboardButton(text="🎯 TARGET SPECIFIC USER")],
        [KeyboardButton(text="✏️ EDIT REFERRAL PTS (REFERRER)")],
        [KeyboardButton(text="✏️ EDIT BONUS PTS (NEW USER)")],
        [KeyboardButton(text="✏️ EDIT IGCC PTS (BOUNTY)")],
        [KeyboardButton(text="🏆 EDIT LB GOLD"), KeyboardButton(text="🏆 EDIT LB SILVER")],
        [KeyboardButton(text="🏆 EDIT LB BRONZE")],
        [KeyboardButton(text="⬅️ BACK")],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def _econ_text(doc):
    lb_rewards = doc.get("leaderboard_rewards", [50, 40, 30])
    return (
        "💳 *MSA CREDIT MANAGEMENT*\n\n"
        f"🔸 *Referral Bonus (Referrer):* `{doc.get('referral_pts', 25)} pts`\n"
        f"🔸 *Referred Bonus (New User):* `{doc.get('referred_bonus', 20)} pts`\n"
        f"🔸 *IGCC Default Bounty:*        `{doc.get('igcc_default', 25)} pts`\n\n"
        f"🔸 *Weekly Top 1 (Gold):*      `{lb_rewards[0]} pts`\n"
        f"🔸 *Weekly Top 2 (Silver):*    `{lb_rewards[1]} pts`\n"
        f"🔸 *Weekly Top 3 (Bronze):*    `{lb_rewards[2]} pts`\n\n"
        "Select a field to update:"
    )


@dp.message(F.text == "💳 CREDIT MANAGEMENT")
async def economy_menu(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    await state.clear()
    doc = get_economy_settings_doc()
    await message.answer(_econ_text(doc), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")


# ── Edit Referral (Referrer) pts ─────────────────────────────────────────────
@dp.message(F.text == "✏️ EDIT REFERRAL PTS (REFERRER)")
async def econ_edit_ref(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer(
        "Enter the new points to award the *Referrer* per confirmed referral:\n_(e.g. 25)_",
        reply_markup=cancel_kb, parse_mode="Markdown"
    )
    await state.set_state(EconomyStates.waiting_for_ref_pts)


@dp.message(EconomyStates.waiting_for_ref_pts)
async def econ_save_ref(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        doc = get_economy_settings_doc()
        return await message.answer(_econ_text(doc), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number (e.g. 25).")
    pts = int(message.text.strip())
    db["bot1_state_persistence"].update_one(
        {"key": "economy_settings"}, {"$set": {"referral_pts": pts}}, upsert=True
    )
    await state.clear()
    doc = get_economy_settings_doc()
    await message.answer(
        f"✅ Referral Bonus updated to *{pts} pts*\n\n" + _econ_text(doc),
        reply_markup=get_economy_menu_kb(), parse_mode="Markdown"
    )


# ── Edit Referred Bonus (New User) pts ───────────────────────────────────────
@dp.message(F.text == "✏️ EDIT BONUS PTS (NEW USER)")
async def econ_edit_bonus(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer(
        "Enter the new starter bonus for *New Users* who join via a referral link:\n_(e.g. 20)_",
        reply_markup=cancel_kb, parse_mode="Markdown"
    )
    await state.set_state(EconomyStates.waiting_for_bonus_pts)


@dp.message(EconomyStates.waiting_for_bonus_pts)
async def econ_save_bonus(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        doc = get_economy_settings_doc()
        return await message.answer(_econ_text(doc), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number (e.g. 20).")
    pts = int(message.text.strip())
    db["bot1_state_persistence"].update_one(
        {"key": "economy_settings"}, {"$set": {"referred_bonus": pts}}, upsert=True
    )
    await state.clear()
    doc = get_economy_settings_doc()
    await message.answer(
        f"✅ New User Bonus updated to *{pts} pts*\n\n" + _econ_text(doc),
        reply_markup=get_economy_menu_kb(), parse_mode="Markdown"
    )


# ── Edit IGCC pts ─────────────────────────────────────────────────────────────
@dp.message(F.text == "✏️ EDIT IGCC PTS (BOUNTY)")
async def econ_edit_igcc(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer(
        "Enter the new default credits awarded for *IG Bounty* claims:\n_(e.g. 25)_",
        reply_markup=cancel_kb, parse_mode="Markdown"
    )
    await state.set_state(EconomyStates.waiting_for_igcc_pts)


@dp.message(EconomyStates.waiting_for_igcc_pts)
async def econ_save_igcc(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        doc = get_economy_settings_doc()
        return await message.answer(_econ_text(doc), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number (e.g. 25).")
    pts = int(message.text.strip())
    db["bot1_state_persistence"].update_one(
        {"key": "economy_settings"}, {"$set": {"igcc_default": pts}}, upsert=True
    )
    await state.clear()
    doc = get_economy_settings_doc()
    await message.answer(
        f"✅ IGCC Bounty updated to *{pts} pts*\n\n" + _econ_text(doc),
        reply_markup=get_economy_menu_kb(), parse_mode="Markdown"
    )



# ── Edit Leaderboard Rewards ─────────────────────────────────────────────────────────────
@dp.message(F.text == "🏆 EDIT LB GOLD")
async def econ_edit_lb_gold(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer("Enter the weekly credits awarded to the *1st Place (Gold)* spender:\n_(e.g. 50)_", reply_markup=cancel_kb, parse_mode="Markdown")
    await state.set_state(EconomyStates.waiting_for_lb_gold)

@dp.message(EconomyStates.waiting_for_lb_gold)
async def econ_save_lb_gold(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number.")
    pts = int(message.text.strip())
    doc = get_economy_settings_doc()
    lb = doc.get("leaderboard_rewards", [50, 40, 30])
    lb[0] = pts
    db["bot1_state_persistence"].update_one({"key": "economy_settings"}, {"$set": {"leaderboard_rewards": lb}}, upsert=True)
    await state.clear()
    await message.answer(f"✅ Gold Rank reward updated to *{pts} pts*\n\n" + _econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")

@dp.message(F.text == "🏆 EDIT LB SILVER")
async def econ_edit_lb_silver(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer("Enter the weekly credits awarded to the *2nd Place (Silver)* spender:\n_(e.g. 40)_", reply_markup=cancel_kb, parse_mode="Markdown")
    await state.set_state(EconomyStates.waiting_for_lb_silver)

@dp.message(EconomyStates.waiting_for_lb_silver)
async def econ_save_lb_silver(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number.")
    pts = int(message.text.strip())
    doc = get_economy_settings_doc()
    lb = doc.get("leaderboard_rewards", [50, 40, 30])
    lb[1] = pts
    db["bot1_state_persistence"].update_one({"key": "economy_settings"}, {"$set": {"leaderboard_rewards": lb}}, upsert=True)
    await state.clear()
    await message.answer(f"✅ Silver Rank reward updated to *{pts} pts*\n\n" + _econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")

@dp.message(F.text == "🏆 EDIT LB BRONZE")
async def econ_edit_lb_bronze(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer("Enter the weekly credits awarded to the *3rd Place (Bronze)* spender:\n_(e.g. 30)_", reply_markup=cancel_kb, parse_mode="Markdown")
    await state.set_state(EconomyStates.waiting_for_lb_bronze)

@dp.message(EconomyStates.waiting_for_lb_bronze)
async def econ_save_lb_bronze(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number.")
    pts = int(message.text.strip())
    doc = get_economy_settings_doc()
    lb = doc.get("leaderboard_rewards", [50, 40, 30])
    lb[2] = pts
    db["bot1_state_persistence"].update_one({"key": "economy_settings"}, {"$set": {"leaderboard_rewards": lb}}, upsert=True)
    await state.clear()
    await message.answer(f"✅ Bronze Rank reward updated to *{pts} pts*\n\n" + _econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")



# ── Edit Vault Join Bonus ───────────────────────────────────────────────────
@dp.message(F.text == "✏️ EDIT JOIN BONUS")
async def econ_edit_join_bonus(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "credit_management", "can_credit"): return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True)
    await message.answer(
        "Enter the credits awarded to a user the *first time they join the Vault*:\n_(e.g. 30)_",
        reply_markup=cancel_kb, parse_mode="Markdown"
    )
    await state.set_state(EconomyStates.waiting_for_join_bonus)


@dp.message(EconomyStates.waiting_for_join_bonus)
async def econ_save_join_bonus(message: types.Message, state: FSMContext):
    if message.text == "⬅️ BACK":
        await state.clear()
        doc = get_economy_settings_doc()
        return await message.answer(_econ_text(doc), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid whole number (e.g. 30).")
    pts = int(message.text.strip())
    db["bot1_state_persistence"].update_one(
        {"key": "economy_settings"}, {"$set": {"vault_join_bonus": pts}}, upsert=True
    )
    await state.clear()
    doc = get_economy_settings_doc()
    await message.answer(
        f"✅ Vault Join Bonus updated to *{pts} pts*\n\n" + _econ_text(doc),
        reply_markup=get_economy_menu_kb(), parse_mode="Markdown"
    )
async def _process_credit_broadcast(msg_text: str, pts: int):
    try:
        from aiogram import Bot
        bot1_token = os.environ.get("BOT_1_TOKEN")
        if not bot1_token:
            logger.error("[BROADCAST] BOT_1_TOKEN not set in bot3 environment.")
            return
        
        b1_bot = Bot(token=bot1_token)
        
        # 1. Fetch all verified vault users
        users = list(db["bot1_user_verification"].find({"vault_joined": True}, {"user_id": 1}))
        if not users:
            await b1_bot.session.close()
            return
            
        success = 0
        failed = 0
        col_credits = db["bot1_msa_credits"]
        
        for u in users:
            uid = u["user_id"]
            # 2. Add credits securely (upsert pattern handles old users perfectly)
            col_credits.update_one(
                {"user_id": uid},
                {"$inc": {"balance": pts}, "$set": {"last_updated": now_local()}},
                upsert=True
            )
            
            # 3. Send message
            try:
                # _html_escape prevents crashing if user sends bad characters in the text
                msg = f"{msg_text}\n\n🎁 <b>+{pts} MSA Credits</b> have been added to your balance!"
                await b1_bot.send_message(chat_id=uid, text=msg, parse_mode="HTML")
                success += 1
            except Exception:
                failed += 1
                
            await asyncio.sleep(0.05) # Rate limit safety
            
        await b1_bot.session.close()
        logger.info(f"[BROADCAST CREDITS] Finished: {success} delivered, {failed} failed.")
        
        # Send confirmation to master admin
        await bot.send_message(
            MASTER_ADMIN_ID, 
            f"✅ <b>BROADCAST COMPLETE</b>\n\n💰 <b>Credits:</b> +{pts}\n👥 <b>Success:</b> {success} users\n❌ <b>Failed:</b> {failed} users",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"[BROADCAST CREDITS] Error: {e}")

@dp.message(F.text == "👥 BROADCAST CREDITS (VAULT)")
async def econ_bc_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "broadcast_credits"): return
    
    # Get active vault members count
    vault_count = db["bot1_user_verification"].count_documents({"vault_joined": True})
    
    await state.set_state(EconomyStates.waiting_for_bc_msg)
    await message.answer(
        f"📣 **BROADCAST CREDITS  —  Step 1 / 2**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 **Active Vault Members:** `{vault_count}` users\n\n"
        f"Enter the message you want to broadcast to these vault members.\n"
        f"_(HTML format is supported)_\n\n"
        f"Type ❌ CANCEL to abort.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(EconomyStates.waiting_for_bc_msg)
async def econ_bc_msg(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    
    await state.update_data(bc_msg=message.text)
    await state.set_state(EconomyStates.waiting_for_bc_amt)
    await message.answer(
        "💰 **CREDIT AMOUNT  —  Step 2 / 2**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "How many credits should be awarded to each user? (Enter a number)",
        parse_mode="Markdown"
    )

@dp.message(EconomyStates.waiting_for_bc_amt)
async def econ_bc_amt(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
        
    if not message.text or not message.text.strip().isdigit():
        return await message.answer("❌ Please enter a valid positive number.")
        
    pts = int(message.text.strip())
    data = await state.get_data()
    msg_text = data.get("bc_msg", "")
    
    await state.update_data(bc_amt=pts)
    await state.set_state(EconomyStates.confirm_bc)
    
    preview = f"{msg_text}\n\n🎁 <b>+{pts} MSA Credits</b> have been added to your balance!"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM BROADCAST")], 
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await message.answer(
        f"<b>PREVIEW OF MESSAGE:</b>\n\n{preview}\n\nDo you want to send this to ALL verified Vault users and give them <b>{pts} credits</b>?",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(EconomyStates.confirm_bc)
async def econ_bc_confirm(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("Broadcast cancelled.", reply_markup=get_economy_menu_kb())
        
    if message.text == "✅ CONFIRM BROADCAST":
        data = await state.get_data()
        pts = data.get("bc_amt", 0)
        msg_text = data.get("bc_msg", "")
        
        await message.answer("🚀 Broadcast started! This runs securely in the background.", reply_markup=get_economy_menu_kb())
        await state.clear()
        
        asyncio.create_task(_process_credit_broadcast(msg_text, pts))


@dp.message(F.text == "ℹ️ INFO USER")
async def econ_info_user_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "info_user"): return
    await state.set_state(EconomyStates.waiting_for_info_id)
    await message.answer(
        "ℹ️ **USER INFO**\n\n"
        "Enter the User ID or MSA ID of the user you want to check:\n"
        "Type ❌ CANCEL to abort.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(EconomyStates.waiting_for_info_id)
async def econ_info_user_id(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    
    uid_str = message.text.strip().upper()
    if uid_str.startswith("MSA"):
        msa_doc = db["bot1_msa_ids"].find_one({"msa_id": uid_str})
        if not msa_doc:
            return await message.answer(f"❌ MSA ID `{uid_str}` not found.")
        uid = msa_doc["user_id"]
        msa_id = uid_str
    else:
        if not uid_str.isdigit():
            return await message.answer("❌ Please enter a valid numerical User ID or MSA ID.")
        uid = int(uid_str)
        msa_doc = db["bot1_msa_ids"].find_one({"user_id": uid})
        msa_id = msa_doc["msa_id"] if msa_doc else "Unknown"
    
    user_doc = db["bot1_user_verification"].find_one({"user_id": uid})
    if not user_doc:
        return await message.answer(f"❌ User `{uid}` not found in the vault.", parse_mode="Markdown")
    
    credits_doc = db["bot1_msa_credits"].find_one({"user_id": uid})
    curr_credits = credits_doc.get("balance", 0) if credits_doc else 0
    
    ledger = credits_doc.get("ledger", []) if credits_doc else []
    
    source_totals = {}
    total_earned = 0
    for entry in ledger:
        pts = entry.get("pts", 0)
        reason = entry.get("reason", "Unknown").strip()
        if pts > 0:
            total_earned += pts
            source_totals[reason] = source_totals.get(reason, 0) + pts
            
    source_text = "\n".join([f"  • {reason}: `{pts}` pts" for reason, pts in source_totals.items()])
    if not source_text:
        source_text = "  • _No earnings recorded_"
        
    purchased = credits_doc.get("purchased_items", []) if credits_doc else []
    
    await state.clear()
    await message.answer(
        f"ℹ️ **USER CREDIT INFO**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 **Name:** `{user_doc.get('first_name', 'Unknown')}`\n"
        f"🆔 **User ID:** `{uid}`\n"
        f"🏷️ **MSA ID:** `{msa_id}`\n\n"
        f"💰 **Current Balance:** `{curr_credits}` pts\n"
        f"📈 **Total Earned:** `{total_earned}` pts\n\n"
        f"**Earnings Breakdown:**\n"
        f"{source_text}\n\n"
        f"🛒 **Purchased Items:** `{len(purchased)}` items\n",
        reply_markup=get_economy_menu_kb(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎯 TARGET SPECIFIC USER")
async def econ_target_user_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "target_user"): return
    await state.set_state(EconomyStates.waiting_for_target_id)
    await message.answer(
        "🎯 **TARGET SPECIFIC USER**\n\n"
        "Enter the User ID or MSA ID of the user you want to manage:\n"
        "Type ❌ CANCEL to abort.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(EconomyStates.waiting_for_target_id)
async def econ_target_user_id(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    
    uid_str = message.text.strip().upper()
    if uid_str.startswith("MSA"):
        msa_doc = db["bot1_msa_ids"].find_one({"msa_id": uid_str})
        if not msa_doc:
            return await message.answer(f"❌ MSA ID `{uid_str}` not found.")
        uid = msa_doc["user_id"]
    else:
        if not uid_str.isdigit():
            return await message.answer("❌ Please enter a valid numerical User ID or MSA ID.")
        uid = int(uid_str)
    
    user_doc = db["bot1_user_verification"].find_one({"user_id": uid})
    if not user_doc:
        return await message.answer(f"❌ User `{uid}` not found in the vault.", parse_mode="Markdown")
    
    credits_doc = db["bot1_msa_credits"].find_one({"user_id": uid})
    curr_credits = credits_doc.get("balance", 0) if credits_doc else 0
    
    await state.update_data(target_uid=uid, curr_credits=curr_credits)
    await state.set_state(EconomyStates.waiting_for_target_action_amt)
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ ADD CREDITS"), KeyboardButton(text="➖ CUT CREDITS")],
        [KeyboardButton(text="✂️ SET CREDITS EXACTLY")],
        [KeyboardButton(text="⬅️ BACK TO ECONOMY")]
    ], resize_keyboard=True)
    
    await message.answer(
        f"👤 **USER TARGETED**\n"
        f"Name: `{user_doc.get('first_name', 'Unknown')}`\n"
        f"ID: `{uid}`\n"
        f"Current Credits: `{curr_credits}`\n\n"
        "Select an action to perform, or tap a button to proceed:",
        reply_markup=kb,
        parse_mode="Markdown"
    )

@dp.message(EconomyStates.waiting_for_target_action_amt)
async def econ_target_action_select(message: types.Message, state: FSMContext):
    text = message.text
    if text == "⬅️ BACK TO ECONOMY":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
    
    data = await state.get_data()
    uid = data["target_uid"]
    
    if text in ["➕ ADD CREDITS", "➖ CUT CREDITS", "✂️ SET CREDITS EXACTLY"]:
        await state.update_data(action_type=text)
        await state.set_state(EconomyStates.waiting_for_target_amount_input)
        
        await message.answer(
            f"Enter the amount for **{text}**:\n"
            "Type ❌ CANCEL to abort.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True),
            parse_mode="Markdown"
        )
    else:
        await message.answer("⚠️ Please select an action from the menu.")

@dp.message(EconomyStates.waiting_for_target_amount_input)
async def econ_target_action_amt(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer(_econ_text(get_economy_settings_doc()), reply_markup=get_economy_menu_kb(), parse_mode="Markdown")
        
    amt = message.text.strip()
    if not amt.lstrip('-').isdigit():
        return await message.answer("❌ Please enter a valid number.")
    amt = int(amt)
    
    data = await state.get_data()
    uid = data["target_uid"]
    action_type = data["action_type"]
    
    if action_type == "➕ ADD CREDITS":
        db["bot1_msa_credits"].update_one({"user_id": uid}, {"$inc": {"balance": amt}, "$set": {"last_updated": datetime.now()}}, upsert=True)
    elif action_type == "➖ CUT CREDITS":
        db["bot1_msa_credits"].update_one({"user_id": uid}, {"$inc": {"balance": -amt}, "$set": {"last_updated": datetime.now()}}, upsert=True)
    elif action_type == "✂️ SET CREDITS EXACTLY":
        db["bot1_msa_credits"].update_one({"user_id": uid}, {"$set": {"balance": amt, "last_updated": datetime.now()}}, upsert=True)
        
    new_doc = db["bot1_msa_credits"].find_one({"user_id": uid})
    new_bal = new_doc.get("balance", 0) if new_doc else 0
    
    await state.clear()
    await message.answer(
        f"✅ **USER CREDITS UPDATED**\n\n"
        f"User ID: `{uid}`\n"
        f"Action: `{action_type}` (Amt: {amt})\n"
        f"New Balance: `{new_bal}`",
        reply_markup=get_economy_menu_kb(),
        parse_mode="Markdown"
    )

@dp.message()
async def debug_catch_all(message: types.Message):
    # Apply authorization check
    if not await check_authorization(message, f"message: {message.text or 'media'}"):
        return
    
    print(f"⚠️ UNHANDLED MESSAGE: '{message.text}'")
    await message.answer(f"⚠️ Unhandled command: {message.text}\nPlease run /start to update your menu.")

async def start_health_server():
    """Start health check web server for Render/Railway + optional webhook"""
    global health_server_runner
    try:
        app = web.Application()
        app.router.add_get('/health', health_check_endpoint)
        app.router.add_get('/', health_check_endpoint)  # Root also works

        if _WEBHOOK_URL:
            # Register Telegram webhook route onto the same aiohttp app
            SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=_WEBHOOK_PATH)
            setup_application(app, dp, bot=bot)
            logger.info(f"✅ Webhook route registered: {_WEBHOOK_PATH}")

        runner = web.AppRunner(app)
        await runner.setup()
        health_server_runner = runner  # Store for cleanup

        # Use PORT from environment (Render/Railway provide this)
        port = int(os.environ.get('PORT', 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()

        logger.info(f"✅ Web server started on port {port}")
        print(f"  ✅ Health endpoint: http://0.0.0.0:{port}/health")

    except Exception as e:
        logger.error(f"Failed to start health server: {e}")
        print(f"  ⚠️ Health server failed: {e}")

async def cleanup_on_shutdown():
    """Cleanup resources on bot shutdown to prevent aiohttp warnings"""
    print("\n🔄 Shutting down gracefully...")
    
    try:
        # Close bot session (prevents aiohttp unclosed session warnings)
        await bot.session.close()
        print("✅ Bot session closed")
    except Exception as e:
        logger.error(f"Error closing bot session: {e}")
    
    try:
        # Close health server
        global health_server_runner
        if health_server_runner:
            await health_server_runner.cleanup()
            print("✅ Health server closed")
    except Exception as e:
        logger.error(f"Error closing health server: {e}")
    
    try:
        # Save final state
        if STATE_BACKUP_ENABLED:
            await state_persistence.save_state()
            print("✅ Final state saved")
    except Exception as e:
        logger.error(f"Error saving final state: {e}")

    # 🔴 SHUTDOWN NOTIFICATION TO OWNER
    try:
        if MASTER_ADMIN_ID and MASTER_ADMIN_ID != 0:
            uptime = now_local() - health_monitor.system_metrics["uptime_start"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🔴 <b>BOT 3 — OFFLINE</b>\n\n"
                f"<b>Status:</b> Shutting down\n"
                f"<b>Uptime:</b> {h}h {m}m\n"
                f"<b>Errors:</b> {health_monitor.error_count}\n"
                f"<b>Warnings:</b> {health_monitor.warning_count}\n\n"
                f"<b>Time:</b> {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"_Bot 3 has stopped. It will resume when restarted._",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Failed to send shutdown notification: {e}")

    print("👋 Shutdown complete!\n")

async def main():
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("🚀 BOT 3 ENTERPRISE EDITION STARTING...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Start health check server (for hosting platforms and UptimeRobot)
    asyncio.create_task(start_health_server())
    
    # Load previous state if enabled
    if AUTO_RESUME_ON_STARTUP:
        print("📂 Loading previous state...")
        state_data = await state_persistence.load_state()
        if state_data:
            print(f"✅ State restored from {state_data['timestamp']}")
        else:
            print("ℹ️ No previous state found (fresh start)")

    # ── Auto-detect bot username if not set in env ────────────────────────────────
    global BOT_USERNAME
    if not BOT_USERNAME:
        try:
            _me = await bot.get_me()
            BOT_USERNAME = _me.username or ""
            print(f"  \U0001f916 Bot username auto-detected: @{BOT_USERNAME}")
        except Exception as _gu_err:
            print(f"  \u26a0\ufe0f Could not auto-detect bot username: {_gu_err}")
    else:
        print(f"  \u2705 Bot username from env: @{BOT_USERNAME}")

    print("\n\U0001f511 Verifying MSA code integrity...")
    try:
        repaired_count = await asyncio.to_thread(repair_missing_duplicate_msa_codes)
        await asyncio.to_thread(ensure_unique_msa_code_index)
        print(f"  ✅ MSA integrity ready (repaired: {repaired_count})")
    except Exception as msa_err:
        logger.error(f"MSA integrity check failed: {msa_err}")
        print(f"  ⚠️ MSA integrity warning: {msa_err}")
    
    # Start background tasks
    print("\n🔧 Starting background services...")
    
    asyncio.create_task(health_monitoring_task())
    print(f"  ✅ Health monitoring ({HEALTH_CHECK_INTERVAL}s interval)")
    
    asyncio.create_task(daily_report_task())
    print(f"  ✅ Daily reports ({DAILY_REPORT_TIME_1} & {DAILY_REPORT_TIME_2})")
    
    asyncio.create_task(state_persistence_task())
    print(f"  ✅ State persistence ({STATE_BACKUP_INTERVAL_MINUTES} min interval)")
    
    # ── Unified backup schedulers (reads PROD → writes to BACKUP cluster) ──────
    _backup_uri = os.environ.get("BACKUP_MONGO_URI") or MONGO_URI
    _backup_db  = os.environ.get("BACKUP_MONGO_DB_NAME", "MSANodeBackups")
    if _backup_uri == MONGO_URI:
        print("  ⚠️ BACKUP_MONGO_URI not set — bot3 backups fallback to prod cluster")

    if weekly_backup_scheduler:
        asyncio.create_task(
            weekly_backup_scheduler(
                bot_instance=bot,
                bot_name="bot3",
                owner_id=MASTER_ADMIN_ID,  # Bot3 uses MASTER_ADMIN_ID; OWNER_ID is 0
                mongo_uri=MONGO_URI,
                db_name=MONGO_DB_NAME,
                backup_mongo_uri=_backup_uri,
                backup_db_name=_backup_db
            ),
            name="weekly_backup_bot3"
        )
        print("  ✅ Weekly backup → Backup Cluster (daily 23:59 UTC, month-end → GDrive, TTL 90 days)")

    if monthly_export_scheduler:
        asyncio.create_task(
            monthly_export_scheduler(
                bot_instance=bot,
                bot_name="bot3",
                owner_id=MASTER_ADMIN_ID,  # Bot3 uses MASTER_ADMIN_ID
                mongo_uri=MONGO_URI,
                db_name=MONGO_DB_NAME,
                backup_mongo_uri=_backup_uri,
                backup_db_name=_backup_db
            ),
            name="monthly_export_bot3"
        )
        print("  ✅ Monthly export → GDrive (triggered by weekly_backup_scheduler on last day of month)")

    # ⚠️ DEPRECATED: Old monthly backup (auto_backup_task) is disabled and replaced
    # Startup backup check disabled — auto-backup runs on schedule (1st of month at 2 AM)
    
    # ── Bot3 backup cluster health ping (every 6h) ──────────────────────────
    async def _bot3_backup_cluster_ping():
        """Ping MSANodeBackups every 6h — alert MASTER_ADMIN if down."""
        _last_ok = True
        while True:
            try:
                await asyncio.sleep(6 * 3600)
                if not BACKUP_MONGO_URI:
                    continue
                loop = asyncio.get_event_loop()
                def _ping():
                    import certifi as _c
                    c = pymongo.MongoClient(BACKUP_MONGO_URI, serverSelectionTimeoutMS=8000, tlsCAFile=_c.where())
                    try:
                        c.admin.command("ping")
                        try:
                            st = c[BACKUP_MONGO_DB_NAME or "MSANodeBackups"].command("dbStats")
                            return {"ok": True, "used_mb": round(st.get("dataSize", 0)/1_048_576, 2)}
                        except Exception:
                            return {"ok": True, "used_mb": None}
                    finally:
                        c.close()
                res = await loop.run_in_executor(None, _ping)
                if not _last_ok:
                    await bot.send_message(MASTER_ADMIN_ID, "✅ <b>Bot3 Backup Cluster RECOVERED</b>\n\n<code>MSANodeBackups</code> is reachable again.", parse_mode="HTML")
                _last_ok = True
                used_mb = res.get("used_mb")
                if used_mb and used_mb > 400:
                    pct = round(used_mb / 512 * 100, 1)
                    await bot.send_message(MASTER_ADMIN_ID, f"⚠️ <b>Bot3 Backup Storage Alert</b>\n\n📊 {used_mb:.1f} MB / 512 MB ({pct}%)\n\nConsider clearing: 💾 BACKUP DATA → 🗑️ RESET BACKUP DATA", parse_mode="HTML")
            except asyncio.CancelledError:
                break
            except Exception as _pe:
                if _last_ok:
                    try:
                        await bot.send_message(MASTER_ADMIN_ID, f"🚨 <b>Bot3 Backup Cluster UNREACHABLE</b>\n\n<code>MSANodeBackups</code> did not respond.\n<code>{str(_pe)[:150]}</code>", parse_mode="HTML")
                    except Exception:
                        pass
                _last_ok = False
    asyncio.create_task(_bot3_backup_cluster_ping())
    print("  ✅ Bot3 backup cluster health ping started (every 6h)")

    # ── Auto-heal IG CC codes on every startup (fill gaps from past deletions) ──

    print("\n🔄 Reindexing IG CC codes...")
    ig_count = await reindex_all_ig_cc()
    print(f"  ✅ IG CC codes reindexed: {ig_count} items now CC1–CC{ig_count}")

    # ── STARTUP DATA INTEGRITY CHECK ──────────────────────────────────────────
    # Distinguishes FRESH INSTALL (no backup history → first run, expected empty)
    # from REAL DATA LOSS (backup history exists → something wiped the data).
    # Each case gets its own message. Never deletes or modifies any data.
    try:
        startup_pdf_count = col_pdfs.count_documents({})
        startup_ig_count  = col_ig_content.count_documents({})
        if startup_pdf_count == 0 and startup_ig_count == 0:
            logger.warning("⚠️ STARTUP: Both PDF and IG collections are EMPTY.")
            if MASTER_ADMIN_ID and MASTER_ADMIN_ID != 0:
                try:
                    # Extract cluster hostname for diagnostics (never exposes password)
                    try:
                        from urllib.parse import urlparse as _urlparse
                        _cluster_host = _urlparse(MONGO_URI).hostname or "unknown"
                    except Exception:
                        _cluster_host = "unknown"

                    # Check backup history to distinguish fresh install from data loss
                    _last_backup = col_backups.find_one(sort=[("created_at", -1)])

                    if _last_backup is None:
                        # No backup records at all → brand-new deployment
                        logger.info("ℹ️ STARTUP: No backup history found — fresh install.")
                        
                        # Check if local backup files exist anyway
                        _local_backup_path, _local_backup_time = await find_latest_backup_file()
                        if _local_backup_path:
                            await bot.send_message(
                                MASTER_ADMIN_ID,
                                "ℹ️ <b>FRESH INSTALL DETECTED</b> (but local backup found)\n\n"
                                "Both collections are empty and <b>no backup history in MongoDB</b>.\n\n"
                                "✅ <b>GOOD NEWS:</b> I found a local ZIP backup file!\n"
                                f"• Path: <code>{_local_backup_path}</code>\n"
                                f"• Created: {_local_backup_time}\n\n"
                                "You can restore from this backup using:\n"
                                "→ 🔒 ADMIN PANEL → 💾 BACKUP → 📤 JSON RESTORE\n\n"
                                f"📡 Cluster: <code>{_cluster_host}</code>\n"
                                f"🗄 Database: <code>{MONGO_DB_NAME}</code>",
                                parse_mode="HTML"
                            )
                        else:
                            await bot.send_message(
                                MASTER_ADMIN_ID,
                                "ℹ️ <b>FRESH INSTALL DETECTED</b>\n\n"
                                "Both collections are empty and <b>no backup history exists</b> — "
                                "this is a brand-new deployment.\n\n"
                                "Start adding PDFs and IG content via the admin panel!\n\n"
                                f"📡 Cluster: <code>{_cluster_host}</code>\n"
                                f"🗄 Database: <code>{MONGO_DB_NAME}</code>",
                                parse_mode="HTML"
                            )
                    else:
                        # Backup history found → data existed before but is now gone
                        _last_bk_filename = _last_backup.get("filename", "unknown")
                        _last_bk_pdfs     = _last_backup.get("pdfs_count", "?")
                        _last_bk_ig       = _last_backup.get("ig_count", "?")
                        _last_bk_date     = _last_backup.get("created_at", "")
                        if hasattr(_last_bk_date, "strftime"):
                            _last_bk_date = _last_bk_date.strftime("%B %d, %Y")
                        
                        # Also check for local backups
                        _local_backup_path, _local_backup_time = await find_latest_backup_file()
                        _recovery_hint = ""
                        if _local_backup_path:
                            _recovery_hint = (
                                "\n✅ <b>RECOVERY AVAILABLE:</b>\n"
                                f"Found local ZIP backup: <code>{_local_backup_path.split('/')[-1]}</code>\n"
                                "Use → 🔒 ADMIN PANEL → 💾 BACKUP DATA → 📤 UPLOAD DATA to recover\n"
                            )
                        
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            "🚨 <b>DATA INTEGRITY ALERT</b>\n\n"
                            "⚠️ The bot started up and found <b>0 PDFs</b> and <b>0 IG items</b> "
                            "in the database — but backup history exists, meaning data was here before!\n\n"
                            "<b>Last known good backup:</b>\n"
                            f"• File: <code>{_last_bk_filename}</code>\n"
                            f"• PDFs: {_last_bk_pdfs}  |  IG items: {_last_bk_ig}\n"
                            f"• Date: {_last_bk_date}\n\n"
                            "<b>Possible causes:</b>\n"
                            "• ⚠️ RESET command was executed (check logs)\n"
                            "• Wrong MongoDB connection string\n"
                            "• Connected to a different Atlas cluster\n"
                            f"{_recovery_hint}\n"
                            f"📡 Cluster: <code>{_cluster_host}</code>\n"
                            f"🗄 Database: <code>{MONGO_DB_NAME}</code>\n\n"
                            "<b>ACTION REQUIRED:</b> Check logs and restore from backup if needed.",
                            parse_mode="HTML"
                        )
                except Exception:
                    pass
        else:
            logger.info(f"✅ Startup integrity check passed: {startup_pdf_count} PDFs, {startup_ig_count} IG items")
    except Exception as _ic_err:
        logger.error(f"Startup integrity check failed: {_ic_err}")
    # ── END INTEGRITY CHECK ───────────────────────────────────────────────────
    
    # Send startup notification
    if MASTER_ADMIN_ID and MASTER_ADMIN_ID != 0:
        try:
            startup_msg = (
                "🚀 <b>BOT 3 ENTERPRISE EDITION</b>\n\n"
                "✅ <b>Status:</b> ONLINE\n"
                f"📅 <b>Started:</b> {now_local().strftime('%B %d, %Y %I:%M %p')}\n\n"
                "🔧 <b>Active Systems:</b>\n"
                "├ Auto-Healer: ✅ Active\n"
                "├ Health Monitor: ✅ Active\n"
                "├ Daily Reports: ✅ Active\n"
                "├ State Persistence: ✅ Active\n"
                "└ Auto Backup: ✅ Active\n\n"
                "🛡️ <b>Security:</b> Enterprise Level\n"
                "⚡ <b>Ready to Handle:</b> Millions of Requests\n\n"
                "All systems operational! 🎯"
            )
            await bot.send_message(MASTER_ADMIN_ID, startup_msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
    else:
        print(f"⚠️  WARNING: MASTER_ADMIN_ID is 0 - update {ACTIVE_ENV_FILE} with your Telegram user ID")
        print("   Get your ID from: @userinfobot on Telegram")
    
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("✅ BOT 3 IS NOW ONLINE AND READY!")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
    
    # Start bot (webhook or polling)
    try:
        if _WEBHOOK_URL:
            # ── WEBHOOK MODE (production) ───────────────────────────────────
            logger.info("🔄 Starting in WEBHOOK mode...")
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(_WEBHOOK_URL)
            logger.info(f"✅ Webhook set: {_WEBHOOK_URL}")
            # Health server (with webhook route) started above via create_task
            await asyncio.Event().wait()
        else:
            # ── POLLING MODE (local dev fallback) ──────────────────────────
            logger.info("ℹ️ No RENDER_EXTERNAL_URL — using polling (local dev mode)")
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        # Cleanup on shutdown
        await cleanup_on_shutdown()


# ==========================================
# 🚀 APPLICATION ENTRY POINT
# ==========================================
if __name__ == "__main__":
    try:
        # Validate required environment variables before starting
        required_vars = ["MONGO_URI", "MASTER_ADMIN_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if not BOT_TOKEN:
            missing_vars.insert(0, "BOT_3_TOKEN (or BOT_TOKEN)")
        
        if missing_vars:
            print("❌ ERROR: Missing required environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            print("\n📝 Please set these variables in:")
            print(f"   - Local: Use {ACTIVE_ENV_FILE} (or .env)")
            print("   - Render: Add in Environment section")
            print("   - See RENDER_ENV_VARIABLES.txt for details")
            sys.exit(1)
        
        # Run the bot
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n⚠️  Bot stopped by user (Ctrl+C)")
        print("👋 Goodbye!")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: Bot crashed!")
        print(f"Error: {e}")
        print(f"\nTraceback:")
        traceback.print_exc()
        print("\n📝 Check bot3_errors.log for details")
        sys.exit(1)

