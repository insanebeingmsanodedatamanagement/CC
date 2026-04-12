import asyncio
import os
import sys
import json
import html
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from aiohttp import web as aiohttp_web

# --- 🔧 NEW: UNIFIED BACKUP SYSTEM FINAL ARCHITECTURE ---
# All backup functions are now natively combined into this bot2.py script.
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pymongo import MongoClient
from bson.objectid import ObjectId
from aiogram.fsm.storage.memory import MemoryStorage
import aiohttp
from aiogram.exceptions import TelegramNetworkError, TelegramServerError, TelegramRetryAfter
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.types import TelegramObject
from typing import Callable, Dict, Any, Awaitable

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# ── Bot 2 logging: suppress noisy library output, keep our prints ──────────
import logging as _logging
_logging.basicConfig(
    level=_logging.WARNING,
    format='[BOT2] %(asctime)s %(levelname)s %(name)s: %(message)s',
    handlers=[_logging.StreamHandler(sys.stdout)],
    force=True
)
for _noisy in ("aiogram", "aiogram.event", "aiogram.dispatcher",
               "aiohttp", "asyncio"):
    _logging.getLogger(_noisy).setLevel(_logging.WARNING)
for _ultra_noisy in ("pymongo", "pymongo.client", "pymongo.pool", "pymongo.topology"):
    _logging.getLogger(_ultra_noisy).setLevel(_logging.CRITICAL)
del _noisy
del _ultra_noisy

# ==============================================
# BOT 2 - BROADCAST MANAGEMENT SYSTEM
# ==============================================
# Bot 2: Admin interface for managing broadcasts
# Bot 1:  Actual delivery bot that sends to users
# This ensures broadcasts appear to come from Bot 1
# ==============================================


# ==========================================
# 🧱 INJECTED BACKUP SCHEDULERS SYSTEM
# ==========================================
"""
MSA Node — Unified Backup Scheduler System (v3 — Complete)

Provides:
  - _LOCAL_ROOT            : Base path for local JSON backup folders
  - create_manual_zip      : Export bot collections from PROD → in-memory ZIP
  - force_backup_to_cluster: Snapshot bot collections → MSANodeBackups cluster
  - list_available_local_backups: List available month folders on disk
  - monthly_gdrive_upload  : Upload a monthly local ZIP folder to Google Drive
  - present_gdrive_upload  : Upload present MSANodeBackups records → GDrive, then delete
  - weekly_backup_scheduler: Async task — weekly Sunday 23:59 UTC auto-backup
  - monthly_export_scheduler: Async task — last day of month 23:59 UTC ZIP delivery

Backup tiers:
  1. Manual (instant) → create_manual_zip / force_backup_to_cluster
  2. Weekly auto-scheduler (Sunday 23:59 UTC) → MSANodeBackups cluster
  3. Monthly ZIP (last day 23:59 UTC) → Local folder
  4. GDrive upload (manual or auto) → permanent remote storage
"""

import io
import logging
import zipfile
from calendar import monthrange
from datetime import datetime, timezone, timedelta

import certifi
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ServerSelectionTimeoutError

logger = _logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Root folder for local JSON backups
_LOCAL_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSANode_Local_Backups")

# Render detection — on Render, disk is ephemeral so skip local file writes
_IS_RENDER = os.environ.get("RENDER", "").lower() in ("true", "1", "yes")

# Google Drive folder IDs — lazily resolved after load_dotenv (see bottom of config block)
# Do NOT read os.environ here — load_dotenv hasn't run yet at this point.
_BOT1_GDRIVE_FOLDER_ID = ""  # set after load_dotenv
_BOT2_GDRIVE_FOLDER_ID = ""  # set after load_dotenv
_BOT3_GDRIVE_FOLDER_ID = ""  # set after load_dotenv

# TTL for backup cluster docs: 90 days
_TTL_SECONDS = 90 * 24 * 3600

# Bot collection mappings — which mongo collections belong to which bot
_BOT_COLLECTIONS = {
    "bot1": [
        "bot1_msa_ids",
        "bot1_user_verification",
        "bot1_support_tickets",
        "bot1_banned_users",
        "bot1_suspended_features",
        "bot1_permanently_banned_msa",
        "bot1_offline_log",
        "bot1_settings",
    ],
    "bot2": [
        "bot2_broadcasts",
        "bot2_user_tracking",
        "bot2_access_attempts",
        "bot2_admins",
        "bot2_live_terminal_logs",
        "bot2_cleanup_logs",
        "bot2_cleanup_backups",
    ],
    # ── Bot 3 — strictly isolated, never mixed with bot1/bot2 ──────────────
    "bot3": [
        "bot3_pdfs",
        "bot3_ig_content",
        "bot3_admins",
        "bot3_settings",
        "bot3_banned_users",
        "bot3_logs",
        "bot3_user_activity",
        "bot3_backups",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_ttl_index(col):
    """Create 90-day TTL index on backup_date field (idempotent)."""
    try:
        col.create_index(
            [("backup_date", ASCENDING)],
            expireAfterSeconds=_TTL_SECONDS,
            name="backup_ttl_90d",
            background=True
        )
    except Exception as e:
        # Non-fatal: backup cluster may have transient SSL/network issues.
        # Silenced to DEBUG so it doesn't pollute startup logs.
        logger.debug(f"[BACKUP] TTL index setup skipped (non-fatal): {type(e).__name__}")


def _backup_mongo_client(uri: str, **kwargs) -> MongoClient:
    """Create a MongoClient for the backup cluster using the certifi CA bundle.

    Uses tlsCAFile=certifi.where() — identical to the production client —
    to present a valid CA chain to Atlas and avoid TLSV1_ALERT_INTERNAL_ERROR.
    The previous tlsInsecure=True bypass caused Atlas to reject the handshake
    at the server side.
    """
    opts = {
        "serverSelectionTimeoutMS": 10000,
        "tlsCAFile": certifi.where(),  # Proper CA bundle — fixes TLSV1_ALERT_INTERNAL_ERROR
    }
    opts.update(kwargs)
    return MongoClient(uri, **opts)


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
    """
    Export all collections for a bot from PROD DB.
    Returns: { col_name: { count, documents:[...] } }
    """
    result = {}
    cols = _BOT_COLLECTIONS.get(bot_name, [])
    # Also try any extra collections that match the prefix but aren't hardcoded
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
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for col_name, col_data in collections.items():
            docs = col_data.get("documents", [])
            payload = json.dumps(docs, default=str, indent=2, ensure_ascii=False)
            zf.writestr(f"{col_name}.json", payload)
        meta = {
            "bot": bot_name,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "collections": {k: v.get("count", 0) for k, v in collections.items()},
        }
        zf.writestr("_metadata.json", json.dumps(meta, indent=2))
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API — Synchronous (called via run_in_executor from bot2)
# ─────────────────────────────────────────────────────────────────────────────

def create_manual_zip(bot_name: str, mongo_uri: str, db_name: str) -> io.BytesIO:
    """
    Export all of a bot's live collections from PROD → in-memory BytesIO ZIP.
    Called as: await loop.run_in_executor(None, create_manual_zip, "bot1", MONGO_URI, MONGO_DB_NAME)
    Returns a seeked BytesIO ready for .read().
    """
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    try:
        db = client[db_name]
        collections = _export_bot_collections(db, bot_name)
        return _build_zip_from_collections(bot_name, collections)
    finally:
        client.close()


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
    Called as: await loop.run_in_executor(None, force_backup_to_cluster, "bot1", ...)
    """
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name

    try:
        prod_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_client  = _backup_mongo_client(_write_uri)
        prod_db = prod_client[db_name]
        bkp_db  = bkp_client[_write_db]

        collections = _export_bot_collections(prod_db, bot_name)
        prod_client.close()

        col_count = len(collections)
        doc_count = sum(v.get("count", 0) for v in collections.values())
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y%m%d_%H%M%S")

        # Store full snapshot in MSANodeBackups
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
        logger.error(f"[BACKUP] force_backup_to_cluster failed for {bot_name}: {e}")
        return {"status": "error", "error": str(e), "collections": 0, "docs": 0}


def list_available_local_backups(bot_name: str) -> list:
    """
    Scan _LOCAL_ROOT/<bot_name>/ and return a sorted list of month folder names.
    E.g. ["January 2026", "February 2026"]
    """
    bot_dir = os.path.join(_LOCAL_ROOT, bot_name)
    if not os.path.isdir(bot_dir):
        return []

    months = []
    for year_folder in sorted(os.listdir(bot_dir)):
        year_path = os.path.join(bot_dir, year_folder)
        if not os.path.isdir(year_path):
            continue
        for month_folder in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month_folder)
            if os.path.isdir(month_path):
                months.append(month_folder)

    return months


def monthly_gdrive_upload(
    bot_name: str,
    month_label: str,
    mongo_uri: str,
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
    cutoff_date: datetime = None,
) -> dict:
    """
    Package the local monthly backup folder into a ZIP and upload to Google Drive.

    IMPORTANT: This function DOES NOT apply any TTL/auto-deletion flags and does
    NOT delete any local or cluster data. Call apply_gdrive_ttl_flag() separately
    ONLY after the user explicitly confirms they want to enable auto-deletion.

    Args:
        bot_name:        "bot1" or "bot2"
        month_label:     e.g. "March 2026" (matches folder name in _LOCAL_ROOT)
        mongo_uri:       production URI (not used for data — only kept for signature compat)
        backup_mongo_uri: backup cluster URI (not used here — see apply_gdrive_ttl_flag)
        backup_db_name:  backup DB name
        cutoff_date:     optional UTC datetime upper bound — only files on/before this
                         date are included in the ZIP (default = now UTC).
                         Handles partial months (e.g. April when today is April 15).

    Returns:
        {
          status:    "success" | "error",
          zip_name:  filename,
          size_mb:   float,
          file_id:   GDrive file ID,
          weeks_included: list[str],   # e.g. ["Week 1", "Week 2"]
          days_count: int,             # number of day folders included
          cutoff_str: "2026-04-15",
          message:   str,
        }
    """
    if cutoff_date is None:
        cutoff_date = datetime.now(timezone.utc)

    try:
        # ── Find the local month folder ─────────────────────────────────────
        year      = month_label.split()[-1]
        month_dir = os.path.join(_LOCAL_ROOT, bot_name, year, month_label)

        if not os.path.isdir(month_dir):
            return {"status": "error", "message": f"Local folder not found: {month_dir}"}

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        # ── Collect files respecting cutoff ────────────────────────────────
        # Structure: month_dir / Week N / YYYY-MM-DD / *.json
        # Include a day folder only if its name (YYYY-MM-DD) <= cutoff_str.
        included_weeks: set = set()
        days_count = 0
        file_list: list = []  # list of (abs_path, zip_arcname)

        for week_name in sorted(os.listdir(month_dir)):
            week_path = os.path.join(month_dir, week_name)
            if not os.path.isdir(week_path):
                continue
            for day_name in sorted(os.listdir(week_path)):
                if day_name > cutoff_str:
                    continue  # future date — skip
                day_path = os.path.join(week_path, day_name)
                if not os.path.isdir(day_path):
                    continue
                for fname in os.listdir(day_path):
                    fpath = os.path.join(day_path, fname)
                    arcname = os.path.join(month_label, week_name, day_name, fname)
                    file_list.append((fpath, arcname))
                included_weeks.add(week_name)
                days_count += 1

        if not file_list:
            return {
                "status":  "error",
                "message": f"No backup files found on or before {cutoff_str} in {month_dir}",
            }

        # ── Build ZIP in-memory ─────────────────────────────────────────────
        ts_str   = cutoff_date.strftime("%Y%m%d_%H%M%S")
        zip_name = f"monthly_{bot_name}_{month_label.replace(' ', '_')}_till_{cutoff_str}_{ts_str}.zip"

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fpath, arcname in file_list:
                zf.write(fpath, arcname)
        zip_bytes = buf.getvalue()
        size_mb   = len(zip_bytes) / (1024 * 1024)

        # ── Upload to GDrive (duplicate guard) ─────────────────────────────
        service = _get_gdrive_service()
        if bot_name == "bot1":   target_folder = _BOT1_GDRIVE_FOLDER_ID
        elif bot_name == "bot2": target_folder = _BOT2_GDRIVE_FOLDER_ID
        else:                    target_folder = _BOT3_GDRIVE_FOLDER_ID

        if _gdrive_file_exists(service, zip_name, target_folder):
            return {"status": "error", "message": f"File '{zip_name}' already exists in GDrive."}

        file_id = _gdrive_upload_bytes(service, zip_bytes, zip_name, target_folder)

        return {
            "status":          "success",
            "zip_name":        zip_name,
            "size_mb":         size_mb,
            "file_id":         file_id,
            "weeks_included":  sorted(included_weeks),
            "days_count":      days_count,
            "cutoff_str":      cutoff_str,
            "message":         "Upload successful",
        }

    except Exception as e:
        logger.error(f"[BACKUP] monthly_gdrive_upload failed: {e}")
        return {"status": "error", "message": str(e)}


def apply_gdrive_ttl_flag(
    bot_name: str,
    file_id: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
    month_label: str = "",
    cutoff_str: str = "",
) -> dict:
    """
    Mark MSANodeBackups records as gdrive_uploaded=True for the given bot_name.
    Only records with backup_date on/before cutoff_str are flagged.

    IMPORTANT:
    - Only call this AFTER explicit user confirmation in the Telegram UI.
    - Does NOT delete any data. The 90-day TTL index on backup_date handles
      actual expiry automatically within MongoDB (not triggered here).
    - Main DB (MSANodeDB) NEVER touched.

    Returns: { status, updated_count, message }
    """
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        bkp_col    = bkp_db[f"bot{bot_name[-1]}_backups"]

        filter_q: dict = {"bot": bot_name}
        if cutoff_str:
            # Only flag records whose window_key (YYYY-MM-DD) is <= cutoff_str
            filter_q["window_key"] = {"$lte": cutoff_str}

        result = bkp_col.update_many(
            filter_q,
            {"$set": {
                "gdrive_uploaded":    True,
                "gdrive_file_id":     file_id,
                "gdrive_uploaded_at": datetime.now(timezone.utc),
            }}
        )
        updated_count = result.modified_count
        bkp_client.close()

        logger.info(f"[BACKUP] GDrive TTL flag applied: {updated_count} records for {bot_name} ≤ {cutoff_str}")

        return {
            "status":        "ok",
            "updated_count": updated_count,
            "message":       f"auto-deletion flag set on {updated_count} record(s) in MSANodeBackups",
        }

    except Exception as e:
        logger.error(f"[BACKUP] apply_gdrive_ttl_flag failed for {bot_name}: {e}")
        return {"status": "error", "updated_count": 0, "message": str(e)}




def present_gdrive_upload(
    bot_name: str,
    prod_mongo_uri: str,
    prod_db_name: str = "MSANodeDB",
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
) -> dict:
    """
    Snapshot the bot's LIVE collections from the production cluster (MSANodeDB),
    package them as a ZIP, and upload to Google Drive.

    NOTE: Production data is NEVER deleted — this is a read-only snapshot upload.
    The backup cluster (MSANodeBackups) is not used here due to SSL issues.

    Args:
        bot_name:      "bot1" or "bot2"
        prod_mongo_uri: production cluster URI
        prod_db_name:  production DB name  (default: MSANodeDB)

    Returns:
        { status: "success"|"error", zip_name, size_mb, file_id, doc_count, message }
    """
    try:
        # ── Read from production DB ──────────────────────────────────────────
        prod_client = MongoClient(prod_mongo_uri, serverSelectionTimeoutMS=10000)
        prod_db     = prod_client[prod_db_name]

        collections = _export_bot_collections(prod_db, bot_name)
        prod_client.close()

        doc_count = sum(v.get("count", 0) for v in collections.values())

        if doc_count == 0:
            return {"status": "error", "message": f"No data found in production DB for {bot_name} — skipping upload."}

        # ── Build ZIP in-memory ──────────────────────────────────────────────
        ts_str   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        zip_name = f"present_gdrive_{bot_name}_{ts_str}.zip"

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            meta = {
                "bot":         bot_name,
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "total_docs":  doc_count,
                "source":      prod_db_name,
                "note":        "Snapshot of live production data — nothing deleted",
            }
            zf.writestr("_metadata.json", json.dumps(meta, default=str, indent=2))
            for col_name, col_data in collections.items():
                docs    = col_data.get("documents", [])
                payload = json.dumps(docs, default=str, indent=2, ensure_ascii=False)
                zf.writestr(f"{col_name}.json", payload)

        zip_bytes = buf.getvalue()
        size_mb   = len(zip_bytes) / (1024 * 1024)

        # ── Upload to GDrive (no duplicates) ───────────────────────────────
        service = _get_gdrive_service()
        if bot_name == "bot1": target_folder = _BOT1_GDRIVE_FOLDER_ID
        elif bot_name == "bot2": target_folder = _BOT2_GDRIVE_FOLDER_ID
        else: target_folder = _BOT3_GDRIVE_FOLDER_ID
        if _gdrive_file_exists(service, zip_name, target_folder):
            return {"status": "error", "message": f"File '{zip_name}' already exists in GDrive."}

        file_id = _gdrive_upload_bytes(service, zip_bytes, zip_name, target_folder)

        # ── Log to production DB history ─────────────────────────────────────
        try:
            _log_present_gdrive_prod(bot_name, zip_name, doc_count, prod_mongo_uri, prod_db_name)
        except Exception:
            pass

        return {
            "status":    "success",
            "zip_name":  zip_name,
            "size_mb":   size_mb,
            "file_id":   file_id,
            "doc_count": doc_count,
            "message":   f"Uploaded {zip_name} to GDrive ({doc_count:,} docs). Production data untouched.",
        }

    except Exception as e:
        logger.error(f"[BACKUP] present_gdrive_upload failed for {bot_name}: {e}")
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# BOT 2 CLUSTER DOWNLOAD HELPERS
# Strictly scoped to one bot — reads only from MSANodeBackups, never from
# production. Never writes or deletes anything.
# ─────────────────────────────────────────────────────────────────────────────

def list_cluster_backup_months(
    bot_name: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
) -> list:
    """
    Return a deduplicated, sorted list of month+year groups available in the
    MSANodeBackups cluster for bot_name.

    Each entry is a dict:
      {
        "label":   "April 2026",   # human-readable month label
        "year":    2026,
        "month":   4,              # integer 1–12
        "count":   15,             # number of daily snapshots stored
        "earliest": "2026-04-01",  # first backup_date in that month (YYYY-MM-DD)
        "latest":   "2026-04-15",  # last  backup_date in that month (YYYY-MM-DD)
      }

    Results are returned newest-month-first.  Only entries with backup_date ≤
    now (UTC) are included — no future-dated records are ever shown.
    """
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        now = datetime.now(timezone.utc)

        # Pull only the backup_date + window_key — exclude heavy 'data' field
        cursor = bkp_col.find(
            {"bot": bot_name, "backup_date": {"$lte": now}},
            {"backup_date": 1, "window_key": 1, "_id": 0}
        )

        # Group by (year, month)
        month_map: dict = {}  # key = (year, month)  →  {count, min_date, max_date}
        for rec in cursor:
            bd = rec.get("backup_date")
            if not bd:
                continue
            # Ensure timezone-aware datetime
            if bd.tzinfo is None:
                bd = bd.replace(tzinfo=timezone.utc)
            key = (bd.year, bd.month)
            if key not in month_map:
                month_map[key] = {"count": 0, "min_date": bd, "max_date": bd}
            month_map[key]["count"]   += 1
            if bd < month_map[key]["min_date"]:
                month_map[key]["min_date"] = bd
            if bd > month_map[key]["max_date"]:
                month_map[key]["max_date"] = bd

        bkp_client.close()

        # Build result list, sorted newest first
        result = []
        for (year, month), info in sorted(month_map.items(), key=lambda x: x[0], reverse=True):
            label = datetime(year, month, 1).strftime("%B %Y")  # e.g. "April 2026"
            result.append({
                "label":    label,
                "year":     year,
                "month":    month,
                "count":    info["count"],
                "earliest": info["min_date"].strftime("%Y-%m-%d"),
                "latest":   info["max_date"].strftime("%Y-%m-%d"),
            })

        return result

    except Exception as e:
        logger.error(f"[BACKUP] list_cluster_backup_months failed for {bot_name}: {e}")
        return []


def download_cluster_backup_for_month(
    bot_name: str,
    year: int,
    month: int,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
    cutoff_date: datetime = None,
) -> tuple:
    """
    Download all backup records for bot_name in the given (year, month) from
    MSANodeBackups, merging and deduplicating them into a single in-memory ZIP.

    - cutoff_date: timezone-aware UTC datetime upper bound (default = now).
      Only records with backup_date <= cutoff_date are included.
    - Records are merged collection-by-collection. Documents with the same
      '_id' are deduplicated — the latest snapshot's version is kept.
    - Does NOT modify or delete any records. Read-only.
    - Strictly scoped to bot_name — no other bot's data is touched.

    Returns: (zip_bytes: bytes, filename: str)  or  (None, None) on error.
    """
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        if cutoff_date is None:
            cutoff_date = datetime.now(timezone.utc)

        # Month boundaries (inclusive start, inclusive end clipped to cutoff)
        month_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        # Last second of the month or cutoff_date — whichever is earlier
        if month + 1 > 12:
            month_end_naive = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            month_end_naive = datetime(year, month + 1, 1) - timedelta(seconds=1)
        month_end = month_end_naive.replace(tzinfo=timezone.utc)
        effective_end = min(month_end, cutoff_date)

        # Fetch all records for this bot in the date range, oldest first
        records = list(
            bkp_col.find(
                {
                    "bot": bot_name,
                    "backup_date": {"$gte": month_start, "$lte": effective_end},
                }
            ).sort("backup_date", 1)  # oldest first → newest overwrites
        )
        bkp_client.close()

        if not records:
            return None, None

        # ── Merge all records, deduplicate by _id per collection ──────────────
        # We iterate oldest→newest so the newest record's version of each doc wins.
        merged: dict = {}  # col_name → {str(_id): doc}

        for rec in records:
            collections_data = rec.get("data", {})
            for col_name_in_rec, col_data in collections_data.items():
                docs = col_data.get("documents", [])
                if col_name_in_rec not in merged:
                    merged[col_name_in_rec] = {}
                for doc in docs:
                    if not isinstance(doc, dict):
                        continue
                    doc_id = str(doc.get("_id", id(doc)))
                    merged[col_name_in_rec][doc_id] = doc

        # ── Package into ZIP ───────────────────────────────────────────────────
        ts_str    = effective_end.strftime("%Y%m%d")
        month_lbl = datetime(year, month, 1).strftime("%B_%Y")
        filename  = f"{bot_name}_cluster_backup_{month_lbl}_till_{ts_str}.zip"

        buf = io.BytesIO()
        total_docs = 0
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for col_name_out, id_map in merged.items():
                docs_list = list(id_map.values())
                total_docs += len(docs_list)
                payload = json.dumps(docs_list, default=str, indent=2, ensure_ascii=False)
                zf.writestr(f"{col_name_out}.json", payload)

            meta = {
                "bot":            bot_name,
                "month":          f"{year}-{month:02d}",
                "records_merged": len(records),
                "total_docs":     total_docs,
                "cutoff_date":    effective_end.isoformat(),
                "exported_at":    datetime.now(timezone.utc).isoformat(),
                "note":           "Deduplicated merge of all daily snapshots in this month range.",
            }
            zf.writestr("_metadata.json", json.dumps(meta, indent=2))

        buf.seek(0)
        return buf.read(), filename

    except Exception as e:
        logger.error(f"[BACKUP] download_cluster_backup_for_month failed for {bot_name} {year}-{month}: {e}")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# BOT 3 CLUSTER BACKUP HELPERS
# Strictly scoped: only read/write bot3_backups in MSANodeBackups.
# Never touches bot1 or bot2 data.
# ─────────────────────────────────────────────────────────────────────────────

def list_cluster_backups(
    bot_name: str,
    backup_mongo_uri: str,
    backup_db_name: str = "MSANodeBackups",
) -> list:
    """
    List all backup records for bot_name from MSANodeBackups cluster.
    Returns sorted list (newest first) of metadata dicts:
      {_id, backup_date, backup_type, window_key, docs, collections, gdrive_uploaded}
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
                {"data": 0}   # exclude the heavy payload for listing
            ).sort("backup_date", -1)
        )
        bkp_client.close()

        # Convert ObjectId → str so it can be serialised / passed via FSM state
        for r in records:
            r["_id"] = str(r["_id"])

        return records
    except Exception as e:
        logger.error(f"[BACKUP] list_cluster_backups failed for {bot_name}: {e}")
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
    Bot 3 data only — strictly isolated.
    """
    from bson import ObjectId
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
        ts_str      = record.get("window_key", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
        filename    = f"bot3_backup_{ts_str}.zip"

        buf = _build_zip_from_collections(bot_name, collections)
        return buf.read(), filename

    except Exception as e:
        logger.error(f"[BACKUP] download_cluster_backup failed for {bot_name}/{backup_id}: {e}")
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
    - On SUCCESS: marks gdrive_uploaded=True in the record. The existing 90-day TTL
      index will naturally expire the record 90 days from backup_date.
    - On FAILURE: leaves the record untouched. NEVER deletes on failure.
    Returns: {status, zip_name, size_mb, file_id, message}
    """
    from bson import ObjectId
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
        ts_str      = record.get("window_key", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
        zip_name    = f"gdrive_{bot_name}_{ts_str}.zip"

        buf       = _build_zip_from_collections(bot_name, collections)
        zip_bytes = buf.read()
        size_mb   = len(zip_bytes) / (1024 * 1024)

        # ── Upload to GDrive (duplicate guard) ────────────────────────────────
        service = _get_gdrive_service()
        if _gdrive_file_exists(service, zip_name, _folder):
            bkp_client.close()
            return {"status": "error", "message": f"File '{zip_name}' already exists in GDrive — skipped duplicate."}

        file_id = _gdrive_upload_bytes(service, zip_bytes, zip_name, _folder)

        # ── Mark uploaded in the record (TTL index handles 90-day cleanup) ───
        bkp_col.update_one(
            {"_id": ObjectId(backup_id)},
            {"$set": {"gdrive_uploaded": True, "gdrive_file_id": file_id, "gdrive_uploaded_at": datetime.now(timezone.utc)}}
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
        logger.error(f"[BACKUP] gdrive_upload_cluster_backup failed for {bot_name}/{backup_id}: {e}")
        return {"status": "error", "message": str(e)}


def _log_present_gdrive(bot_name: str, zip_name: str, deleted_count: int, backup_mongo_uri: str, backup_db_name: str):
    """Write a history entry for the present GDrive upload (legacy — backup cluster)."""
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=6000)
        bkp_db     = bkp_client[backup_db_name]
        history_col = bkp_db["bot_backup_history"]
        history_col.insert_one({
            "bot":         bot_name,
            "action":      "Gdrive Upload Present",
            "status":      "success",
            "message":     f"Uploaded {zip_name} to GDrive. Deleted {deleted_count} records from {backup_db_name}.",
            "backup_date": datetime.now(timezone.utc),
        })
        bkp_client.close()
    except Exception:
        pass


def _log_present_gdrive_prod(bot_name: str, zip_name: str, doc_count: int, prod_mongo_uri: str, prod_db_name: str):
    """Write a history entry for the present GDrive upload (production DB path)."""
    try:
        prod_client = MongoClient(prod_mongo_uri, serverSelectionTimeoutMS=6000)
        prod_db     = prod_client[prod_db_name]
        history_col = prod_db["bot_backup_history"]
        history_col.insert_one({
            "bot":         bot_name,
            "action":      "Gdrive Upload Present",
            "status":      "success",
            "message":     f"Uploaded {zip_name} to GDrive ({doc_count:,} docs from production). Data untouched.",
            "backup_date": datetime.now(timezone.utc),
        })
        prod_client.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-SCHEDULERS — Async background tasks registered at bot startup
# ─────────────────────────────────────────────────────────────────────────────

def _save_local_daily_backup(bot_name: str, prod_db, now: datetime):
    """
    Save daily JSON export to local folder hierarchy:
    _LOCAL_ROOT/<bot>/<year>/<Month Year>/Week <1-5>/<YYYY-MM-DD>/

    Weeks are week-of-month (not ISO year-week):
      Week 1 = days  1–7
      Week 2 = days  8–14
      Week 3 = days 15–21
      Week 4 = days 22–28
      Week 5 = days 29–31  (months with 29/30/31 days)
    """
    try:
        year     = str(now.year)
        month    = now.strftime("%B %Y")          # e.g. "March 2026"
        week_num = (now.day - 1) // 7 + 1        # week-of-month: 1..5
        day      = now.strftime("%Y-%m-%d")

        day_dir = os.path.join(_LOCAL_ROOT, bot_name, year, month, f"Week {week_num}", day)
        os.makedirs(day_dir, exist_ok=True)

        collections = _export_bot_collections(prod_db, bot_name)
        for col_name, col_data in collections.items():
            docs = col_data.get("documents", [])
            if docs:
                fpath = os.path.join(day_dir, f"{col_name}.json")
                with open(fpath, "w", encoding="utf-8") as f:
                    json.dump(docs, f, default=str, indent=2, ensure_ascii=False)

        logger.info(f"[BACKUP] Daily local export saved: {day_dir}")
    except Exception as e:
        logger.error(f"[BACKUP] _save_local_daily_backup failed for {bot_name}: {e}")


def _upsert_cluster_snapshot(bot_name: str, prod_db, bkp_db, now: datetime):
    """Upsert today's snapshot into MSANodeBackups cluster (one doc per day per bot).
    Adds week_num, week_label, month_label, year for structured list display."""
    try:
        bot_num    = bot_name[-1]
        bkp_col    = bkp_db[f'bot{bot_num}_backups']
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
        logger.info(f'[BACKUP] Cluster snapshot upserted: {bot_name} {day_key} ({doc_count} docs)')
    except Exception as e:
        logger.error(f'[BACKUP] _upsert_cluster_snapshot failed for {bot_name}: {e}')


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
    Weekly backup — runs every Sunday at 23:59 UTC (and a lightweight daily at 23:59 every night).
    Reads from PROD (mongo_uri/db_name), writes to BACKUP cluster.
    Also saves local JSON hierarchy daily.
    Auto-restarts on crash with owner Telegram alert.
    """
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name

    while True:
        try:
            while True:
                now = datetime.now(timezone.utc)

                # ── Next 23:59 UTC (daily for local export + cluster upsert) ──
                next_run = now.replace(hour=23, minute=59, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                # ── RUN DAILY BACKUP ──────────────────────────────────────────
                try:
                    run_now    = datetime.now(timezone.utc)
                    prod_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
                    bkp_client  = _backup_mongo_client(_write_uri)
                    prod_db     = prod_client[db_name]
                    bkp_db      = bkp_client[_write_db]

                    # Save to local folder (skipped on Render — ephemeral disk)
                    if not _IS_RENDER:
                        _save_local_daily_backup(bot_name, prod_db, run_now)
                    else:
                        logger.info(f"[BACKUP] Render detected — skipping local disk write for {bot_name}")

                    # Upsert to cluster (every day)
                    _upsert_cluster_snapshot(bot_name, prod_db, bkp_db, run_now)

                    prod_client.close()

                    # ── Monthly: last day of month → also build month ZIP ─────
                    _, last_day = monthrange(run_now.year, run_now.month)
                    if run_now.day == last_day:
                        await _auto_monthly_zip_and_gdrive(
                            bot_name         = bot_name,
                            now              = run_now,
                            bot_instance     = bot_instance,
                            owner_id         = owner_id,
                            backup_mongo_uri = backup_mongo_uri,
                            backup_db_name   = backup_db_name,
                        )

                    is_sunday = run_now.weekday() == 6  # 6 = Sunday
                    period    = "weekly" if is_sunday else "daily"
                    col_count = len(_BOT_COLLECTIONS.get(bot_name, []))
                    bkp_client.close()

                    await bot_instance.send_message(
                        chat_id=owner_id,
                        text=(
                            f"✅ <b>{'Weekly' if is_sunday else 'Daily'} Backup</b> — <code>{bot_name}</code>\n"
                            f"📅 {run_now.strftime('%B %d, %Y — %I:%M %p UTC')}\n"
                            f"📂 Local folder saved\n"
                            f"🗄️ Cluster snapshot upserted\n"
                            f"🕐 TTL: 90 days on cluster"
                        ),
                        parse_mode="HTML"
                    )

                except Exception as e:
                    logger.error(f"[BACKUP] Daily run failed for {bot_name}: {e}")
                    try:
                        await bot_instance.send_message(
                            chat_id=owner_id,
                            text=f"❌ <b>Daily backup FAILED</b> — <code>{bot_name}</code>\n<code>{str(e)[:300]}</code>",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                await asyncio.sleep(60)  # avoid double-run

        except asyncio.CancelledError:
            logger.info(f"[BACKUP] weekly_backup_scheduler cancelled for {bot_name}")
            return
        except Exception as crash_err:
            logger.error(f"[BACKUP] weekly_backup_scheduler CRASHED for {bot_name}: {crash_err}")
            try:
                await bot_instance.send_message(
                    chat_id=owner_id,
                    text=f"⚠️ <b>Backup scheduler CRASHED</b> — <code>{bot_name}</code>\nAuto-restarting in 5 min.\n<code>{str(crash_err)[:200]}</code>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            await asyncio.sleep(300)


async def _auto_monthly_zip_and_gdrive(
    bot_name: str,
    now: datetime,
    bot_instance,
    owner_id: int,
    backup_mongo_uri: str = None,
    backup_db_name: str = 'MSANodeBackups',
):
    """
    Auto month-end: build ZIP from cluster for full month → upload to GDrive → notify.
    No TTL applied (manual only via ACTIVATE TTL button).
    """
    month_label = now.strftime('%B %Y')
    try:
        loop = asyncio.get_event_loop()
        from calendar import monthrange as _mr
        _, last_day = _mr(now.year, now.month)
        from_key = now.strftime('%Y-%m-01')
        to_key   = now.strftime(f'%Y-%m-{last_day:02d}')

        def _build_zip():
            return download_cluster_backup_for_month(
                bot_name=bot_name, year=now.year, month=now.month,
                backup_mongo_uri=backup_mongo_uri or MONGO_URI,
                backup_db_name=backup_db_name, cutoff_date=now,
            )

        zip_bytes, filename = await loop.run_in_executor(None, _build_zip)
        if not zip_bytes:
            await bot_instance.send_message(
                chat_id=owner_id,
                text=(
                    f'Auto Monthly GDrive — {bot_name.upper()}\n'
                    f'Month: {month_label}\n'
                    f'No cluster data found for this month.\n'
                    f'Ensure daily backups ran this month.'
                ), parse_mode='HTML'
            )
            return

        size_mb = len(zip_bytes) / (1024 * 1024)

        def _upload():
            service   = _get_gdrive_service()
            folder_id = _BOT1_GDRIVE_FOLDER_ID if bot_name == 'bot1' else _BOT2_GDRIVE_FOLDER_ID
            if _gdrive_file_exists(service, filename, folder_id):
                return 'exists', ''
            fid = _gdrive_upload_bytes(service, zip_bytes, filename, folder_id)
            return 'ok', fid

        gd_status, file_id = await loop.run_in_executor(None, _upload)

        if gd_status == 'exists':
            await bot_instance.send_message(
                chat_id=owner_id,
                text=(
                    f'Auto Monthly GDrive — {bot_name.upper()}: {month_label}\n'
                    f'File already on GDrive: {filename}\n'
                    f'No duplicate uploaded. No TTL applied.'
                ), parse_mode='HTML'
            )
            return

        try:
            col_backup_history.insert_one({
                'bot': bot_name, 'action': 'Auto Monthly GDrive Upload',
                'details': f'{month_label} | {size_mb:.2f}MB | GDrive:{file_id}',
                'timestamp': now,
            })
        except Exception:
            pass

        await bot_instance.send_message(
            chat_id=owner_id,
            text=(
                f'Auto Monthly GDrive SUCCESS\n\n'
                f'Bot: {bot_name.upper()}\n'
                f'Month: {month_label}\n'
                f'File: {filename}\n'
                f'Size: {size_mb:.2f} MB\n'
                f'GDrive ID: {file_id}\n\n'
                f'No TTL applied — use ACTIVATE TTL to enable auto-deletion.\n'
                f'Main DB untouched.'
            ), parse_mode='HTML'
        )

    except Exception as gdrive_err:
        logger.error(f'[BACKUP] Auto monthly GDrive FAILED for {bot_name}: {gdrive_err}')
        try:
            await bot_instance.send_message(
                chat_id=owner_id,
                text=(
                    f'Auto Monthly GDrive FAILED\n'
                    f'Bot: {bot_name.upper()} | Month: {month_label}\n'
                    f'Error: {str(gdrive_err)[:300]}\n'
                    f'Cluster data is safe. Retry via GDRIVE SYSTEM.'
                ), parse_mode='HTML'
            )
        except Exception:
            pass


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
    The actual monthly export logic is now embedded in weekly_backup_scheduler
    (triggered on the last day of month).
    This function is a no-op loop to prevent ImportError.
    """
    logger.info(f"[BACKUP] monthly_export_scheduler started for {bot_name} (handled by weekly_backup_scheduler)")
    while True:
        try:
            await asyncio.sleep(86400)  # sleep 24h, do nothing (handled by weekly_backup_scheduler)
        except asyncio.CancelledError:
            logger.info(f"[BACKUP] monthly_export_scheduler cancelled for {bot_name}")
            return
        except Exception:
            await asyncio.sleep(3600)


# Helper function for retry logic with exponential backoff
async def retry_operation(operation, max_retries=3, base_delay=1.0, operation_name="operation"):
    """Retry an async operation with exponential backoff for network errors"""
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await operation()
        except (TelegramNetworkError, TelegramServerError, aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:  # Don't delay on last attempt
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"⚠️ {operation_name} failed (attempt {attempt + 1}/{max_retries}): {str(e)[:50]}...")
                print(f"🔄 Retrying in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
            else:
                print(f"❌ {operation_name} failed after {max_retries} attempts: {str(e)}")
        except Exception as e:
            # Non-network errors - don't retry
            print(f"❌ {operation_name} failed with non-network error: {str(e)}")
            raise e
    
    # If we get here, all retries failed
    raise last_exception

BOT_TOKEN = os.getenv("BOT_2_TOKEN")
BOT_1_TOKEN = os.getenv("BOT_1_TOKEN")  # Bot 1 for delivery
MASTER_ADMIN_ID = int(os.getenv("MASTER_ADMIN_ID", "0"))
OWNER_ID = MASTER_ADMIN_ID  # Alias for compatibility with auto-healer notifications
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")   # Set on Render; never hardcode here

# ── GDrive folder IDs — read AFTER load_dotenv so bot2.env values are picked up ──
_BOT1_GDRIVE_FOLDER_ID = os.getenv("BOT1_GDRIVE_FOLDER_ID", "")
_BOT2_GDRIVE_FOLDER_ID = os.getenv("BOT2_GDRIVE_FOLDER_ID", "")
_BOT3_GDRIVE_FOLDER_ID = os.getenv("BOT3_GDRIVE_FOLDER_ID", "")

# In-memory set of master-admin IDs that have completed password auth this session
_admin_authenticated: set = set()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "MSANodeDB")  # MongoDB database name
REVIEW_LOG_CHANNEL = int(os.getenv("REVIEW_LOG_CHANNEL", 0))  # Support ticket channel
# Render web-service health check port (Render sets PORT automatically)
PORT = int(os.getenv("PORT", 8090))

# Dedicated backup cluster (separate from production — zero risk to live data)
BACKUP_MONGO_URI    = os.getenv("BACKUP_MONGO_URI")
BACKUP_MONGO_DB_NAME = os.getenv("BACKUP_MONGO_DB_NAME", "MSANodeBackups")
if not BACKUP_MONGO_URI:
    print("⚠️ BACKUP_MONGO_URI not set — backups will store to PRODUCTION cluster (not recommended)")

# ==========================================
# 🌐 WEBHOOK CONFIGURATION
# ==========================================
_WEBHOOK_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
_WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
# Only set Webhook URL if we are actually running on Render, preventing local polling clash
_WEBHOOK_URL = f"{_WEBHOOK_BASE_URL}{_WEBHOOK_PATH}" if (_WEBHOOK_BASE_URL and _IS_RENDER) else ""

# Validate critical config at startup
if not BOT_TOKEN:
    print("❌ FATAL: BOT_2_TOKEN not set in .env")
    sys.exit(1)
if not BOT_1_TOKEN:
    print("❌ FATAL: BOT_1_TOKEN not set in .env")
    sys.exit(1)
if not MASTER_ADMIN_ID:
    print("❌ FATAL: MASTER_ADMIN_ID not set in .env")
    sys.exit(1)
if not MONGO_URI:
    print("❌ FATAL: MONGO_URI not set in .env")
    sys.exit(1)

print(f"🔄 Initializing Bot 2 - Broadcast Management System")
print(f"🤖 Bot 2 Token: {BOT_TOKEN[:20]}...")
print(f"🤖 Bot 1 Token: {BOT_1_TOKEN[:20]}...")

# MongoDB Connection — Single database: MSANodeDB (shared by bot1, bot2)
import certifi
client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=5,
    maxIdleTimeMS=30000,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=10000,
    socketTimeoutMS=30000,
    retryWrites=True,
    retryReads=True,
    w="majority",
    tlsCAFile=certifi.where()
)
db = client[MONGO_DB_NAME]  # MSANodeDB on Render
# Guard: refuse to start if pointed at the wrong database
if db.name != "MSANodeDB":
    print(f"❌ FATAL: MONGO_DB_NAME is '{db.name}' — must be 'MSANodeDB'. Fix your env vars and restart.")
    sys.exit(1)
print(f"✅ Database guard passed: writing to '{db.name}'")

# ── Dedicated BACKUP cluster (separate MongoDB Atlas account — backup data only) ──
# Reads production data from MSANodeDB; writes snapshots to MSANodeBackups.
# Zero risk to live collections — completely isolated connection.
_backup_mongo_uri  = BACKUP_MONGO_URI or MONGO_URI
_backup_db_name    = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
if not BACKUP_MONGO_URI:
    print("⚠️  BACKUP_MONGO_URI not set — bot1/bot2 backup collections falling back to PROD cluster!")
    backup_client = client  # Use existing connection pool to prevent SSL handshake flood limits on MongoDB Free Tier
else:
    print(f"✅ Backup cluster connected: {_backup_db_name}")
    backup_client = MongoClient(
        _backup_mongo_uri,
        maxPoolSize=10,
        minPoolSize=1,
        maxIdleTimeMS=30000,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        retryWrites=True,
        retryReads=True,
        w="majority",
        tlsCAFile=certifi.where(),
        tlsAllowInvalidCertificates=True
    )
    # ── Startup connectivity ping — alert owner if backup cluster unreachable ──
    try:
        backup_client.admin.command("ping")
        print("✅ Backup cluster ping OK — MSANodeBackups reachable")
    except Exception as _bk_ping_err:
        print(f"🚨 BACKUP CLUSTER UNREACHABLE at startup: {_bk_ping_err}")
        import threading as _thread
        def _send_bk_alert():
            import time as _t; _t.sleep(15)  # Wait for bot to be ready before sending
            try:
                import asyncio as _aio
                _loop = _aio.new_event_loop()
                _loop.run_until_complete(
                    bot.send_message(
                        MASTER_ADMIN_ID,
                        "🚨 <b>BACKUP CLUSTER UNREACHABLE</b>\n\n"
                        "⚠️ Could not ping <code>MSANodeBackups</code> at startup.\n"
                        f"Error: <code>{str(_bk_ping_err)[:200]}</code>\n\n"
                        "All automatic backup writes will fail silently until this is resolved.\n"
                        "Check <code>BACKUP_MONGO_URI</code> in your environment.",
                        parse_mode="HTML"
                    )
                )
                _loop.close()
            except Exception: pass
        _thread.Thread(target=_send_bk_alert, daemon=True).start()
backup_db = backup_client[_backup_db_name]  # MSANodeBackups

# ── Bot 2 private collections ──────────────────────────────────────────────
col_broadcasts        = db["bot2_broadcasts"]
col_bot2_backups      = backup_db["bot2_backups"]    # Bot 2 backups → BACKUP cluster only
col_admins            = db["bot2_admins"]             # Bot 2 admin management
col_access_attempts   = db["bot2_access_attempts"]   # Unauthorized access tracking
col_cleanup_backups   = db["bot2_cleanup_backups"]   # Automated cleanup backups
col_cleanup_logs      = db["bot2_cleanup_logs"]       # Cleanup history logs
col_backup_history    = db["bot2_backup_history"]    # PERMANENT backup action log — MSANodeDB (prod, NO TTL)

# ── Bot 1 user data collections ─────────────────────────────────────────────
col_user_tracking     = db["bot2_user_tracking"]     # User source tracking (bot1 writes)
col_support_tickets   = db["bot1_support_tickets"]   # Bot 1 support tickets
col_banned_users      = db["bot1_banned_users"]       # Bot 1 bans
col_suspended_features= db["bot1_suspended_features"]# Feature suspensions
col_bot1_settings     = db["bot1_settings"]           # Bot 1 global settings
col_user_verification = db["bot1_user_verification"] # Bot 1 user verification
col_msa_ids           = db["bot1_msa_ids"]             # Bot 1 MSA+ ID registry
col_bot1_backups      = backup_db["bot1_backups"]    # Bot 1 backups → BACKUP cluster only
col_permanently_banned_msa = db["bot1_permanently_banned_msa"]  # Permanently banned MSA IDs
col_offline_log       = db["bot1_offline_log"]        # Bot 1 ON/OFF event log (dedicated)
col_bot2_restore_data = backup_db["bot2_restore_data"]  # Bot 2 restore snapshot → BACKUP cluster only
col_bot1_restore_data = backup_db["bot1_restore_data"]  # Bot 1 restore snapshot → BACKUP cluster only


print(f"💾 Connected to MongoDB: {MONGO_DB_NAME} (single shared database for Bot 1 + Bot 2)")
print(f"📁 Collections: msa_ids, user_verification, banned_users, suspended_features, support_tickets,")
print(f"               bot2_user_tracking, bot1_offline_log")

# Create unique indexes to prevent duplicates
try:
    col_broadcasts.create_index("broadcast_id", unique=True)
    col_broadcasts.create_index("index", unique=True)
    col_user_tracking.create_index("user_id", unique=True)  # One user = one record
    
    # Support tickets performance indexes (CRITICAL for scaling to millions of users)
    col_support_tickets.create_index([("status", 1), ("created_at", -1)])  # List by status
    col_support_tickets.create_index([("user_id", 1), ("created_at", -1)])  # User lookups
    col_support_tickets.create_index([("msa_id", 1)])  # MSA ID lookups
    col_support_tickets.create_index([("status", 1), ("resolved_at", 1)])  # Cleanup queries
    col_support_tickets.create_index([("user_name", "text"), ("username", "text")])  # Text search
    
    # Cleanup collection indexes
    col_cleanup_backups.create_index([("backup_date", -1)])  # Latest backup queries
    col_cleanup_logs.create_index([("cleanup_date", -1)])  # Latest log queries
    
    # Bot 2 backups collection indexes (backup cluster — may have transient SSL)
    try:
        col_bot2_backups.create_index([("backup_date", -1)])  # Latest backup first
        col_bot2_backups.create_index([("backup_type", 1)])  # Filter by type
    except Exception as _bk2_err:
        print(f"⚠️ Backup cluster index warning (bot2_backups): {type(_bk2_err).__name__} — non-fatal, retrying later")

    # Bot 1 backups collection indexes (backup cluster — may have transient SSL)
    try:
        col_bot1_backups.create_index([("backup_date", -1)])
        col_bot1_backups.create_index([("backup_type", 1)])
        col_bot1_backups.create_index([("bot", 1)])
    except Exception as _bk1_err:
        print(f"⚠️ Backup cluster index warning (bot1_backups): {type(_bk1_err).__name__} — non-fatal, retrying later")

    # Bot 1 offline log index
    col_offline_log.create_index([("triggered_at", -1)])   # Latest events first

    # Permanently banned MSA index
    col_permanently_banned_msa.create_index("user_id")
    col_permanently_banned_msa.create_index("msa_id")

    # Banned users — enforce one record per user_id at DB level
    col_banned_users.create_index("user_id", unique=True)

    # Suspended features — matches upsert logic, one doc per user_id
    col_suspended_features.create_index("user_id", unique=True)
    
    # Admin collection indexes
    col_admins.create_index("user_id", unique=True)  # One admin record per user
    col_admins.create_index([("added_at", -1)])  # Latest admins first
    
    # Access attempts indexes for spam detection
    col_access_attempts.create_index([("user_id", 1), ("attempted_at", -1)])  # Spam queries
    col_access_attempts.create_index([("attempted_at", -1)])  # Cleanup old attempts
    
    # Runtime state index (restart recovery)
    db["bot2_runtime_state"].create_index("state_key", unique=True)

    # ── RESOLVED TICKET AUTO-DELETE ───────────────────────────────────────────
    # Resolved support tickets older than 60 days are automatically removed.
    # Open tickets are NEVER touched by this TTL (sparse=True + resolved_at only set on resolution)
    try:
        try:
            col_support_tickets.drop_index("resolved_at_ttl_60d")
        except Exception:
            pass
        col_support_tickets.create_index(
            [("resolved_at", 1)],
            expireAfterSeconds=15_552_000,  # 180 days (6 months)
            sparse=True,                   # Only fires on docs that HAVE resolved_at
            name="resolved_at_ttl_180d"
        )
        print("✅ TTL index set: bot1_support_tickets resolved → 180-day auto-delete")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (support_tickets resolved): {_ttl_err}")
    
    # ── TTL AUTO-EXPIRY INDEXES ────────────────────────────────────────────────
    # These prevent unbounded growth in log/attempt collections.
    # Each is in its own try/except so a failure never blocks startup.
    # Drop-before-create avoids the "already exists with different options" error on redeploy.

    # bot2_access_attempts — auto-delete after 7 days
    try:
        try:
            col_access_attempts.drop_index("attempted_at_ttl_7d")
        except Exception:
            pass
        col_access_attempts.create_index(
            [("attempted_at", 1)],
            expireAfterSeconds=604_800,   # 7 days
            sparse=True,
            name="attempted_at_ttl_7d"
        )
        print("✅ TTL index set: bot2_access_attempts → 7-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (access_attempts): {_ttl_err}")

    # cleanup_logs — auto-delete after 30 days
    try:
        try:
            col_cleanup_logs.drop_index("cleanup_date_ttl_30d")
        except Exception:
            pass
        col_cleanup_logs.create_index(
            [("cleanup_date", 1)],
            expireAfterSeconds=2_592_000,  # 30 days
            sparse=True,
            name="cleanup_date_ttl_30d"
        )
        print("✅ TTL index set: cleanup_logs → 30-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (cleanup_logs): {_ttl_err}")

    # bot1_offline_log — auto-delete after 90 days (maintenance ON/OFF events)
    try:
        try:
            col_offline_log.drop_index("triggered_at_ttl_90d")
        except Exception:
            pass
        col_offline_log.create_index(
            [("triggered_at", 1)],
            expireAfterSeconds=7_776_000,  # 90 days
            sparse=True,
            name="triggered_at_ttl_90d"
        )
        print("✅ TTL index set: bot1_offline_log → 90-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (offline_log): {_ttl_err}")

    # live_terminal_logs — auto-delete after 3 days
    # Bot1 middleware logs EVERY message here; adding TTL prevents unbounded growth
    # on top of the existing manual trim (belt and suspenders)
    try:
        _live_logs_col = db["bot2_live_terminal_logs"]  # col_live_logs defined later — use db[] directly here
        try:
            _live_logs_col.drop_index("created_at_ttl_3d")
        except Exception:
            pass
        _live_logs_col.create_index(
            [("created_at", 1)],
            expireAfterSeconds=259_200,  # 3 days
            sparse=True,
            name="created_at_ttl_3d"
        )
        print("✅ TTL index set: live_terminal_logs → 3-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (live_terminal_logs): {_ttl_err}")

    # bot3_user_activity — click dedup records; auto-expire after 180 days
    try:
        _activity_col = db["bot3_user_activity"]
        for _old_idx in ("first_click_at_ttl_90d", "first_click_at_ttl_180d"):
            try:
                _activity_col.drop_index(_old_idx)
            except Exception:
                pass
        _activity_col.create_index(
            [("first_click_at", 1)],
            expireAfterSeconds=15_552_000,  # 180 days (matches existing Atlas index)
            sparse=True,
            name="first_click_at_ttl_180d"
        )
        print("✅ TTL index set: bot3_user_activity → 180-day auto-purge")
    except Exception as _ttl_err:
        print(f"⚠️ TTL index warning (bot3_user_activity): {_ttl_err}")

    print("✅ Database indexes created for optimal performance")
except Exception as e:
    print(f"⚠️ Index creation warning: {str(e)}")  # May already exist

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)  # Bot 2 - Admin interface
bot_1 = Bot(token=BOT_1_TOKEN)  # Bot 1 - Message delivery
dp = Dispatcher(storage=MemoryStorage())


class Bot2BanBlockMiddleware(BaseMiddleware):
    """Silently drop all incoming messages from users banned in Bot 2 scope."""
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get("event_from_user")
        if user and user.id != MASTER_ADMIN_ID:
            if col_banned_users.find_one({"user_id": user.id, "scope": "bot2"}):
                return
        return await handler(event, data)


# Global gate: once auto-banned in bot2 scope, all messages are ignored silently
dp.message.middleware(Bot2BanBlockMiddleware())

print(f"⚙️ Bot instances initialized")
print(f"📱 Bot 2: Admin interface ready")
print(f"📤 Bot 1: Message delivery ready")

# ==========================================
# 🕐 TIMEZONE CONFIGURATION
# ==========================================
_BOT2_TZ_STR = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")
try:
    _BOT2_TZ = ZoneInfo(_BOT2_TZ_STR)
except Exception:
    _BOT2_TZ = ZoneInfo("Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(_BOT2_TZ).replace(tzinfo=None)

# ==========================================
# ENTERPRISE HEALTH TRACKING (Global State)
# Defined after now_local() so bot_start_time is correct
# ==========================================
bot2_health = {
    "errors_caught": 0,
    "auto_healed": 0,
    "owner_notified": 0,
    "last_error": None,
    "last_error_type": None,
    "bot_start_time": now_local(),
    "consecutive_failures": 0,
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def format_datetime(dt):
    """Format datetime to 12-hour AM/PM format in local timezone"""
    if not dt:
        return "N/A"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except:
            return dt
    # If naive, assume it was stored in local time (consistent with now_local())
    return dt.strftime("%b %d, %Y %I:%M %p")

# ==========================================
# FSM STATES
# ==========================================

class BroadcastStates(StatesGroup):
    selecting_category = State()
    waiting_for_message = State()
    waiting_for_edit_id = State()
    waiting_for_edit_content = State()
    waiting_for_edit_confirm = State()
    waiting_for_delete_id = State()
    waiting_for_delete_confirm = State()
    waiting_for_list_search = State()

class SupportStates(StatesGroup):
    waiting_for_ticket_search = State()
    waiting_for_resolve_id = State()
    waiting_for_reply_id = State()
    waiting_for_reply_message = State()
    waiting_for_delete_ticket_id = State()
    waiting_for_user_search = State()
    waiting_for_priority_id = State()
    waiting_for_priority_level = State()
    waiting_for_view_channel_id = State()

class FindStates(StatesGroup):
    waiting_for_search = State()  # Waiting for MSA ID or User ID input

class ShootStates(StatesGroup):
    waiting_for_ban_id = State()
    waiting_for_ban_confirm = State()
    waiting_for_unban_id = State()
    waiting_for_unban_confirm = State()
    waiting_for_delete_id = State()
    waiting_for_delete_confirm = State()
    waiting_for_suspend_id = State()
    selecting_suspend_features = State()
    waiting_for_unsuspend_id = State()
    waiting_for_reset_id = State()
    waiting_for_reset_confirm = State()
    waiting_for_shoot_search_id = State()
    waiting_for_temp_ban_id = State()
    selecting_temp_ban_duration = State()
    waiting_for_temp_ban_confirm = State()

class BroadcastWithButtonsStates(StatesGroup):
    selecting_category = State()
    waiting_for_message = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    confirming_buttons = State()

class BackupStates(StatesGroup):
    viewing_menu              = State()
    # Legacy states — kept for compat with older inline keyboard callbacks
    selecting_clear_target    = State()
    waiting_for_clear_confirm1= State()
    waiting_for_clear_confirm2= State()
    upload_awaiting_db_choice = State()
    upload_awaiting_file      = State()
    download_selecting_bot    = State()
    download_awaiting_index   = State()
    download_backup_bot        = State()
    download_backup_target_select = State()
    download_backup_format    = State()
    cluster_dl_bot            = State()
    cluster_dl_month_page     = State()
    cluster_dl_format         = State()
    history_bot_select        = State()
    db_conn_bot_select        = State()
    gdrive_flow_start         = State()
    gdrive_bot_select         = State()
    gdrive_monthly_select     = State()
    gdrive_ttl_confirm        = State()
    report_selecting_bot      = State()
    restore_bot_select        = State()
    restore_confirm1          = State()
    restore_confirm2          = State()
    waiting_for_json_file     = State()

class ResetDataStates(StatesGroup):
    selecting_reset_type = State()        # Choose: Bot1 / Bot2 / ALL
    waiting_for_first_confirm = State()  # Bot1 first confirmation
    waiting_for_final_confirm = State()  # Bot1 final confirmation
    bot2_first_confirm = State()        # Bot2 first confirmation
    bot2_final_confirm = State()        # Bot2 final confirmation
    all_first_confirm = State()          # ALL first confirmation
    all_final_confirm = State()          # ALL final confirmation

class TerminalStates(StatesGroup):
    viewing_bot1 = State()
    viewing_bot2 = State()

class AdminStates(StatesGroup):
    waiting_for_new_admin_id = State()
    waiting_for_admin_role = State()
    waiting_for_remove_admin_id = State()
    waiting_for_remove_confirm = State()
    waiting_for_permission_admin_id = State()
    selecting_permissions = State()
    toggling_permissions = State()
    waiting_for_role_admin_id = State()
    waiting_for_role_type = State()
    selecting_role = State()
    waiting_for_lock_user_id = State()
    waiting_for_lock_action = State()
    waiting_for_unlock_user_id = State()
    waiting_for_ban_user_id = State()
    waiting_for_ban_config_id = State()
    waiting_for_ban_config_confirm = State()
    waiting_for_admin_search = State()
    # Owner transfer flow
    owner_transfer_first_confirm = State()   # Step 1: "type CONFIRM"
    owner_transfer_second_confirm = State()  # Step 2: "type TRANSFER"
    owner_transfer_password = State()        # Step 3: enter secret password
    # Admin session authentication (password gate on /start)
    waiting_for_admin_pw_1 = State()
    waiting_for_admin_pw_2 = State()

class Bot1SettingsStates(StatesGroup):
    viewing_menu    = State()
    choosing_method = State()   # Auto / Templates / Custom choice
    entering_custom = State()   # Typing custom broadcast message

class GuideStates(StatesGroup):
    selecting         = State()   # user is on the guide selector screen
    viewing_bot2     = State()   # paginated Bot 2 admin guide
    viewing_bot1      = State()   # Bot 1 user guide (from inside bot2)

# ==========================================
# 🤖 BOT 1 SETTINGS — BROADCAST TEMPLATES
# ==========================================

_OFFLINE_TEMPLATES = [
    {"title": "🔧 System Upgrade",        "text": "👤 **Dear Valued Member,**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nYour MSA Node Agent is currently undergoing a **premium infrastructure upgrade** to deliver you an even more powerful experience.\n\n🚫 **During Upgrade:**\n• Start links are not active\n• All bot features are temporarily paused\n• No new sessions can begin\n\n⏳ **Status:** Coming back online very soon.\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nThank you for your patience. The upgrade ensures you receive the **best possible service**.\n\n_— MSA Node Systems_"},
    {"title": "🛠 Maintenance Window",     "text": "🛠 **SCHEDULED MAINTENANCE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n**MSA NODE is currently in a scheduled maintenance window.**\n\nOur team is performing essential updates to keep the system running at peak performance.\n\n⏸ **Services on hold:**\n• Content access temporarily unavailable\n• All start links paused\n• Support queue on standby\n\n🔄 **We'll be back shortly.** Thank you for your understanding.\n\n_— MSA NODE Operations Team_"},
    {"title": "⚠️ Emergency Maintenance",  "text": "⚠️ **EMERGENCY MAINTENANCE IN PROGRESS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nWe have detected a critical issue requiring **immediate attention**.\n\nOur engineering team is working around the clock to resolve this as quickly as possible.\n\n🚫 **All bot features are temporarily offline.**\n\n⏳ **Estimated downtime:** Minimal. We're moving fast.\n\nWe apologize for any inconvenience and appreciate your patience.\n\n_— MSA NODE Emergency Response_"},
    {"title": "📅 Scheduled Downtime",     "text": "📅 **SCHEDULED DOWNTIME NOTICE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nAs part of our **regular system maintenance schedule**, MSA NODE Agent is currently offline.\n\nThis downtime was planned to ensure:\n• System stability\n• Performance improvements\n• Database optimization\n\n✅ **All your data and access are safe.** We'll notify you the moment we're back.\n\n_— MSA NODE Systems_"},
    {"title": "🏗 Infrastructure Update",  "text": "🏗 **INFRASTRUCTURE UPDATE IN PROGRESS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nWe are upgrading the **core infrastructure** behind MSA NODE to bring you:\n\n⚡ Faster response times\n🔒 Enhanced security\n📈 Better reliability\n🌐 Improved global access\n\n⏳ **The agent will return shortly with a significantly improved experience.**\n\n_— MSA NODE Engineering_"},
    {"title": "🔴 Critical Fix In Progress","text": "🔴 **CRITICAL FIX IN PROGRESS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nOur team has identified and is actively resolving a **critical issue** in the MSA NODE system.\n\nTo maintain integrity and protect your experience, the agent has been **temporarily suspended**.\n\n🛡 **Your data and access remain fully protected.**\n\nWe will notify you immediately once the fix is deployed and the agent is restored.\n\n_— MSA NODE Tech Support_"},
    {"title": "🚀 Premium Feature Update", "text": "🚀 **PREMIUM FEATURE UPDATE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nExciting things are happening behind the scenes!\n\nWe are currently deploying a **major premium feature update** to your MSA NODE Agent.\n\nNew capabilities and improvements are being integrated right now.\n\n⏳ **The agent will return with even more power. Stay tuned.**\n\n_— MSA NODE Development Team_"},
    {"title": "🔒 Security Maintenance",   "text": "🔒 **SECURITY MAINTENANCE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nWe are performing **critical security hardening** on the MSA NODE system.\n\nDuring this process, all services are temporarily suspended to ensure:\n• Complete system integrity\n• Protection of all member data\n• Zero-tolerance security standards\n\n🛡 **Your account and data are fully secure.**\n\nWe'll be back online shortly.\n\n_— MSA NODE Security Team_"},
    {"title": "💾 Database Optimization",  "text": "💾 **DATABASE OPTIMIZATION IN PROGRESS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nWe are currently **optimizing our database architecture** to ensure:\n\n📊 Faster data retrieval\n🔄 Smoother user experience\n📈 Higher throughput for all members\n🗂 Better organization of your content\n\n⏳ **This optimization will be complete shortly.**\n\n_— MSA NODE Database Team_"},
    {"title": "📦 New Updates in Agent",   "text": "📦 **NEW UPDATES INCOMING — AGENT OFFLINE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🚧 **We are installing new updates to your MSA NODE Agent.**\n\nFresh features, improved workflows, and enhanced content delivery are being prepared for you.\n\n🔧 **What's being updated:**\n• New agent capabilities\n• Enhanced search features\n• Improved dashboard\n• Backend performance boosts\n\n⏳ **Stand by — the new version launches soon.**\n\n_— MSA NODE Development_"},
]

_ONLINE_TEMPLATES = [
    {"title": "✅ Back Online",            "text": "✅ **MSA NODE AGENT — BACK ONLINE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🟢 Your MSA Node Agent has completed its upgrade and is now **fully operational**.\n\n**All features are now available:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📺 Tutorial\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n• All start links are active\n\nThank you for your patience during the upgrade.\n\n_— MSA Node Systems_"},
    {"title": "🔧 System Restored",        "text": "🔧 **SYSTEM FULLY RESTORED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n✅ The MSA NODE system has been fully restored after maintenance.\n\n**Your full access has been reinstated:**\n• 📊 Dashboard — Active\n• 🔍 Search Code — Active\n• 📺 Tutorial — Active\n• 📜 Rules — Active\n• 📖 Agent Guide — Active\n• 📞 Support — Active\n\nWe appreciate your patience and look forward to serving you.\n\n_— MSA NODE Operations_"},
    {"title": "🟢 All Systems Green",      "text": "🟢 **ALL SYSTEMS GREEN**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n**MSA NODE Agent status: FULLY OPERATIONAL**\n\nEvery system has been verified and cleared for full operation.\n\n🚦 **System Status:**\n• 📊 Dashboard .................. ✅ Online\n• 🔍 Search ..................... ✅ Online\n• 📺 Tutorial ................... ✅ Online\n• 📜 Rules ...................... ✅ Online\n• 📖 Guide ...................... ✅ Online\n• 📞 Support .................... ✅ Online\n\nWelcome back!\n\n_— MSA NODE Systems_"},
    {"title": "✨ Premium Upgrade Complete","text": "✨ **PREMIUM UPGRADE COMPLETE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nThe premium upgrade to your MSA NODE Agent has been **successfully completed**.\n\nYour experience has been enhanced with improved speed, reliability, and features.\n\n**Everything you need is ready:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\nThank you for being a valued MSA NODE member.\n\n_— MSA NODE Development_"},
    {"title": "🆕 New Features Available", "text": "🆕 **NEW FEATURES AVAILABLE NOW**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🎉 MSA NODE Agent is back online with **exciting new features and improvements!**\n\nWe've been working hard to make your experience better. Explore everything that's new and improved.\n\n**All services restored:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\n_— MSA NODE Development Team_"},
    {"title": "⚡ Agent Update Deployed",  "text": "⚡ **AGENT UPDATE SUCCESSFULLY DEPLOYED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nYour MSA NODE Agent update has been **deployed and verified**.\n\nThe agent is now running at peak performance with all enhancements active.\n\n**Resume your activities:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\n_— MSA NODE Engineering_"},
    {"title": "💎 Enhanced Experience",    "text": "💎 **ENHANCED EXPERIENCE READY**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nYour **enhanced MSA NODE experience** is now live!\n\nWe've upgraded performance, security, and features to give you the best possible agent experience.\n\n**Full access restored:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\n_— MSA NODE Premium Division_"},
    {"title": "🌐 MSA NODE Next Level",    "text": "🌐 **MSA NODE — NEXT LEVEL ONLINE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🟢 MSA NODE has been elevated to its **next performance tier**.\n\nFaster. More powerful. Smarter.\n\n**Your access:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\nUse /start to begin.\n\n_— MSA NODE Systems_"},
    {"title": "🔓 Elite Access Restored",  "text": "🔓 **ELITE ACCESS RESTORED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nYour **elite MSA NODE membership** has been fully restored.\n\nAll premium tools and features are available to you again.\n\n**Available now:**\n• 📊 Dashboard\n• 🔍 Search Code\n• 📜 Rules\n• 📖 Agent Guide\n• 📞 Support\n\nWelcome back to the elite tier.\n\n_— MSA NODE Elite Division_"},
    {"title": "📦 Agent Session Unlocked", "text": "📦 **AGENT SESSION UNLOCKED — UPDATES LIVE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🎯 **Your MSA NODE Agent has been updated and unlocked.**\n\nAll the new features from our latest session are now **live and ready** for you.\n\n**Explore what's new:**\n• 📊 Dashboard — Enhanced\n• 🔍 Search Code — Faster\n• 📜 Rules — Updated\n• 📖 Agent Guide — Expanded\n• 📞 Support — Improved\n\nUse /start to get started.\n\n_— MSA NODE Development_"},
]

_TPLS_PER_PAGE = 5   # templates shown per InlineKeyboard page


def _build_template_kb(templates: list, page: int, direction: str) -> InlineKeyboardMarkup:
    """Build paginated template selection InlineKeyboard."""
    total   = len(templates)
    total_p = (total + _TPLS_PER_PAGE - 1) // _TPLS_PER_PAGE
    start   = page * _TPLS_PER_PAGE
    end     = min(start + _TPLS_PER_PAGE, total)

    rows = []
    for idx in range(start, end):
        rows.append([InlineKeyboardButton(
            text=templates[idx]["title"],
            callback_data=f"b8t_sel:{direction}:{idx}"
        )])

    # Navigation row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ PREV", callback_data=f"b8t_pg:{direction}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{total_p}", callback_data="b8t_noop"))
    if page < total_p - 1:
        nav.append(InlineKeyboardButton(text="NEXT ▶️", callback_data=f"b8t_pg:{direction}:{page+1}"))
    rows.append(nav)

    rows.append([
        InlineKeyboardButton(text="✏️ CUSTOM MESSAGE", callback_data=f"b8t_custom:{direction}"),
        InlineKeyboardButton(text="❌ CANCEL",          callback_data="b8t_cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ==========================================
# ==========================================
# LIVE TERMINAL LOGGING SYSTEM
# ==========================================

# In-memory log storage (circular buffer) — also backed by MongoDB for Render cross-process support
MAX_LOGS = 50  # Keep last 50 logs per bot

# MongoDB collection for persistent logs (shared across processes / Render services)
col_live_logs = db["bot2_live_terminal_logs"]

# Initialize with startup message
start_time = now_local().strftime('%I:%M:%S %p')
bot1_logs = [{
    "timestamp": start_time,
    "action": "SYSTEM",
    "user_id": 0,
    "details": "Bot 1 log tracking initialized",
    "full_text": f"[{start_time}] SYSTEM > Bot 1 log tracking initialized"
}]
bot2_logs = [{
    "timestamp": start_time,
    "action": "SYSTEM",
    "user_id": 0,
    "details": "Bot 2 log tracking initialized",
    "full_text": f"[{start_time}] SYSTEM > Bot 2 log tracking initialized"
}]

# ==========================================
# 🗄️ MONGODB STORAGE STATS HELPER
# ==========================================
def get_mongo_storage_stats() -> dict:
    """
    Get MongoDB storage usage. Works on Atlas M0 (free 512MB) and paid tiers.
    Returns a dict with all fields needed to display a storage bar + alert.
    """
    try:
        stats      = db.command("dbStats")
        data_mb    = stats.get("dataSize",    0) / 1_048_576
        storage_mb = stats.get("storageSize", 0) / 1_048_576
        index_mb   = stats.get("indexSize",   0) / 1_048_576
        total_mb   = stats.get("totalSize",   0) / 1_048_576
        fs_total   = stats.get("fsTotalSize", 0) / 1_048_576
        fs_used    = stats.get("fsUsedSize",  0) / 1_048_576

        if fs_total > 0:
            # Dedicated/paid tier — use real filesystem values
            used_mb   = fs_used
            cap_mb    = fs_total
            cap_label = f"{cap_mb:.0f}MB filesystem"
        else:
            # Atlas M0 free — cap is 512MB on dataSize+indexSize
            used_mb   = total_mb
            cap_mb    = 512.0
            cap_label = "512MB Atlas M0 free tier"

        pct    = min(used_mb / cap_mb * 100, 100) if cap_mb > 0 else 0.0
        filled = round(pct / 5)
        empty  = 20 - filled
        bar    = "█" * filled + "░" * empty

        if pct >= 90:
            risk_icon  = "🔴"
            risk_label = "CRITICAL — upgrade NOW"
        elif pct >= 75:
            risk_icon  = "🟠"
            risk_label = "HIGH — plan upgrade soon"
        elif pct >= 60:
            risk_icon  = "🟡"
            risk_label = "MODERATE — monitor"
        else:
            risk_icon  = "🟢"
            risk_label = "HEALTHY"

        return {
            "ok":        True,
            "used_mb":   used_mb,
            "cap_mb":    cap_mb,
            "pct":       pct,
            "bar":       bar,
            "risk_icon": risk_icon,
            "risk_label": risk_label,
            "cap_label": cap_label,
            "data_mb":   data_mb,
            "index_mb":  index_mb,
            "storage_mb": storage_mb,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}


def log_action(action_type, user_id, details="", bot="bot2"):
    """Log actions to console, memory, AND MongoDB for live terminal display (works on Render)"""
    timestamp = now_local().strftime('%I:%M:%S %p')

    # Color codes for console terminal
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

    # Console output with colors
    print(f"{CYAN}[{timestamp}]{RESET} {BOLD}{action_type}{RESET}")
    if details:
        print(f"  📋 {details}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Build log entry
    log_entry = {
        "timestamp": timestamp,
        "created_at": now_local(),
        "bot": bot,
        "action": action_type,
        "user_id": user_id,
        "details": details,
        "full_text": f"[{timestamp}] {action_type}" + (f"\n  {details}" if details else "")
    }

    # Add to in-memory list
    if bot == "bot1":
        bot1_logs.append(log_entry)
        if len(bot1_logs) > MAX_LOGS:
            bot1_logs.pop(0)
    else:
        bot2_logs.append(log_entry)
        if len(bot2_logs) > MAX_LOGS:
            bot2_logs.pop(0)
 
    # Persist to MongoDB (for Render cross-process live view)
    try:
        col_live_logs.insert_one(log_entry)
        # Keep collection trimmed — delete oldest beyond MAX_LOGS*2 per bot
        count = col_live_logs.count_documents({"bot": bot})
        if count > MAX_LOGS * 2:
            oldest = list(col_live_logs.find({"bot": bot}, {"_id": 1}).sort("created_at", 1).limit(count - MAX_LOGS))
            if oldest:
                col_live_logs.delete_many({"_id": {"$in": [d["_id"] for d in oldest]}})
    except Exception:
        pass  # Never let logging break the bot

def get_terminal_logs(bot="bot2", limit=50):
    """Get raw terminal logs — reads from MongoDB first (Render-safe), falls back to memory"""
    try:
        # Read from MongoDB for cross-process / Render support
        docs = list(col_live_logs.find({"bot": bot}, {"_id": 0}).sort("created_at", -1).limit(limit))
        if docs:
            docs.reverse()  # Oldest first (terminal style)
            log_lines = []
            MAX_CHARS = 3500
            current_length = 0
            for doc in docs:
                ts = doc.get("timestamp", "??:??:?? ?M")
                action = doc.get("action", "")
                detail = doc.get("details", "")
                line = f"[{ts}] {action}" + (f" > {detail}" if detail else "")
                if current_length + len(line) + 1 > MAX_CHARS:
                    break
                log_lines.append(line)
                current_length += len(line) + 1
            return "\n".join(log_lines) if log_lines else ">> NO LOGS YET..."
    except Exception:
        pass

    # Fallback to in-memory
    logs = bot1_logs if bot == "bot1" else bot2_logs
    if not logs:
        return ">> SYSTEM INITIALIZED. WAITING FOR EVENTS..."
    recent_logs = logs[-limit:]
    MAX_CHARS = 3500
    final_lines = []
    current_length = 0
    for log in reversed(recent_logs):
        line = f"[{log['timestamp']}] {log['action']} > {log['details']}"
        if current_length + len(line) + 1 > MAX_CHARS:
            break
        final_lines.insert(0, line)
        current_length += len(line) + 1
    return "\n".join(final_lines)

# ==========================================
# MENU FUNCTIONS
# ==========================================
# ACCESS CONTROL FUNCTIONS
# ==========================================

async def is_admin(user_id: int) -> bool:
    """Check if user is an admin or the master admin AND is unlocked"""
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False
    
    # Check if admin is locked (inactive)
    if admin.get('locked', False):
        return False  # Locked admins cannot access Bot 2
    
    return True  # Admin exists and is unlocked

async def notify_owner_unauthorized_access(
    user_id: int,
    user_name: str,
    username: str,
    attempt_count: int,
    was_banned: bool = False,
    attempt_type: str = "NON-ADMIN",
):
    """Notify owner about unauthorized /start attempts with strict 12-hour timestamp."""
    timestamp = now_local().strftime('%B %d, %Y — %I:%M:%S %p')
    uname = f"@{username}" if username else "N/A"

    msg = (
        f"🚨 **UNAUTHORIZED /START ATTEMPT**\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"📝 Name: {user_name or 'Unknown'}\n"
        f"🔗 Username: {uname}\n"
        f"📌 Type: **{attempt_type}**\n"
        f"🕐 Time (12h): {timestamp}\n"
        f"🔢 Attempts (5m window): **{attempt_count}**"
    )

    if was_banned:
        msg += "\n\n🚫 **AUTO-BANNED (BOT 2)**\nReason: 3+ unauthorized /start attempts within 5 minutes."

    try:
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
        log_action("🚨 UNAUTHORIZED ACCESS", user_id, f"Owner notified ({attempt_type}) - Attempt #{attempt_count}")
    except Exception as e:
        print(f"❌ Failed to notify owner: {e}")

async def has_permission(user_id: int, permission: str) -> bool:
    """Check if admin has specific permission"""
    # Master admin always has all permissions
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False

    # Locked admins have NO permissions — even if they manually type a command
    if admin.get('locked', False):
        return False

    perms = admin.get('permissions', [])
    return 'all' in perms or permission in perms

# ==========================================
# MENU FUNCTIONS
# ==========================================

async def get_main_menu(user_id: int = None):
    """Main menu keyboard - shows only permitted features"""
    # Master admin and no user_id = show all
    if user_id is None or user_id == MASTER_ADMIN_ID:
        keyboard = [
            [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔍 FIND")],
            [KeyboardButton(text="📊 TRAFFIC"), KeyboardButton(text="🩺 DIAGNOSIS")],
            [KeyboardButton(text="📸 SHOOT"), KeyboardButton(text="💬 SUPPORT")],
            [KeyboardButton(text="💾 BACKUP"), KeyboardButton(text="🖥️ TERMINAL")],
            [KeyboardButton(text="🤖 BOT 1 SETTINGS"), KeyboardButton(text="👥 ADMINS")],
            [KeyboardButton(text="⚠️ RESET DATA"), KeyboardButton(text="📖 GUIDE")]
        ]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    # Get user permissions
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        # Not an admin - show stripped minimal menu
        keyboard = [[KeyboardButton(text="📖 GUIDE")]]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

    # Locked admins are treated as inactive/non-admin until unlocked
    if admin.get('locked', False):
        keyboard = [[KeyboardButton(text="📖 GUIDE")]]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    perms = admin.get('permissions', [])
    has_all = 'all' in perms
    
    # Permission to button mapping
    perm_buttons = {
        'broadcast': "📢 BROADCAST",
        'find': "🔍 FIND",
        'traffic': "📊 TRAFFIC",
        'diagnosis': "🩺 DIAGNOSIS",
        'shoot': "📸 SHOOT",
        'support': "💬 SUPPORT",
        'backup': "💾 BACKUP",
        'terminal': "🖥️ TERMINAL",
        'admins': "👥 ADMINS",
        'bot1': "🤖 BOT 1 SETTINGS"
    }
    
    # Build keyboard with only permitted features
    available_buttons = []
    for perm, button_text in perm_buttons.items():
        if has_all or perm in perms:
            available_buttons.append(button_text)
    
    # Always show GUIDE (ADMINS is now Owner Only)
    available_buttons.append("📖 GUIDE")
    
    # Arrange in rows of 2
    keyboard = []
    for i in range(0, len(available_buttons), 2):
        row = available_buttons[i:i+2]
        keyboard.append([KeyboardButton(text=btn) for btn in row])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


async def _push_instant_user_menu_refresh(user_id: int, context: str = "updated"):
    """Push the current effective menu to a user immediately after role/permission/lock changes."""
    try:
        admin_doc = col_admins.find_one({"user_id": user_id})
        if not admin_doc or admin_doc.get("locked", False):
            await bot.send_message(
                user_id,
                "🔒 Access is currently inactive. Your menu remains restricted until unlock.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        refreshed_menu = await get_main_menu(user_id)
        await bot.send_message(
            user_id,
            f"📋 Your Bot 2 menu was {context} instantly.",
            reply_markup=refreshed_menu
        )
    except Exception as e:
        log_action("⚠️ INSTANT MENU REFRESH FAILED", user_id, str(e))



def get_backup_menu():
    """Backup management submenu — 5 rows layout"""
    keyboard = [
        [KeyboardButton(text="💾 DOWNLOAD BACKUP"), KeyboardButton(text="📤 UPLOAD BACKUP")],
        [KeyboardButton(text="☁️ GDRIVE SYSTEM"),   KeyboardButton(text="📊 BACKUP STATUS")],
        [KeyboardButton(text="📜 HISTORY"),          KeyboardButton(text="⏳ ACTIVATE TTL")],
        [KeyboardButton(text="🗑️ RESET BACKUP DATA")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def get_broadcast_menu():
    """Broadcast management submenu"""
    keyboard = [
        [KeyboardButton(text="📤 SEND BROADCAST")],
        [KeyboardButton(text="🗑️ DELETE BROADCAST"), KeyboardButton(text="✏️ EDIT BROADCAST")],
        [KeyboardButton(text="📋 LIST BROADCASTS")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def _format_broadcast_msg(text: str, is_caption: bool = False) -> str:
    """
    Wrap a broadcast message in MSA NODE official formatting.
    is_caption=True  →  lightweight footer only (Telegram caption ≤ 1024 chars).
    is_caption=False →  full header + footer for text-only broadcasts.
    """
    try:
        dt = now_local().strftime("%b %d, %Y  ·  %I:%M %p")
    except Exception:
        dt = "MSA NODE"

    body = (text or "").strip()

    if is_caption:
        footer = (
            "\n\n──────────────────────────────"
            "\n📢  MSA NODE  ·  Official"
            f"\n🕐  {dt}"
        )
        # No truncation — if caption > 1024, caller handles split (send text separately)
        return body + footer
    else:
        header = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  📢  MSA NODE  ·  BROADCAST\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        footer = (
            "\n\n──────────────────────────────"
            "\n🌐  MSA NODE Ecosystem  ·  Official"
            f"\n🕐  {dt}"
        )
        return header + body + footer


def _esc_md(text: str) -> str:
    """Escape Telegram Markdown v1 special chars in dynamic content (exception msgs, DB values)."""
    for ch in ('*', '_', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text


# ── CHECK LINKS pagination store (in-memory, keyed by user_id) ──────────────
# Each entry: list of page strings. TTL is implicit — overwritten on next /check.
_chk_links_pages: dict[int, list[str]] = {}

def _paginate_report(text: str, max_len: int = 3800) -> list[str]:
    """
    Split a report string into pages of at most max_len chars,
    breaking only at newline boundaries to preserve formatting.
    """
    if len(text) <= max_len:
        return [text]
    pages: list[str] = []
    lines = text.split("\n")
    current: list[str] = []
    current_len = 0
    for line in lines:
        # +1 for the '\n' we'll rejoin with
        needed = len(line) + 1
        if current and current_len + needed > max_len:
            pages.append("\n".join(current))
            current = [line]
            current_len = needed
        else:
            current.append(line)
            current_len += needed
    if current:
        pages.append("\n".join(current))
    return pages


def _preview_cap(text: str, limit: int = 3700) -> str:
    """Truncate broadcast text for safe display inside a Telegram message (≤4096 chars total)."""
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    return t[:limit].rsplit(" ", 1)[0] + "… _(truncated for preview)_"


def _split_text(text: str, max_len: int = 4000) -> list:
    """Split long text into chunks at newline boundaries (max max_len chars each)."""
    if len(text) <= max_len:
        return [text]
    chunks = []
    lines = text.split("\n")
    current: list = []
    current_len = 0
    for line in lines:
        needed = len(line) + 1
        if current and current_len + needed > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = needed
        else:
            current.append(line)
            current_len += needed
    if current:
        chunks.append("\n".join(current))
    return chunks


def get_broadcast_type_menu():
    """Broadcast type selection menu"""
    keyboard = [
        [KeyboardButton(text="📝 NORMAL BROADCAST")],
        [KeyboardButton(text="🔗 BROADCAST WITH BUTTONS")],
        [KeyboardButton(text="⬅️ BACK")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_support_management_menu():
    """Support ticket management submenu"""
    keyboard = [
        [KeyboardButton(text="🎫 PENDING TICKETS"), KeyboardButton(text="📋 ALL TICKETS")],
        [KeyboardButton(text="✅ RESOLVE TICKET"), KeyboardButton(text="📨 REPLY")],
        [KeyboardButton(text="🔍 SEARCH TICKETS"), KeyboardButton(text="🗑️ DELETE")],
        [KeyboardButton(text="👁 VIEW CHANNEL"), KeyboardButton(text="📊 MORE OPTIONS")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_support_more_menu():
    """Support advanced options submenu"""
    keyboard = [
        [KeyboardButton(text="📈 STATISTICS"), KeyboardButton(text="🚨 PRIORITY")],
        [KeyboardButton(text="⏰ AUTO-CLOSE"), KeyboardButton(text="📤 EXPORT")],
        [KeyboardButton(text="⬅️ BACK TO SUPPORT")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_category_menu():
    """Category selection menu for broadcasts"""
    keyboard = [
        [KeyboardButton(text="📺 YT"), KeyboardButton(text="📸 IG")],
        [KeyboardButton(text="📎 IG CC"), KeyboardButton(text="🔗 YTCODE")],
        [KeyboardButton(text="👥 ALL"), KeyboardButton(text="👤 UNKNOWN")],
        [KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_admin_menu():
    """Admin management submenu"""
    keyboard = [
        [KeyboardButton(text="➕ NEW ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN")],
        [KeyboardButton(text="🔐 PERMISSIONS"), KeyboardButton(text="👔 MANAGE ROLES")],
        [KeyboardButton(text="🔒 LOCK/UNLOCK USER"), KeyboardButton(text="🚫 BAN CONFIG")],
        [KeyboardButton(text="📋 LIST ADMINS"), KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


_ADMIN_OV_PREV = "⬅️ PREV OVERVIEW"
_ADMIN_OV_NEXT = "NEXT OVERVIEW ➡️"
_ADMIN_OV_MAX_CHARS = 2800


def _get_admin_overview_keyboard(page: int, total_pages: int) -> ReplyKeyboardMarkup:
    """Admin menu + optional overview pagination controls."""
    rows = []
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(KeyboardButton(text=_ADMIN_OV_PREV))
        if page < total_pages - 1:
            nav.append(KeyboardButton(text=_ADMIN_OV_NEXT))
        if nav:
            rows.append(nav)

    rows.extend([
        [KeyboardButton(text="➕ NEW ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN")],
        [KeyboardButton(text="🔐 PERMISSIONS"), KeyboardButton(text="👔 MANAGE ROLES")],
        [KeyboardButton(text="🔒 LOCK/UNLOCK USER"), KeyboardButton(text="🚫 BAN CONFIG")],
        [KeyboardButton(text="📋 LIST ADMINS"), KeyboardButton(text="⬅️ MAIN MENU")]
    ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _build_admin_overview_pages(admins: list[dict]) -> list[str]:
    """Build character-safe admin overview pages with status/role/permissions."""
    role_icons = {
        "Owner": "👑", "Manager": "🔴", "Admin": "🟡", "Moderator": "🟢", "Support": "🔵"
    }

    entries = []
    for idx, a in enumerate(admins, start=1):
        uid = a.get("user_id")
        name = a.get("name", str(uid))
        role = a.get("role", "Admin")
        role_icon = role_icons.get(role, "👤")
        locked = bool(a.get("locked", False))
        status_line = "🔒 LOCKED (Inactive)" if locked else "🔓 UNLOCKED (Active)"

        perms = a.get("permissions", []) or []
        if "all" in perms:
            perms_text = "ALL"
        else:
            labels = [_PERM_LABELS.get(p, p) for p in perms]
            perms_text = ", ".join(labels) if labels else "None"

        if name == str(uid):
            title = f"{idx}. {status_line} — {role_icon} {role}"
            user_line = f"   👤 ID: {uid}"
        else:
            title = f"{idx}. {status_line} — {role_icon} {role}"
            user_line = f"   👤 {name} ({uid})"

        entries.append(
            f"{title}\n"
            f"{user_line}\n"
            f"   🔐 Permissions: {perms_text}\n"
        )

    pages = []
    current = ""
    for entry in entries:
        if current and (len(current) + len(entry) + 1) > _ADMIN_OV_MAX_CHARS:
            pages.append(current.rstrip())
            current = entry
        else:
            current += ("\n" + entry) if current else entry

    if current:
        pages.append(current.rstrip())
    return pages or ["_No sub-admins found._"]


async def _send_admin_overview_page(message: types.Message, state: FSMContext, page: int = 0):
    """Render one page of admin overview with clean details and pagination."""
    admins = list(col_admins.find({}).sort("added_at", -1))
    admin_count = len(admins)

    pages = _build_admin_overview_pages(admins) if admins else ["_No sub-admins found._"]
    total_pages = len(pages)
    page = max(0, min(page, total_pages - 1))

    await state.update_data(admin_overview_pages=pages, admin_overview_page=page)

    body = pages[page]
    header = (
        f"👥 ADMIN MANAGEMENT\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 Owner: MSA ({MASTER_ADMIN_ID})\n"
        f"📊 Sub-admins: {admin_count}\n"
        f"📄 Page: {page + 1}/{total_pages}\n\n"
    )

    await message.answer(
        header + body + "\n\nSelect an option:",
        reply_markup=_get_admin_overview_keyboard(page, total_pages)
    )

# ──────────────────────────────────────────────────────────────────────────────
# ROLE PERMISSION TEMPLATES — Auto-applied when using AUTO ROLES mode
# ──────────────────────────────────────────────────────────────────────────────
_ROLE_PERMISSION_TEMPLATES = {
    "Owner":     ["broadcast", "find", "traffic", "diagnosis", "shoot", "support", "backup", "terminal", "admins", "bot1"],
    "Manager":   ["broadcast", "find", "traffic", "diagnosis", "shoot", "support", "backup", "terminal", "admins", "bot1"],
    "Admin":     ["broadcast", "find", "traffic", "support", "backup"],
    "Moderator": ["support", "find", "traffic"],
    "Support":   ["support"],
}
_ROLE_DESCRIPTIONS = {
    "Owner":     ("👑", "All 10 permissions — full control"),
    "Manager":   ("🔴", "All 10 permissions — full management"),
    "Admin":     ("🟡", "Broadcast · Find · Traffic · Support · Backup"),
    "Moderator": ("🟢", "Support · Find · Traffic"),
    "Support":   ("🔵", "Support only"),
}
_PERM_LABELS = {
    'broadcast': '📢 Broadcast', 'find': '🔍 Find',
    'traffic': '📊 Traffic',    'diagnosis': '🩺 Diagnosis',
    'shoot': '📸 Shoot',        'support': '💬 Support',
    'backup': '💾 Backup',      'terminal': '🖥️ Terminal',
    'admins': '👥 Admins',      'bot1': '🤖 Bot 1',
}

def _admin_btn(admin: dict) -> str:
    """Build admin selection button label: '👤 @username (user_id)' or '👤 Name (user_id)'"""
    uid  = admin['user_id']
    name = admin.get('name', str(uid))
    # Avoid showing 'uid (uid)' when name == uid fallback
    if name == str(uid):
        return f"👤 ({uid})"
    return f"👤 {name} ({uid})"

def _parse_admin_uid(text: str) -> int:
    """Parse user_id from '👤 Name (user_id)' or '🔒 Name (user_id) — Role' button text."""
    if '(' in text and ')' in text:
        start = text.index('(') + 1
        end   = text.index(')', start)
        return int(text[start:end].strip())
    if '[' in text and ']' in text:
        return int(text.split('[')[-1].rstrip(']'))
    if ' - ' in text:
        return int(text.split(' - ')[0].strip())
    return int(text.strip())

# Bot 1 main-menu keyboard — sent to all users when bot comes back online
_BOT1_MAIN_MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 DASHBOARD")],
        [KeyboardButton(text="🔍 SEARCH CODE")],
        [KeyboardButton(text="📺 WATCH TUTORIAL")],
        [KeyboardButton(text="📖 AGENT GUIDE")],
        [KeyboardButton(text="📜 RULES")],
        [KeyboardButton(text="📞 SUPPORT")],
    ],
    resize_keyboard=True,
)

def get_bot1_settings_menu():
    """Bot 1 Settings Menu — TURN ON/OFF, Stats, Log."""
    settings = col_bot1_settings.find_one({"setting": "maintenance_mode"})
    is_maintenance = settings.get("value", False) if settings else False

    if is_maintenance:
        toggle_btn = "🟢 TURN BOT ON"
    else:
        toggle_btn = "🔴 TURN BOT OFF"

    keyboard = [
        [KeyboardButton(text=toggle_btn)],
        [KeyboardButton(text="📊 BOT STATS"), KeyboardButton(text="📜 OFFLINE LOG")],
        [KeyboardButton(text="⬅️ MAIN MENU")],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==========================================
# BROADCAST HELPER FUNCTIONS
# ==========================================

def reindex_broadcasts():
    """Re-number all broadcasts sequentially (1, 2, 3, ...) with no gaps.
    Updates both 'index' and 'broadcast_id' fields to stay consistent."""
    all_brd = list(col_broadcasts.find({}, {"_id": 1}).sort("index", 1))
    for new_idx, doc in enumerate(all_brd, start=1):
        col_broadcasts.update_one(
            {"_id": doc["_id"]},
            {"$set": {"index": new_idx, "broadcast_id": f"brd{new_idx}"}}
        )
    print(f"🔄 Reindexed {len(all_brd)} broadcasts sequentially.")

def get_next_broadcast_id():
    """Get next sequential broadcast ID (brd1, brd2, etc.) after reindex."""
    existing = list(col_broadcasts.find({}, {"broadcast_id": 1, "index": 1}).sort("index", 1))
    
    if not existing:
        return "brd1", 1
    
    next_index = len(existing) + 1
    return f"brd{next_index}", next_index

# ==========================================
# COMMAND HANDLERS
# ==========================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Start command - shows main menu (ADMIN ONLY)"""
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    username = message.from_user.username
    
    # 1. Check if user is Bot 2-banned - complete silent ignore
    if col_banned_users.find_one({"user_id": user_id, "scope": "bot2"}):
        log_action("🚫 BANNED ACCESS BLOCKED", user_id, "Bot2-banned user tried /start")
        return  # Complete silence

    # ── Password gate: master admin must authenticate once per session ──────
    if user_id == MASTER_ADMIN_ID and ADMIN_PASSWORD and user_id not in _admin_authenticated:
        await state.set_state(AdminStates.waiting_for_admin_pw_1)
        await message.answer(
            "🔐 <b>Authentication Required</b>\n\nEnter your access password:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Cancel")]],
                resize_keyboard=True,
                one_time_keyboard=True,
            ),
            parse_mode="HTML",
        )
        return
    # ────────────────────────────────────────────────────────────────────────
    
    # 2. Check if user is admin
    if await is_admin(user_id):
        # Admin access granted
        log_action("✅ ADMIN ACCESS", user_id, f"{user_name} started bot")
        menu = await get_main_menu(user_id)  # Pass user_id for permission filtering
        await message.answer(
            f"👋 Welcome to Bot 2!\n\n"
            f"Select an option from the menu below:",
            reply_markup=menu
        )
        return
    
    # 3. Unauthorized /start attempt (non-admin OR locked admin)
    admin_doc = col_admins.find_one({"user_id": user_id})
    is_locked_admin = bool(admin_doc and admin_doc.get("locked", False))
    attempt_type = "LOCKED ADMIN" if is_locked_admin else "NON-ADMIN"

    log_action("❌ UNAUTHORIZED START", user_id, f"{attempt_type} tried /start")

    # Record this attempt (used for anti-spam auto-ban)
    col_access_attempts.insert_one({
        "user_id": user_id,
        "user_name": user_name,
        "username": username,
        "attempt_type": attempt_type,
        "attempted_at": now_local(),
    })

    # Spam policy: 3+ unauthorized /start attempts in 5 minutes => auto-ban
    five_min_ago = now_local() - timedelta(minutes=5)
    recent_attempts = col_access_attempts.count_documents({
        "user_id": user_id,
        "attempted_at": {"$gte": five_min_ago}
    })

    if recent_attempts >= 3:
        ban_doc = {
            "user_id": user_id,
            "banned_by": "SYSTEM",
            "banned_at": now_local(),
            "reason": "Automated: 3+ unauthorized /start attempts in 5 minutes",
            "status": "banned",
            "scope": "bot2",
        }
        col_banned_users.update_one(
            {"user_id": user_id, "scope": "bot2"},
            {"$setOnInsert": ban_doc},
            upsert=True,
        )
        log_action("🚫 AUTO-BAN", user_id, f"Auto-banned for spam unauthorized /start ({recent_attempts}/5m)")
        await notify_owner_unauthorized_access(
            user_id, user_name, username, recent_attempts, was_banned=True, attempt_type=attempt_type
        )
    else:
        await notify_owner_unauthorized_access(
            user_id, user_name, username, recent_attempts, was_banned=False, attempt_type=attempt_type
        )

    # Silent reject — no response to user
    return


# ──────────────────────────────────────────────────────────────────────────────
# 🔐 ADMIN PASSWORD GATE (master-admin only, once per session, double confirmation)
# ──────────────────────────────────────────────────────────────────────────────

@dp.message(AdminStates.waiting_for_admin_pw_1)
async def admin_pw_first(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    # Cancel = skip auth this session (owner ID already verified by /start gate)
    if message.text and message.text.strip() == "❌ Cancel":
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
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
        await state.set_state(AdminStates.waiting_for_admin_pw_2)
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


@dp.message(AdminStates.waiting_for_admin_pw_2)
async def admin_pw_second(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    # Cancel = skip auth this session (owner ID already verified by /start gate)
    if message.text and message.text.strip() == "❌ Cancel":
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return

    try:
        await message.delete()
    except Exception:
        pass

    data = await state.get_data()
    attempts = data.get("pw_attempts_2", 0)

    if not ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return

    if message.text == ADMIN_PASSWORD and data.get("pw_first_ok"):
        _admin_authenticated.add(user_id)
        await state.clear()
        await message.answer("✅ Authentication complete.", parse_mode="HTML")
        await cmd_start(message, state)
        return

    attempts += 1
    remaining = 3 - attempts
    if remaining <= 0:
        await state.clear()
        await message.answer(
            "❌ Authentication failed. Use /start to try again.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await state.update_data(pw_attempts_2=attempts)
    await message.answer(
        f"❌ Incorrect confirmation password. <b>{remaining}</b> attempt(s) remaining.",
        parse_mode="HTML",
    )


@dp.message(Command("report"))
async def cmd_report(message: types.Message):
    """/report — On-demand full daily report (owner only)"""
    if message.from_user.id != MASTER_ADMIN_ID:
        return
    generating_msg = await message.answer("📊 Generating report...")
    try:
        report_text = await generate_daily_report()
        await generating_msg.delete()
        await message.answer(report_text, parse_mode="Markdown")
    except Exception as e:
        await generating_msg.edit_text(f"❌ Report generation failed: {str(e)[:100]}")


@dp.message(Command("health"))
async def cmd_health(message: types.Message):
    """/health — Show bot2 auto-healer health stats (owner only)"""
    if message.from_user.id != MASTER_ADMIN_ID:
        return
    uptime = now_local() - bot2_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)

    try:
        t0 = time.time()
        client.admin.command('ping')
        db_ms = (time.time() - t0) * 1000
        db_status = f"✅ Online ({db_ms:.0f}ms)"
    except Exception:
        db_status = "❌ OFFLINE"

    healed = bot2_health["auto_healed"]
    errors = bot2_health["errors_caught"]
    success_rate = (healed / errors * 100) if errors > 0 else 100.0

    await message.answer(
        f"🏥 **BOT 2 HEALTH STATUS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ **System:**\n"
        f"• Bot 2: ✅ Running\n"
        f"• Database: {db_status}\n"
        f"• Auto-Healer: ✅ Active\n"
        f"• Health Monitor: ✅ Running\n\n"
        f"⏱️ **Uptime:** {h}h {m}m\n"
        f"**Started:** {bot2_health['bot_start_time'].strftime('%b %d, %I:%M %p')}\n\n"
        f"📊 **Error Stats:**\n"
        f"• Total Caught: `{errors}`\n"
        f"• Auto-Healed: `{healed}`\n"
        f"• Success Rate: `{success_rate:.1f}%`\n"
        f"• Owner Alerts: `{bot2_health['owner_notified']}`\n"
        f"• Consecutive Fails: `{bot2_health['consecutive_failures']}`\n\n"
        f"🕐 **Last Error:** {bot2_health['last_error'].strftime('%b %d %I:%M %p') if bot2_health['last_error'] else 'None'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Health checks every hour | Reports at 8:40 AM & PM_",
        parse_mode="Markdown"
    )


# ==========================================
# MENU HANDLERS (Placeholders)
# ==========================================

@dp.message(F.text == "📢 BROADCAST")
async def broadcast_handler(message: types.Message):
    """Show broadcast management menu"""
    log_action("📢 BROADCAST MENU", message.from_user.id, "Opened broadcast management")
    await message.answer(
        "📢 **BROADCAST MANAGEMENT**\n\n"
        "Select an option:",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "⬅️ MAIN MENU")
async def back_to_main(message: types.Message, state: FSMContext):
    """Return to main menu"""
    await state.clear()
    await message.answer(
        "📋 **Main Menu**",
        reply_markup=await get_main_menu(message.from_user.id),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🤖 BOT 1 SETTINGS")
async def bot1_settings_handler(message: types.Message, state: FSMContext):
    """Show Bot 1 Settings menu — TURN ON/OFF, Stats, Log."""
    if not await has_permission(message.from_user.id, "bot1"):
        await message.answer("⛔ Access Denied: You don't have permission to manage Bot 1 settings.")
        return

    await state.clear()
    log_action("🤖 BOT 1 SETTINGS", message.from_user.id, "Opened Bot 1 settings")

    settings       = col_bot1_settings.find_one({"setting": "maintenance_mode"})
    is_maintenance = settings.get("value", False) if settings else False
    status_icon    = "🔴 OFFLINE (Maintenance)" if is_maintenance else "🟢 ONLINE"
    updated_at     = settings.get("updated_at", None) if settings else None
    updated_str    = updated_at.strftime("%b %d, %Y %I:%M %p") if updated_at else "Never"

    await message.answer(
        f"🤖 **BOT 1 SETTINGS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📡 **Current Status:** {status_icon}\n"
        f"🕐 **Last Changed:** {updated_str}\n\n"
        f"Use the buttons below to manage Bot 1:",
        reply_markup=get_bot1_settings_menu(),
        parse_mode="Markdown"
    )


@dp.message(F.text.in_({"🔴 TURN BOT OFF", "🟢 TURN BOT ON"}))
async def bot1_toggle_handler(message: types.Message, state: FSMContext):
    """Handle TURN BOT OFF / TURN BOT ON → drive broadcast method selection."""
    if not await has_permission(message.from_user.id, "bot1"):
        await message.answer("⛔ Access Denied: You don't have permission to manage Bot 1 settings.")
        return

    # Determine direction from the button text that was actually pressed
    direction = "OFF" if "OFF" in message.text else "ON"
    await state.update_data(b8_direction=direction)

    templates   = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
    action_word = "going OFFLINE" if direction == "OFF" else "coming ONLINE"

    method_kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 AUTO BROADCAST")],
        [KeyboardButton(text="📋 SELECT TEMPLATE")],
        [KeyboardButton(text="✏️ CUSTOM MESSAGE")],
        [KeyboardButton(text="❌ CANCEL")],
    ], resize_keyboard=True)

    log_action(f"🤖 BOT 1 TOGGLE ({direction})", message.from_user.id, f"Bot is {action_word}")

    await message.answer(
        f"🤖 **BOT IS {action_word}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"How would you like to notify users?\n\n"
        f"**🤖 AUTO** — Use default template instantly\n"
        f"**📋 TEMPLATES** — Pick from {len(templates)} curated professional templates\n"
        f"**✏️ CUSTOM** — Write your own message\n\n"
        f"Or **❌ CANCEL** to abort.",
        reply_markup=method_kb,
        parse_mode="Markdown"
    )
    await state.set_state(Bot1SettingsStates.choosing_method)


@dp.message(Bot1SettingsStates.choosing_method)
async def b1_method_handler(message: types.Message, state: FSMContext):
    """Handle method choice for Bot 1 on/off notification."""
    if not await has_permission(message.from_user.id, "bot1"):
        await state.clear()
        return

    text = message.text
    data = await state.get_data()
    direction = data.get("b8_direction", "OFF")

    if text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_bot1_settings_menu())
        return

    if text == "🤖 AUTO BROADCAST":
        # Use first / default template immediately
        templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
        broadcast_text = templates[0]["text"]
        await _b8_execute_toggle(message, state, direction, broadcast_text)
        return

    if text == "📋 SELECT TEMPLATE":
        templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
        kb = _build_template_kb(templates, 0, direction)
        await message.answer(
            f"📋 **SELECT TEMPLATE**\n\n"
            f"Choose a template for the {'OFFLINE' if direction=='OFF' else 'ONLINE'} broadcast:\n\n"
            f"_(Tap a template name to preview & confirm)_",
            reply_markup=kb,
            parse_mode="Markdown"
        )
        # Stay in choosing_method state so we can still cancel via keyboard
        return

    if text == "✏️ CUSTOM MESSAGE":
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True
        )
        await message.answer(
            f"✏️ **CUSTOM MESSAGE**\n\n"
            f"Type the message you want to broadcast to all users.\n\n"
            f"_This will be sent when the bot is turned {'OFF' if direction=='OFF' else 'ON'}._",
            reply_markup=cancel_kb,
            parse_mode="Markdown"
        )
        await state.set_state(Bot1SettingsStates.entering_custom)
        return

    # Unexpected input — re-offer choice silently
    await message.answer("⚠️ Please use the buttons provided.", parse_mode="Markdown")


@dp.message(Bot1SettingsStates.entering_custom)
async def b1_custom_input_handler(message: types.Message, state: FSMContext):
    """Receive custom broadcast text → show preview + confirm inline keyboard."""
    if not await has_permission(message.from_user.id, "bot1"):
        await state.clear()
        return

    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_bot1_settings_menu())
        return

    custom_text = (message.text or "").strip()
    if len(custom_text) < 10:
        await message.answer("⚠️ Message too short (minimum 10 characters). Please try again.")
        return

    data = await state.get_data()
    direction = data.get("b8_direction", "OFF")
    await state.update_data(b8_custom_text=custom_text)

    preview = custom_text[:300] + ("…" if len(custom_text) > 300 else "")
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ CONFIRM & SEND", callback_data=f"b8c_confirm:{direction}"),
        InlineKeyboardButton(text="❌ CANCEL",         callback_data="b8c_cancel"),
    ]])
    await message.answer(
        f"📋 **PREVIEW — CUSTOM MESSAGE**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{preview}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👉 Confirm to broadcast this to all users and turn bot {'OFF' if direction=='OFF' else 'ON'}.",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )


# ─── InlineKeyboard callbacks for template browsing & confirm ────────

@dp.callback_query(F.data.startswith("b8t_pg:"))
async def b8_template_page_callback(callback: types.CallbackQuery):
    """Navigate template pages."""
    _, direction, page_str = callback.data.split(":")
    page      = int(page_str)
    templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
    kb        = _build_template_kb(templates, page, direction)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("b8t_sel:"))
async def b8_template_select_callback(callback: types.CallbackQuery, state: FSMContext):
    """User selected a template — show preview + confirm."""
    _, direction, idx_str = callback.data.split(":")
    idx       = int(idx_str)
    templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
    tpl       = templates[idx]

    # Store selection in state
    await state.update_data(b8_direction=direction, b8_tpl_idx=idx)

    preview = tpl["text"][:400] + ("…" if len(tpl["text"]) > 400 else "")
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ CONFIRM & SEND", callback_data=f"b8t_conf:{direction}:{idx}"),
        InlineKeyboardButton(text="◀️ BACK",           callback_data=f"b8t_back:{direction}"),
    ]])
    await callback.message.edit_text(
        f"📋 **TEMPLATE PREVIEW**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**{tpl['title']}**\n\n"
        f"{preview}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Tap ✅ to broadcast this and turn bot {'OFF' if direction=='OFF' else 'ON'}.",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("b8t_back:"))
async def b8_template_back_callback(callback: types.CallbackQuery):
    """Go back to template page 0."""
    direction = callback.data.split(":")[1]
    templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
    kb        = _build_template_kb(templates, 0, direction)
    await callback.message.edit_text(
        f"📋 **SELECT TEMPLATE**\n\n"
        f"Choose a template for the {'OFFLINE' if direction=='OFF' else 'ONLINE'} broadcast:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("b8t_conf:"))
async def b8_template_confirm_callback(callback: types.CallbackQuery, state: FSMContext):
    """Execute broadcast + toggle after template confirmation."""
    parts     = callback.data.split(":")
    direction = parts[1]
    idx       = int(parts[2])
    templates = _OFFLINE_TEMPLATES if direction == "OFF" else _ONLINE_TEMPLATES
    text      = templates[idx]["text"]

    await callback.message.edit_text("📡 Executing broadcast…")
    await callback.answer()
    await _b8_execute_toggle_from_callback(callback, state, direction, text)


@dp.callback_query(F.data.startswith("b8c_confirm:"))
async def b8_custom_confirm_callback(callback: types.CallbackQuery, state: FSMContext):
    """Execute broadcast + toggle after custom message confirmation."""
    direction = callback.data.split(":")[1]
    data      = await state.get_data()
    text      = data.get("b8_custom_text", "")
    if not text:
        await callback.answer("⚠️ No message found. Please try again.", show_alert=True)
        return
    await callback.message.edit_text("📡 Executing broadcast…")
    await callback.answer()
    await _b8_execute_toggle_from_callback(callback, state, direction, text)


@dp.callback_query(F.data == "b8c_cancel")
async def b8_custom_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    """Cancel custom message confirmation."""
    await state.clear()
    await callback.message.edit_text("❌ Broadcast cancelled.")
    await callback.answer()


@dp.callback_query(F.data == "b8t_cancel")
async def b8_template_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    """Cancel template selection."""
    await state.clear()
    await callback.message.edit_text("❌ Template selection cancelled.")
    await callback.answer()


@dp.callback_query(F.data == "b8t_noop")
async def b8_template_noop_callback(callback: types.CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data.startswith("b8t_custom:"))
async def b8_template_custom_callback(callback: types.CallbackQuery, state: FSMContext):
    """Switch from template list to custom message input."""
    direction = callback.data.split(":")[1]
    await state.update_data(b8_direction=direction)
    await state.set_state(Bot1SettingsStates.entering_custom)
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await callback.message.edit_text("✏️ **Type your custom message below:**", parse_mode="Markdown")
    await callback.message.answer("✏️ Go ahead — type your broadcast message:", reply_markup=cancel_kb)
    await callback.answer()


# ─── Shared executor ──────────────────────────────────────────────────

async def _b8_execute_toggle(message: types.Message, state: FSMContext, direction: str, broadcast_text: str):
    """Toggle maintenance mode and broadcast to all users (called from reply-keyboard flow)."""
    turn_on = (direction == "OFF")  # "OFF" means turn maintenance ON

    col_bot1_settings.update_one(
        {"setting": "maintenance_mode"},
        {"$set": {"value": turn_on, "updated_at": now_local(), "updated_by": message.from_user.id}},
        upsert=True
    )
    # Save to dedicated offline log collection (never mixed with settings)
    col_offline_log.insert_one({
        "direction": direction,
        "message": broadcast_text[:200],
        "triggered_by": message.from_user.id,
        "triggered_at": now_local(),
    })

    status = "ENABLED" if turn_on else "DISABLED"
    log_action(f"🛠 MAINTENANCE {status}", message.from_user.id, f"Bot turned {'OFF' if turn_on else 'ON'}")

    all_users  = list(col_user_tracking.find({}, {"user_id": 1}))
    sent, fail = 0, 0
    progress   = await message.answer(f"📡 Broadcasting to {len(all_users)} users…")
    # turn_on=True  → maintenance ON  → hide keyboard (ReplyKeyboardRemove)
    # turn_on=False → maintenance OFF → restore keyboard (_BOT1_MAIN_MENU_KB)
    _broadcast_kb = ReplyKeyboardRemove() if turn_on else _BOT1_MAIN_MENU_KB
    for i, doc in enumerate(all_users, 1):
        uid = doc.get("user_id")
        if not uid: continue
        for _attempt in range(3):
            try:
                await bot_1.send_message(uid, broadcast_text, parse_mode="Markdown", reply_markup=_broadcast_kb)
                sent += 1
                await asyncio.sleep(0.04)  # ~25 msgs/sec — within Telegram rate limits
                break
            except TelegramRetryAfter as rafe:
                await asyncio.sleep(rafe.retry_after + 1)
                if _attempt == 2:
                    fail += 1
            except Exception:
                fail += 1
                break
        if i % 50 == 0 or i == len(all_users):
            try:
                await progress.edit_text(
                    f"📡 Broadcasting… {i}/{len(all_users)} — ✅ {sent} sent / ❌ {fail} failed"
                )
            except Exception:
                pass
    try:
        await progress.delete()
    except Exception:
        pass

    await state.clear()
    await message.answer(
        f"{'🔴 BOT OFFLINE' if turn_on else '🟢 BOT ONLINE'}\n\n"
        f"✅ Maintenance mode **{'ENABLED' if turn_on else 'DISABLED'}**.\n\n"
        f"📊 **Broadcast Result:**\n• ✅ Sent: {sent} users\n• ❌ Failed: {fail} users",
        reply_markup=get_bot1_settings_menu(),
        parse_mode="Markdown"
    )


async def _b8_execute_toggle_from_callback(callback: types.CallbackQuery, state: FSMContext, direction: str, broadcast_text: str):
    """Same as _b8_execute_toggle but starts from a callback query context."""
    turn_on = (direction == "OFF")

    col_bot1_settings.update_one(
        {"setting": "maintenance_mode"},
        {"$set": {"value": turn_on, "updated_at": now_local(), "updated_by": callback.from_user.id}},
        upsert=True
    )
    # Save to dedicated offline log collection (never mixed with settings)
    col_offline_log.insert_one({
        "direction": direction,
        "message": broadcast_text[:200],
        "triggered_by": callback.from_user.id,
        "triggered_at": now_local(),
    })

    status = "ENABLED" if turn_on else "DISABLED"
    log_action(f"🛠 MAINTENANCE {status}", callback.from_user.id, f"Bot turned {'OFF' if turn_on else 'ON'} via template")

    all_users  = list(col_user_tracking.find({}, {"user_id": 1}))
    sent, fail = 0, 0
    _broadcast_kb = ReplyKeyboardRemove() if turn_on else _BOT1_MAIN_MENU_KB
    progress = await callback.message.answer(f"📡 Broadcasting to {len(all_users)} users…")
    for i, doc in enumerate(all_users, 1):
        uid = doc.get("user_id")
        if not uid: continue
        for _attempt in range(3):
            try:
                await bot_1.send_message(uid, broadcast_text, parse_mode="Markdown", reply_markup=_broadcast_kb)
                sent += 1
                await asyncio.sleep(0.04)  # ~25 msgs/sec — within Telegram rate limits
                break
            except TelegramRetryAfter as rafe:
                await asyncio.sleep(rafe.retry_after + 1)
                if _attempt == 2:
                    fail += 1
            except Exception:
                fail += 1
                break
        if i % 50 == 0 or i == len(all_users):
            try:
                await progress.edit_text(
                    f"📡 Broadcasting… {i}/{len(all_users)} — ✅ {sent} sent / ❌ {fail} failed"
                )
            except Exception:
                pass
    try:
        await progress.delete()
    except Exception:
        pass

    await state.clear()
    await callback.message.answer(
        f"{'🔴 BOT OFFLINE' if turn_on else '🟢 BOT ONLINE'}\n\n"
        f"✅ Maintenance mode **{'ENABLED' if turn_on else 'DISABLED'}**.\n\n"
        f"📊 **Broadcast Result:**\n• ✅ Sent: {sent} users\n• ❌ Failed: {fail} users",
        reply_markup=get_bot1_settings_menu(),
        parse_mode="Markdown"
    )


# ─── BOT STATS ────────────────────────────────────────────────────────

@dp.message(F.text == "📊 BOT STATS")
async def b8_stats_handler(message: types.Message):
    """Show Bot 1 live statistics."""
    if not await has_permission(message.from_user.id, "bot1"):
        return

    # ── User counts ─────────────────────────────────────────────────
    total_tracking = col_user_tracking.count_documents({})         # Users who started the bot
    total_msa      = col_msa_ids.count_documents({"retired": {"$ne": True}})  # Active MSA members (vault)
    total_banned   = col_banned_users.count_documents({})          # Total banned
    total_suspended = col_suspended_features.count_documents({})   # Feature-suspended users

    # ── Support tickets ─────────────────────────────────────────────
    open_tickets   = col_support_tickets.count_documents({"status": "open"})
    closed_tickets = col_support_tickets.count_documents({"status": "resolved"})
    total_tickets  = open_tickets + closed_tickets

    # ── Broadcast records (bot2 stored broadcasts in MSANodeDB) ──
    total_bc       = col_broadcasts.count_documents({})

    # ── Offline events log ──────────────────────────────────────────
    total_off_events = col_offline_log.count_documents({})

    # ── Maintenance status ──────────────────────────────────────────
    settings   = col_bot1_settings.find_one({"setting": "maintenance_mode"})
    is_maint   = settings.get("value", False) if settings else False
    status_str = "🔴 Offline (Maintenance)" if is_maint else "🟢 Online"

    await message.answer(
        f"📊 **BOT 1 LIVE STATS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📡 **Status:** {status_str}\n\n"
        f"👥 **Users:**\n"
        f"• Started Bot (tracked): `{total_tracking}`\n"
        f"• Verified MSA Members: `{total_msa}`\n"
        f"• Banned: `{total_banned}`\n"
        f"• Feature Suspended: `{total_suspended}`\n\n"
        f"🎫 **Support Tickets:**\n"
        f"• Open: `{open_tickets}`\n"
        f"• Resolved: `{closed_tickets}`\n"
        f"• Total: `{total_tickets}`\n\n"
        f"📢 **Broadcast Records:** `{total_bc}`\n"
        f"📜 **Offline Log Events:** `{total_off_events}`\n\n"
        f"🕒 _Live snapshot: {now_local().strftime('%b %d, %Y %I:%M %p')}_",
        reply_markup=get_bot1_settings_menu(),
        parse_mode="Markdown"
    )


# ─── OFFLINE LOG ──────────────────────────────────────────────────────

@dp.message(F.text == "📜 OFFLINE LOG")
async def b8_offline_log_handler(message: types.Message):
    """Show history of bot on/off events."""
    if not await has_permission(message.from_user.id, "bot1"):
        return
    events = list(col_offline_log.find(
        {},
        sort=[("triggered_at", -1)],
    ).limit(10))

    if not events:
        await message.answer(
            "📜 **OFFLINE LOG**\n\n_No events recorded yet._",
            reply_markup=get_bot1_settings_menu(),
            parse_mode="Markdown"
        )
        return

    lines = ["📜 **OFFLINE LOG** _(last 10 events)_\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for e in events:
        ts  = e.get("triggered_at")
        dir_= e.get("direction", "?")
        uid = e.get("triggered_by", "?")
        ts_str = ts.strftime("%b %d  %I:%M %p") if ts else "—"
        icon = "🔴" if dir_ == "OFF" else "🟢"
        lines.append(f"{icon} **{'OFFLINE' if dir_=='OFF' else 'ONLINE'}** · {ts_str} · by `{uid}`")
    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━")

    await message.answer(
        "\n".join(lines),
        reply_markup=get_bot1_settings_menu(),
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.selecting_category)
async def process_category_selection(message: types.Message, state: FSMContext):
    """Process category selection"""
    # Check for back - return to broadcast type selection
    if message.text in ["⬅️ BACK", "/cancel_back"]:
        await state.clear()
        await message.answer(
            "📤 **SEND BROADCAST**\n\n"
            "Select broadcast type:\n\n"
            "📝 **NORMAL BROADCAST**\n"
            "   └─ Text, images, videos, voice messages\n"
            "   └─ Simple one-way communication\n\n"
            "🔗 **BROADCAST WITH BUTTONS**\n"
            "   └─ Add clickable inline buttons\n"
            "   └─ Include links and actions\n"
            "   └─ More interactive\n\n"
            "Choose your broadcast type:",
            reply_markup=get_broadcast_type_menu(),
            parse_mode="Markdown"
        )
        return

    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    category_map = {
        "📺 YT": "YT",
        "📸 IG": "IG",
        "📎 IG CC": "IGCC",
        "🔗 YTCODE": "YTCODE",
        "👥 ALL": "ALL",
        "👤 UNKNOWN": "UNKNOWN",
    }
    
    if message.text not in category_map:
        await message.answer("⚠️ Please select a valid category from the buttons.")
        return
    
    category = category_map[message.text]
    await state.update_data(category=category)
    await state.set_state(BroadcastStates.waiting_for_message)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"✅ Category: **{category}**\n\n"
        "📝 Now send me the broadcast message\n"
        "(text, photo, video, or document)",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_message)
async def process_direct_broadcast(message: types.Message, state: FSMContext):
    """Process and send broadcast immediately"""
    print(f"📝 MESSAGE RECEIVED: Type={message.content_type}, From={message.from_user.first_name}")
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        print(f"❌ User cancelled message input")
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    category = data.get("category", "ALL")
    
    print(f"📊 Processing broadcast for category: {category}")
    print(f"📝 Content type: {message.content_type}")
    
    # Get next available ID
    broadcast_id, index = get_next_broadcast_id()
    print(f"🆔 Generated broadcast ID: {broadcast_id} (index: {index})")
    
    # Prepare message data for sending
    message_text = message.text or message.caption or ""
    media_type = None
    file_id = None
    
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    elif message.video:
        media_type = "video" 
        file_id = message.video.file_id
    elif message.animation:  # Added GIF support
        media_type = "animation"
        file_id = message.animation.file_id
    elif message.document:
        media_type = "document"
        file_id = message.document.file_id
    elif message.audio:  # Added audio support
        media_type = "audio"
        file_id = message.audio.file_id
    elif message.voice:  # Added voice support
        media_type = "voice"
        file_id = message.voice.file_id
    
    # Find target users based on category
    if category == "ALL":
        # Use user_tracking as authoritative source — all users who ever started the bot,
        # locked to their permanent source. This keeps ALL count consistent with
        # per-source counts and properly reflects dead-user cleanup.
        target_users = list(col_user_tracking.find({}, {"user_id": 1}))
    else:
        target_users = list(col_user_tracking.find({"source": category}))
    
    print(f"🎯 Found {len(target_users)} target users for category '{category}'")
    
    if not target_users:
        print(f"⚠️ No users found for category: {category}")
        await message.answer(
            f"⚠️ **No users found for category: {category}**\n\n"
            "Users need to start Bot 1 before receiving broadcasts.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Send immediately
    print(f"📤 Starting broadcast delivery...")
    print(f"🆔 Broadcast ID: {broadcast_id}")
    print(f"📂 Category: {category}")
    print(f"👥 Target users: {len(target_users)}")
    print(f"🤖 Delivery method: Bot 1")

    # ── PRE-DOWNLOAD MEDIA ONCE (avoid re-downloading per user) ─────────────
    _media_bytes = None
    if media_type and file_id:
        try:
            print(f"📥 Pre-downloading {media_type} (file_id={file_id[:20]}…) from Bot 2…")
            _file_obj = await bot.get_file(file_id)
            _fd = await bot.download_file(_file_obj.file_path)
            _media_bytes = _fd.read()
            print(f"✅ Pre-download complete — {len(_media_bytes):,} bytes")
        except Exception as _dl_err:
            print(f"⚠️ Media pre-download failed: {_dl_err}  (will attempt per-user)")
    # ────────────────────────────────────────────────────────────────────────

    # ── PRE-COMPUTE CAPTION / TEXT (once, shared across all users) ───────────
    _bcast_caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else ""
    _bcast_caption_split = len(_bcast_caption) > 1024   # True = too long for caption
    _bcast_full_text = _format_broadcast_msg(message_text or "📢 MSA NODE Broadcast", is_caption=False)
    _bcast_text_chunks = _split_text(_bcast_full_text)  # list of ≤4000-char chunks; usually just 1
    # ─────────────────────────────────────────────────────────────────────────

    status_msg = await message.answer(
        f"📤 **Sending Broadcast via Bot 1...**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {len(target_users)}\n"
        f"🤖 Delivery Bot: Bot 1\n\n"
        f"⏳ Preparing to send...",
        parse_mode="Markdown"
    )

    success_count = 0
    failed_count = 0
    blocked_count = 0
    error_details = []
    sent_message_ids = {}  # Store message IDs for later deletion

    # Send to each user with progress updates
    for i, user_doc in enumerate(target_users, 1):
        # ── Safe user_id access — skip doc if field missing ─────────────────
        user_id = user_doc.get('user_id')
        if not user_id:
            failed_count += 1
            error_details.append(f"Skipped doc #{i}: missing user_id field")
            continue
        
        # Update progress every 5 users or for small batches
        if i % 5 == 0 or len(target_users) <= 10:
            try:
                await status_msg.edit_text(
                    f"📤 **Sending via Bot 1...**\n\n"
                    f"🆔 ID: `{broadcast_id}`\n"
                    f"📂 Category: {category}\n"
                    f"👥 Target Users: {len(target_users)}\n"
                    f"🤖 Via: Bot 1\n\n"
                    f"📝 Progress: {i}/{len(target_users)} users\n"
                    f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
                    parse_mode="Markdown"
                )
            except:
                pass  # Ignore edit errors during sending
        
        for _attempt in range(3):
            try:
                # CROSS-BOT MEDIA: use pre-downloaded bytes, fall back to per-user download if needed
                _bytes = _media_bytes  # pre-downloaded bytes (may be None if pre-download failed)

                if media_type == "photo" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    photo_input = BufferedInputFile(_bytes, filename="broadcast_photo.jpg")
                    if message_text and message_text.strip():
                        if _bcast_caption_split:
                            sent_msg = await bot_1.send_photo(user_id, photo_input)
                            for _chunk in _bcast_text_chunks:
                                await bot_1.send_message(user_id, _chunk)
                        else:
                            sent_msg = await bot_1.send_photo(user_id, photo_input, caption=_bcast_caption)
                    else:
                        sent_msg = await bot_1.send_photo(user_id, photo_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                elif media_type == "video" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    video_input = BufferedInputFile(_bytes, filename="broadcast_video.mp4")
                    if message_text and message_text.strip():
                        if _bcast_caption_split:
                            sent_msg = await bot_1.send_video(user_id, video_input)
                            for _chunk in _bcast_text_chunks:
                                await bot_1.send_message(user_id, _chunk)
                        else:
                            sent_msg = await bot_1.send_video(user_id, video_input, caption=_bcast_caption)
                    else:
                        sent_msg = await bot_1.send_video(user_id, video_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                elif media_type == "animation" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    anim_input = BufferedInputFile(_bytes, filename="broadcast_animation.gif")
                    if message_text and message_text.strip():
                        if _bcast_caption_split:
                            sent_msg = await bot_1.send_animation(user_id, anim_input)
                            for _chunk in _bcast_text_chunks:
                                await bot_1.send_message(user_id, _chunk)
                        else:
                            sent_msg = await bot_1.send_animation(user_id, anim_input, caption=_bcast_caption)
                    else:
                        sent_msg = await bot_1.send_animation(user_id, anim_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                elif media_type == "document" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    doc_input = BufferedInputFile(_bytes, filename="broadcast_document")
                    if message_text and message_text.strip():
                        if _bcast_caption_split:
                            sent_msg = await bot_1.send_document(user_id, doc_input)
                            for _chunk in _bcast_text_chunks:
                                await bot_1.send_message(user_id, _chunk)
                        else:
                            sent_msg = await bot_1.send_document(user_id, doc_input, caption=_bcast_caption)
                    else:
                        sent_msg = await bot_1.send_document(user_id, doc_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                elif media_type == "audio" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    audio_input = BufferedInputFile(_bytes, filename="broadcast_audio.mp3")
                    if message_text and message_text.strip():
                        if _bcast_caption_split:
                            sent_msg = await bot_1.send_audio(user_id, audio_input)
                            for _chunk in _bcast_text_chunks:
                                await bot_1.send_message(user_id, _chunk)
                        else:
                            sent_msg = await bot_1.send_audio(user_id, audio_input, caption=_bcast_caption)
                    else:
                        sent_msg = await bot_1.send_audio(user_id, audio_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                elif media_type == "voice" and file_id:
                    if not _bytes:
                        _f = await bot.get_file(file_id)
                        _fd = await bot.download_file(_f.file_path)
                        _bytes = _fd.read()
                    voice_input = BufferedInputFile(_bytes, filename="broadcast_voice.ogg")
                    sent_msg = await bot_1.send_voice(user_id, voice_input)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                else:
                    # Plain text — send in chunks if message exceeds 4096 chars
                    sent_msg = None
                    for _chunk in _bcast_text_chunks:
                        sent_msg = await bot_1.send_message(user_id, _chunk)
                    sent_message_ids[str(user_id)] = sent_msg.message_id

                success_count += 1
                # ── Rate limit: 20 msgs/sec (0.05s) — safely under Telegram's 30/sec cap ──
                await asyncio.sleep(0.05)
                # ── Batch pause: every 25 users, rest 1s to avoid burst spikes ─────────────
                if success_count % 25 == 0:
                    await asyncio.sleep(1.0)
                break  # success — exit retry loop

            except TelegramRetryAfter as rafe:
                # Telegram told us to wait — obey strictly, then retry
                wait_sec = max(rafe.retry_after, 1) + 1
                print(f"⏳ FloodWait for {user_id}: sleeping {wait_sec}s (attempt {_attempt+1}/3)")
                await asyncio.sleep(wait_sec)
                if _attempt == 2:
                    failed_count += 1
                    error_details.append(f"User {user_id}: FloodWait — all retries exhausted")
            except Exception as e:
                failed_count += 1
                error_msg = str(e)

                # Categorize error types — most specific first, no retry needed
                _em = error_msg.lower()
                if "bot was blocked" in _em or "user is deactivated" in _em:
                    blocked_count += 1  # User blocked bot or account dead — silent skip
                elif "unauthorized" in _em or "forbidden" in _em:
                    blocked_count += 1  # Never started Bot 1 — silent skip
                    error_details.append(f"User {user_id}: Never started Bot 1")
                elif "blocked" in _em:
                    blocked_count += 1
                elif "not found" in _em or "chat not found" in _em:
                    error_details.append(f"User {user_id}: Account deleted")
                elif "restricted" in _em:
                    error_details.append(f"User {user_id}: Restricted")
                else:
                    error_details.append(f"User {user_id}: {error_msg[:50]}")
                break  # don't retry non-flood errors
    
    # Final status update after all sends complete
    print(f"✅ Broadcast sending complete! Success: {success_count}, Failed: {failed_count}")
    try:
        await status_msg.edit_text(
            f"✅ **Broadcast Complete!**\n\n"
            f"🆔 ID: `{broadcast_id}`\n"
            f"📂 Category: {category}\n"
            f"👥 Target Users: {len(target_users)}\n"
            f"🤖 Via: Bot 1\n\n"
            f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
            parse_mode="Markdown"
        )
    except:
        pass
    
    # Save broadcast to database after sending
    print(f"💾 Saving broadcast to database...")
    print(f"🆔 ID: {broadcast_id}, Category: {category}, Success: {success_count}, Failed: {failed_count}")
    broadcast_data = {
        "broadcast_id": broadcast_id,
        "index": index,
        "category": category,
        "message_text": message_text,
        "message_type": "text" if message.text else "media",
        "created_by": message.from_user.id,
        "created_at": now_local(),
        "status": "sent",
        "sent_count": success_count,
        "last_sent": now_local()
    }
    
    # Add media type label if applicable — file_id NOT stored (keep DB clean, no media blobs)
    if media_type:
        broadcast_data["media_type"] = media_type
    
    # Store message IDs for later deletion (convert keys to strings for MongoDB)
    broadcast_data["message_ids"] = {str(k): v for k, v in sent_message_ids.items()}
    
    # Save to database with error handling
    try:
        result = col_broadcasts.insert_one(broadcast_data)
        print(f"✅ Broadcast saved to database successfully! DB ID: {result.inserted_id}")
    except Exception as e:
        print(f"❌ ERROR saving broadcast to database: {str(e)}")
        # Still continue to show report to user
    
    # Send completion report
    sent_time = format_datetime(now_local())
    
    # Create detailed report
    report = f"✅ **Broadcast Complete & Saved!**\n\n"
    report += f"🆔 ID: `{broadcast_id}`\n"
    report += f"📂 Category: {category}\n"
    report += f"🤖 Delivered via: **Bot 1**\n"
    report += f"🕐 Sent At: {sent_time}\n\n"
    report += f"📊 **Delivery Report:**\n"
    report += f"✅ **Success: {success_count}** users received\n"
    report += f"❌ **Failed: {failed_count}** users (blocked/inactive)\n"
    if blocked_count > 0:
        report += f"🚫 **Blocked: {blocked_count}** users blocked the bot\n"
    report += f"📈 **Total Attempted: {len(target_users)}** users\n"
    
    delivery_rate = (success_count / len(target_users) * 100) if len(target_users) > 0 else 0
    report += f"💯 **Delivery Rate: {delivery_rate:.1f}%**"
    
    # Add error details if any (max 3 examples)
    if error_details and len(error_details) <= 3:
        report += f"\n\n⚠️ **Error Details:**\n"
        for error in error_details[:3]:
            report += f"• {error}\n"
    
    try:
        await status_msg.edit_text(report, parse_mode="Markdown")
    except:
        await message.answer(report, parse_mode="Markdown")
    
    # Auto-return to broadcast menu after completion
    await asyncio.sleep(2)  # Brief pause for user to read results
    await message.answer(
        "🔄 **Returning to Broadcast Menu...**",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )
    
    await state.clear()

@dp.message(F.text == "📋 LIST BROADCASTS")
async def list_broadcasts_handler(message: types.Message, state: FSMContext):
    """List broadcasts with reply keyboard pagination"""
    reindex_broadcasts()
    await show_broadcast_list_page(message, state, page=0)
    
async def show_broadcast_list_page(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated broadcast list with reply keyboard"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "📋 **NO BROADCASTS**\n\n"
            "No broadcasts created yet.",
            parse_mode="Markdown"
        )
        return
    
    response = f"📋 **BROADCASTS (Page {page + 1})** - Total: {total}\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count — live, consistent with actual send targets (no retired/dead users)
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})  # All tracked users (live)
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 **Send ID or Index to view full message**"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="⬅️ BROADCAST MENU")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(list_page=page)
    await state.set_state(BroadcastStates.waiting_for_list_search)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_list_search)
async def process_list_search(message: types.Message, state: FSMContext):
    """Handle pagination or search broadcast by ID or index"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("list_page", 0)
        if current_page > 0:
            await show_broadcast_list_page(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("list_page", 0)
        await show_broadcast_list_page(message, state, page=current_page + 1)
        return
    
    # Check for back to menu
    if message.text in ["⬅️ BROADCAST MENU", "⬅️ MAIN MENU"]:
        await state.clear()
        if message.text == "⬅️ MAIN MENU":
            await message.answer(
                "📋 **Main Menu**",
                reply_markup=await get_main_menu(message.from_user.id),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                "📢 **Broadcast Menu**",
                reply_markup=get_broadcast_menu(),
                parse_mode="Markdown"
            )
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Try to find by ID first
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    
    # If not found, try by index
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"\u274c Broadcast `{search}` not found.\n\n"
            "Send a valid ID (brd1) or index (1).",
            parse_mode="Markdown"
        )
        return
    
    # Display full broadcast details
    response = f"\U0001f4cb **BROADCAST DETAILS**\n\n"
    response += f"\U0001f194 ID: `{broadcast['broadcast_id']}`\n"
    response += f"\U0001f4cd Index: {broadcast['index']}\n"
    response += f"\U0001f4c2 Category: {broadcast.get('category', 'ALL')}\n"
    response += f"\U0001f4dd Type: {broadcast['message_type'].title()}\n"
    response += f"\U0001f4ca Status: {broadcast['status'].title()}\n"
    response += f"\U0001f4e4 Sent: {broadcast.get('sent_count', 0)} users\n"
    response += f"\U0001f550 Created: {format_datetime(broadcast.get('created_at'))}\n"
    if broadcast.get('last_edited'):
        response += f"\U0001f4dd Last Edited: {format_datetime(broadcast.get('last_edited'))}\n"
    if broadcast.get('last_sent'):
        response += f"\U0001f4e4 Last Sent: {format_datetime(broadcast.get('last_sent'))}\n"
    # Guard message text length before appending
    _full_text = (broadcast.get('message_text') or "").strip()
    _header_len = len(response) + len("\n\U0001f4ac **Full Message:**\n")
    _text_cap = 4000 - _header_len
    if len(_full_text) > _text_cap:
        _full_text = _full_text[:max(_text_cap - 3, 0)].rsplit(" ", 1)[0] + "\u2026"
    response += f"\n\U0001f4ac **Full Message:**\n{_full_text}"
    
    await message.answer(response, parse_mode="Markdown")

    # Show media type info — actual file not retrievable (file_id not stored by design)
    if broadcast.get("media_type"):
        _m_type = broadcast["media_type"]
        _type_icons = {"photo": "📷", "video": "🎥", "animation": "🎞️", "document": "📄", "audio": "🎵", "voice": "🎙️"}
        _icon = _type_icons.get(_m_type, "📎")
        await message.answer(
            f"{_icon} **Media Type:** {_m_type.capitalize()}\n"
            f"_Media files are not stored in the database (text only policy)._",
            parse_mode="Markdown"
        )

@dp.message(F.text == "✏️ EDIT BROADCAST")
async def edit_broadcast_handler(message: types.Message, state: FSMContext):
    """Start broadcast editing - show list first"""
    await show_edit_broadcast_list(message, state, page=0)

async def show_edit_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for editing"""
    reindex_broadcasts()  # Ensure sequential, duplicate-free indexes
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to edit.",
            parse_mode="Markdown"
        )
        return
    
    response = f"✏️ **EDIT BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category — consistent with actual send targets
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})  # All tracked users (live)
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 Send **ID** (brd1) or **Index** (1) to edit"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(edit_page=page)
    await state.set_state(BroadcastStates.waiting_for_edit_id)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_edit_id)
async def process_edit_id(message: types.Message, state: FSMContext):
    """Process broadcast ID or index for editing"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("edit_page", 0)
        if current_page > 0:
            await show_edit_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("edit_page", 0)
        await show_edit_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Find broadcast by ID or index
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"❌ Broadcast `{search}` not found.\n\n"
            "Please send a valid broadcast ID or index.",
            parse_mode="Markdown"
        )
        return
    
    # Store broadcast ID in state
    await state.update_data(edit_broadcast_id=broadcast['broadcast_id'])
    await state.set_state(BroadcastStates.waiting_for_edit_content)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    created = format_datetime(broadcast.get('created_at'))
    last_edited = format_datetime(broadcast.get('last_edited'))
    
    # Guard current message text length for display
    _cur_msg = (broadcast.get('message_text') or "").strip()
    if len(_cur_msg) > 3600:
        _cur_msg = _cur_msg[:3600].rsplit(" ", 1)[0] + "\u2026 _(truncated for display)_"

    await message.answer(
        f"\u270f\ufe0f **Editing: {broadcast['broadcast_id']}**\n\n"
        f"\U0001f4c2 Category: {broadcast.get('category', 'ALL')}\n"
        f"\U0001f550 Created: {created}\n"
        f"\U0001f4dd Last Edited: {last_edited}\n\n"
        f"**Current message:**\n{_cur_msg}\n\n"
        "Send the new content for this broadcast.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_edit_content)
async def process_edit_content(message: types.Message, state: FSMContext):
    """Store new content and ask for confirmation"""
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    broadcast_id = data.get("edit_broadcast_id")
    
    # Prepare update data
    update_data = {
        "message_text": message.text or message.caption or "",
        "message_type": "text" if message.text else "media",
        "last_edited": now_local()
    }
    
    # Handle media updates
    if message.photo:
        update_data["media_type"] = "photo"
        update_data["file_id"] = message.photo[-1].file_id
    elif message.video:
        update_data["media_type"] = "video"
        update_data["file_id"] = message.video.file_id
    elif message.document:
        update_data["media_type"] = "document"
        update_data["file_id"] = message.document.file_id
    
    # Store in state for confirmation
    await state.update_data(update_data=update_data)
    await state.set_state(BroadcastStates.waiting_for_edit_confirm)
    
    # Show confirmation
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    _edit_preview_fmt = _format_broadcast_msg(update_data['message_text']) if update_data.get('message_type') == 'text' else _format_broadcast_msg(update_data['message_text'], is_caption=True)
    await message.answer(
        f"\U0001f4dd **Preview New Content (with template):**\n\n"
        f"{_preview_cap(_edit_preview_fmt)}\n\n"
        f"\u2705 Confirm to update broadcast `{broadcast_id}`?",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_edit_confirm)
async def process_edit_confirm(message: types.Message, state: FSMContext):
    """Confirm and apply broadcast edit"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer(
            "❌ Edit cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    if message.text != "✅ CONFIRM":
        await message.answer("⚠️ Please click ✅ CONFIRM or ❌ CANCEL")
        return
    
    data = await state.get_data()
    broadcast_id = data.get("edit_broadcast_id")
    update_data = data.get("update_data", {})
    
    # Get the broadcast to retrieve message_ids
    broadcast = col_broadcasts.find_one({"broadcast_id": broadcast_id})
    if not broadcast:
        await message.answer("❌ Broadcast not found!", reply_markup=get_broadcast_menu())
        await state.clear()
        return
    
    message_ids = broadcast.get("message_ids", {})
    new_text = update_data.get("message_text", "")
    message_type = update_data.get("message_type", "text")

    # For button broadcasts: reconstruct inline keyboard so buttons are preserved after edit
    has_buttons = broadcast.get("has_buttons", False)
    orig_buttons = broadcast.get("buttons", [])
    orig_reply_markup = None
    if has_buttons and orig_buttons:
        inline_btns = [[InlineKeyboardButton(text=b['text'], url=b['url'])] for b in orig_buttons]
        orig_reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_btns)

    print(f"\n📝 EDITING BROADCAST {broadcast_id}")
    print(f"📊 Updating {len(message_ids)} messages for users...")
    
    # Edit messages for all users
    edited_count = 0
    failed_count = 0
    
    # Pre-resolve cross-bot media: if admin sent NEW photo/video via bot2, download bytes once
    _new_input_media = None
    _new_file_bytes = None
    _new_file_name = "media"
    new_media_type = update_data.get("media_type")
    new_file_id    = update_data.get("file_id")

    if message_type == "media" and new_file_id and new_media_type:
        try:
            _file_info = await bot.get_file(new_file_id)
            _new_file_bytes = await bot.download_file(_file_info.file_path)
            if hasattr(_new_file_bytes, "read"):
                _new_file_bytes = _new_file_bytes.read()
            _new_file_name = "photo.jpg" if new_media_type == "photo" else (
                "video.mp4" if new_media_type == "video" else "document.bin"
            )
            print(f"📥 Pre-downloaded new {new_media_type} for broadcast edit ({len(_new_file_bytes)} bytes)")
        except Exception as dl_err:
            print(f"⚠️ Could not pre-download new media: {dl_err}")

    # Detect original message type from DB record (to know if we should edit caption vs text)
    orig_media_type = broadcast.get("media_type")  # set when originally sent

    # Pre-format once — consistent MSA NODE template + timestamp across all edited messages
    _fmt_text    = _format_broadcast_msg(new_text) if new_text else ""
    _fmt_caption = _format_broadcast_msg(new_text, is_caption=True) if new_text else ""

    for user_id, msg_id in message_ids.items():
        try:
            if message_type == "text" and not orig_media_type:
                # Pure text broadcast — edit text (preserve inline buttons for button broadcasts)
                await bot_1.edit_message_text(
                    chat_id=int(user_id),
                    message_id=msg_id,
                    text=_fmt_text,
                    reply_markup=orig_reply_markup
                )
            elif message_type == "text" and orig_media_type:
                # Original was media; admin only sent new text → update caption only
                await bot_1.edit_message_caption(
                    chat_id=int(user_id),
                    message_id=msg_id,
                    caption=_fmt_caption,
                    reply_markup=orig_reply_markup
                )
            elif message_type == "media":
                if _new_file_bytes:
                    # Admin sent new media → cross-bot safe: use BufferedInputFile
                    from aiogram.types import BufferedInputFile, InputMediaPhoto, InputMediaVideo, InputMediaDocument
                    buf = BufferedInputFile(_new_file_bytes, filename=_new_file_name)
                    if new_media_type == "photo":
                        new_media = InputMediaPhoto(media=buf, caption=_fmt_caption)
                    elif new_media_type == "video":
                        new_media = InputMediaVideo(media=buf, caption=_fmt_caption)
                    else:
                        new_media = InputMediaDocument(media=buf, caption=_fmt_caption)
                    await bot_1.edit_message_media(
                        chat_id=int(user_id),
                        message_id=msg_id,
                        media=new_media
                    )
                else:
                    # No new media file — just update caption
                    await bot_1.edit_message_caption(
                        chat_id=int(user_id),
                        message_id=msg_id,
                        caption=_fmt_caption
                    )

            edited_count += 1
            print(f"✅ Edited message for user {user_id}")
            await asyncio.sleep(0.03)  # mild rate-limit throttle

        except TelegramRetryAfter as rafe:
            await asyncio.sleep(rafe.retry_after + 1)
            try:
                if message_type == "text" and not orig_media_type:
                    await bot_1.edit_message_text(chat_id=int(user_id), message_id=msg_id, text=_fmt_text, reply_markup=orig_reply_markup)
                elif message_type == "text" and orig_media_type:
                    await bot_1.edit_message_caption(chat_id=int(user_id), message_id=msg_id, caption=_fmt_caption, reply_markup=orig_reply_markup)
                edited_count += 1
            except Exception as _re:
                failed_count += 1
                print(f"⚠️ Edit retry failed for {user_id}: {_re}")
        except Exception as e:
            failed_count += 1
            print(f"⚠️ Failed to edit message for user {user_id}: {str(e)}")
    
    # Apply update to database — strip file_id (not stored), always refresh last_edited
    _db_update = {k: v for k, v in update_data.items() if k != "file_id"}
    col_broadcasts.update_one(
        {"broadcast_id": broadcast_id},
        {"$set": {**_db_update, "last_edited": now_local()}}
    )
    
    print(f"✅ Database updated for {broadcast_id}")
    print(f"📊 Results: {edited_count} edited, {failed_count} failed\n")
    
    await state.clear()
    await message.answer(
        f"✅ **Broadcast Updated!**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"✏️ **Messages Edited:** {edited_count}\n"
        f"⚠️ **Failed:** {failed_count}\n\n"
        f"All user messages have been updated!",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🗑️ DELETE BROADCAST")
async def delete_broadcast_handler(message: types.Message, state: FSMContext):
    """Start broadcast deletion - show list first"""
    await show_delete_broadcast_list(message, state, page=0)

async def show_delete_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for deletion"""
    reindex_broadcasts()  # Ensure sequential, duplicate-free indexes
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to delete.",
            parse_mode="Markdown"
        )
        return
    
    response = f"🗑️ **DELETE BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category — consistent with actual send targets
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})  # All tracked users (live)
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 Send **ID(s)** (brd1 or brd1,brd2) or **Index(es)** (1 or 1,2,3) to delete"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(delete_page=page)
    await state.set_state(BroadcastStates.waiting_for_delete_id)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_delete_id)
async def process_delete_broadcast(message: types.Message, state: FSMContext):
    """Parse delete request and show confirmation"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("delete_page", 0)
        if current_page > 0:
            await show_delete_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("delete_page", 0)
        await show_delete_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Parse multiple IDs or indices (comma-separated)
    items = [item.strip() for item in search.split(',')]
    
    # Find broadcasts to delete
    broadcasts_to_delete = []
    not_found = []
    
    for item in items:
        # Try to find by ID first
        broadcast = col_broadcasts.find_one({"broadcast_id": item.lower()})
        
        # If not found, try by index
        if not broadcast and item.isdigit():
            broadcast = col_broadcasts.find_one({"index": int(item)})
        
        if broadcast:
            broadcasts_to_delete.append(broadcast)
        else:
            not_found.append(item)
    
    if not broadcasts_to_delete:
        await message.answer(
            f"❌ No broadcasts found for: `{search}`\n\n"
            "Please send valid ID(s) or index(es).",
            parse_mode="Markdown"
        )
        return
    
    # Show confirmation
    response = f"⚠️ **CONFIRM DELETION**\n\n"
    response += f"🗑️ You're about to delete **{len(broadcasts_to_delete)} broadcast(s)**:\n\n"
    
    for brd in broadcasts_to_delete:
        category = brd.get('category', 'ALL')
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   🕐 {created}\n\n"
    
    if not_found:
        response += f"⚠️ Not found: {', '.join(not_found)}\n\n"
    
    response += f"❌ **This action cannot be undone!**\n\n"
    response += "✅ Confirm to proceed?"
    
    # Store broadcasts to delete in state
    await state.update_data(broadcasts_to_delete=[b['broadcast_id'] for b in broadcasts_to_delete])
    await state.set_state(BroadcastStates.waiting_for_delete_confirm)
    
    # Confirmation keyboard
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(response, parse_mode="Markdown", reply_markup=confirm_kb)

@dp.message(BroadcastStates.waiting_for_delete_confirm)
async def confirm_delete_broadcast(message: types.Message, state: FSMContext):
    """Actually delete broadcasts after confirmation"""
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Deletion cancelled. No broadcasts were deleted.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Check for confirmation
    if message.text != "✅ CONFIRM DELETE":
        await message.answer("⚠️ Please click ✅ CONFIRM DELETE or ❌ CANCEL")
        return
    
    # Get broadcasts to delete from state
    data = await state.get_data()
    broadcast_ids = data.get("broadcasts_to_delete", [])
    
    if not broadcast_ids:
        await state.clear()
        await message.answer(
            "❌ No broadcasts to delete.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Delete broadcasts and their messages
    deleted_count = 0
    deleted_messages_count = 0
    failed_message_deletes = 0
    
    print(f"🗑️ Starting deletion of {len(broadcast_ids)} broadcast(s)...")
    
    for broadcast_id in broadcast_ids:
        # First, get the broadcast to retrieve message IDs
        broadcast = col_broadcasts.find_one({"broadcast_id": broadcast_id})
        
        if broadcast:
            # Delete messages from users
            message_ids = broadcast.get("message_ids", {})
            print(f"📤 Deleting {len(message_ids)} messages for broadcast {broadcast_id}...")
            
            for user_id, message_id in message_ids.items():
                try:
                    await bot_1.delete_message(chat_id=int(user_id), message_id=message_id)
                    deleted_messages_count += 1
                    print(f"✅ Deleted message {message_id} from user {user_id}")
                    await asyncio.sleep(0.03)  # gentle rate-limit
                except Exception as e:
                    failed_message_deletes += 1
                    print(f"⚠️ Could not delete msg {message_id} for user {user_id}: {str(e)[:60]}")
                    # Continue — user may have deleted msg themselves or bot was blocked
            
            # Then delete the broadcast record from database
            result = col_broadcasts.delete_one({"broadcast_id": broadcast_id})
            if result.deleted_count > 0:
                deleted_count += 1
                print(f"✅ Deleted broadcast {broadcast_id} from database")
    
    # Always re-index so indices stay clean (1, 2, 3, ...)
    reindex_broadcasts()

    await state.clear()
    
    response = f"✅ **Deletion Complete!**\n\n"
    response += f"🗑️ **Broadcasts Deleted:** {deleted_count}\n\n"
    response += f"📨 **Messages Deleted:** {deleted_messages_count} messages removed from users\n"
    if failed_message_deletes > 0:
        response += f"⚠️ **Failed:** {failed_message_deletes} messages (already deleted by users)\n\n"
    else:
        response += "\n"
    response += "✅ Broadcasts re-indexed cleanly (1, 2, 3, ...)"
    
    await message.answer(
        response,
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# SEND BROADCAST HANDLERS
# ==========================================

@dp.message(F.text == "⬅️ BACK")
async def handle_back_button(message: types.Message, state: FSMContext):
    """Universal ⬅️ BACK handler — clears any FSM state and routes to correct menu"""
    current_state = await state.get_state()
    await state.clear()

    # Route based on which FSM was active
    if current_state is None:
        # At broadcast type-selection screen — go to broadcast menu
        await message.answer(
            "📢 **Broadcast Management**",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        BroadcastStates.selecting_category,
        BroadcastStates.waiting_for_message,
        BroadcastWithButtonsStates.selecting_category,
        BroadcastWithButtonsStates.waiting_for_message,
        BroadcastWithButtonsStates.waiting_for_button_text,
        BroadcastWithButtonsStates.waiting_for_button_url,
        BroadcastWithButtonsStates.confirming_buttons,
    ]:
        await message.answer(
            "📢 **Broadcast Management**",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        FindStates.waiting_for_search,
    ]:
        user_id = message.from_user.id
        menu = await get_main_menu(user_id)
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=menu,
            parse_mode="Markdown"
        )
    elif current_state in [
        ShootStates.waiting_for_ban_id,
        ShootStates.waiting_for_ban_confirm,
        ShootStates.waiting_for_unban_id,
        ShootStates.waiting_for_unban_confirm,
        ShootStates.waiting_for_delete_id,
        ShootStates.waiting_for_delete_confirm,
        ShootStates.waiting_for_suspend_id,
        ShootStates.selecting_suspend_features,
        ShootStates.waiting_for_unsuspend_id,
        ShootStates.waiting_for_reset_id,
        ShootStates.waiting_for_reset_confirm,
        ShootStates.waiting_for_shoot_search_id,
        ShootStates.waiting_for_temp_ban_id,
        ShootStates.selecting_temp_ban_duration,
        ShootStates.waiting_for_temp_ban_confirm,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_shoot_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        SupportStates.waiting_for_ticket_search,
        SupportStates.waiting_for_resolve_id,
        SupportStates.waiting_for_reply_id,
        SupportStates.waiting_for_reply_message,
        SupportStates.waiting_for_delete_ticket_id,
        SupportStates.waiting_for_user_search,
        SupportStates.waiting_for_priority_id,
        SupportStates.waiting_for_priority_level,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        AdminStates.waiting_for_new_admin_id,
        AdminStates.waiting_for_admin_role,
        AdminStates.waiting_for_remove_admin_id,
        AdminStates.waiting_for_remove_confirm,
        AdminStates.waiting_for_permission_admin_id,
        AdminStates.selecting_permissions,
        AdminStates.toggling_permissions,
        AdminStates.waiting_for_role_admin_id,
        AdminStates.selecting_role,
        AdminStates.waiting_for_lock_user_id,
        AdminStates.waiting_for_unlock_user_id,
        AdminStates.waiting_for_ban_user_id,
        AdminStates.waiting_for_admin_search,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        BroadcastStates.waiting_for_list_search,
        BroadcastStates.waiting_for_edit_id,
        BroadcastStates.waiting_for_edit_content,
        BroadcastStates.waiting_for_edit_confirm,
        BroadcastStates.waiting_for_delete_id,
        BroadcastStates.waiting_for_delete_confirm,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    else:
        # Fallback — any unknown state goes to main menu
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=await get_main_menu(message.from_user.id),
            parse_mode="Markdown"
        )

@dp.message(F.text == "📤 SEND BROADCAST")
async def select_broadcast_type(message: types.Message, state: FSMContext):
    """Show broadcast type selection menu"""
    await state.clear()
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) clicked 'SEND BROADCAST'")
    
    await message.answer(
        "📤 **SEND BROADCAST**\n\n"
        "Select broadcast type:\n\n"
        "📝 **NORMAL BROADCAST**\n"
        "   └─ Text, images, videos, voice messages\n"
        "   └─ Simple one-way communication\n\n"
        "🔗 **BROADCAST WITH BUTTONS**\n"
        "   └─ Add clickable inline buttons\n"
        "   └─ Include links and actions\n"
        "   └─ More interactive\n\n"
        "Choose your broadcast type:",
        reply_markup=get_broadcast_type_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📝 NORMAL BROADCAST")
async def direct_send_broadcast(message: types.Message, state: FSMContext):
    """Start normal broadcast - select category and send immediately"""
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) selected 'NORMAL BROADCAST'")
    print(f"🔍 Fetching user counts for all categories...")
    
    # Get live user counts for each category
    yt_count = col_user_tracking.count_documents({"source": "YT"})
    ig_count = col_user_tracking.count_documents({"source": "IG"})
    igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
    ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
    unknown_count = col_user_tracking.count_documents({"source": "UNKNOWN"})
    all_count = col_user_tracking.count_documents({})  # All tracked users (source-locked, live)
    
    print(f"📀 User counts: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, UNKNOWN={unknown_count}, ALL={all_count}")
    
    await state.set_state(BroadcastStates.selecting_category)
    await message.answer(
        "📤 **NORMAL BROADCAST**\n\n"
        "Select broadcast category:\n\n"
        f"📺 **YT** - Users from YouTube links ({yt_count} users)\n"
        f"📸 **IG** - Users from Instagram links ({ig_count} users)\n"
        f"📎 **IG CC** - Users from IG CC links ({igcc_count} users)\n"
        f"🔗 **YTCODE** - Users from YTCODE links ({ytcode_count} users)\n"
        f"👤 **UNKNOWN** - Users with no referral link ({unknown_count} users)\n"
        f"👥 **ALL** - All users ({all_count} users)\n\n"
        "Type /cancel to abort.",
        reply_markup=get_category_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔗 BROADCAST WITH BUTTONS")
async def broadcast_with_buttons_start(message: types.Message, state: FSMContext):
    """Start broadcast with buttons - select category first"""
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) selected 'BROADCAST WITH BUTTONS'")
    print(f"🔍 Fetching user counts for all categories...")
    
    # Get live user counts for each category
    yt_count = col_user_tracking.count_documents({"source": "YT"})
    ig_count = col_user_tracking.count_documents({"source": "IG"})
    igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
    ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
    unknown_count = col_user_tracking.count_documents({"source": "UNKNOWN"})
    all_count = col_user_tracking.count_documents({})  # All tracked users (source-locked, live)
    
    print(f"📀 User counts: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, UNKNOWN={unknown_count}, ALL={all_count}")
    
    await state.set_state(BroadcastWithButtonsStates.selecting_category)
    await message.answer(
        "🔗 **BROADCAST WITH BUTTONS**\n\n"
        "Select broadcast category:\n\n"
        f"📺 **YT** - Users from YouTube links ({yt_count} users)\n"
        f"📸 **IG** - Users from Instagram links ({ig_count} users)\n"
        f"📎 **IG CC** - Users from IG CC links ({igcc_count} users)\n"
        f"🔗 **YTCODE** - Users from YTCODE links ({ytcode_count} users)\n"
        f"👤 **UNKNOWN** - Users with no referral link ({unknown_count} users)\n"
        f"👥 **ALL** - All users ({all_count} users)\n\n"
        "Type /cancel to abort.",
        reply_markup=get_category_menu(),
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.selecting_category)
async def process_button_broadcast_category(message: types.Message, state: FSMContext):
    """Process category selection for button broadcast"""
    # Check for back - return to broadcast type selection
    if message.text in ["⬅️ BACK", "/cancel_back"]:
        await state.clear()
        await message.answer(
            "📤 **SEND BROADCAST**\n\n"
            "Select broadcast type:\n\n"
            "📝 **NORMAL BROADCAST**\n"
            "   └─ Text, images, videos, voice messages\n"
            "   └─ Simple one-way communication\n\n"
            "🔗 **BROADCAST WITH BUTTONS**\n"
            "   └─ Add clickable inline buttons\n"
            "   └─ Include links and actions\n"
            "   └─ More interactive\n\n"
            "Choose your broadcast type:",
            reply_markup=get_broadcast_type_menu(),
            parse_mode="Markdown"
        )
        return

    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    # Map button text to category
    category_map = {
        "📺 YT": "YT",
        "📸 IG": "IG",
        "📎 IG CC": "IGCC",
        "🔗 YTCODE": "YTCODE",
        "👥 ALL": "ALL",
        "👤 UNKNOWN": "UNKNOWN",
    }
    
    if message.text not in category_map:
        await message.answer("⚠️ Invalid category. Please select from the menu.", parse_mode="Markdown")
        return
    
    category = category_map[message.text]
    await state.update_data(category=category)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_message)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🔗 **BROADCAST WITH BUTTONS** - {category}\n\n"
        f"📝 Send your broadcast message:\n\n"
        f"Supported formats:\n"
        f"  • Text\n"
        f"  • Photos (with caption)\n"
        f"  • Videos (with caption)\n\n"
        f"Type /cancel to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_message)
async def process_button_broadcast_message(message: types.Message, state: FSMContext):
    """Process broadcast message and ask for buttons"""
    if message.text and message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    # Store message details
    data = {}
    data['message_type'] = 'text'
    
    if message.text:
        data['message_type'] = 'text'
        data['text'] = message.text
    elif message.photo:
        data['message_type'] = 'photo'
        data['file_id'] = message.photo[-1].file_id
        data['caption'] = message.caption or ""
    elif message.video:
        data['message_type'] = 'video'
        data['file_id'] = message.video.file_id
        data['caption'] = message.caption or ""
    else:
        await message.answer("⚠️ Unsupported message type. Please send text, photo, or video.", parse_mode="Markdown")
        return
    
    await state.update_data(**data, buttons=[])
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_text)
    
    await message.answer(
        "🔘 **ADD BUTTON**\n\n"
        "Enter button text (e.g., `Visit Channel`, `Join Now`, `Get Access`):\n\n"
        "Type `DONE` to finish adding buttons (minimum 1 button required).\n"
        "Type /cancel to abort.",
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_button_text)
async def process_button_text(message: types.Message, state: FSMContext):
    """Process button text input"""
    if message.text and message.text.upper() in ["DONE", "❌ CANCEL", "/CANCEL"]:
        data = await state.get_data()
        buttons = data.get('buttons', [])
        
        if message.text.upper() in ["❌ CANCEL", "/CANCEL"]:
            await state.clear()
            await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
            return
        
        if len(buttons) == 0:
            await message.answer("⚠️ Please add at least one button first.", parse_mode="Markdown")
            return
        
        # Show preview and confirm
        await show_button_broadcast_preview(message, state)
        return
    
    button_text = message.text.strip()
    if len(button_text) > 50:
        await message.answer("⚠️ Button text too long (max 50 characters). Please try again.", parse_mode="Markdown")
        return
    
    await state.update_data(current_button_text=button_text)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_url)
    
    await message.answer(
        f"🔗 **BUTTON URL**\n\n"
        f"Button Text: `{button_text}`\n\n"
        f"Enter the URL for this button:\n"
        f"(Must start with http:// or https://)\n\n"
        f"Type /cancel to abort.",
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_button_url)
async def process_button_url(message: types.Message, state: FSMContext):
    """Process button URL input"""
    if message.text and message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    url = message.text.strip()
    if not url.startswith(('http://', 'https://')):
        await message.answer("⚠️ Invalid URL. Must start with http:// or https://", parse_mode="Markdown")
        return
    
    # Add button to list
    data = await state.get_data()
    buttons = data.get('buttons', [])
    button_text = data.get('current_button_text')
    
    buttons.append({'text': button_text, 'url': url})
    await state.update_data(buttons=buttons)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_text)
    
    await message.answer(
        f"✅ **BUTTON ADDED**\n\n"
        f"Current buttons: {len(buttons)}\n\n"
        f"Add another button (enter text) or type `DONE` to finish:",
        parse_mode="Markdown"
    )

async def show_button_broadcast_preview(message: types.Message, state: FSMContext):
    """Show preview of broadcast with buttons and confirm"""
    data = await state.get_data()
    category = data.get('category')
    buttons = data.get('buttons', [])
    message_type = data.get('message_type')
    
    # Get target users count
    if category == "ALL":
        target_count = col_user_tracking.count_documents({})  # All tracked users (live)
    else:
        target_count = col_user_tracking.count_documents({"source": category})
    
    # Build preview
    preview = (
        f"📋 **BROADCAST PREVIEW**\n\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {target_count}\n"
        f"📝 Message Type: {message_type.capitalize()}\n"
        f"🔘 Buttons: {len(buttons)}\n\n"
        f"**Buttons:**\n"
    )
    
    for i, btn in enumerate(buttons, 1):
        preview += f"{i}. {btn['text']} → {btn['url'][:30]}...\n"
    
    preview += "\n✅ Type **CONFIRM** to send or **CANCEL** to abort."
    
    confirm_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await state.set_state(BroadcastWithButtonsStates.confirming_buttons)
    await message.answer(preview, reply_markup=confirm_keyboard, parse_mode="Markdown")

@dp.message(BroadcastWithButtonsStates.confirming_buttons)
async def confirm_button_broadcast(message: types.Message, state: FSMContext):
    """Confirm and send broadcast with buttons"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM" in message.text:
        data = await state.get_data()
        category = data.get('category')
        buttons = data.get('buttons', [])
        message_type = data.get('message_type')
        
        # Get target users
        if category == "ALL":
            # Use user_tracking as authoritative source — consistent with per-source broadcasts
            target_users = list(col_user_tracking.find({}, {"user_id": 1}))
        else:
            target_users = list(col_user_tracking.find({"source": category}))
        
        if not target_users:
            await message.answer("❌ No users found in this category.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
            await state.clear()
            return
        
        # Build inline keyboard
        inline_buttons = []
        for btn in buttons:
            inline_buttons.append([InlineKeyboardButton(text=btn['text'], url=btn['url'])])
        
        reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
        
        # Send status message
        status_msg = await message.answer(
            f"⏳ **Sending broadcast...**\n\n"
            f"📂 Category: {category}\n"
            f"👥 Target: {len(target_users)} users\n"
            f"🔘 Buttons: {len(buttons)}\n\n"
            f"Please wait...",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        
        success = 0
        failed = 0

        # Pre-download media once (cross-bot: Bot2 file_id → bytes → Bot1 upload)
        photo_bytes = None
        video_bytes = None
        if message_type == 'photo' and data.get('file_id'):
            try:
                photo_file = await bot.get_file(data['file_id'])
                raw = await bot.download_file(photo_file.file_path)
                photo_bytes = raw.read()
            except Exception as dl_err:
                print(f"⚠️ Could not pre-download photo: {dl_err}")
        elif message_type == 'video' and data.get('file_id'):
            try:
                video_file = await bot.get_file(data['file_id'])
                raw = await bot.download_file(video_file.file_path)
                video_bytes = raw.read()
            except Exception as dl_err:
                print(f"⚠️ Could not pre-download video: {dl_err}")

        # Precompute caption once — split if > 1024 (send text separately to avoid truncation)
        _btn_raw_cap_text = data.get('caption') or data.get('text', '')
        _btn_caption     = _format_broadcast_msg(_btn_raw_cap_text, is_caption=True) if _btn_raw_cap_text else ""
        _btn_cap_split   = len(_btn_caption) > 1024
        _btn_full_text   = _format_broadcast_msg(_btn_raw_cap_text, is_caption=False) if _btn_raw_cap_text else ""

        # Send to all users
        sent_message_ids = {}  # Track per-user message IDs so edit/delete work later
        btn_error_details = []
        btn_blocked = 0
        for user_doc in target_users:
            # Safe user_id access
            user_id = user_doc.get('user_id')
            if not user_id:
                failed += 1
                continue
            for _attempt in range(3):
                try:
                    sent_msg = None
                    if message_type == 'text':
                        sent_msg = await bot_1.send_message(
                            user_id,
                            _format_broadcast_msg(data.get('text', '')),
                            reply_markup=reply_markup
                        )
                    elif message_type == 'photo' and photo_bytes:
                        photo_input = BufferedInputFile(photo_bytes, filename="broadcast_photo.jpg")
                        if _btn_caption:
                            if _btn_cap_split:
                                # Caption too long for Telegram: send media clean, then text + buttons
                                await bot_1.send_photo(user_id, photo_input)
                                sent_msg = await bot_1.send_message(user_id, _btn_full_text, reply_markup=reply_markup)
                            else:
                                sent_msg = await bot_1.send_photo(user_id, photo_input, caption=_btn_caption, reply_markup=reply_markup)
                        else:
                            sent_msg = await bot_1.send_photo(user_id, photo_input, reply_markup=reply_markup)
                    elif message_type == 'video' and video_bytes:
                        video_input = BufferedInputFile(video_bytes, filename="broadcast_video.mp4")
                        if _btn_caption:
                            if _btn_cap_split:
                                await bot_1.send_video(user_id, video_input)
                                sent_msg = await bot_1.send_message(user_id, _btn_full_text, reply_markup=reply_markup)
                            else:
                                sent_msg = await bot_1.send_video(user_id, video_input, caption=_btn_caption, reply_markup=reply_markup)
                        else:
                            sent_msg = await bot_1.send_video(user_id, video_input, reply_markup=reply_markup)
                    else:
                        # fallback: pure text
                        sent_msg = await bot_1.send_message(
                            user_id,
                            _format_broadcast_msg(data.get('text', data.get('caption', '📢 MSA NODE Broadcast'))),
                            reply_markup=reply_markup
                        )

                    if sent_msg:
                        sent_message_ids[str(user_id)] = sent_msg.message_id
                    success += 1
                    await asyncio.sleep(0.04)  # ~25 msgs/sec
                    break  # success — exit retry loop

                except TelegramRetryAfter as rafe:
                    print(f"⏳ Flood wait for {user_id}: {rafe.retry_after}s (attempt {_attempt+1}/3)")
                    await asyncio.sleep(rafe.retry_after + 1)
                    if _attempt == 2:
                        failed += 1
                        btn_error_details.append(f"User {user_id}: Flood wait — all retries exhausted")
                except Exception as e:
                    failed += 1
                    _em = str(e).lower()
                    if "bot was blocked" in _em or "user is deactivated" in _em:
                        btn_blocked += 1
                    elif "unauthorized" in _em or "forbidden" in _em:
                        btn_blocked += 1
                        btn_error_details.append(f"User {user_id}: Unauthorized (never started Bot 1)")
                    elif "blocked" in _em:
                        btn_blocked += 1
                    elif "not found" in _em or "chat not found" in _em:
                        btn_error_details.append(f"User {user_id}: Account not found")
                    else:
                        btn_error_details.append(f"User {user_id}: {str(e)[:50]}")
                    break  # don't retry for non-flood errors

        # Save broadcast record to database
        try:
            brd_id, brd_index = get_next_broadcast_id()
            msg_text_for_db = data.get('text') or data.get('caption', '')
            brd_doc = {
                "broadcast_id": brd_id,
                "index": brd_index,
                "category": category,
                "message_text": msg_text_for_db,
                "message_type": message_type,
                "has_buttons": True,
                "buttons": buttons,
                "created_by": message.from_user.id,
                "created_at": now_local(),
                "status": "sent",
                "sent_count": success,
                "last_sent": now_local(),
                "message_ids": sent_message_ids,  # Required for edit/delete support
            }
            if message_type in ('photo', 'video', 'animation', 'document', 'audio', 'voice'):
                brd_doc["media_type"] = message_type
                # file_id NOT stored — keep DB clean, no media references
            col_broadcasts.insert_one(brd_doc)
            print(f"✅ Button broadcast saved to DB as {brd_id} with {len(sent_message_ids)} message IDs")
        except Exception as db_err:
            print(f"⚠️ Could not save button broadcast to DB: {db_err}")

        delivery_rate = (success / len(target_users) * 100) if len(target_users) > 0 else 0
        btn_report = (
            f"✅ **BROADCAST COMPLETE & SAVED**\n\n"
            f"📂 Category: {category}\n"
            f"🔘 Buttons: {len(buttons)}\n\n"
            f"📊 **Delivery Report:**\n"
            f"✅ Success: **{success}** users\n"
            f"❌ Failed: **{failed}** users\n"
            f"💯 Delivery Rate: **{delivery_rate:.1f}%**"
        )
        if btn_blocked > 0:
            btn_report += f"\n🚫 Blocked/Unauthorized: **{btn_blocked}** users"
        if btn_error_details:
            btn_report += "\n\n⚠️ **Sample Errors:**\n"
            for err in btn_error_details[:3]:
                btn_report += f"• {err}\n"

        try:
            await status_msg.edit_text(btn_report, parse_mode="Markdown")
        except Exception:
            await message.answer(btn_report, parse_mode="Markdown")

        await state.clear()
        await message.answer(
            "🔄 **Returning to Broadcast Menu...**",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        print(f"✅ Button broadcast sent to {success} users")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM** or **❌ CANCEL**", parse_mode="Markdown")

async def show_send_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for sending"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to send.",
            parse_mode="Markdown"
        )
        return
    
    response = f"📤 **SEND BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count — live, consistent with actual send targets (no retired/dead users)
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})  # All tracked users (live)
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        last_sent = format_datetime(brd.get('last_sent'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n"
        if brd.get('last_sent'):
            response += f"   📤 Last Sent: {last_sent}\n"
        response += "\n"
    
    response += "💡 Send **ID** (brd1) or **Index** (1) to send"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(send_page=page)
    # await state.set_state(BroadcastStates.waiting_for_send_id)  # DISABLED - old workflow
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

async def process_send_broadcast(message: types.Message, state: FSMContext):
    """Send broadcast to filtered users"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("send_page", 0)
        if current_page > 0:
            await show_send_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("send_page", 0)
        await show_send_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Find broadcast by ID or index
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"❌ Broadcast `{search}` not found.\n\n"
            "Send a valid ID (brd1) or index (1).",
            parse_mode="Markdown"
        )
        return
    
    await state.clear()
    
    # Get broadcast details
    broadcast_id = broadcast['broadcast_id']
    category = broadcast.get('category', 'ALL')
    message_text = broadcast.get('message_text', '')
    media_type = broadcast.get('media_type')
    file_id = broadcast.get('file_id')
    
    # Build user filter based on category
    if category == "ALL":
        # Use user_tracking as single source of truth — consistent with per-source broadcasts
        # and properly reflects dead-user cleanup (purged records are gone from both).
        target_users = list(col_user_tracking.find({}, {"user_id": 1}))
    else:
        # Send only to users who started via specific source
        target_users = list(col_user_tracking.find({"source": category}, {"user_id": 1}))
    
    if not target_users:
        # Debug information
        total_users = col_user_tracking.count_documents({})
        category_breakdown = ""
        if total_users > 0:
            yt_count = col_user_tracking.count_documents({"source": "YT"})
            ig_count = col_user_tracking.count_documents({"source": "IG"})
            igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
            ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
            
            category_breakdown = f"\n\n📊 **Available Users:**\n"
            category_breakdown += f"📺 YT: {yt_count} users\n"
            category_breakdown += f"📸 IG: {ig_count} users\n"
            category_breakdown += f"📎 IGCC: {igcc_count} users\n"
            category_breakdown += f"🔗 YTCODE: {ytcode_count} users\n"
            category_breakdown += f"👥 Total: {total_users} users"
        
        await message.answer(
            f"⚠️ **NO USERS FOUND**\n\n"
            f"📂 Category: **{category}**\n"
            f"❌ No users available for this category.{category_breakdown}\n\n"
            f"💡 Users are tracked when they start Bot 1 via links.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Send broadcast
    status_msg = await message.answer(
        f"📤 **Sending Broadcast via Bot 1...**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {len(target_users)}\n"
        f"🤖 Delivery Bot: Bot 1\n\n"
        f"⏳ Preparing to send...",
        parse_mode="Markdown"
    )
    
    success_count = 0
    failed_count = 0
    blocked_count = 0
    error_details = []

    # Precompute caption / text once — split if > 1024 to avoid Telegram truncation (resend flow)
    _rsnd_caption   = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else ""
    _rsnd_cap_split = len(_rsnd_caption) > 1024
    _rsnd_full_text = _format_broadcast_msg(message_text or "📢 MSA NODE Broadcast", is_caption=False)

    # Send to each user with progress updates
    for i, user_doc in enumerate(target_users, 1):
        user_id = user_doc['user_id']
        
        # Update progress every 5 users or for small batches
        if i % 5 == 0 or len(target_users) <= 10:
            try:
                await status_msg.edit_text(
                    f"📤 **Sending via Bot 1...**\n\n"
                    f"🆔 ID: `{broadcast_id}`\n"
                    f"📂 Category: {category}\n"
                    f"👥 Target Users: {len(target_users)}\n"
                    f"🤖 Via: Bot 1\n\n"
                    f"📝 Progress: {i}/{len(target_users)} users\n"
                    f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
                    parse_mode="Markdown"
                )
            except:
                pass  # Ignore edit errors during sending
        
        try:
            # CROSS-BOT MEDIA FIX - Download from Bot 2 and send through Bot 1
            if media_type == "photo" and file_id:
                photo_file = await bot.get_file(file_id)
                photo_bytes = await bot.download_file(photo_file.file_path)
                photo_input = BufferedInputFile(photo_bytes, filename="broadcast_photo.jpg")
                if message_text and message_text.strip():
                    if _rsnd_cap_split:
                        await bot_1.send_photo(user_id, photo_input)
                        await bot_1.send_message(user_id, _rsnd_full_text)
                    else:
                        await bot_1.send_photo(user_id, photo_input, caption=_rsnd_caption)
                else:
                    await bot_1.send_photo(user_id, photo_input)
            elif media_type == "video" and file_id:
                video_file = await bot.get_file(file_id)
                video_bytes = await bot.download_file(video_file.file_path)
                video_input = BufferedInputFile(video_bytes, filename="broadcast_video.mp4")
                if message_text and message_text.strip():
                    if _rsnd_cap_split:
                        await bot_1.send_video(user_id, video_input)
                        await bot_1.send_message(user_id, _rsnd_full_text)
                    else:
                        await bot_1.send_video(user_id, video_input, caption=_rsnd_caption)
                else:
                    await bot_1.send_video(user_id, video_input)
            elif media_type == "animation" and file_id:
                animation_file = await bot.get_file(file_id)
                animation_bytes = await bot.download_file(animation_file.file_path)
                animation_input = BufferedInputFile(animation_bytes, filename="broadcast_animation.gif")
                if message_text and message_text.strip():
                    if _rsnd_cap_split:
                        await bot_1.send_animation(user_id, animation_input)
                        await bot_1.send_message(user_id, _rsnd_full_text)
                    else:
                        await bot_1.send_animation(user_id, animation_input, caption=_rsnd_caption)
                else:
                    await bot_1.send_animation(user_id, animation_input)
            elif media_type == "document" and file_id:
                document_file = await bot.get_file(file_id)
                document_bytes = await bot.download_file(document_file.file_path)
                document_input = BufferedInputFile(document_bytes, filename="broadcast_document")
                if message_text and message_text.strip():
                    if _rsnd_cap_split:
                        await bot_1.send_document(user_id, document_input)
                        await bot_1.send_message(user_id, _rsnd_full_text)
                    else:
                        await bot_1.send_document(user_id, document_input, caption=_rsnd_caption)
                else:
                    await bot_1.send_document(user_id, document_input)
            elif media_type == "audio" and file_id:
                audio_file = await bot.get_file(file_id)
                audio_bytes = await bot.download_file(audio_file.file_path)
                audio_input = BufferedInputFile(audio_bytes, filename="broadcast_audio.mp3")
                if message_text and message_text.strip():
                    if _rsnd_cap_split:
                        await bot_1.send_audio(user_id, audio_input)
                        await bot_1.send_message(user_id, _rsnd_full_text)
                    else:
                        await bot_1.send_audio(user_id, audio_input, caption=_rsnd_caption)
                else:
                    await bot_1.send_audio(user_id, audio_input)
            elif media_type == "voice" and file_id:
                voice_file = await bot.get_file(file_id)
                voice_bytes = await bot.download_file(voice_file.file_path)
                voice_input = BufferedInputFile(voice_bytes, filename="broadcast_voice.ogg")
                await bot_1.send_voice(user_id, voice_input)
            else:
                await bot_1.send_message(user_id, _rsnd_full_text)
            
            success_count += 1
            
            # Small delay to avoid rate limits
            if len(target_users) > 10:
                await asyncio.sleep(0.1)  # 100ms delay for large broadcasts
        except Exception as e:
            failed_count += 1
            error_msg = str(e)

            # Categorize error types — most specific first
            _em = error_msg.lower()
            if "bot was blocked" in _em or "user is deactivated" in _em:
                blocked_count += 1
            elif "unauthorized" in _em or "forbidden" in _em:
                blocked_count += 1
                error_details.append(f"User {user_id}: Unauthorized (never started Bot 1)")
            elif "blocked" in _em:
                blocked_count += 1
            elif "not found" in _em or "chat not found" in _em:
                error_details.append(f"User {user_id}: Account not found")
            elif "restricted" in _em:
                error_details.append(f"User {user_id}: Restricted")
            else:
                error_details.append(f"User {user_id}: {error_msg[:50]}")

            continue
    
    # Update broadcast sent count
    col_broadcasts.update_one(
        {"broadcast_id": broadcast_id},
        {
            "$inc": {"sent_count": success_count},
            "$set": {"status": "sent", "last_sent": now_local()}
        }
    )
    
    # Send completion report
    sent_time = format_datetime(now_local())
    
    # Create detailed report
    report = f"✅ **Broadcast Complete!**\n\n"
    report += f"🆔 ID: `{broadcast_id}`\n"
    report += f"📂 Category: {category}\n"
    report += f"🤖 Delivered via: **Bot 1**\n"
    report += f"🕐 Sent At: {sent_time}\n\n"
    report += f"📊 **Delivery Report:**\n"
    report += f"✅ **Success: {success_count}** users received\n"
    report += f"❌ **Failed: {failed_count}** users (blocked/inactive)\n"
    if blocked_count > 0:
        report += f"🚫 **Blocked: {blocked_count}** users blocked the bot\n"
    report += f"📈 **Total Attempted: {len(target_users)}** users\n"
    
    delivery_rate = (success_count / len(target_users) * 100) if len(target_users) > 0 else 0
    report += f"💯 **Delivery Rate: {delivery_rate:.1f}%**\n\n"
    
    # Add error details if any (max 3 examples)
    if error_details and len(error_details) <= 3:
        report += f"⚠️ **Error Details:**\n"
        for error in error_details[:3]:
            report += f"• {error}\n"
        report += "\n"
    elif len(error_details) > 3:
        report += f"⚠️ **Sample Errors ({len(error_details)} total):**\n"
        for error in error_details[:2]:
            report += f"• {error}\n"
        report += f"• ...and {len(error_details) - 2} more\n\n"
    
    try:
        await status_msg.edit_text(report, parse_mode="Markdown")
    except Exception:
        await message.answer(report, parse_mode="Markdown")

    # Return to broadcast menu
    await message.answer(
        "🔄 **Returning to Broadcast Menu...**",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# CANCEL HANDLERS
# ==========================================

@dp.message(Command("cancel"))
async def cancel_command_handler(message: types.Message, state: FSMContext):
    """Cancel current operation via command"""
    await state.clear()
    await message.answer(
        "❌ Operation cancelled.",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "❌ CANCEL")
async def cancel_button_handler(message: types.Message, state: FSMContext):
    """Cancel current operation via button - go back one step"""
    current_state = await state.get_state()
    await state.clear()
    
    # Determine appropriate menu based on where user was
    if current_state:
        state_str = str(current_state)
        
        # Support-related states → Return to support menu
        if "Support" in state_str:
            reply_markup = get_support_management_menu()
            menu_text = "💬 **Support Menu**"
        # Broadcast-related states → Return to broadcast menu
        elif "Broadcast" in state_str:
            reply_markup = get_broadcast_menu()
            menu_text = "📢 **Broadcast Menu**"
        else:
            # Unknown state → Main menu
            reply_markup = await get_main_menu()
            menu_text = "📋 **Main Menu**"
    else:
        # No state → Main menu
        reply_markup = await get_main_menu()
        menu_text = "📋 **Main Menu**"
    
    await message.answer(
        "❌ Operation cancelled.",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔍 FIND")
async def find_handler(message: types.Message, state: FSMContext):
    """Find user by MSA ID or User ID"""
    print(f"🔍 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) accessed FIND feature")

    await state.set_state(FindStates.waiting_for_search)

    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK")]],
        resize_keyboard=True
    )

    await message.answer(
        "🔍 **FIND USER**\n\n"
        "Enter one of the following:\n"
        "• **MSA ID** (e.g., `MSA001`)\n"
        "• **User ID** (e.g., `123456789`)\n\n"
        "I'll fetch their complete profile and activity details.\n\n"
        "Type **⬅️ BACK** to return to main menu.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )


@dp.message(FindStates.waiting_for_search)
async def process_find_search(message: types.Message, state: FSMContext):
    """Process MSA ID or User ID search — full cross-collection profile"""

    if message.text and message.text.strip() in ["⬅️ BACK", "/cancel", "❌ CANCEL"]:
        await state.clear()
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=await get_main_menu(message.from_user.id),
            parse_mode="Markdown"
        )
        return

    search_input = message.text.strip() if message.text else ""

    if not search_input:
        await message.answer(
            "⚠️ **INVALID INPUT**\n\nPlease enter a valid MSA ID or User ID.",
            parse_mode="Markdown"
        )
        return

    print(f"🔎 FIND searching for: {search_input}")
    loading_msg = await message.answer("⏳ Searching database...", parse_mode="Markdown")

    try:
        # ── Step 1: Resolve user_id + tracking doc ──────────────────────────
        tracking_doc = None
        msa_doc      = None
        search_clean = search_input.strip()

        if search_clean.upper().startswith("MSA"):
            # Search by MSA ID in both collections (MSA ID is in msa_ids AND user_tracking)
            tracking_doc = col_user_tracking.find_one({"msa_id": search_clean.upper()})
            msa_doc      = col_msa_ids.find_one({"msa_id": search_clean.upper()})
        elif search_clean.isdigit():
            uid = int(search_clean)
            tracking_doc = col_user_tracking.find_one({"user_id": uid})
            msa_doc      = col_msa_ids.find_one({"user_id": uid})
        else:
            # Try case-insensitive name search in user_tracking
            tracking_doc = col_user_tracking.find_one(
                {"first_name": {"$regex": f"^{search_clean}$", "$options": "i"}}
            )
            if tracking_doc:
                msa_doc = col_msa_ids.find_one({"user_id": tracking_doc.get("user_id")})

        # ── Step 2: If nothing found, clear and report ───────────────────────
        if not tracking_doc and not msa_doc:
            await loading_msg.delete()
            hint = "MSA ID" if search_clean.upper().startswith("MSA") else \
                   "User ID" if search_clean.isdigit() else "name"
            await message.answer(
                f"❌ **NOT FOUND**\n\n"
                f"No user found matching {hint}: `{search_clean}`\n\n"
                f"• For MSA ID use format `MSA001`\n"
                f"• For User ID enter numeric ID only\n"
                f"• Make sure user has started Bot 1",
                parse_mode="Markdown"
            )
            return

        # ── Step 3: Merge data from both docs ────────────────────────────────
        # Prefer tracking_doc for activity fields, msa_doc for allocation fields
        primary = tracking_doc or msa_doc
        user_id   = primary.get("user_id")
        msa_id    = (tracking_doc or {}).get("msa_id") or (msa_doc or {}).get("msa_id", "N/A")
        first_name = (tracking_doc or {}).get("first_name") or (msa_doc or {}).get("first_name", "Unknown")
        username   = (tracking_doc or {}).get("username") or (msa_doc or {}).get("username", "N/A")
        source     = (tracking_doc or {}).get("source", "N/A")

        first_start_dt = (tracking_doc or {}).get("first_start")
        last_start_dt  = (tracking_doc or {}).get("last_start")
        assigned_at_dt = (msa_doc or {}).get("assigned_at")

        # ── Step 4: Cross-collection lookups (all by user_id) ────────────────
        ban_doc    = col_banned_users.find_one({"user_id": user_id}) if user_id else None
        susp_doc   = col_suspended_features.find_one({"user_id": user_id}) if user_id else None
        susp_list  = (susp_doc or {}).get("bot1_suspended_features", [])
        ticket_total = col_support_tickets.count_documents({"user_id": user_id}) if user_id else 0
        ticket_open  = col_support_tickets.count_documents({"user_id": user_id, "status": "open"}) if user_id else 0

        # ── Step 5: Format timestamps safely ──────────────────────────────────
        def _fmt_dt(dt):
            if not dt: return "N/A"
            if hasattr(dt, 'strftime'): return dt.strftime("%b %d, %Y  %I:%M %p")
            return str(dt)

        first_start_str  = _fmt_dt(first_start_dt)
        last_start_str   = _fmt_dt(last_start_dt)
        assigned_at_str  = _fmt_dt(assigned_at_dt)

        # Time since first join
        if first_start_dt:
            diff = now_local() - first_start_dt
            d, h, m = diff.days, diff.seconds // 3600, (diff.seconds % 3600) // 60
            time_since = (f"{d}d {h}h {m}m ago" if d > 0 else
                          f"{h}h {m}m ago"       if h > 0 else
                          f"{m}m ago")
        else:
            time_since = "N/A"

        # ── Step 6: Build display strings ─────────────────────────────────────
        source_map = {
            "YT":     "📺 YouTube Link",
            "IG":     "📸 Instagram Link",
            "IGCC":   "📎 Instagram CC Link",
            "YTCODE": "🔗 YouTube Code Link",
        }
        source_display   = source_map.get(source, f"❓ Unknown ({source})")
        username_display = f"@{username}" if username not in ("N/A", "unknown", None, "") else "—"

        # Account status
        if ban_doc:
            ban_type = ban_doc.get("ban_type", "permanent")
            if ban_type == "temporary":
                acc_status = "⏰ TEMP BANNED"
            else:
                acc_status = "🔴 BANNED"
        elif susp_list:
            acc_status = "⚠️ SUSPENDED (partial)"
        else:
            acc_status = "🟢 Active"

        # ── Step 7: Build profile message ─────────────────────────────────────
        profile = (
            f"👤 **USER PROFILE**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"👤 **Name:** {_esc_md(str(first_name))}\n"
            f"📱 **Username:** {username_display}\n"
            f"🔒 **Status:** {acc_status}\n\n"

            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 **ACTIVITY**\n\n"

            f"🔗 **Entry Source:** {source_display}\n"
            f"📅 **First Joined:** {first_start_str}\n"
            f"🆔 **MSA Allocated:** {assigned_at_str}\n"
            f"⏰ **Last Active:** {last_start_str}\n"
            f"🕐 **Member Since:** {time_since}\n\n"

            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **ACCOUNT DETAILS**\n\n"

            f"🎫 **Support Tickets:** {ticket_total} total ({ticket_open} open)\n"
            f"⏸️ **Suspended Features:** {len(susp_list) if susp_list else 0}\n"
        )

        # List suspended features if any
        if susp_list:
            profile += "".join(f"   └─ {f.replace('_', ' ').title()}\n" for f in susp_list)

        # Ban details block
        if ban_doc:
            ban_at_str  = _fmt_dt(ban_doc.get("banned_at"))
            ban_by      = ban_doc.get("banned_by", "N/A")
            ban_reason  = _esc_md(str(ban_doc.get("reason", "N/A")))
            ban_type_lbl = "⏰ Temporary" if ban_doc.get("ban_type") == "temporary" else "🔴 Permanent"
            profile += (
                f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🚫 **BAN DETAILS**\n\n"
                f"   └─ Type: {ban_type_lbl}\n"
                f"   └─ Banned At: {ban_at_str}\n"
                f"   └─ Banned By: {ban_by}\n"
                f"   └─ Reason: {ban_reason}\n"
            )
            if ban_doc.get("ban_expires"):
                profile += f"   └─ Expires: {_fmt_dt(ban_doc['ban_expires'])}\n"

        # ── VERIFICATION DETAILS ────────────────────────────────────────────
        verif_doc = col_user_verification.find_one({"user_id": user_id}) if user_id else None
        if verif_doc:
            vault_joined   = verif_doc.get("vault_joined", False)
            ever_verified  = verif_doc.get("ever_verified", False)
            is_verified    = verif_doc.get("verified", False)
            first_start_v  = verif_doc.get("first_start")
            profile += (
                f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔐 **VERIFICATION**\n\n"
                f"   └─ Vault Joined: {'✅ Yes' if vault_joined else '❌ No'}\n"
                f"   └─ Currently Verified: {'✅ Yes' if is_verified else '❌ No'}\n"
                f"   └─ Ever Verified: {'✅ Yes' if ever_verified else '❌ No'}\n"
                f"   └─ First Start: {_fmt_dt(first_start_v)}\n"
            )

        # ── RECENT SUPPORT TICKETS ──────────────────────────────────────────
        recent_tickets = list(col_support_tickets.find({"user_id": user_id}).sort("created_at", -1).limit(3)) if user_id else []
        if recent_tickets:
            profile += (
                f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎫 **RECENT TICKETS** _(latest 3)_\n\n"
            )
            for tk in recent_tickets:
                tk_id     = tk.get("ticket_id", tk.get("_id", "?"))
                tk_status = tk.get("status", "?")
                tk_date   = _fmt_dt(tk.get("created_at"))
                tk_subj   = _esc_md(str(tk.get("subject") or tk.get("message") or "")[:40])
                status_icon = "🟢" if tk_status == "open" else "🔴" if tk_status == "resolved" else "⚪"
                profile += f"   {status_icon} `{tk_id}` — {tk_date}\n"
                if tk_subj:
                    profile += f"      _{tk_subj}_\n"

        # ── BROADCASTS RECEIVED ─────────────────────────────────────────────
        if user_id:
            # Count broadcasts targeting this user's source
            user_source       = source
            bc_for_user       = col_broadcasts.count_documents({"category": "ALL"}) + \
                                (col_broadcasts.count_documents({"category": user_source}) if user_source not in (None, "N/A", "") else 0)
            profile += (
                f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📢 **BROADCASTS**\n\n"
                f"   └─ Broadcasts Targeting This User: {bc_for_user}\n"
                f"   └─ Source Category: {source_display}\n"
            )

        profile += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Search another user or press ⬅️ BACK"
        )

        await loading_msg.delete()
        back_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]],
            resize_keyboard=True
        )
        await message.answer(profile, reply_markup=back_keyboard, parse_mode="Markdown")
        print(f"✅ FIND: {msa_id} (uid={user_id}) — ban={bool(ban_doc)} susp={len(susp_list)} tickets={ticket_total}")

        # State stays active — admin can search another user immediately

    except Exception as e:
        try:
            await loading_msg.delete()
        except Exception:
            pass
        await message.answer(
            f"❌ **ERROR**\n\nSearch failed: {_esc_md(str(e)[:120])}\n\nPlease try again.",
            parse_mode="Markdown"
        )
        print(f"❌ FIND search error: {e}")

def _traffic_keyboard() -> ReplyKeyboardMarkup:
    """Shared keyboard for all traffic sub-views."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔄 REFRESH TRAFFIC"), KeyboardButton(text="🏆 TOP ANALYTICS")],
            [KeyboardButton(text="🔗 CHECK LINKS"),      KeyboardButton(text="⬅️ MAIN MENU")],
        ],
        resize_keyboard=True,
    )


async def _fetch_traffic_data() -> dict:
    """
    Single source-of-truth for all traffic numbers.
    Returns a dict with all counts so every view is consistent with no duplication.

    Data integrity approach:
      - vault_members     = col_user_verification.count_documents({vault_joined: True}) — currently IN vault
      - total_msa         = col_msa_ids.count_documents({retired: {$ne: True}}) — ACTIVE (non-retired) MSA IDs
      - total_allocated   = col_msa_ids.count_documents({})  — all IDs ever issued incl. retired (for pool display)
      - total_tracking    = col_user_tracking.count_documents({}) — users who have a tracking record
      - yt/ig/igcc/ytcode/unknown = exact source counts from user_tracking (locked on first click)
      - other_source      = tracking records whose source is not one of the 5 known values
      - untracked         = active MSA members with NO entry in user_tracking at all
      - coverage_pct      = total_tracking / total_msa * 100
    """
    yt_count      = col_user_tracking.count_documents({"source": "YT"})
    ig_count      = col_user_tracking.count_documents({"source": "IG"})
    igcc_count    = col_user_tracking.count_documents({"source": "IGCC"})
    ytcode_count  = col_user_tracking.count_documents({"source": "YTCODE"})
    unknown_count = col_user_tracking.count_documents({"source": "UNKNOWN"})

    # Users in tracking with a truly unrecognised source (not one of the 5 known values)
    other_count  = col_user_tracking.count_documents(
        {"source": {"$nin": ["YT", "IG", "IGCC", "YTCODE", "UNKNOWN", None, ""]}}
    )

    # total_msa = ACTIVE members only (excludes retired/reset MSA IDs).
    # total_allocated = all IDs ever issued (active + retired) — used for the pool section.
    total_msa       = col_msa_ids.count_documents({"retired": {"$ne": True}})
    total_allocated = col_msa_ids.count_documents({})
    total_tracking = col_user_tracking.count_documents({})

    # Currently inside the vault right now (vault_joined=True in user_verification)
    vault_members  = col_user_verification.count_documents({"vault_joined": True})

    # "Untracked" = verified MSA members who have NO entry in user_tracking at all.
    # After dead-user cleanup both collections shrink together, so this stays meaningful.
    untracked_count = max(0, total_msa - total_tracking)

    known_sources   = yt_count + ig_count + igcc_count + ytcode_count + unknown_count
    # Coverage: what % of active MSA members have a tracking record.
    # Cap at 100 % — tracking can slightly exceed msa during the gap between a
    # user starting the bot and their MSA being confirmed or after cleanup lag.
    coverage_pct    = min(100.0, (total_tracking / total_msa * 100)) if total_msa > 0 else 0.0
    # pct_base: denominator for per-source % breakdown — use total_tracking so all
    # five source percentages always add up to ≤ 100 % regardless of MSA count.
    pct_base        = total_tracking if total_tracking > 0 else 1

    return {
        "yt":              yt_count,
        "ig":              ig_count,
        "igcc":            igcc_count,
        "ytcode":          ytcode_count,
        "unknown":         unknown_count,
        "other":           other_count,
        "untracked":       untracked_count,
        "total_msa":       total_msa,
        "total_allocated": total_allocated,
        "vault_members":   vault_members,
        "tracking":        total_tracking,
        "known":           known_sources,
        "coverage":        coverage_pct,
        "pct_base":        pct_base,
        "snapshot_ts":     now_local().strftime("%b %d, %Y  %I:%M:%S %p"),
    }


@dp.message(F.text == "📊 TRAFFIC")
async def traffic_handler(message: types.Message):
    """Traffic analytics — live, direct DB query, no caching."""
    if not await has_permission(message.from_user.id, "traffic"):
        return
    print(f"📊 TRAFFIC accessed by {message.from_user.first_name} ({message.from_user.id})")

    loading_msg = await message.answer("⏳ Fetching live traffic data...", parse_mode="Markdown")

    try:
        d = await _fetch_traffic_data()

        # Per-source percentages — denominator is total TRACKING records so all
        # five sources always sum to ≤ 100 % ("other" and "untracked" make up the rest).
        def _pct(n): return n / d["pct_base"] * 100

        # Bot 1 live status
        try:
            b8   = await bot_1.get_me()
            b8_status   = "🟢 Online"
            b8_username = f"@{b8.username}" if b8.username else "N/A"
            b8_name     = b8.first_name
        except Exception as be:
            b8_status   = "🔴 Offline"
            b8_username = "N/A"
            b8_name     = "Unknown"
            print(f"⚠️ Bot 1 status check failed: {be}")

        report = (
            "📊 **TRAFFIC ANALYTICS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"

            "👥 **USER SOURCE BREAKDOWN**\n"
            "Live from database — no cache\n\n"

            f"📺 **YouTube Links (YT)**\n"
            f"   └─ {d['yt']:,} users  ({_pct(d['yt']):.1f}%)\n\n"

            f"📸 **Instagram Links (IG)**\n"
            f"   └─ {d['ig']:,} users  ({_pct(d['ig']):.1f}%)\n\n"

            f"📎 **Instagram CC Links (IGCC)**\n"
            f"   └─ {d['igcc']:,} users  ({_pct(d['igcc']):.1f}%)\n\n"

            f"🔗 **YouTube Code Links (YTCODE)**\n"
            f"   └─ {d['ytcode']:,} users  ({_pct(d['ytcode']):.1f}%)\n\n"

            f"👤 **Direct Access (UNKNOWN)**\n"
            f"   └─ {d['unknown']:,} users  ({_pct(d['unknown']):.1f}%)\n"
            f"   └─ Started bot directly — no referral link used\n\n"

            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📋 **DATA INTEGRITY**\n\n"
            f"   🆔 Total Verified MSA Members : {d['total_msa']:,}  (msa\\_ids)\n"
            f"   📡 Users with Tracking Record  : {d['tracking']:,}  (user\\_tracking)\n"
            f"   📊 Tracking Coverage           : {d['coverage']:.1f}%\n\n"

            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 **BOT 1 STATUS**\n\n"
            f"   └─ Name     : {b8_name}\n"
            f"   └─ Username : {b8_username}\n"
            f"   └─ Status   : {b8_status}\n\n"

            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🆔 **MSA ID POOL**\n\n"
            f"   └─ Total Possible : 900,000,000\n"
            f"   └─ Active Members : {d['total_msa']:,}\n"
            f"   └─ Retired IDs    : {d['total_allocated'] - d['total_msa']:,}\n"
            f"   └─ Total Allocated: {d['total_allocated']:,}\n"
            f"   └─ Available      : {900_000_000 - d['total_allocated']:,}\n"
            f"   └─ Pool Used      : {(d['total_allocated'] / 900_000_000 * 100):.6f}%\n\n"

            f"🕒 **Live snapshot:** {d['snapshot_ts']}"
        )

        print(
            f"📈 TRAFFIC — YT={d['yt']} IG={d['ig']} IGCC={d['igcc']} YTCODE={d['ytcode']} "
            f"unknown={d['unknown']} other={d['other']} untracked={d['untracked']} "
            f"total_msa={d['total_msa']} vault_members={d['vault_members']} "
            f"tracking={d['tracking']} coverage={d['coverage']:.1f}%"
        )

        await loading_msg.delete()
        await message.answer(report, parse_mode="Markdown", reply_markup=_traffic_keyboard())

    except Exception as e:
        try:
            await loading_msg.edit_text(
                f"❌ **ERROR**\n\nFailed to fetch traffic data:\n{_esc_md(str(e)[:120])}",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        print(f"❌ Traffic handler error: {e}")


@dp.message(F.text == "🔄 REFRESH TRAFFIC")
async def traffic_refresh_handler(message: types.Message):
    """Refresh — re-runs the main traffic handler for a fresh live pull."""
    await traffic_handler(message)


@dp.message(F.text == "🏆 TOP ANALYTICS")
async def top_analytics_handler(message: types.Message):
    """Ranked source view using the same shared data fetch — no separate queries."""
    if not await has_permission(message.from_user.id, "traffic"):
        return

    loading = await message.answer("⏳ Generating rankings...", parse_mode="Markdown")

    try:
        d = await _fetch_traffic_data()

        sources = [
            ("📺 YT",              d["yt"]),
            ("📸 IG",              d["ig"]),
            ("📎 IGCC",            d["igcc"]),
            ("🔗 YTCODE",          d["ytcode"]),
            ("👤 Direct (UNKNOWN)", d["unknown"]),
        ]
        sources.sort(key=lambda x: x[1], reverse=True)

        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣"]
        denom  = d["pct_base"]  # Use tracking base — same denominator as traffic view

        report = (
            "🏆 **TOP TRAFFIC SOURCES — LIVE RANKINGS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        for idx, (name, cnt) in enumerate(sources):
            pct       = cnt / denom * 100
            bar_fill  = round(pct / 10)
            bar       = "█" * bar_fill + "░" * (10 - bar_fill)
            medal     = medals[idx] if idx < len(medals) else "▪️"
            report   += f"{medal} **{name}**\n   {bar}  {cnt:,} users  ({pct:.1f}%)\n\n"

        top_name, top_cnt = sources[0]
        report += (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 **Top source:** {top_name} — {top_cnt:,} users\n"
            f"🏠 **Currently in vault:** {d['vault_members']:,}\n"
            f"🆔 **Total MSA IDs issued:** {d['total_msa']:,}\n"
            f"📡 **With tracking record:** {d['tracking']:,}  ({d['coverage']:.1f}% coverage)\n"
            f"🕒 **Snapshot:** {d['snapshot_ts']}"
        )

        await loading.delete()
        await message.answer(report, parse_mode="Markdown", reply_markup=_traffic_keyboard())

    except Exception as e:
        try:
            await loading.edit_text(f"❌ **Error:** {_esc_md(str(e)[:100])}", parse_mode="Markdown")
        except Exception:
            pass
        print(f"❌ Top analytics error: {e}")


@dp.message(F.text == "🔗 CHECK LINKS")
async def check_links_handler(message: types.Message):
    """
    Deep link health check — reads real data from live collections.
    """
    if not await has_permission(message.from_user.id, "traffic"):
        return

    loading = await message.answer("⏳ Checking all links and systems...", parse_mode="Markdown")
    issues: list[str] = []

    try:
        # ── 1. Bot 1 live status ─────────────────────────────────────────────
        try:
            b8_info    = await bot_1.get_me()
            b8_ok      = True
            b8_uname   = b8_info.username or ""
            b8_display = f"@{b8_uname}" if b8_uname else "(no username)"
            b8_name    = b8_info.first_name or "Bot 1"
            base_link  = f"https://t.me/{b8_uname}" if b8_uname else ""
        except Exception as be:
            b8_ok = False
            b8_uname = b8_display = b8_name = "N/A"
            base_link = ""
            issues.append(f"Bot 1 unreachable: {str(be)[:60]}")

        # ── 2. MongoDB ping ───────────────────────────────────────────────────
        try:
            client.admin.command("ping")
            db_ok  = True
            db_str = "✅ Connected"
        except Exception as dbe:
            db_ok  = False
            db_str = f"❌ FAILED — {str(dbe)[:60]}"
            issues.append("MongoDB ping failed")

        # ── 5. Review / log channel ───────────────────────────────────────────
        log_channel_id = REVIEW_LOG_CHANNEL
        if log_channel_id:
            try:
                ch_info   = await bot_1.get_chat(log_channel_id)
                ch_status = f"✅ {_esc_md(ch_info.title)}"
            except Exception as ce:
                ch_status = f"❌ Cannot access — {_esc_md(str(ce)[:60])}"
                issues.append("Review/log channel inaccessible")
        else:
            ch_status = "⚠️ REVIEW_LOG_CHANNEL not configured"
            issues.append("REVIEW_LOG_CHANNEL not set")

        # ── 6. Build report ───────────────────────────────────────────────────
        ok_icon = "✅" if b8_ok else "❌"

        report = (
            "🔗 **LINK & SYSTEM HEALTH CHECK**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"

            "🤖 **BOT 1 STATUS**\n"
            f"   └─ Status   : {ok_icon} {'Online' if b8_ok else 'OFFLINE'}\n"
            f"   └─ Name     : {_esc_md(b8_name)}\n"
            f"   └─ Username : {b8_display}\n"
            f"   └─ Base URL : {_esc_md(base_link) if base_link else '—'}\n\n"

            "🗄️ **DATABASE**\n"
            f"   └─ MongoDB  : {db_str}\n\n"

            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📢 **REVIEW / LOG CHANNEL**\n"
            f"   └─ {ch_status}\n\n"
        )

        # ── 7. Summary ────────────────────────────────────────────────────────
        if issues:
            report += "━━━━━━━━━━━━━━━━━━━━━━\n"
            report += f"⚠️ **{len(issues)} ISSUE(S) FOUND:**\n"
            for i, iss in enumerate(issues, 1):
                report += f"   {i}. {_esc_md(iss)}\n"
            report += "\n"
        else:
            report += "━━━━━━━━━━━━━━━━━━━━━━\n"
            report += "✅ **All systems are healthy.**\n\n"

        report += f"🕒 Checked: {now_local().strftime('%b %d, %Y  %I:%M:%S %p')}"

        print(
            f"🔗 CHECK LINKS — bot1={'ok' if b8_ok else 'FAIL'} db={'ok' if db_ok else 'FAIL'} "
            f"issues={len(issues)}"
        )
        try:
            await loading.delete()
        except Exception:
            pass

        # ── Pagination: split if report exceeds Telegram's safe limit ────────
        pages = _paginate_report(report, max_len=3800)
        uid   = message.from_user.id

        if len(pages) == 1:
            # Single page — no nav needed
            await message.answer(pages[0], parse_mode="Markdown", reply_markup=_traffic_keyboard())
        else:
            # Store pages for navigation
            _chk_links_pages[uid] = pages
            nav_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀️ Prev", callback_data=f"chk_lnk_pg:{uid}:0:prev"),
                InlineKeyboardButton(text="📄 1 / " + str(len(pages)), callback_data="chk_lnk_noop"),
                InlineKeyboardButton(text="Next ▶️", callback_data=f"chk_lnk_pg:{uid}:0:next"),
            ]])
            await message.answer(
                pages[0] + f"\n\n_Page 1 of {len(pages)}_",
                parse_mode="Markdown",
                reply_markup=nav_kb
            )

    except Exception as e:
        try:
            await loading.edit_text(
                f"❌ **Check Links Error**\n\n{_esc_md(str(e)[:150])}",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        print(f"❌ CHECK LINKS error: {e}")

# ==================== CHECK LINKS PAGINATION ====================

@dp.callback_query(F.data.startswith("chk_lnk_pg:"))
async def chk_links_page_nav(callback: types.CallbackQuery):
    """Navigate CHECK LINKS report pages (◀️ Prev / Next ▶️)."""
    try:
        # Format: chk_lnk_pg:{uid}:{current_page}:{direction}
        parts    = callback.data.split(":")
        uid      = int(parts[1])
        cur_page = int(parts[2])
        direction = parts[3]   # "prev" or "next"

        pages = _chk_links_pages.get(uid)
        if not pages:
            await callback.answer("⏳ Session expired — please run 🔗 CHECK LINKS again.", show_alert=True)
            return

        total = len(pages)
        if direction == "next":
            new_page = min(cur_page + 1, total - 1)
        else:
            new_page = max(cur_page - 1, 0)

        nav_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️ Prev",  callback_data=f"chk_lnk_pg:{uid}:{new_page}:prev"),
            InlineKeyboardButton(text=f"📄 {new_page + 1} / {total}", callback_data="chk_lnk_noop"),
            InlineKeyboardButton(text="Next ▶️",  callback_data=f"chk_lnk_pg:{uid}:{new_page}:next"),
        ]])
        await callback.message.edit_text(
            pages[new_page] + f"\n\n_Page {new_page + 1} of {total}_",
            parse_mode="Markdown",
            reply_markup=nav_kb
        )
        await callback.answer()
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ chk_links_page_nav error: {e}")


@dp.callback_query(F.data == "chk_lnk_noop")
async def chk_links_noop(callback: types.CallbackQuery):
    """No-op: page indicator button in CHECK LINKS nav bar."""
    await callback.answer()

# ================================================================
# ==================== SUPPORT PAGINATION CALLBACKS ====================
@dp.callback_query(F.data.startswith("pending_page_"))
async def pending_page_navigation(callback: types.CallbackQuery):
    """Navigate through pending tickets pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer()
        await show_pending_tickets_page(callback.message, page=page)
        log_action("NAV", callback.from_user.id, f"Viewed Pending Tickets page {page}", "bot2")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ Pending page navigation error: {e}")

@dp.callback_query(F.data.startswith("all_page_"))
async def all_page_navigation(callback: types.CallbackQuery):
    """Navigate through all tickets pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer()
        await show_all_tickets_page(callback.message, page=page)
        log_action("NAV", callback.from_user.id, f"Viewed All Tickets page {page}", "bot2")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ All tickets page navigation error: {e}")

@dp.callback_query(F.data.startswith("backup_page_"))
async def backup_page_navigation(callback: types.CallbackQuery):
    """Navigate through backups pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer("Backup history pagination removed — use 📜 BACKUP HISTORY", show_alert=True)
        log_action("NAV", callback.from_user.id, f"Viewed Backups page {page}", "bot2")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ Backup page navigation error: {e}")
# ======================================================================

@dp.message(F.text == "🩺 DIAGNOSIS")
async def diagnosis_menu(message: types.Message):
    """Diagnosis menu"""
    if not await has_permission(message.from_user.id, "diagnosis"):
        await message.answer("⛔ You don't have permission to use DIAGNOSIS.", parse_mode="Markdown")
        return
    log_action("CMD", message.from_user.id, "Opened Diagnosis Menu")
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 BOT 1 DIAGNOSIS"), KeyboardButton(text="🎛️ BOT 2 DIAGNOSIS")],
            [KeyboardButton(text="⬅️ MAIN MENU")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🩺 **SYSTEM DIAGNOSIS CENTER**\n\n"
        "Advanced diagnostic tools for system health monitoring.\n"
        "Select a system to diagnose:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text == "📱 BOT 1 DIAGNOSIS")
async def bot1_diagnosis(message: types.Message):
    """Run comprehensive diagnosis on Bot 1 system"""
    if not await has_permission(message.from_user.id, "diagnosis"):
        await message.answer("⛔ You don't have permission to use DIAGNOSIS.", parse_mode="Markdown")
        return
    log_action("DIAGNOSIS", message.from_user.id, "Running Bot 1 Diagnosis", "bot1")
    
    status_msg = await message.answer(
        "🔄 **INITIALIZING BOT 1 DIAGNOSTICS**\n\n"
        "⏳ Scanning system components...\n"
        "📊 Analyzing database health...\n"
        "🔍 Checking data integrity...",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(1.2)
    
    # Initialize tracking
    issues = []
    warnings = []
    info_items = []
    total_checks = 0
    checks_passed = 0
    
    # ═══════════════════════════════════════
    # PHASE 1: DATABASE CONNECTION & LATENCY
    # ═══════════════════════════════════════
    total_checks += 1
    db_status = "Unknown"
    db_latency = 0
    
    try:
        start = time.time()
        client.admin.command('ping')
        db_latency = (time.time() - start) * 1000
        
        if db_latency < 50:
            db_status = f"✅ Excellent ({db_latency:.1f}ms)"
            checks_passed += 1
        elif db_latency < 150:
            db_status = f"⚠️ Acceptable ({db_latency:.1f}ms)"
            warnings.append(f"Database latency is elevated: {db_latency:.1f}ms (normal <50ms)")
        else:
            db_status = f"❌ Slow ({db_latency:.1f}ms)"
            issues.append(f"**Database Performance Critical:** Latency {db_latency:.1f}ms exceeds safe threshold.")
            
    except Exception as e:
        db_status = "❌ Connection Failed"
        issues.append(f"**Database Connection Error:** {str(e)[:100]}")
    
    # ═══════════════════════════════════════
    # PHASE 2: COLLECTION VERIFICATION
    # ═══════════════════════════════════════
    total_checks += 1
    collections_ok = True
    
    try:
        expected_collections = [
            "bot1_msa_ids", "bot1_user_verification", "bot1_support_tickets",
            "bot1_banned_users", "bot1_suspended_features"
        ]
        existing = db.list_collection_names()
        missing = [c for c in expected_collections if c not in existing]
        
        if missing:
            warnings.append(f"**Missing Collections:** {', '.join(missing)}")
            collections_ok = False
        else:
            checks_passed += 1
            info_items.append(f"All {len(expected_collections)} core collections present")
            
    except Exception as e:
        issues.append(f"**Collection Check Failed:** {str(e)[:80]}")
        collections_ok = False
    
    # ═══════════════════════════════════════
    # PHASE 3: USER DATA HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Exclude retired MSA IDs (from RESET USER DATA) so health check reflects real active users
        total_users = col_msa_ids.count_documents({"retired": {"$ne": True}})
        pending_vers = col_user_verification.count_documents({})
        banned_users = col_banned_users.count_documents({})
        suspended_users = col_suspended_features.count_documents({})
        
        if total_users == 0:
            warnings.append("**No Users Found:** Database appears to be empty or not initialized.")
        else:
            checks_passed += 1
            info_items.append(f"{total_users:,} registered users")
            
            # Verification queue check
            if pending_vers > 50:
                issues.append(f"**Verification Crisis:** {pending_vers} users stuck in queue! Bot may be offline.")
            elif pending_vers > 20:
                warnings.append(f"**High Verification Queue:** {pending_vers} pending. Monitor closely.")
            
            # Ban rate analysis
            if total_users > 0:
                ban_rate = (banned_users / total_users) * 100
                if ban_rate > 30:
                    issues.append(f"**Extreme Ban Rate:** {ban_rate:.1f}% ({banned_users}/{total_users}) - Possible attack or misconfiguration")
                elif ban_rate > 15:
                    warnings.append(f"**High Ban Rate:** {ban_rate:.1f}% ({banned_users}/{total_users})")
                else:
                    info_items.append(f"Ban rate: {ban_rate:.1f}%")
                    
    except Exception as e:
        issues.append(f"**User Data Check Failed:** {str(e)[:80]}")
    
    # ═══════════════════════════════════════
    # PHASE 4: SUPPORT SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        total_tickets = col_support_tickets.count_documents({})
        
        if open_tickets > 20:
            issues.append(f"**Support Overload:** {open_tickets} open tickets! Urgent admin attention required.")
        elif open_tickets > 10:
            warnings.append(f"**Support Backlog:** {open_tickets} open tickets pending review.")
        elif open_tickets > 5:
            info_items.append(f"{open_tickets} open support tickets (manageable)")
        else:
            checks_passed += 1
            info_items.append(f"Support queue healthy ({open_tickets} open)")
            
    except Exception as e:
        warnings.append(f"Support check error: {str(e)[:60]}")
    
    # ═══════════════════════════════════════
    # PHASE 6: LOG ERROR ANALYSIS
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        error_keywords = ['error', 'failed', 'exception', 'crash']
        error_logs = [
            l for l in bot1_logs 
            if any(kw in l.get('details', '').lower() for kw in error_keywords)
        ]
        
        if error_logs:
            if len(error_logs) > 5:
                issues.append(f"**High Error Rate:** {len(error_logs)} errors detected in recent logs.")
            else:
                warnings.append(f"**Recent Errors:** {len(error_logs)} error events logged.")
        else:
            checks_passed += 1
            info_items.append("No errors detected in recent logs")
            
    except Exception as e:
        info_items.append("Log analysis skipped")

    # ═══════════════════════════════════════
    # PHASE 7: DATABASE STORAGE SPACE
    # ═══════════════════════════════════════
    total_checks += 1
    db_space_line = ""
    db_bar_line   = ""

    try:
        stats      = db.command("dbStats")
        data_mb    = stats.get("dataSize",    0) / 1_048_576
        storage_mb = stats.get("storageSize", 0) / 1_048_576
        index_mb   = stats.get("indexSize",   0) / 1_048_576
        total_mb   = stats.get("totalSize",   0) / 1_048_576
        fs_total   = stats.get("fsTotalSize", 0) / 1_048_576
        fs_used    = stats.get("fsUsedSize",  0) / 1_048_576

        if fs_total > 0:
            pct    = min(fs_used / fs_total * 100, 100)
            filled = round(pct / 5)
            empty  = 20 - filled
            risk   = ("🔴 CRITICAL" if pct > 90 else
                      "🟠 HIGH"     if pct > 75 else
                      "🟡 MODERATE"  if pct > 50 else
                      "🟢 HEALTHY")
            bar    = "█" * filled + "░" * empty
            db_bar_line = (
                f"**Filesystem:** `[{bar}]` "
                f"{pct:.1f}% ({fs_used:.0f}MB / {fs_total:.0f}MB) — {risk}"
            )
            if pct > 90:
                issues.append(
                    f"**STORAGE CRITICAL:** {pct:.1f}% filesystem used "
                    f"({fs_used:.0f}/{fs_total:.0f}MB) — free space urgently needed"
                )
            elif pct > 80:
                warnings.append(f"Storage high: {pct:.1f}% used ({fs_used:.0f}/{fs_total:.0f}MB)")
            else:
                checks_passed += 1
        else:
            m0_cap = 512.0
            pct    = min(total_mb / m0_cap * 100, 100)
            filled = round(pct / 5)
            empty  = 20 - filled
            risk   = ("🔴 CRITICAL" if pct > 90 else
                      "🟠 HIGH"     if pct > 75 else
                      "🟡 MODERATE"  if pct > 50 else
                      "🟢 HEALTHY")
            bar    = "█" * filled + "░" * empty
            db_bar_line = (
                f"**DB Used:** `[{bar}]` "
                f"{pct:.1f}% of 512MB M0 cap ({total_mb:.1f}MB) — {risk}"
            )
            checks_passed += 1

        db_space_line = (
            f"📦 Data: `{data_mb:.1f}MB`  "
            f"💾 Storage: `{storage_mb:.1f}MB`  "
            f"🔖 Indexes: `{index_mb:.1f}MB`"
        )
        info_items.append(f"DB space — data:{data_mb:.1f}MB storage:{storage_mb:.1f}MB idx:{index_mb:.1f}MB")
    except Exception as space_err:
        db_space_line = ""
        db_bar_line   = ""
        info_items.append(f"DB space check skipped: {str(space_err)[:50]}")

    # ═══════════════════════════════════════
    # GENERATE COMPREHENSIVE REPORT
    # ═══════════════════════════════════════
    
    scan_time = now_local().strftime('%Y-%m-%d %H:%M:%S')
    health_percentage = int((checks_passed / total_checks) * 100) if total_checks > 0 else 0
    
    # Determine overall status
    if health_percentage >= 90:
        status_icon = "✅"
        status_text = "EXCELLENT"
    elif health_percentage >= 70:
        status_icon = "⚠️"
        status_text = "GOOD"
    elif health_percentage >= 50:
        status_icon = "⚠️"
        status_text = "DEGRADED"
    else:
        status_icon = "❌"
        status_text = "CRITICAL"
    
    report = f"📱 **BOT 1 DIAGNOSTIC REPORT**\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"🕐 **Scan Time:** {scan_time}\n"
    report += f"💾 **Database:** {db_status}\n"
    report += f"📊 **Health Score:** {checks_passed}/{total_checks} ({health_percentage}%)\n"
    report += f"🎯 **Status:** {status_icon} {status_text}\n"
    if db_space_line:
        report += f"🗄️ **Space:** {db_space_line}\n"
    if db_bar_line:
        report += f"📊 {db_bar_line}\n"
    report += "\n"
    
    # Critical issues section
    if issues:
        report += f"❌ **CRITICAL ISSUES ({len(issues)}):**\n"
        for i, issue in enumerate(issues, 1):
            report += f"{i}. {_esc_md(issue)}\n"
        report += "\n"
    
    # Warnings section
    if warnings:
        report += f"⚠️ **WARNINGS ({len(warnings)}):**\n"
        for i, warning in enumerate(warnings, 1):
            report += f"{i}. {_esc_md(warning)}\n"
        report += "\n"
    
    # System info
    if info_items:
        report += "ℹ️ **SYSTEM INFO:**\n"
        for info in info_items[:5]:  # Limit to prevent message overflow
            report += f"• {_esc_md(info)}\n"
        report += "\n"
    
    # Solutions section
    solutions = []
    for issue in issues:
        il = issue.lower()
        if "database" in il or "latency" in il:
            solutions.append("🔧 DB slow: Check MongoDB Atlas cluster load, upgrade tier, or add indexes")
        if "verification queue" in il or "stuck in queue" in il:
            solutions.append("🔧 Verification queue: Restart Bot 1, check CHANNEL_ID is correct, verify bot has admin rights in vault")
        if "ban rate" in il:
            solutions.append("🔧 High ban rate: Review recent ban reasons in SHOOT panel, check if auto-ban threshold is too low")
        if "support overload" in il:
            solutions.append("🔧 Support backlog: Go to 💬 SUPPORT → resolve tickets, or increase response team")
        if "missing collections" in il:
            solutions.append("🔧 Missing collections will be auto-created on first write — restart Bot 1 to trigger initialization")
        if "high error rate" in il:
            solutions.append("🔧 Error logs: Check DIAGNOSIS → logs for specific error patterns, may need bot restart")
    for warn in warnings:
        wl = warn.lower()
        if "no users found" in wl:
            solutions.append("💡 No users yet — share start links (IG/YT/IGCC/YTCODE) or wait for vault joins")
        if "latency" in wl:
            solutions.append("💡 DB latency elevated — likely temporary; retry in a few minutes")
        if "support backlog" in wl:
            solutions.append("💡 Review open tickets in 💬 SUPPORT section")

    # Final verdict
    if not issues and not warnings:
        report += "✅ **ALL SYSTEMS OPERATIONAL**\n"
        report += "No issues detected. Bot 1 is healthy."
    elif issues:
        report += "🚨 **ACTION REQUIRED**\n"
        report += "Critical issues detected. Address immediately."
    else:
        report += "✅ **SYSTEM FUNCTIONAL**\n"
        report += "Minor warnings — no immediate action needed."

    if solutions:
        report += "\n\n💡 **POSSIBLE SOLUTIONS:**\n"
        for s in solutions[:5]:
            report += f"• {s}\n"

    await status_msg.edit_text(report, parse_mode="Markdown")

@dp.message(F.text == "🎛️ BOT 2 DIAGNOSIS")
async def bot2_diagnosis(message: types.Message):
    """Run comprehensive diagnosis on Bot 2 admin system"""
    if not await has_permission(message.from_user.id, "diagnosis"):
        await message.answer("⛔ You don't have permission to use DIAGNOSIS.", parse_mode="Markdown")
        return
    log_action("DIAGNOSIS", message.from_user.id, "Running Bot 2 Diagnosis", "bot2")

    status_msg = await message.answer(
        "🔄 **INITIALIZING BOT 2 DIAGNOSTICS**\n\n"
        "⏳ Scanning system components...\n"
        "📊 Analyzing admin database health...\n"
        "🔍 Checking configurations...",
        parse_mode="Markdown"
    )

    await asyncio.sleep(1.2)
    
    # Initialize tracking
    issues = []
    warnings = []
    info_items = []
    total_checks = 0
    checks_passed = 0
    
    # ═══════════════════════════════════════
    # PHASE 1: SYSTEM FILES & CONFIGURATION
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Check files that are actually expected in this deployment
        # bot2.py = the running script; credentials.json = Google Drive API key (optional)
        # .env is NOT checked — Render injects env vars directly, no file needed
        required_files = ["bot2.py"]
        optional_files = ["credentials.json"]

        missing_req = [f for f in required_files if not os.path.exists(f)]
        missing_opt = [f for f in optional_files if not os.path.exists(f)]

        if missing_req:
            issues.append(f"**Missing Core Files:** {', '.join(missing_req)}")
        else:
            checks_passed += 1
            info_items.append("Core bot file present (bot2.py)")

        if missing_opt:
            info_items.append(f"Optional not found: {', '.join(missing_opt)} (Drive backups may be unavailable)")

    except Exception as e:
        issues.append(f"**File System Check Failed:** {_esc_md(str(e)[:80])}")
    
    # ═══════════════════════════════════════
    # PHASE 2: BACKUP SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            issues.append("**Backup System Error:** Backup directory does not exist. Create it immediately!")
        else:
            backup_files = [f for f in os.listdir(backup_dir) if f.endswith(('.json', '.csv', '.txt'))]
            
            if not backup_files:
                warnings.append("**No Backups Found:** Backup directory is empty. Run first backup now.")
            else:
                # Get newest backup
                backup_files.sort(key=lambda x: os.path.getmtime(os.path.join(backup_dir, x)), reverse=True)
                newest = backup_files[0]
                newest_path = os.path.join(backup_dir, newest)
                last_backup_time = datetime.fromtimestamp(os.path.getmtime(newest_path))
                backup_age = (now_local() - last_backup_time).days
                backup_size = os.path.getsize(newest_path) / 1024  # KB
                
                if backup_age > 7:
                    issues.append(f"**Backup Crisis:** Last backup is {backup_age} days old! Critical data loss risk.")
                elif backup_age > 3:
                    warnings.append(f"**Backup Warning:** Last backup is {backup_age} days old. Backup soon.")
                else:
                    checks_passed += 1
                    info_items.append(f"Latest backup: {backup_age}d ago ({backup_size:.1f}KB)")
                
                # Check backup count
                if len(backup_files) < 3:
                    warnings.append(f"**Low Backup Count:** Only {len(backup_files)} backups exist. Increase retention.")
                else:
                    info_items.append(f"{len(backup_files)} backups stored")
                    
    except Exception as e:
        warnings.append(f"Backup check error: {_esc_md(str(e)[:60])}")
    
    # ═══════════════════════════════════════
    # PHASE 3: LOG SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        bot1_log_count = len(bot1_logs)
        bot2_log_count = len(bot2_logs)
        
        log_health = True
        
        if bot2_log_count >= MAX_LOGS:
            warnings.append(f"**Log Buffer Full:** Bot 2 buffer at capacity ({MAX_LOGS}). Active rotation.")
            log_health = False
            
        if bot1_log_count >= MAX_LOGS:
            warnings.append(f"**Log Buffer Full:** Bot 1 tracking buffer at capacity.")
            log_health = False
        
        if log_health:
            checks_passed += 1
            info_items.append(f"Logs: Bot1={bot1_log_count}, Bot2={bot2_log_count}")
            
        # Check for error patterns
        error_count_bot2 = sum(1 for l in bot2_logs if 'error' in l.get('details', '').lower())
        if error_count_bot2 > 5:
            warnings.append(f"**Admin Errors Detected:** {error_count_bot2} error events in Bot 2 logs.")
            
    except Exception as e:
        warnings.append(f"Log system check skipped: {_esc_md(str(e)[:50])}")
    
    # ═══════════════════════════════════════
    # PHASE 4: DATABASE CONNECTION
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Test MongoDB connection from admin side
        start = time.time()
        client.admin.command('ping')
        db_latency = (time.time() - start) * 1000
        
        if db_latency < 100:
            checks_passed += 1
            info_items.append(f"DB responsive ({db_latency:.1f}ms)")
        else:
            warnings.append(f"**DB Latency High:** {db_latency:.1f}ms (admin operations may be slow)")
            
    except Exception as e:
        issues.append(f"**DB Connection Error:** {_esc_md(str(e)[:80])}")
    
    # ═══════════════════════════════════════
    # PHASE 5: ENVIRONMENT & SECURITY
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Check the actual required environment variables for Bot 2
        env_vars = ['BOT_2_TOKEN', 'BOT_1_TOKEN', 'MONGO_URI', 'MASTER_ADMIN_ID', 'BACKUP_MONGO_URI']
        missing_env = []

        for var in env_vars:
            if not os.getenv(var):
                missing_env.append(var)
        
        if missing_env:
            issues.append(f"**Missing Env Variables:** {', '.join(missing_env)}")
        else:
            checks_passed += 1
            info_items.append("All environment vars configured")
            
    except Exception as e:
        warnings.append(f"Environment check skipped: {_esc_md(str(e)[:50])}")
    
    # ═══════════════════════════════════════
    # PHASE 6: DRIVE API STATUS (if using)
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        if os.path.exists('token.json'):
            with open('token.json', 'r') as f:
                token_data = json.load(f)
                if 'token' in token_data or 'access_token' in token_data:
                    checks_passed += 1
                    info_items.append("Drive API token valid")
                else:
                    warnings.append("**Drive Token Malformed:** Backup uploads may fail.")
        else:
            warnings.append("**No Drive Token:** Cloud backups unavailable.")
            
    except Exception as e:
        info_items.append("Drive check skipped")

    # ═══════════════════════════════════════
    # PHASE 7: DATABASE STORAGE SPACE
    # ═══════════════════════════════════════
    total_checks += 1
    db_space_line = ""
    db_bar_line   = ""

    try:
        stats      = db.command("dbStats")
        data_mb    = stats.get("dataSize",    0) / 1_048_576
        storage_mb = stats.get("storageSize", 0) / 1_048_576
        index_mb   = stats.get("indexSize",   0) / 1_048_576
        total_mb   = stats.get("totalSize",   0) / 1_048_576
        fs_total   = stats.get("fsTotalSize", 0) / 1_048_576
        fs_used    = stats.get("fsUsedSize",  0) / 1_048_576

        if fs_total > 0:
            pct    = min(fs_used / fs_total * 100, 100)
            filled = round(pct / 5)
            empty  = 20 - filled
            risk   = ("🔴 CRITICAL" if pct > 90 else
                      "🟠 HIGH"     if pct > 75 else
                      "🟡 MODERATE"  if pct > 50 else
                      "🟢 HEALTHY")
            bar    = "█" * filled + "░" * empty
            db_bar_line = (
                f"<b>Filesystem:</b> <code>[{bar}]</code> "
                f"{pct:.1f}% ({fs_used:.0f}MB / {fs_total:.0f}MB) — {risk}"
            )
            if pct > 90:
                issues.append(
                    f"**STORAGE CRITICAL:** {pct:.1f}% filesystem used "
                    f"({fs_used:.0f}/{fs_total:.0f}MB) — free space urgently needed"
                )
            elif pct > 80:
                warnings.append(f"Storage high: {pct:.1f}% used ({fs_used:.0f}/{fs_total:.0f}MB)")
            else:
                checks_passed += 1
        else:
            m0_cap = 512.0
            pct    = min(total_mb / m0_cap * 100, 100)
            filled = round(pct / 5)
            empty  = 20 - filled
            risk   = ("🔴 CRITICAL" if pct > 90 else
                      "🟠 HIGH"     if pct > 75 else
                      "🟡 MODERATE"  if pct > 50 else
                      "🟢 HEALTHY")
            bar    = "█" * filled + "░" * empty
            db_bar_line = (
                f"<b>DB Used:</b> <code>[{bar}]</code> "
                f"{pct:.1f}% of 512MB M0 cap ({total_mb:.1f}MB) — {risk}"
            )
            checks_passed += 1

        db_space_line = (
            f"📦 Data: <code>{data_mb:.1f}MB</code>  "
            f"💾 Storage: <code>{storage_mb:.1f}MB</code>  "
            f"🔖 Indexes: <code>{index_mb:.1f}MB</code>"
        )
        info_items.append(f"DB space — data:{data_mb:.1f}MB storage:{storage_mb:.1f}MB idx:{index_mb:.1f}MB")
    except Exception as space_err:
        db_space_line = ""
        db_bar_line   = ""
        info_items.append(f"DB space check skipped: {html.escape(str(space_err)[:50])}")

    # ═══════════════════════════════════════
    # GENERATE COMPREHENSIVE REPORT
    # ═══════════════════════════════════════
    
    scan_time = now_local().strftime('%Y-%m-%d %H:%M:%S')
    health_percentage = int((checks_passed / total_checks) * 100) if total_checks > 0 else 0
    
    # Determine overall status
    if health_percentage >= 90:
        status_icon = "✅"
        status_text = "EXCELLENT"
    elif health_percentage >= 70:
        status_icon = "⚠️"
        status_text = "GOOD"
    elif health_percentage >= 50:
        status_icon = "⚠️"
        status_text = "NEEDS ATTENTION"
    else:
        status_icon = "❌"
        status_text = "CRITICAL"
    
    report = f"🎛️ <b>BOT 2 DIAGNOSTIC REPORT</b>\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"🕐 <b>Scan Time:</b> {scan_time}\n"
    report += f"💻 <b>Version:</b> Administrator v2.1\n"
    report += f"📊 <b>Health Score:</b> {checks_passed}/{total_checks} ({health_percentage}%)\n"
    report += f"🎯 <b>Status:</b> {status_icon} {status_text}\n"
    if db_space_line:
        report += f"🗄️ <b>Space:</b> {db_space_line}\n"
    if db_bar_line:
        report += f"📊 {db_bar_line}\n"
    report += "\n"
    
    # Critical issues section
    if issues:
        report += f"❌ <b>CRITICAL ALERTS ({len(issues)}):</b>\n"
        for i, issue in enumerate(issues, 1):
            report += f"{i}. {html.escape(str(issue))}\n"
        report += "\n"
    
    # Warnings section
    if warnings:
        report += f"⚠️ <b>WARNINGS ({len(warnings)}):</b>\n"
        for i, warning in enumerate(warnings, 1):
            report += f"{i}. {html.escape(str(warning))}\n"
        report += "\n"
    
    # System info
    if info_items:
        report += "ℹ️ <b>SYSTEM STATUS:</b>\n"
        for info in info_items[:5]:
            report += f"• {html.escape(str(info))}\n"
        report += "\n"
    
    # Final verdict
    if not issues and not warnings:
        report += "✅ <b>ALL SYSTEMS OPERATIONAL</b>\n"
        report += "Bot 2 admin panel is healthy and ready."
    elif issues:
        report += "🚨 <b>IMMEDIATE ACTION REQUIRED</b>\n"
        report += "Critical issues detected. Resolve to restore full admin functionality."
    else:
        report += "✅ <b>SYSTEM FUNCTIONAL</b>\n"
        report += "Minor warnings present. Monitor but system is operational."

    # ═══════════════════════════════════════
    # AUTO SOLUTIONS
    # ═══════════════════════════════════════
    solutions = []
    combined = issues + warnings

    for item in combined:
        item_l = item.lower()
        if "mongodb" in item_l or "database" in item_l or "db" in item_l:
            solutions.append(
                "<b>DB:</b> Check MONGO_URI in Render env vars. Ensure MongoDB Atlas IP Whitelist "
                "includes 0.0.0.0/0 (or your server IP). Verify Atlas cluster is not paused."
            )
        if "broadcast" in item_l or "broadcast collection" in item_l:
            solutions.append(
                "<b>Broadcast:</b> Use CANCEL BROADCAST from the broadcast menu to clear stuck entries."
            )
        if "backup" in item_l or "backups" in item_l:
            solutions.append(
                "Backups: Trigger manual backup via BACKUP MENU -> CREATE BACKUP. "
                "Check that the backups collection exists in MongoDB."
            )
        if "drive" in item_l or "token" in item_l:
            solutions.append(
                "Drive: Delete token.json and re-run Google Drive auth flow. "
                "Ensure DRIVE FOLDER ID env var is set in Render."
            )
        if "environment" in item_l or "env" in item_l or "missing" in item_l:
            solutions.append(
                "Env Vars: Open Render dashboard -> Environment -> add the missing variable, then redeploy."
            )
        if "latency" in item_l or "slow" in item_l or "timeout" in item_l:
            solutions.append(
                "Latency: Upgrade MongoDB Atlas tier (M0 to M10). Add indexes on frequently queried fields. "
                "Check Render region matches Atlas region for low ping."
            )
        if "msa" in item_l:
            solutions.append(
                "MSA IDs: Verify the msa ids collection is intact. "
                "Do NOT manually delete documents from it."
            )
        if "ban" in item_l or "banned" in item_l:
            solutions.append(
                "Bans: Review ban triggers in bot1 auto-ban logic. "
                "Use SHOOT -> SEARCH USER to inspect individual cases."
            )

    if not solutions and (issues or warnings):
        solutions.append(
            "General Fix: Restart Bot 2 service on Render. "
            "If issue persists, check Render logs for stack traces and contact developer."
        )

    if solutions:
        unique_solutions = list(dict.fromkeys(solutions))
        report += "\nPOSSIBLE SOLUTIONS:\n"
        for idx, sol in enumerate(unique_solutions, 1):
            report += f"{idx}. {sol}\n\n"

    # Safe truncation: cut at last newline to avoid splitting mid-sentence or mid-entity
    if len(report) > 3800:
        cut = report[:3750]
        last_nl = cut.rfind('\n')
        report = (cut[:last_nl] if last_nl > 0 else cut) + "\n\n(report truncated)"
    try:
        await status_msg.edit_text(report, parse_mode="HTML")
    except Exception as _diag_err:
        # Fallback: send plain text so the admin always gets something readable
        try:
            plain = f"BOT 2 DIAGNOSIS ERROR\n\nParse error: {str(_diag_err)[:200]}\n\nCheck Render logs."
            await status_msg.edit_text(plain)
        except Exception:
            pass

def _resolve_user(search_input: str):
    """
    Cross-collection user lookup for all SHOOT action handlers.
    Checks col_user_tracking AND col_msa_ids — so a user is found even when
    only one collection has a record (e.g. vault-joined but no tracking entry,
    or tracking exists but msa_id field was empty).
    Returns a merged dict or None if not found in either collection.
    Returned keys: user_id, msa_id, first_name, username, source,
                   first_start, last_start, assigned_at
    """
    s = search_input.strip()
    tracking_doc = None
    msa_doc = None

    if s.upper().startswith("MSA"):
        tracking_doc = col_user_tracking.find_one({"msa_id": s.upper()})
        msa_doc      = col_msa_ids.find_one({"msa_id": s.upper()})
    elif s.isdigit():
        uid = int(s)
        tracking_doc = col_user_tracking.find_one({"user_id": uid})
        msa_doc      = col_msa_ids.find_one({"user_id": uid})
    else:
        # Name search fallback
        tracking_doc = col_user_tracking.find_one(
            {"first_name": {"$regex": f"^{s}$", "$options": "i"}}
        )
        if tracking_doc:
            msa_doc = col_msa_ids.find_one({"user_id": tracking_doc.get("user_id")})

    if not tracking_doc and not msa_doc:
        return None

    # If found only in msa_doc, try resolving tracking by user_id (activity fields)
    if not tracking_doc and msa_doc:
        tracking_doc = col_user_tracking.find_one({"user_id": msa_doc.get("user_id")})

    return {
        "user_id":     (tracking_doc or msa_doc).get("user_id"),
        # msa_doc is the authoritative source for the MSA ID
        "msa_id":      (msa_doc or {}).get("msa_id") or (tracking_doc or {}).get("msa_id", "N/A"),
        "first_name":  (tracking_doc or {}).get("first_name") or (msa_doc or {}).get("first_name", "Unknown"),
        "username":    (tracking_doc or {}).get("username") or (msa_doc or {}).get("username", "N/A"),
        "source":      (tracking_doc or {}).get("source", "N/A"),
        "first_start": (tracking_doc or {}).get("first_start"),
        "last_start":  (tracking_doc or {}).get("last_start"),
        "assigned_at": (msa_doc or {}).get("assigned_at"),
    }


def get_shoot_menu():
    """Shoot (Admin Control) submenu"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚫 BAN USER"), KeyboardButton(text="✅ UNBAN USER")],
            [KeyboardButton(text="⏰ TEMPORARY BAN"), KeyboardButton(text="🗑️ DELETE USER")],
            [KeyboardButton(text="⏸️ SUSPEND FEATURES"), KeyboardButton(text="▶️ UNSUSPEND")],
            [KeyboardButton(text="🔄 RESET USER DATA"), KeyboardButton(text="🔍 SEARCH USER")],
            [KeyboardButton(text="⬅️ MAIN MENU")]
        ],
        resize_keyboard=True
    )

@dp.message(F.text == "📸 SHOOT")
async def shoot_handler(message: types.Message, state: FSMContext):
    """Shoot (Admin Control) feature - User management"""
    if not await has_permission(message.from_user.id, "shoot"):
        await message.answer("⛔ You don't have permission to use SHOOT.", parse_mode="Markdown")
        return
    await state.clear()
    await message.answer(
        "📸 **SHOOT - ADMIN CONTROL**\n\n"
        "Manage users and their access:\n\n"
        "🚫 **BAN USER** - Block all bot access\n"
        "✅ **UNBAN USER** - Restore bot access\n"
        "🗑️ **DELETE USER** - Permanently remove user\n"
        "⏸️ **SUSPEND FEATURES** - Disable specific features\n"
        "▶️ **UNSUSPEND** - Remove all suspended features\n"
        "🔄 **RESET USER DATA** - Reset user information\n"
        "🔍 **SEARCH USER** - View detailed user info\n\n"
        "⚠️ **Warning:** These actions affect Bot 1 users.",
        reply_markup=get_shoot_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# BAN USER HANDLERS
# ==========================================

@dp.message(F.text == "🚫 BAN USER")
async def ban_user_start(message: types.Message, state: FSMContext):
    """Start ban user flow"""
    await state.set_state(ShootStates.waiting_for_ban_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚫 **BAN USER**\n\n"
        "Enter the user's **MSA ID** or **User ID** to ban:\n\n"
        "⚠️ Banned users will:\n"
        "  • Lose all Bot 1 access\n"
        "  • See only SUPPORT button\n"
        "  • Receive ban notification\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_ban_id)
async def process_ban_id(message: types.Message, state: FSMContext):
    """Process ban user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`\n\n"
                f"Please try again with a valid MSA ID or User ID.",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        
        # Check if already banned
        is_banned = col_banned_users.find_one({"user_id": user_id})
        if is_banned:
            await loading_msg.delete()
            await message.answer(
                f"⚠️ **ALREADY BANNED**\n\n"
                f"User {first_name} (`{msa_id}`) is already banned.\n\n"
                f"Banned on: {is_banned.get('banned_at', now_local()).strftime('%b %d, %Y at %I:%M:%S %p')}",
                parse_mode="Markdown"
            )
            return

        # Check if user is an admin — warn but still allow ban (admin record auto-removed on confirm)
        admin_doc = col_admins.find_one({"user_id": user_id})
        is_admin_user = bool(admin_doc)
        admin_role = admin_doc.get('role', 'Admin') if admin_doc else None

        # Store user data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name,
            username=username,
            is_admin_user=is_admin_user,
            admin_role=admin_role
        )
        await state.set_state(ShootStates.waiting_for_ban_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM BAN"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        admin_note = (
            f"\n⚠️ **Note:** This user is a **{admin_role}** admin — "
            f"their admin record will be removed automatically.\n"
        ) if is_admin_user else ""

        await loading_msg.delete()
        await message.answer(
            f"🚫 **CONFIRM BAN**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"📱 **Username:** @{username if username != 'N/A' else 'None'}\n"
            f"{admin_note}\n"
            f"⚠️ **This will:**\n"
            f"  • Ban user from all Bot 1 functions\n"
            f"  • Hide all menus and buttons\n"
            f"  • Show only SUPPORT option\n"
            f"  • Send ban notification to user\n\n"
            f"Type **✅ CONFIRM BAN** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_ban_confirm)
async def process_ban_confirm(message: types.Message, state: FSMContext):
    """Process ban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Ban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM BAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        is_admin_user = data.get("is_admin_user", False)
        admin_role = data.get("admin_role", "Admin")
        
        try:
            # If target is an admin, remove their admin record first
            if is_admin_user:
                col_admins.delete_one({"user_id": user_id})

            # Add to banned_users collection
            col_banned_users.insert_one({
                "user_id": user_id,
                "msa_id": msa_id,
                "first_name": first_name,
                "username": data.get("username"),
                "banned_at": now_local(),
                "banned_by": message.from_user.id,
                "reason": "Admin action — Permanent ban",
                "ban_type": "permanent"
            })

            # ── SOFT-ARCHIVE: preserve all records, just flag them as permanently banned ──
            # We NEVER delete MSA IDs or user data on permanent ban.
            # Reasons:
            #   1. If records are deleted, the system forgets them and they could re-register.
            #   2. Keeping data ensures a permanent, tamper-proof audit trail.
            #   3. The bot1_permanently_banned_msa collection acts as a dedicated banned registry.
            msa_record = col_msa_ids.find_one({"user_id": user_id})
            archived_msa_id = msa_id
            if msa_record:
                archived_msa_id = msa_record.get("msa_id", msa_id)

            # 1. Upsert a full snapshot into the permanent ban archive
            col_permanently_banned_msa.update_one(
                {"user_id": user_id},
                {"$set": {
                    "user_id":       user_id,
                    "msa_id":        archived_msa_id,
                    "first_name":    first_name,
                    "username":      data.get("username"),
                    "banned_at":     now_local(),
                    "banned_by":     message.from_user.id,
                    "reason":        "Permanent ban",
                    "status":        "permanently_banned",
                }},
                upsert=True
            )

            # 2. Flag the MSA ID record as permanently banned (DO NOT DELETE)
            if msa_record:
                col_msa_ids.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "is_permanently_banned": True,
                        "banned_at":             now_local(),
                        "banned_by":             message.from_user.id,
                    }}
                )

            # 3. Flag the user_verification record as permanently banned (DO NOT DELETE or unset msa_id)
            col_user_verification.update_one(
                {"user_id": user_id},
                {"$set": {
                    "is_permanently_banned": True,
                    "msa_revoked":           True,
                    "msa_revoked_at":        now_local(),
                    "banned_by":             message.from_user.id,
                }}
            )
            
            # Notify user and immediately clear their keyboard (permanent ban)
            try:
                ban_message = (
                    "🚫 **ACCOUNT PERMANENTLY BANNED**\n\n"
                    "Your account has been permanently restricted.\n\n"
                    "⚠️ All features and buttons are disabled.\n"
                    "This action is permanent."
                )
                # ReplyKeyboardRemove clears their keyboard right away — no buttons at all
                await bot_1.send_message(
                    user_id, ban_message,
                    reply_markup=ReplyKeyboardRemove(),
                    parse_mode="Markdown"
                )
            except Exception:
                pass  # User might have blocked bot
            
            admin_removed_note = f"\n🔓 Admin record ({admin_role}) removed automatically." if is_admin_user else ""
            await state.clear()
            await message.answer(
                f"✅ **USER BANNED**\n\n"
                f"👤 {first_name} (`{msa_id}`) has been banned from Bot 1.\n"
                f"{admin_removed_note}\n"
                f"🕐 Banned at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"User will see ban notification on next interaction.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🚫 User {user_id} ({msa_id}) banned by admin {message.from_user.id}{'  [admin record removed]' if is_admin_user else ''}")
        
        except Exception as e:
            await message.answer(f"❌ **BAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM BAN** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# TEMPORARY BAN USER HANDLERS
# ==========================================

@dp.message(F.text == "⏰ TEMPORARY BAN")
async def temp_ban_user_start(message: types.Message, state: FSMContext):
    """Start temporary ban user flow"""
    await state.set_state(ShootStates.waiting_for_temp_ban_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "⏰ **TEMPORARY BAN**\n\n"
        "Enter the user's **MSA ID** or **User ID** to temporarily ban:\n\n"
        "⚠️ Temporary ban will:\n"
        "  • Block all Bot 1 access for selected duration\n"
        "  • Show countdown timer to user\n"
        "  • Auto-unban when time expires\n"
        "  • Allow user to appeal via support\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_temp_ban_id)
async def process_temp_ban_id(message: types.Message, state: FSMContext):
    """Process temporary ban user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`\n\n"
                f"Please try again with a valid MSA ID or User ID.",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        
        # Check if already banned
        is_banned = col_banned_users.find_one({"user_id": user_id})
        if is_banned:
            ban_type = "temporary" if is_banned.get('ban_expires') else "permanent"
            await loading_msg.delete()
            await message.answer(
                f"⚠️ **ALREADY BANNED**\n\n"
                f"User {first_name} (`{msa_id}`) is already {ban_type} banned.\n\n"
                f"Banned on: {is_banned.get('banned_at', now_local()).strftime('%b %d, %Y at %I:%M:%S %p')}",
                parse_mode="Markdown"
            )
            return
        
        # Store user data and show duration menu
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name,
            username=username
        )
        await state.set_state(ShootStates.selecting_temp_ban_duration)
        
        duration_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⏱️ 1 HOUR"), KeyboardButton(text="⏱️ 6 HOURS")],
                [KeyboardButton(text="⏱️ 12 HOURS"), KeyboardButton(text="⏱️ 1 DAY")],
                [KeyboardButton(text="⏱️ 3 DAYS"), KeyboardButton(text="⏱️ 7 DAYS")],
                [KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"⏰ **SELECT BAN DURATION**\n\n"
            f"👤 **User:** {first_name} (`{msa_id}`)\n\n"
            f"Select how long to ban this user:\n\n"
            f"⏱️ **1 HOUR** - Short timeout\n"
            f"⏱️ **6 HOURS** - Medium restriction\n"
            f"⏱️ **12 HOURS** - Half day\n"
            f"⏱️ **1 DAY** - Full day\n"
            f"⏱️ **3 DAYS** - Extended period\n"
            f"⏱️ **7 DAYS** - One week\n\n"
            f"User will be auto-unbanned after duration expires.",
            reply_markup=duration_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.selecting_temp_ban_duration)
async def process_temp_ban_duration(message: types.Message, state: FSMContext):
    """Process temporary ban duration selection"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    # Map duration buttons to hours
    duration_map = {
        "⏱️ 1 HOUR": 1,
        "⏱️ 6 HOURS": 6,
        "⏱️ 12 HOURS": 12,
        "⏱️ 1 DAY": 24,
        "⏱️ 3 DAYS": 72,
        "⏱️ 7 DAYS": 168
    }
    
    if message.text not in duration_map:
        await message.answer("⚠️ Please select a duration from the menu.", parse_mode="Markdown")
        return
    
    hours = duration_map[message.text]
    data = await state.get_data()
    
    # Calculate expiry time
    ban_expires = now_local() + timedelta(hours=hours)
    
    # Store duration info
    await state.update_data(
        ban_duration_hours=hours,
        ban_expires=ban_expires,
        ban_duration_text=message.text.replace("⏱️ ", "")
    )
    await state.set_state(ShootStates.waiting_for_temp_ban_confirm)
    
    confirm_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM TEMP BAN"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    first_name = data.get("first_name")
    msa_id = data.get("msa_id")
    user_id = data.get("user_id")
    
    await message.answer(
        f"⏰ **CONFIRM TEMPORARY BAN**\n\n"
        f"👤 **Name:** {first_name}\n"
        f"🆔 **MSA ID:** `{msa_id}`\n"
        f"👁️ **User ID:** `{user_id}`\n\n"
        f"⏱️ **Duration:** {message.text.replace('⏱️ ', '')}\n"
        f"🕐 **Ban Until:** {ban_expires.strftime('%b %d, %Y at %I:%M:%S %p')}\n\n"
        f"⚠️ **This will:**\n"
        f"  • Block user from all Bot 1 functions\n"
        f"  • Show countdown timer to user\n"
        f"  • Auto-unban on {ban_expires.strftime('%b %d at %I:%M %p')}\n"
        f"  • Send notification with countdown\n\n"
        f"Type **✅ CONFIRM TEMP BAN** to proceed or **❌ CANCEL** to abort.",
        reply_markup=confirm_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_temp_ban_confirm)
async def process_temp_ban_confirm(message: types.Message, state: FSMContext):
    """Process temporary ban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Temporary ban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM TEMP BAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        ban_expires = data.get("ban_expires")
        ban_duration_text = data.get("ban_duration_text")
        ban_duration_hours = data.get("ban_duration_hours")
        
        try:
            # Add to banned_users collection with expiry
            col_banned_users.insert_one({
                "user_id": user_id,
                "msa_id": msa_id,
                "first_name": first_name,
                "username": data.get("username"),
                "banned_at": now_local(),
                "banned_by": message.from_user.id,
                "reason": f"Temporary ban - {ban_duration_text}",
                "ban_type": "temporary",
                "ban_expires": ban_expires,
                "ban_duration_hours": ban_duration_hours
            })
            
            # Calculate time remaining for display
            time_diff = ban_expires - now_local()
            total_seconds = (ban_duration_hours or 0) * 3600
            if total_seconds > 0:
                elapsed_seconds = total_seconds - time_diff.total_seconds()
                progress_percentage = min(100.0, max(0.0, (elapsed_seconds / total_seconds) * 100))
            else:
                elapsed_seconds = 0
                progress_percentage = 0.0
            
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
            
            # Generate progress bar (20 blocks)
            filled = int((progress_percentage / 100) * 20)
            empty = 20 - filled
            progress_bar = "▰" * filled + "▱" * empty
            
            # Try to notify user via Bot 1
            try:
                ban_message = (
                    "⏰ **TEMPORARY RESTRICTION**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Your account access has been temporarily limited due to policy violations.\n\n"
                    f"⏱️ **Ban Duration:** {ban_duration_text}\n"
                    f"🕐 **Ban Start:** {now_local().strftime('%b %d at %I:%M %p')}\n"
                    f"🕐 **Ban Expires:** {ban_expires.strftime('%b %d at %I:%M %p')}\n"
                    f"⏳ **Time Remaining:** {time_remaining}\n\n"
                    f"**Ban Progress**\n"
                    f"`[{progress_bar}]` {progress_percentage:.0f}%\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ **Auto-Unban:** Your access will be automatically restored when the timer expires.\n\n"
                    f"⚠️ **Support Access:** You can still use the **📞 SUPPORT** button to contact us if needed.\n\n"
                    f"📋 **Note:** Please review our community guidelines to avoid future restrictions."
                )
                
                # Push the restricted keyboard immediately so user sees SUPPORT only — no /start needed
                support_kb = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="📞 SUPPORT")]],
                    resize_keyboard=True
                )
                await bot_1.send_message(
                    user_id, ban_message,
                    reply_markup=support_kb,
                    parse_mode="Markdown"
                )
            except Exception:
                pass  # User might have blocked bot

            # Schedule auto-unban
            asyncio.create_task(schedule_auto_unban(user_id, msa_id, ban_duration_hours))
            
            await state.clear()
            await message.answer(
                f"✅ **TEMPORARY BAN APPLIED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"⏱️ **Duration:** {ban_duration_text}\n"
                f"🕐 **Until:** {ban_expires.strftime('%b %d, %Y at %I:%M:%S %p')}\n"
                f"⏳ **Auto-unban in:** {time_remaining}\n\n"
                f"User has been notified with countdown.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"⏰ User {user_id} ({msa_id}) temp banned for {ban_duration_hours}h by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **TEMP BAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM TEMP BAN** or **❌ CANCEL**", parse_mode="Markdown")

async def schedule_auto_unban(user_id: int, msa_id: str, hours: int):
    """Schedule auto-unban after specified hours"""
    try:
        # Wait for the ban duration
        await asyncio.sleep(hours * 3600)
        
        # Check if still banned (user might have been manually unbanned)
        ban_doc = col_banned_users.find_one({"user_id": user_id})
        if ban_doc and ban_doc.get('ban_type') == 'temporary':
            # Remove from banned_users
            col_banned_users.delete_one({"user_id": user_id})
            
            # Notify user of auto-unban with menu restoration
            try:
                from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
                
                unban_message = (
                    "✅ **ACCOUNT RESTRICTION LIFTED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your temporary ban has expired.\n\n"
                    "🎉 **Full Access Restored**\n"
                    "All bot features are now available to you.\n\n"
                    "⚠️ **Important Reminder:**\n"
                    "Please follow community guidelines to avoid future restrictions.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your menu has been automatically restored below. 👇\n\n"
                    "Thank you for your patience! 🙏"
                )
                
                # Create full menu keyboard
                menu_keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="📊 DASHBOARD")],
                        [KeyboardButton(text="🔍 SEARCH CODE")],
                        [KeyboardButton(text="📜 RULES")],
                        [KeyboardButton(text="📚 GUIDE")],
                        [KeyboardButton(text="📞 SUPPORT")]
                    ],
                    resize_keyboard=True
                )
                
                await bot_1.send_message(user_id, unban_message, reply_markup=menu_keyboard, parse_mode="Markdown")
            except:
                pass
            
            print(f"✅ Auto-unbanned user {user_id} ({msa_id}) after {hours}h temp ban")
    
    except Exception as e:
        print(f"❌ Auto-unban error for user {user_id}: {str(e)}")

# ==========================================
# UNBAN USER HANDLERS
# ==========================================

async def show_unban_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list of banned users with ban type labels"""
    PER_PAGE = 5
    total = col_banned_users.count_documents({})
    if total == 0:
        await state.clear()
        await message.answer(
            "ℹ️ **NO BANNED USERS**\n\nThere are no currently banned users.",
            reply_markup=get_shoot_menu(), parse_mode="Markdown"
        )
        return

    page = max(0, page)
    skip = page * PER_PAGE
    docs = list(col_banned_users.find({}).skip(skip).limit(PER_PAGE))
    total_pages = (total + PER_PAGE - 1) // PER_PAGE

    report = f"🚫 **BANNED USERS** (Page {page + 1}/{total_pages}) — Total: {total}\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, doc in enumerate(docs, skip + 1):
        name = _esc_md(doc.get("first_name", "Unknown"))
        msa = doc.get("msa_id", "N/A")
        ban_type = doc.get("ban_type", "permanent")
        banned_at = doc.get("banned_at")
        dt_str = banned_at.strftime("%b %d") if banned_at else "N/A"

        if ban_type == "temporary":
            expires = doc.get("ban_expires")
            if expires:
                diff = expires - now_local()
                if diff.total_seconds() > 0:
                    hrs = diff.seconds // 3600
                    mins = (diff.seconds % 3600) // 60
                    exp_str = f"{diff.days}d {hrs}h {mins}m" if diff.days else f"{hrs}h {mins}m"
                else:
                    exp_str = "expired"
            else:
                exp_str = "?"
            type_label = f"⏰ TEMP (expires: {exp_str})"
        else:
            type_label = "🔴 PERMANENT"

        report += f"*{i}. {name}*  (`{msa}`)\n"
        report += f"   {type_label}  ·  📅 {dt_str}\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += "📝 Enter MSA ID or User ID to unban:"

    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV PAGE"))
    if (page + 1) < total_pages:
        nav_row.append(KeyboardButton(text="➡️ NEXT PAGE"))
    keyboard = [nav_row] if nav_row else []
    keyboard.append([KeyboardButton(text="❌ CANCEL")])

    await state.set_state(ShootStates.waiting_for_unban_id)
    await state.update_data(unban_page=page)
    await message.answer(report, parse_mode="Markdown",
                         reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True))


@dp.message(F.text == "✅ UNBAN USER")
async def unban_user_start(message: types.Message, state: FSMContext):
    """Show paginated banned users list then prompt for unban"""
    await show_unban_list(message, state, page=0)

@dp.message(ShootStates.waiting_for_unban_id)
async def process_unban_id(message: types.Message, state: FSMContext):
    """Process unban user ID input or list pagination"""
    # Pagination navigation for the banned list
    if message.text and message.text.strip() in ["⬅️ PREV PAGE", "➡️ NEXT PAGE"]:
        data = await state.get_data()
        page = data.get("unban_page", 0)
        page = max(0, page - 1) if "PREV" in message.text else page + 1
        await show_unban_list(message, state, page=page)
        return

    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find banned user
        ban_doc = None
        if search_input.upper().startswith("MSA"):
            ban_doc = col_banned_users.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            ban_doc = col_banned_users.find_one({"user_id": int(search_input)})
        
        if not ban_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT BANNED**\n\n"
                f"No banned user found with ID: `{search_input}`\n\n"
                f"User may not be banned or ID is incorrect.",
                parse_mode="Markdown"
            )
            return
        
        user_id = ban_doc.get("user_id")
        msa_id = ban_doc.get("msa_id", "N/A")
        first_name = ban_doc.get("first_name", "Unknown")
        banned_at = ban_doc.get("banned_at", now_local())
        ban_type = ban_doc.get("ban_type", "permanent")
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name,
            ban_type=ban_type
        )
        await state.set_state(ShootStates.waiting_for_unban_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM UNBAN"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        _perm_warning = (
            "\n⚠️ **PERMANENT BAN — DATA WIPE ON UNBAN:**\n"
            "All user records (tracking, verification, MSA ID) will be erased.\n"
            "User must /start again as a brand-new member.\n"
        ) if ban_type == "permanent" else "\nThis will restore full bot access.\n"
        
        await loading_msg.delete()
        await message.answer(
            f"✅ **CONFIRM UNBAN**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"🚫 **Banned:** {banned_at.strftime('%b %d, %Y at %I:%M:%S %p')}\n"
            f"📌 **Ban type:** {ban_type.capitalize()}{_perm_warning}\n"
            f"Type **✅ CONFIRM UNBAN** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_unban_confirm)
async def process_unban_confirm(message: types.Message, state: FSMContext):
    """Process unban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Unban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM UNBAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        ban_type = data.get("ban_type", "permanent")
        
        try:
            # Remove from banned_users collection
            result = col_banned_users.delete_one({"user_id": user_id})
            
            if result.deleted_count > 0:
                records_cleared = 0

                # For permanent bans: wipe ALL user records so they start completely fresh
                if ban_type == "permanent":
                    r1 = col_user_tracking.delete_one({"user_id": user_id})
                    r2 = col_user_verification.delete_one({"user_id": user_id})
                    r3 = col_msa_ids.delete_one({"user_id": user_id})
                    # Also clear any suspended features left over
                    col_suspended_features.delete_one({"user_id": user_id})
                    records_cleared = r1.deleted_count + r2.deleted_count + r3.deleted_count
                    print(f"🗑️ Permanent-ban data wipe for user {user_id}: tracking={r1.deleted_count}, verification={r2.deleted_count}, msa_ids={r3.deleted_count}")

                # Notify user with appropriate message
                try:
                    if ban_type == "permanent":
                        unban_message = (
                            "✅ **ACCOUNT UNBANNED**\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            "Your permanent ban has been lifted by an administrator.\n\n"
                            "🆕 **Fresh Start**\n"
                            "Your previous data has been fully cleared.\n"
                            "Use /start to register as a brand-new member.\n\n"
                            "⚠️ **Warning:**\n"
                            "Please follow community guidelines to avoid future restrictions.\n\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━"
                        )
                        await bot_1.send_message(
                            user_id, unban_message,
                            reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown"
                        )
                    else:
                        unban_message = (
                            "✅ **ACCOUNT UNBANNED**\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            "Your account has been unbanned by an administrator.\n\n"
                            "🎉 **Full Access Restored**\n"
                            "All bot features are now available to you.\n\n"
                            "⚠️ **Warning:**\n"
                            "Please follow community guidelines to avoid future restrictions.\n\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            "Your menu has been automatically restored below. 👇"
                        )
                        menu_keyboard = ReplyKeyboardMarkup(
                            keyboard=[
                                [KeyboardButton(text="📊 DASHBOARD")],
                                [KeyboardButton(text="🔍 SEARCH CODE")],
                                [KeyboardButton(text="📜 RULES")],
                                [KeyboardButton(text="📚 GUIDE")],
                                [KeyboardButton(text="📞 SUPPORT")]
                            ],
                            resize_keyboard=True
                        )
                        await bot_1.send_message(
                            user_id, unban_message,
                            reply_markup=menu_keyboard, parse_mode="Markdown"
                        )
                except Exception:
                    pass  # User might have blocked bot
                
                _cleared_note = f"\n🗑️ **Records cleared:** {records_cleared} (fresh start — user must /start again)" if ban_type == "permanent" else ""
                await state.clear()
                await message.answer(
                    f"✅ **USER UNBANNED**\n\n"
                    f"👤 {first_name} (`{msa_id}`) has been unbanned.\n"
                    f"📌 **Ban type was:** {'Permanent (all data wiped)' if ban_type == 'permanent' else 'Temporary'}"
                    f"{_cleared_note}\n"
                    f"🕐 Unbanned at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                    f"{'User must /start again to register as a new member.' if ban_type == 'permanent' else 'User now has full bot access with warning notification sent.'}",
                    reply_markup=get_shoot_menu(),
                    parse_mode="Markdown"
                )
                print(f"✅ User {user_id} ({msa_id}) unbanned by admin {message.from_user.id} (ban_type={ban_type}, records_cleared={records_cleared})")
            else:
                await message.answer("❌ Failed to unban user. Please try again.", parse_mode="Markdown")
        
        except Exception as e:
            await message.answer(f"❌ **UNBAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM UNBAN** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# DELETE USER HANDLERS
# ==========================================

@dp.message(F.text == "🗑️ DELETE USER")
async def delete_user_start(message: types.Message, state: FSMContext):
    """Start delete user flow"""
    await state.set_state(ShootStates.waiting_for_delete_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🗑️ **DELETE USER**\n\n"
        "⚠️ **WARNING:** This permanently removes ALL user data:\n"
        "  • User tracking records\n"
        "  • Ban records\n"
        "  • Suspended features\n"
        "  • Support tickets\n\n"
        "Enter the user's **MSA ID** or **User ID** to delete:\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_delete_id)
async def process_delete_id(message: types.Message, state: FSMContext):
    """Process delete user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Count related data
        ban_count = col_banned_users.count_documents({"user_id": user_id})
        ticket_count = col_support_tickets.count_documents({"user_id": user_id})
        suspend_count = col_suspended_features.count_documents({"user_id": user_id})
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.waiting_for_delete_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⚠️ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"🗑️ **CONFIRM DELETION**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n\n"
            f"📊 **Data to delete:**\n"
            f"  • User tracking: 1 record\n"
            f"  • Ban records: {ban_count}\n"
            f"  • Support tickets: {ticket_count}\n"
            f"  • Suspended features: {suspend_count}\n\n"
            f"⚠️ **THIS ACTION CANNOT BE UNDONE!**\n\n"
            f"Type **⚠️ CONFIRM DELETE** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_delete_confirm)
async def process_delete_confirm(message: types.Message, state: FSMContext):
    """Process delete confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Deletion cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM DELETE" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Delete from all collections (including MSA ID — permanent wipe)
            del1 = col_user_tracking.delete_many({"user_id": user_id})
            del2 = col_banned_users.delete_many({"user_id": user_id})
            del3 = col_support_tickets.delete_many({"user_id": user_id})
            del4 = col_suspended_features.delete_many({"user_id": user_id})
            del5 = col_msa_ids.delete_many({"user_id": user_id})           # Destroy MSA ID forever
            del6 = col_user_verification.delete_many({"user_id": user_id}) # Remove verification
            
            total_deleted = (del1.deleted_count + del2.deleted_count + del3.deleted_count
                            + del4.deleted_count + del5.deleted_count + del6.deleted_count)
            
            await state.clear()
            await message.answer(
                f"✅ **USER DELETED**\n\n"
                f"👤 {first_name} (`{msa_id}`) has been permanently removed.\n\n"
                f"🗑️ Records deleted: {total_deleted}\n"
                f"🕐 Deleted at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"All user data has been permanently erased.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🗑️ User {user_id} ({msa_id}) deleted by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **DELETE FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **⚠️ CONFIRM DELETE** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# RESET USER DATA HANDLERS
# ==========================================

@dp.message(F.text == "🔄 RESET USER DATA")
async def reset_user_start(message: types.Message, state: FSMContext):
    """Start reset user data flow"""
    await state.set_state(ShootStates.waiting_for_reset_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔄 **RESET USER DATA**\n\n"
        "This will reset user's tracking data (keeps MSA ID but resets timestamps).\n\n"
        "Enter the user's **MSA ID** or **User ID** to reset:\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_reset_id)
async def process_reset_id(message: types.Message, state: FSMContext):
    """Process reset user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.waiting_for_reset_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM RESET"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"🔄 **CONFIRM RESET**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n\n"
            f"This will reset:\n"
            f"  • First/Last start timestamps\n"
            f"  • Source tracking\n"
            f"  • Username/name data\n\n"
            f"MSA ID will be preserved.\n\n"
            f"Type **✅ CONFIRM RESET** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_reset_confirm)
async def process_reset_confirm(message: types.Message, state: FSMContext):
    """Process reset confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Reset cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM RESET" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # ── Step 1: DELETE MSA ID permanently — number returned to pool, can be reallocated ──
            # Look up by msa_id string (e.g. "MSA324935688") — avoids user_id type mismatch
            msa_doc = col_msa_ids.find_one({"msa_id": msa_id})
            if not msa_doc and user_id:
                # fallback: try by user_id in case msa_id field is missing
                msa_doc = col_msa_ids.find_one({"user_id": user_id})
            deleted_msa_id = msa_id  # fallback to state data value
            if msa_doc:
                deleted_msa_id = msa_doc.get("msa_id", msa_id)
                col_msa_ids.delete_one({"_id": msa_doc["_id"]})

            # ── Step 2: Delete verification record — bot1 treats user as brand-new ──
            col_user_verification.delete_one({"user_id": user_id})

            # ── Step 3: Delete tracking record ──
            col_user_tracking.delete_one({"user_id": user_id})

            # ── Step 4: Clear any bans / suspensions ──
            col_banned_users.delete_one({"user_id": user_id})
            col_suspended_features.delete_one({"user_id": user_id})

            # ── Step 5: Delete all support tickets for this user ──
            col_support_tickets.delete_many({"user_id": user_id})

            await state.clear()
            await message.answer(
                f"✅ **USER PERMANENTLY ERASED**\n\n"
                f"👤 {first_name} (`{deleted_msa_id}`) has been fully removed.\n\n"
                f"🗑️ **Deleted:** MSA ID, verification, tracking, bans, suspensions, ticket history\n"
                f"🆓 **MSA ID `{deleted_msa_id}` deleted** — number freed back to allocation pool\n\n"
                f"🆕 If this user starts Bot 1 again they will receive a **brand-new MSA ID**.\n\n"
                f"🕒 Erased at: {now_local().strftime('%I:%M:%S %p')}",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🗑️ User {user_id} ({deleted_msa_id}) permanently erased (MSA deleted) by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **RESET FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM RESET** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# SUSPEND FEATURES HANDLERS
# ==========================================

@dp.message(F.text == "⏸️ SUSPEND FEATURES")
async def suspend_features_start(message: types.Message, state: FSMContext):
    """Start suspend features flow"""
    await state.set_state(ShootStates.waiting_for_suspend_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "⏸️ **SUSPEND FEATURES**\n\n"
        "Enter the user's **MSA ID** or **User ID** to suspend specific features:\n\n"
        "You can disable:\n"
        "  • Search Code access\n"
        "  • IG Content viewing\n"
        "  • YT Content viewing\n"
        "  • Menu buttons\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_suspend_id)
async def process_suspend_id(message: types.Message, state: FSMContext):
    """Process suspend features ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Store data
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.selecting_suspend_features)
        
        # ── Pre-load any features ALREADY suspended for this user ────
        # This ensures the keyboard shows pre-ticked items when reopening
        existing_doc = col_suspended_features.find_one({"user_id": user_id})
        current_suspended = list((existing_doc or {}).get("bot1_suspended_features", []))

        await state.update_data(suspended_features=current_suspended)
        await loading_msg.delete()

        _prev_note = ""
        if current_suspended:
            _prev_names = ", ".join([f.replace("_", " ") for f in current_suspended])
            _prev_note = f"\n\n⚠️ Already suspended: **{_prev_names}**\nToggle OFF to unsuspend."

        await message.answer(
            f"⏸️ **SELECT FEATURES TO SUSPEND**\n\n"
            f"👤 **User:** {first_name} (`{msa_id}`){_prev_note}\n\n"
            f"Tap a feature to toggle it on/off.\n"
            f"**✅** = suspended  **☐** = active\n\n"
            f"  • 🔍 SEARCH CODE — Hide search button\n"
            f"  • 📊 DASHBOARD — Hide dashboard button\n"
            f"  • 📺 TUTORIAL — Hide tutorial button\n"
            f"  • 📜 RULES — Hide rules button\n"
            f"  • 📖 GUIDE — Hide agent guide button\n\n"
            f"📞 **Note:** SUPPORT always stays accessible.\n\n"
            f"Tap **✅ DONE** when finished or **❌ CANCEL** to abort.",
            reply_markup=_build_suspend_keyboard(current_suspended),
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

# ──────────────────────────────────────────────────────────────────
# Helper: build the suspend feature keyboard with ✅/☐ indicators
# ──────────────────────────────────────────────────────────────────
def _build_suspend_keyboard(selected: list) -> ReplyKeyboardMarkup:
    """Rebuild suspend-feature keyboard with tick(✅)/cross(☐) on each feature button."""
    def _lbl(code, emoji, name):
        tick = "✅ " if code in selected else "☐ "
        return KeyboardButton(text=f"{tick}{emoji} {name}")

    return ReplyKeyboardMarkup(
        keyboard=[
            [_lbl("SEARCH_CODE", "🔍", "SEARCH CODE"), _lbl("DASHBOARD", "📊", "DASHBOARD")],
            [_lbl("TUTORIAL", "📺", "WATCH TUTORIAL"), _lbl("RULES", "📜", "RULES")],
            [_lbl("GUIDE", "📖", "GUIDE"), KeyboardButton(text="📎 SELECT ALL")],
            [KeyboardButton(text="🚫 DESELECT ALL"), KeyboardButton(text="✅ DONE")],
            [KeyboardButton(text="❌ CANCEL")],
        ],
        resize_keyboard=True,
    )

# ──────────────────────────────────────────────────────────────────
# Mapping: what the user might type → canonical feature key
# Covers both ticked (✅ 🔍 SEARCH CODE) and plain (🔍 SEARCH CODE)
# ──────────────────────────────────────────────────────────────────
_FEATURE_MAP = {
    "SEARCH CODE":   "SEARCH_CODE",
    "DASHBOARD":     "DASHBOARD",
    "WATCH TUTORIAL": "TUTORIAL",
    "RULES":         "RULES",
    "GUIDE":         "GUIDE",
}

def _resolve_feature(text: str):
    """Strip leading tick/emoji prefix and return the canonical feature key, or None."""
    # Remove leading ✅/☐ and strip
    clean = text.lstrip("✅☐ ").strip()
    # Remove leading emoji (any non-alpha character sequence before the first letter)
    import re as _re
    clean = _re.sub(r'^[^A-Za-z]+', '', clean).strip()
    return _FEATURE_MAP.get(clean.upper())


@dp.message(ShootStates.selecting_suspend_features)
async def process_suspend_features(message: types.Message, state: FSMContext):
    """Process feature suspension selection — accumulates toggles, saves only on DONE."""
    txt = (message.text or "").strip()

    # ── CANCEL ──────────────────────────────────────────────────────
    if "CANCEL" in txt:
        await state.clear()
        await message.answer("✅ Suspension cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return

    # Read current selection — always from the SAME key: 'suspended_features'
    data = await state.get_data()
    selected: list = list(data.get("suspended_features", []))

    # ── DONE ────────────────────────────────────────────────────────
    if "DONE" in txt:
        if not selected:
            await message.answer(
                "⚠️ **No features selected.**\n\nTap at least one feature button to suspend it, then tap ✅ DONE.",
                reply_markup=_build_suspend_keyboard(selected),
                parse_mode="Markdown"
            )
            return

        user_id    = data.get("user_id")
        msa_id     = data.get("msa_id")
        first_name = data.get("first_name")

        try:
            # ── Save to DB ──────────────────────────────────────────
            col_suspended_features.update_one(
                {"user_id": user_id},
                {"$set": {
                    "msa_id":                  msa_id,
                    "first_name":              first_name,
                    "bot1_suspended_features": selected,
                    "suspended_at":            now_local(),
                    "suspended_by":            message.from_user.id,
                }},
                upsert=True
            )

            # ── Notify user via Bot 1 ────────────────────────────────
            try:
                notification_text = (
                    "⚠️ **ACCOUNT RESTRICTION**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Some features have been temporarily suspended from your account.\n\n"
                    "**Suspended Features:**\n" +
                    "\n".join([f"  • {f.replace('_', ' ')}" for f in selected]) +
                    "\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "📞 **Support Access:** The SUPPORT button remains available.\n"
                    "💬 **Contact:** If you believe this is an error, please contact support.\n\n"
                    "Thank you for your understanding."
                )
                # Push restricted keyboard to user immediately
                restr_btns = []
                for feat, lbl in [
                    ("DASHBOARD",   "📊 DASHBOARD"),
                    ("SEARCH_CODE", "🔍 SEARCH CODE"),
                    ("TUTORIAL",    "📺 WATCH TUTORIAL"),
                    ("GUIDE",       "📖 AGENT GUIDE"),
                    ("RULES",       "📜 RULES"),
                ]:
                    if feat not in selected:
                        restr_btns.append([KeyboardButton(text=lbl)])
                restr_btns.append([KeyboardButton(text="📞 SUPPORT")])
                await bot_1.send_message(
                    user_id, notification_text,
                    reply_markup=ReplyKeyboardMarkup(keyboard=restr_btns, resize_keyboard=True),
                    parse_mode="Markdown"
                )
            except Exception as notify_err:
                print(f"Failed to send suspension notification: {notify_err}")

            await state.clear()
            feature_lines = "\n".join([f"  • {f.replace('_', ' ')}" for f in selected])
            await message.answer(
                f"✅ **FEATURES SUSPENDED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"⏸️ Suspended features:\n{feature_lines}\n\n"
                f"🕐 Suspended at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"✉️ User has been notified via Bot 1.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"⏸️ Features suspended for {user_id} ({msa_id}) by admin {message.from_user.id}: {selected}")

        except Exception as e:
            await message.answer(f"❌ **SUSPEND FAILED:** {str(e)[:100]}", parse_mode="Markdown")
        return

    # ── SELECT ALL ──────────────────────────────────────────────────
    if "SELECT ALL" in txt:
        selected = ["SEARCH_CODE", "DASHBOARD", "TUTORIAL", "RULES", "GUIDE"]
        await state.update_data(suspended_features=selected)
        feature_lines = "\n".join([f"  ✅ {f.replace('_', ' ')}" for f in selected])
        await message.answer(
            f"✅ **All features selected!**\n\n"
            f"**Currently Selected:**\n{feature_lines}\n\n"
            f"Tap ✅ DONE to confirm or ❌ CANCEL to abort.",
            reply_markup=_build_suspend_keyboard(selected),
            parse_mode="Markdown"
        )
        return

    # ── DESELECT ALL ────────────────────────────────────────────────
    if "DESELECT ALL" in txt:
        selected = []
        await state.update_data(suspended_features=selected)
        await message.answer(
            "🚫 **All features deselected.**\n\n"
            "**Currently Selected:** _(none)_\n\n"
            "Tap features to add them, then tap ✅ DONE.",
            reply_markup=_build_suspend_keyboard(selected),
            parse_mode="Markdown"
        )
        return

    # ── FEATURE TOGGLE ──────────────────────────────────────────────
    feature_key = _resolve_feature(txt)
    if feature_key:
        if feature_key in selected:
            selected.remove(feature_key)
            action = "☐ Removed"
        else:
            selected.append(feature_key)
            action = "✅ Added"

        # Save back under the SAME consistent key
        await state.update_data(suspended_features=selected)

        if selected:
            sel_lines = "\n".join([f"  ✅ {f.replace('_', ' ')}" for f in selected])
        else:
            sel_lines = "  _(none yet)_"

        await message.answer(
            f"{action}: **{feature_key.replace('_', ' ')}**\n\n"
            f"**Currently Selected:**\n{sel_lines}\n\n"
            f"Keep tapping to toggle. Tap ✅ DONE when ready.",
            reply_markup=_build_suspend_keyboard(selected),
            parse_mode="Markdown"
        )
        return

    # ── Unknown input — show current state ──────────────────────────
    await message.answer(
        "ℹ️ Tap the feature buttons above to toggle them on/off.\n"
        "Tap **✅ DONE** when finished or **❌ CANCEL** to abort.",
        reply_markup=_build_suspend_keyboard(selected),
        parse_mode="Markdown"
    )

# ==========================================
# UNSUSPEND HANDLERS
# ==========================================

@dp.message(F.text == "▶️ UNSUSPEND")
async def unsuspend_features_start(message: types.Message, state: FSMContext):
    """Start unsuspend features flow"""
    await state.set_state(ShootStates.waiting_for_unsuspend_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔓 **UNSUSPEND FEATURES**\n\n"
        "Enter the user's **MSA ID** or **User ID** to remove all suspended features:\n\n"
        "This will restore full access to all Bot 1 features.\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_unsuspend_id)
async def process_unsuspend_id(message: types.Message, state: FSMContext):
    """Process unsuspend features ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Check if user has any suspended features
        suspend_doc = col_suspended_features.find_one({"user_id": user_id})
        
        if not suspend_doc:
            await loading_msg.delete()
            await message.answer(
                f"ℹ️ **NO SUSPENDED FEATURES**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"This user has no suspended features.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            return
        
        suspended_features = suspend_doc.get("bot1_suspended_features", [])
        
        # Remove all suspended features
        try:
            col_suspended_features.delete_one({"user_id": user_id})
            
            # Send notification via Bot 1 to user
            try:
                notification_text = (
                    "✅ **FEATURES RESTORED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All suspended features have been removed from your account.\n\n"
                    "🎉 **Full Access Restored**\n"
                    "You now have access to all Bot 1 features.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your menu has been automatically restored below. 👇"
                )
                
                # Create full menu keyboard
                from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
                menu_keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="📊 DASHBOARD")],
                        [KeyboardButton(text="🔍 SEARCH CODE")],
                        [KeyboardButton(text="📜 RULES")],
                        [KeyboardButton(text="📚 GUIDE")],
                        [KeyboardButton(text="📞 SUPPORT")]
                    ],
                    resize_keyboard=True
                )
                
                await bot_1.send_message(user_id, notification_text, reply_markup=menu_keyboard, parse_mode="Markdown")
            except Exception as e:
                print(f"Failed to send unsuspend notification: {e}")
            
            await loading_msg.delete()
            await state.clear()
            await message.answer(
                f"✅ **FEATURES UNSUSPENDED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"🔓 Previously suspended features:\n" + "\n".join([f"  • {f.replace('_', ' ')}" for f in suspended_features]) +
                f"\n\n🕐 Unsuspended at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"✉️ User has been notified and menu restored via Bot 1.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🔓 All features unsuspended for user {user_id} ({msa_id}) by admin {message.from_user.id}")
        
        except Exception as e:
            await loading_msg.delete()
            await message.answer(f"❌ **UNSUSPEND FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

# ==========================================
# SEARCH USER (SHOOT) HANDLERS
# ==========================================

@dp.message(F.text == "🔍 SEARCH USER")
async def shoot_search_user_start(message: types.Message, state: FSMContext):
    """Start shoot search user flow"""
    await state.set_state(ShootStates.waiting_for_shoot_search_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔍 **SEARCH USER - DETAILED VIEW**\n\n"
        "Enter the user's **MSA ID** or **User ID** for complete details:\n\n"
        "This will show:\n"
        "  • Ban status\n"
        "  • Suspended features\n"
        "  • Support tickets\n"
        "  • Activity history\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_shoot_search_id)
async def process_shoot_search(message: types.Message, state: FSMContext):
    """Process shoot search user"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching database...", parse_mode="Markdown")
    
    try:
        # Find user — cross-collection: checks col_user_tracking AND col_msa_ids
        user_doc = _resolve_user(search_input)
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        source = user_doc.get("source", "N/A")
        first_start = user_doc.get("first_start")
        last_start = user_doc.get("last_start")
        
        # Check ban status
        ban_doc = col_banned_users.find_one({"user_id": user_id})
        ban_status = "🟢 Active" if not ban_doc else "🔴 Banned"
        
        ban_date = "N/A"
        if ban_doc and ban_doc.get("banned_at"):
            b_at = ban_doc.get("banned_at")
            ban_date = b_at.strftime("%b %d, %Y at %I:%M:%S %p") if hasattr(b_at, 'strftime') else str(b_at)
        
        # Count suspended features
        suspend_count = col_suspended_features.count_documents({"user_id": user_id})
        
        # Count support tickets
        ticket_count = col_support_tickets.count_documents({"user_id": user_id})
        open_tickets = col_support_tickets.count_documents({"user_id": user_id, "status": "open"})
        
        # Format timestamps
        first_start_str = first_start.strftime("%b %d, %Y at %I:%M:%S %p") if hasattr(first_start, 'strftime') else str(first_start) if first_start else "N/A"
        last_start_str = last_start.strftime("%b %d, %Y at %I:%M:%S %p") if hasattr(last_start, 'strftime') else str(last_start) if last_start else "N/A"
        
        # Build detailed report
        report = (
            f"🔍 **DETAILED USER REPORT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            f"👤 **BASIC INFO**\n"
            f"🆔 MSA ID: `{msa_id}`\n"
            f"👁️ User ID: `{user_id}`\n"
            f"👤 Name: {first_name}\n"
            f"📱 Username: @{username if username != 'N/A' else 'None'}\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **STATUS**\n"
            f"🔒 Account: {ban_status}\n"
            f"⏸️ Suspended Features: {suspend_count}\n"
            f"🎫 Support Tickets: {ticket_count} ({open_tickets} open)\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 **ACTIVITY**\n"
            f"🔗 Entry Source: {source}\n"
            f"📅 First Joined: {first_start_str}\n"
            f"⏰ Last Active: {last_start_str}\n"
        )
        
        if ban_doc:
            ban_type_s = "⏰ TEMPORARY" if ban_doc.get("ban_type") == "temporary" else "🔴 PERMANENT"
            ban_exp_s = ""
            if ban_doc.get("ban_expires"):
                b_exp = ban_doc["ban_expires"]
                b_exp_str = b_exp.strftime('%b %d at %I:%M %p') if hasattr(b_exp, 'strftime') else str(b_exp)
                ban_exp_s = f"\n  └─ Expires: {b_exp_str}"
            report += (
                f"\n🚫 **Ban Details:**\n"
                f"  └─ Type: {ban_type_s}\n"
                f"  └─ Banned: {ban_date}\n"
                f"  └─ Reason: {_esc_md(ban_doc.get('reason', 'N/A'))}{ban_exp_s}\n"
            )

        # MSA allocation date from msa_ids collection
        msa_alloc = col_msa_ids.find_one({"user_id": user_id})
        if msa_alloc and msa_alloc.get("assigned_at"):
            a_at = msa_alloc["assigned_at"]
            a_at_str = a_at.strftime('%b %d, %Y at %I:%M:%S %p') if hasattr(a_at, 'strftime') else str(a_at)
            report += f"\n🆔 **MSA Allocated:** {a_at_str}\n"

        await loading_msg.delete()
        await state.clear()
        await message.answer(report, reply_markup=get_shoot_menu(), parse_mode="Markdown")
        print(f"🔍 Admin {message.from_user.id} searched user {msa_id}")

    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(F.text == "💬 SUPPORT")
async def support_handler(message: types.Message, state: FSMContext):
    """Support ticket management system"""
    await state.clear()
    
    # Count pending and total tickets
    pending_count = col_support_tickets.count_documents({"status": "open"})
    total_count = col_support_tickets.count_documents({})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    
    await message.answer(
        f"💬 **SUPPORT TICKET MANAGEMENT**\n\n"
        f"📊 **Statistics:**\n"
        f"⏳ Pending: **{pending_count}** tickets\n"
        f"✅ Resolved: **{resolved_count}** tickets\n"
        f"📋 Total: **{total_count}** tickets\n\n"
        f"**Select an action:**",
        reply_markup=get_support_management_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎫 PENDING TICKETS")
async def pending_tickets_handler(message: types.Message, state: FSMContext):
    """Show all pending support tickets with pagination"""
    await state.clear()
    await show_pending_tickets_page(message, page=1)

async def show_pending_tickets_page(message: types.Message, page: int = 1):
    """Helper function to display pending tickets with pagination"""
    ITEMS_PER_PAGE = 5  # Show 5 tickets per page to stay within char limit
    
    # Get open tickets count for display
    total_pending = col_support_tickets.count_documents({"status": "open"})
    
    if total_pending == 0:
        await message.answer(
            "✅ **No pending tickets!**\n\n"
            "All support requests have been resolved.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Calculate pagination
    total_pages = (total_pending + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE  # Ceiling division
    page = max(1, min(page, total_pages))  # Clamp page number
    skip = (page - 1) * ITEMS_PER_PAGE
    
    # Get tickets for current page
    tickets = list(col_support_tickets.find({"status": "open"})
                   .sort("created_at", -1)
                   .skip(skip)
                   .limit(ITEMS_PER_PAGE))
    
    response = f"🎫 **PENDING TICKETS** (Page {page}/{total_pages})\n\n"
    response += f"📊 Total Pending: **{total_pending}** tickets\n"
    response += f"📄 Showing: {skip + 1}-{skip + len(tickets)} of {total_pending}\n\n"
    
    _seen_users_page = set()
    for ticket in tickets:
        user_id = ticket.get('user_id')
        user_name = ticket.get('user_name', 'Unknown')
        username = ticket.get('username', 'none')
        msa_id = ticket.get('msa_id', 'Not Assigned') 
        issue_full = ticket.get('issue_text', 'No description')
        issue = issue_full[:80]
        created = ticket.get('created_at', now_local())
        date_str = created.strftime("%b %d, %I:%M %p")
        support_count = ticket.get('support_count', 1)

        # Per-user spam & history info
        _spam_cutoff = now_local() - timedelta(hours=1)
        _recent_count = col_support_tickets.count_documents({
            "user_id": user_id,
            "created_at": {"$gte": _spam_cutoff}
        })
        _total_user = col_support_tickets.count_documents({"user_id": user_id})
        _prev_list = list(
            col_support_tickets.find({"user_id": user_id})
            .sort("created_at", -1).skip(1).limit(1)
        )
        _last_sub = (
            _prev_list[0]["created_at"].strftime("%b %d, %I:%M %p")
            if _prev_list else "First ever"
        )
        _is_dup = user_id in _seen_users_page
        _seen_users_page.add(user_id)

        response += f"━━━━━━━━━━━━━━━━━━━━━\n"
        if _recent_count >= 2:
            response += f"⚠️ **SPAM ALERT** — {_recent_count}x in last hour!\n"
        if _is_dup:
            response += f"♻️ **DUPLICATE** — multiple open tickets\n"
        response += f"👤 **{user_name}** (@{username})\n"
        response += f"🆔 TG: `{user_id}` | MSA: `{msa_id}`\n"
        response += f"🎫 Ticket #{support_count} · {date_str}\n"
        response += f"📊 Total: {_total_user} ticket(s) · Prev sub: {_last_sub}\n"
        response += f"📝 {issue}{'…' if len(issue_full) > 80 else ''}\n\n"
    
    response += "💡 Use **✅ RESOLVE TICKET** to resolve by ID"
    
    # Hard-cap at 3800 chars to stay safely within Telegram's 4096-char limit
    _TG_SAFE = 3800
    if len(response) > _TG_SAFE:
        response = response[:_TG_SAFE] + "\n\n_...more tickets on next page_"
    
    # Create pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"pending_page_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"pending_page_{page+1}"))
    
    keyboard = None
    if buttons:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
    
    # Check if this is being called from callback (edit) or new message
    try:
        if keyboard:
            await message.edit_text(
                response,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        else:
            await message.edit_text(
                response,
                parse_mode="Markdown"
            )
    except:
        # If edit fails (not from callback), send new message
        if keyboard:
            await message.answer(
                response,
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                response,
                reply_markup=get_support_management_menu(),
                parse_mode="Markdown"
            )

@dp.message(F.text == "📋 ALL TICKETS")
async def all_tickets_handler(message: types.Message, state: FSMContext):
    """Show all tickets (pending + resolved) with pagination"""
    await state.clear()
    await show_all_tickets_page(message, page=1)

async def show_all_tickets_page(message: types.Message, page: int = 1):
    """Helper function to display all tickets with pagination"""
    ITEMS_PER_PAGE = 8  # Show 8 tickets per page (compact view)
    
    pending_count = col_support_tickets.count_documents({"status": "open"})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    total_count = pending_count + resolved_count
    
    if total_count == 0:
        await message.answer(
            "📋 **No tickets found!**\n\n"
            "No support requests have been submitted yet.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Calculate pagination
    total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    page = max(1, min(page, total_pages))
    skip = (page - 1) * ITEMS_PER_PAGE
    
    # Get tickets for current page
    tickets = list(col_support_tickets.find({})
                   .sort("created_at", -1)
                   .skip(skip)
                   .limit(ITEMS_PER_PAGE))
    
    response = f"📋 **ALL TICKETS** (Page {page}/{total_pages})\n\n"
    response += f"📊 Total: **{total_count}** · ⏳ Pending: **{pending_count}** · ✅ Resolved: **{resolved_count}**\n\n"
    response += f"Showing {skip + 1}-{skip + len(tickets)} of {total_count}:\n\n"
    
    for ticket in tickets:
        user_name = ticket.get('user_name', 'Unknown')
        msa_id = ticket.get('msa_id', 'N/A')
        status = ticket.get('status', 'unknown')
        status_emoji = "⏳" if status == "open" else "✅"
        created = ticket.get('created_at', now_local())
        date_str = created.strftime("%b %d, %I:%M %p")
        issue = ticket.get('issue_text', 'N/A')[:50]  # First 50 chars
        
        response += f"{status_emoji} **{user_name}** (MSA: `{msa_id}`)\n"
        response += f"   📝 {issue}... · {date_str}\n\n"
    
    # Hard-cap at 3800 chars to stay safely within Telegram's 4096-char limit
    _TG_SAFE = 3800
    if len(response) > _TG_SAFE:
        response = response[:_TG_SAFE] + "\n\n_...more tickets on next page_"
    
    # Create pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"all_page_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"all_page_{page+1}"))
    
    keyboard = None
    if buttons:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
    
    # Check if this is being called from callback (edit) or new message
    try:
        if keyboard:
            await message.edit_text(
                response,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        else:
            await message.edit_text(
                response,
                parse_mode="Markdown"
            )
    except:
        # If edit fails (not from callback), send new message
        if keyboard:
            await message.answer(
                response,
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                response,
                reply_markup=get_support_management_menu(),
                parse_mode="Markdown"
            )

@dp.message(F.text == "✅ RESOLVE TICKET")
async def resolve_ticket_prompt(message: types.Message, state: FSMContext):
    """Prompt for MSA ID or Telegram ID to resolve ticket"""
    await state.set_state(SupportStates.waiting_for_resolve_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "✅ **RESOLVE TICKET**\n\n"
        "Send the **MSA+ ID** (e.g., `MSA001`) or **Telegram ID** (e.g., `123456789`) to resolve the ticket.\n\n"
        "💡 **Resolving will:**\n"
        "• Mark ticket as resolved\n"
        "• Allow user to submit new tickets\n"
        "• Update timestamp\n\n"
        "Send ID below:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_resolve_id)
async def process_resolve_ticket(message: types.Message, state: FSMContext):
    """Process ticket resolution by MSA ID or Telegram ID"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip()
    
    # Try to find ticket by MSA ID first
    ticket = col_support_tickets.find_one({
        "msa_id": search_id.upper(),
        "status": "open"
    })
    
    # If not found, try by Telegram ID
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one({
            "user_id": int(search_id),
            "status": "open"
        })
    
    if not ticket:
        await message.answer(
            f"❌ **Ticket not found!**\n\n"
            f"No open ticket found for ID: `{search_id}`\n\n"
            f"💡 **Tips:**\n"
            f"• Check if ticket is already resolved\n"
            f"• Verify MSA+ ID format (e.g., MSA001)\n"
            f"• Use exact Telegram ID\n\n"
            f"Try again or click ❌ CANCEL",
            parse_mode="Markdown"
        )
        return
    
    # Resolve the ticket
    resolved_at = now_local()
    result = col_support_tickets.update_one(
        {"_id": ticket["_id"]},
        {
            "$set": {
                "status": "resolved",
                "resolved_at": resolved_at
            }
        }
    )
    
    user_name = ticket.get('user_name', 'Unknown')
    user_id = ticket.get('user_id')
    msa_id = ticket.get('msa_id', 'N/A')
    username = ticket.get('username', 'none')
    issue_text = ticket.get('issue_text', 'No description')
    ticket_type = ticket.get('ticket_type', 'Text Only')
    has_photo = ticket.get('has_photo', False)
    has_video = ticket.get('has_video', False)
    support_count = ticket.get('support_count', 1)
    channel_message_id = ticket.get('channel_message_id')
    created = ticket.get('created_at', now_local())
    created_str = created.strftime("%B %d, %Y at %I:%M %p")
    resolved_str = resolved_at.strftime("%B %d, %Y at %I:%M %p")
    
    await state.clear()
    
    if result.modified_count > 0:
        print(f"✅ Ticket resolved for user {user_id} ({user_name})")
        
        # 1. Send premium DM to user via Bot 1
        try:
            await bot_1.send_message(
                user_id,
                f"✨ **Great News, {user_name}!** ✨\n\n"
                f"🎉 We're happy to inform you that your support request has been **successfully resolved** by our admin team!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ **{user_name}, your issue has been addressed.**\n\n"
                f"Everything should be working smoothly now. If you're still experiencing any problems or have additional questions, please don't hesitate to reach out to us again.\n\n"
                f"💡 **Need more help?**\n"
                f"You can submit a new support ticket anytime by clicking **📞 SUPPORT** in the main menu.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🌟 **Thank you for your patience, {user_name}!**\n\n"
                f"We truly appreciate your understanding and are always here to help you with the best possible experience.\n\n"
                f"💎 **MSA NODE Team**",
                parse_mode="Markdown"
            )
            print(f"📧 Sent resolution notification to user {user_id}")
        except Exception as e:
            print(f"⚠️ Failed to send DM to user {user_id}: {str(e)}")
        
        # 2. Edit the channel message with resolved status
        if channel_message_id and REVIEW_LOG_CHANNEL:
            try:
                # Escape Markdown v1 special chars in issue text + cap to prevent 4096 overflow
                _MAX_CHAN_ISSUE = 3400
                safe_issue = (
                    issue_text
                    .replace('*', '\\*')
                    .replace('_', '\\_')
                    .replace('`', '\\`')
                    .replace('[', '\\[')
                )
                if len(safe_issue) > _MAX_CHAN_ISSUE:
                    safe_issue = safe_issue[:_MAX_CHAN_ISSUE] + "\n_\u2026 (truncated \u2014 full text in database)_"
                # Build clean updated ticket message
                updated_ticket_msg = f"""
🎫 **SUPPORT TICKET** - ✅ **RESOLVED**
━━━━━━━━━━━━━━━━━━━━━━━━

📅 **Date:** {created_str}
⏰ **Resolved:** {resolved_str}
📋 **Type:** {ticket_type}

👤 **USER INFORMATION**
━━━━━━━━━━━━━━━━━━━━━━━━

**Name:** {user_name}
**Username:** @{username}
**User ID:** `{user_id}`
**MSA+ ID:** `{msa_id}`
**Total Support Requests:** {support_count}

🔍 **ISSUE DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━

{safe_issue}

━━━━━━━━━━━━━━━━━━━━━━━━

✅ **STATUS:** Resolved
🕐 **Resolved At:** {resolved_str}
🤖 **Source:** MSA NODE Bot

💡 **Actions Completed:**
• User notified via DM
• Ticket status updated
• User can submit new tickets
"""
                
                await bot_1.edit_message_text(
                    chat_id=REVIEW_LOG_CHANNEL,
                    message_id=channel_message_id,
                    text=updated_ticket_msg,
                    parse_mode="Markdown"
                )
                print(f"✏️ Updated channel message {channel_message_id} with resolved status")
            except Exception as e:
                print(f"⚠️ Failed to edit channel message: {str(e)}")
        
        # 3. Confirm to admin
        await message.answer(
            f"✅ **TICKET RESOLVED SUCCESSFULLY!**\n\n"
            f"👤 **User:** {user_name}\n"
            f"🆔 **Telegram ID:** `{user_id}`\n"
            f"💳 **MSA+ ID:** `{msa_id}`\n"
            f"🎫 **Support Ticket:** #{support_count}\n"
            f"📅 **Submitted:** {created_str}\n"
            f"⏰ **Resolved:** {resolved_str}\n\n"
            f"✅ **Actions Completed:**\n"
            f"• ✉️ User notified via DM\n"
            f"• 📝 Channel message updated\n"
            f"• 🔓 User can submit new tickets\n\n"
            f"🎉 **Resolution complete!**",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "⚠️ **Failed to resolve ticket.**\n\nPlease try again.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# 📨 REPLY TO USER
# ==========================================

@dp.message(F.text == "📨 REPLY")
async def reply_to_user_prompt(message: types.Message, state: FSMContext):
    """Send custom message to user about their ticket"""
    await state.set_state(SupportStates.waiting_for_reply_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "📨 **REPLY TO USER**\n\n"
        "Send the **MSA+ ID** or **Telegram ID** of the user you want to message.\n\n"
        "💡 After entering ID, you'll compose your reply message.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_reply_id)
async def process_reply_id(message: types.Message, state: FSMContext):
    """Process user ID for reply"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip().upper()
    
    user_id = None
    user_name = "User"
    msa_id = search_id if search_id.startswith("MSA") else "N/A"
    
    # Check if search term is digit (Telegram ID)
    is_telegram_id = search_id.isdigit()
    
    # 1. Try finding in support tickets first
    ticket = col_support_tickets.find_one({"msa_id": search_id}) if not is_telegram_id else col_support_tickets.find_one({"user_id": int(search_id)})
    
    if ticket:
        user_id = ticket.get('user_id')
        user_name = ticket.get('user_name', 'User')
        msa_id = ticket.get('msa_id', msa_id)
    else:
        # 2. If not found in tickets, search global MSA users collection
        if is_telegram_id:
            user_doc = col_msa_ids.find_one({"user_id": int(search_id)})
            if user_doc:
                user_id = user_doc.get("user_id")
                user_name = user_doc.get("first_name", "User")
                msa_id = user_doc.get("msa_id", "N/A")
        else:
            user_doc = col_msa_ids.find_one({"msa_id": search_id})
            if user_doc:
                user_id = user_doc.get("user_id")
                user_name = user_doc.get("first_name", "User")
                msa_id = user_doc.get("msa_id", search_id)
                
    if not user_id:
        await message.answer(
            f"❌ **User not found!**\n\n"
            f"No records found for ID: `{search_id}`\n\n"
            f"Try again or click ❌ CANCEL",
            parse_mode="Markdown"
        )
        return
    
    # Store user info and move to message composition
    await state.update_data(
        reply_user_id=user_id,
        reply_user_name=user_name,
        reply_msa_id=msa_id
    )
    await state.set_state(SupportStates.waiting_for_reply_message)
    
    await message.answer(
        f"📨 **Messaging: {user_name}**\n\n"
        f"🆔 Telegram ID: `{user_id}`\n"
        f"💳 MSA+ ID: `{msa_id}`\n\n"
        f"📝 **Type your message:**\n"
        f"(This will be sent directly to the user)",
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_reply_message)
async def process_reply_message(message: types.Message, state: FSMContext):
    """Send the reply message to user"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    user_id = data.get('reply_user_id')
    user_name = data.get('reply_user_name')
    reply_text = message.text or message.caption or ""
    
    if len(reply_text) < 5:
        await message.answer(
            "⚠️ **Message too short!**\n\nPlease send a meaningful message (min 5 characters).",
            parse_mode="Markdown"
        )
        return
    
    # Send message to user via Bot 1
    try:
        await bot_1.send_message(
            user_id,
            f"📨 **Message from Admin Team**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{reply_text}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 Need more help? Use **📞 SUPPORT** in the menu.\n\n"
            f"💎 **MSA NODE Team**",
            parse_mode="Markdown"
        )
        
        await state.clear()
        await message.answer(
            f"✅ **Message sent to {user_name}!**\n\n"
            f"🆔 User ID: `{user_id}`\n"
            f"📨 Your message was delivered successfully.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        print(f"📨 Admin sent reply to user {user_id}")
        
    except Exception as e:
        await state.clear()
        await message.answer(
            f"❌ **Failed to send message!**\n\n"
            f"Error: {str(e)}\n\n"
            f"User may have blocked the bot.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        print(f"❌ Failed to send reply to user {user_id}: {str(e)}")

# ==========================================
# 🔍 SEARCH TICKETS & HISTORY
# ==========================================

@dp.message(F.text == "🔍 SEARCH TICKETS")
async def search_user_prompt(message: types.Message, state: FSMContext):
    """Search for user tickets"""
    await state.set_state(SupportStates.waiting_for_user_search)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔍 **SEARCH TICKETS**\n\n"
        "Search by:\n"
        "• User name\n"
        "• Username (without @)\n"
        "• MSA+ ID\n"
        "• Telegram ID\n\n"
        "Send search term:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_user_search)
async def process_user_search(message: types.Message, state: FSMContext):
    """Process user search and show ticket history"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_term = message.text.strip()
    
    # Build search query (supports multiple fields)
    search_query = {
        "$or": [
            {"user_name": {"$regex": search_term, "$options": "i"}},
            {"username": {"$regex": search_term, "$options": "i"}},
            {"msa_id": search_term.upper()}
        ]
    }
    
    # Add numeric search for Telegram ID
    if search_term.isdigit():
        search_query["$or"].append({"user_id": int(search_term)})
    
    tickets = list(col_support_tickets.find(search_query).sort("created_at", -1))
    
    await state.clear()
    
    if not tickets:
        await message.answer(
            f"❌ **No results found!**\n\n"
            f"No tickets found for: `{search_term}`",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Get user info from first ticket
    first_ticket = tickets[0]
    user_name = first_ticket.get('user_name', 'Unknown')
    username = first_ticket.get('username', 'none')
    user_id = first_ticket.get('user_id')
    msa_id = first_ticket.get('msa_id', 'N/A')
    # Send first page instead of truncated list
    await show_admin_search_ticket_page(message, user_id, 0)

async def show_admin_search_ticket_page(message_or_cb, user_id: int, page: int):
    """Show a specific page of a user's ticket history to admin"""
    tickets = list(col_support_tickets.find({"user_id": user_id}).sort("created_at", -1))
    
    if not tickets:
        if isinstance(message_or_cb, types.CallbackQuery):
            await message_or_cb.answer("No tickets found.", show_alert=True)
        return
        
    total = len(tickets)
    page = page % total
    ticket = tickets[page]
    
    user_name = ticket.get('user_name', 'Unknown')
    username = ticket.get('username', 'none')
    msa_id = ticket.get('msa_id', 'N/A')
    
    open_count = sum(1 for t in tickets if t.get('status') == 'open')
    resolved_count = sum(1 for t in tickets if t.get('status') == 'resolved')
    
    status = ticket.get('status', 'unknown')
    status_emoji = "⏳ Awaiting Review" if status == "open" else "✅ Resolved"
    created = ticket.get('created_at', now_local())
    date_str = created.strftime("%b %d, %Y at %I:%M %p")
    issue = ticket.get('issue_text', 'No description')
    ticket_type = ticket.get('ticket_type', 'Text Only')
    # character_count was removed from new tickets (redundant); derive from issue_text directly
    issue_text_raw = ticket.get('issue_text', '')
    char_count = ticket.get('character_count') or len(issue_text_raw)
    support_num = ticket.get('support_count', page + 1)
    
    response = f"🔍 **USER TICKET HISTORY**\n\n"
    response += f"👤 **{user_name}** (@{username})\n"
    response += f"🆔 Telegram ID: `{user_id}`\n"
    response += f"💳 MSA+ ID: `{msa_id}`\n"
    response += f"📊 Total: {total} (⏳ {open_count} | ✅ {resolved_count})\n\n"
    
    response += f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    response += f"🎫 **Ticket #{support_num}** _({page + 1}/{total})_\n\n"
    response += f"**Status:** {status_emoji}\n"
    response += f"**Submitted:** {date_str}\n"
    
    resolved_at = ticket.get('resolved_at')
    if resolved_at:
        response += f"**Resolved:** {resolved_at.strftime('%b %d, %Y at %I:%M %p')}\n"
        
    response += f"**Type:** {ticket_type}\n"
    response += f"**Length:** {char_count} chars\n\n"
    response += f"📝 **Message:**\n"
    response += f"_{_esc_md(issue)}_\n\n"
    response += f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    # Build pagination
    nav_kb = None
    if total > 1:
        prev_pg = (page - 1) % total
        next_pg = (page + 1) % total
        nav_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️", callback_data=f"adm_tkt:{user_id}:{prev_pg}"),
            InlineKeyboardButton(text=f"📄 {page + 1}/{total}", callback_data="adm_noop"),
            InlineKeyboardButton(text="▶️", callback_data=f"adm_tkt:{user_id}:{next_pg}")
        ]])

    # Add "View in Channel" button if this ticket has a channel message linked
    channel_msg_id = ticket.get("channel_message_id")
    if channel_msg_id and REVIEW_LOG_CHANNEL:
        cid_str = str(abs(REVIEW_LOG_CHANNEL))
        if cid_str.startswith("100"):
            cid_str = cid_str[3:]
        view_url = f"https://t.me/c/{cid_str}/{channel_msg_id}"
        view_btn = InlineKeyboardButton(text="📺 View in Channel", url=view_url)
        if nav_kb:
            nav_kb.inline_keyboard.append([view_btn])
        else:
            nav_kb = InlineKeyboardMarkup(inline_keyboard=[[view_btn]])

    if isinstance(message_or_cb, types.Message):
        await message_or_cb.answer(response, reply_markup=nav_kb, parse_mode="Markdown")
        await message_or_cb.answer("Use options below or navigate history above:", reply_markup=get_support_management_menu())
    else:
        await message_or_cb.message.edit_text(response, reply_markup=nav_kb, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_tkt:"))
async def admin_ticket_search_callback(callback: types.CallbackQuery):
    """Handle pagination for admin ticket search"""
    try:
        parts = callback.data.split(":")
        uid = int(parts[1])
        page = int(parts[2])
        await show_admin_search_ticket_page(callback, uid, page)
        await callback.answer()
    except Exception as e:
        print(f"Error in admin ticket pagination: {e}")
        await callback.answer("Error loading page.", show_alert=True)

@dp.callback_query(F.data == "adm_noop")
async def admin_noop_callback(callback: types.CallbackQuery):
    await callback.answer()

# ==========================================
# 🗑️ DELETE TICKET
# ==========================================

@dp.message(F.text == "🗑️ DELETE")
async def delete_ticket_prompt(message: types.Message, state: FSMContext):
    """Delete spam or test tickets"""
    await state.set_state(SupportStates.waiting_for_delete_ticket_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🗑️ **DELETE TICKET**\n\n"
        "⚠️ **Warning:** This permanently deletes the ticket!\n\n"
        "Send **MSA+ ID** or **Telegram ID** to delete their most recent ticket.\n\n"
        "💡 Use this for spam/test tickets only.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_delete_ticket_id)
async def process_delete_ticket(message: types.Message, state: FSMContext):
    """Process ticket deletion"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip()
    
    # Find most recent ticket
    ticket = col_support_tickets.find_one(
        {"msa_id": search_id.upper()},
        sort=[("created_at", -1)]
    )
    
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one(
            {"user_id": int(search_id)},
            sort=[("created_at", -1)]
        )
    
    if not ticket:
        await message.answer(
            f"❌ **Ticket not found!**\n\n"
            f"No tickets found for ID: `{search_id}`",
            parse_mode="Markdown"
        )
        return
    
    user_name = ticket.get('user_name', 'Unknown')
    user_id = ticket.get('user_id')
    created = ticket.get('created_at', now_local())
    created_str = created.strftime("%B %d, %Y at %I:%M %p")
    
    # Delete the ticket
    result = col_support_tickets.delete_one({"_id": ticket["_id"]})
    
    await state.clear()
    
    if result.deleted_count > 0:
        await message.answer(
            f"🗑️ **Ticket Deleted!**\n\n"
            f"👤 User: {user_name}\n"
            f"🆔 User ID: `{user_id}`\n"
            f"📅 Created: {created_str}\n\n"
            f"✅ Ticket removed from database.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        print(f"🗑️ Deleted ticket for user {user_id}")
    else:
        await message.answer(
            "❌ **Failed to delete ticket.**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# 📊 MORE OPTIONS
# ==========================================

@dp.message(F.text == "📊 MORE OPTIONS")
async def more_options_handler(message: types.Message, state: FSMContext):
    """Show advanced support options"""
    await state.clear()
    await message.answer(
        "📊 **ADVANCED OPTIONS**\n\n"
        "Select an option:",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "⬅️ BACK TO SUPPORT")
async def back_to_support(message: types.Message, state: FSMContext):
    """Return to support menu"""
    await state.clear()
    pending_count = col_support_tickets.count_documents({"status": "open"})
    total_count = col_support_tickets.count_documents({})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    
    await message.answer(
        f"💬 **SUPPORT TICKET MANAGEMENT**\n\n"
        f"📊 **Statistics:**\n"
        f"⏳ Pending: **{pending_count}** tickets\n"
        f"✅ Resolved: **{resolved_count}** tickets\n"
        f"📋 Total: **{total_count}** tickets\n\n"
        f"**Select an action:**",
        reply_markup=get_support_management_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# 👁 VIEW CHANNEL
# ==========================================

@dp.message(F.text == "👁 VIEW CHANNEL")
async def view_support_channel_handler(message: types.Message, state: FSMContext):
    """Ask for user/MSA ID then show direct links to that user's channel messages."""
    if not await has_permission(message.from_user.id, "support"):
        return

    if not REVIEW_LOG_CHANNEL:
        await message.answer(
            "⚠️ **Support channel not configured.**\n\n"
            "Please set `REVIEW_LOG_CHANNEL` in the environment.",
            parse_mode="Markdown",
            reply_markup=get_support_management_menu()
        )
        return

    await state.set_state(SupportStates.waiting_for_view_channel_id)
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await message.answer(
        "👁 **VIEW MESSAGES IN SUPPORT CHANNEL**\n\n"
        "Enter the **MSA ID** (e.g. `MSA324935688`) or **Telegram User ID** to jump\n"
        "directly to that user’s ticket messages in the support channel.\n\n"
        "_I will show you a direct link for each message from that user._",
        parse_mode="Markdown",
        reply_markup=cancel_kb
    )


@dp.message(SupportStates.waiting_for_view_channel_id)
async def process_view_channel_id(message: types.Message, state: FSMContext):
    """Look up stored channel_message_ids for the user and return direct t.me links."""
    if message.text and message.text.strip() in ["\u274c CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_support_management_menu(), parse_mode="Markdown")
        return

    search_input = message.text.strip()

    # Build query
    if search_input.upper().startswith("MSA"):
        query = {"msa_id": search_input.upper()}
    elif search_input.isdigit():
        query = {"user_id": int(search_input)}
    else:
        query = {"user_name": {"$regex": f"^{search_input}$", "$options": "i"}}

    tickets = list(col_support_tickets.find(query).sort("created_at", -1))

    await state.clear()

    if not tickets:
        await message.answer(
            f"❌ **No tickets found** for `{search_input}`\n\n"
            "Check MSA ID or Telegram User ID and try again.",
            parse_mode="Markdown",
            reply_markup=get_support_management_menu()
        )
        return

    # Build channel base URL
    cid_str = str(abs(REVIEW_LOG_CHANNEL))
    if cid_str.startswith("100"):
        cid_str = cid_str[3:]

    # Collect tickets that have a stored channel message ID
    linked = [(t, t.get("channel_message_id")) for t in tickets if t.get("channel_message_id")]
    no_link = [t for t in tickets if not t.get("channel_message_id")]

    first = tickets[0]
    user_name  = first.get("user_name", "Unknown")
    user_id    = first.get("user_id", "?")
    msa_id     = first.get("msa_id", "N/A")
    username   = first.get("username", "none")

    header = (
        f"👁 **CHANNEL MESSAGES — {user_name}**\n"
        f"🆔 MSA: `{msa_id}` · TG: `{user_id}` · @{username}\n"
        f"📊 Total tickets: {len(tickets)} · 🔗 With channel link: {len(linked)}\n\n"
        "Click a button below to jump directly to that message in the support channel:"
    )

    if not linked:
        await message.answer(
            header + "\n\n⚠️ No channel message links stored for this user.\n"
            "_Old tickets submitted before message tracking was added have no link._",
            parse_mode="Markdown",
            reply_markup=get_support_management_menu()
        )
        return

    # Build one inline button per linked ticket (max 40 buttons to stay safe)
    buttons = []
    for t, msg_id in linked[:40]:
        created  = t.get("created_at", now_local())
        date_str = created.strftime("%b %d %I:%M %p")
        status   = "✅" if t.get("status") == "resolved" else "⏳"
        label    = f"{status} {date_str} — #{t.get('support_count', '?')}"
        url      = f"https://t.me/c/{cid_str}/{msg_id}"
        buttons.append([InlineKeyboardButton(text=label, url=url)])

    if no_link:
        header += f"\n\n⚠️ {len(no_link)} older ticket(s) have no channel link stored."

    nav_kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer(header, parse_mode="Markdown", reply_markup=nav_kb)
    await message.answer(
        "_Each button above opens that exact message in the support channel._",
        parse_mode="Markdown",
        reply_markup=get_support_management_menu()
    )


# ==========================================
# 📈 STATISTICS
# ==========================================

@dp.message(F.text == "📈 STATISTICS")
async def statistics_handler(message: types.Message, state: FSMContext):
    """Show advanced ticket statistics"""
    await state.clear()
    
    # Overall stats
    total = col_support_tickets.count_documents({})
    open_count = col_support_tickets.count_documents({"status": "open"})
    resolved = col_support_tickets.count_documents({"status": "resolved"})
    
    # Today's stats
    today_start = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    today_tickets = col_support_tickets.count_documents({"created_at": {"$gte": today_start}})
    today_resolved = col_support_tickets.count_documents({
        "status": "resolved",
        "resolved_at": {"$gte": today_start}
    })
    
    # Most active users
    pipeline = [
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}, "user_name": {"$first": "$user_name"}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_users = list(col_support_tickets.aggregate(pipeline))
    
    # Average resolution time (for resolved tickets)
    resolved_tickets = list(col_support_tickets.find({
        "status": "resolved",
        "resolved_at": {"$exists": True}
    }).limit(50))
    
    if resolved_tickets:
        resolution_times = []
        for ticket in resolved_tickets:
            created = ticket.get('created_at')
            resolved_at = ticket.get('resolved_at')
            if created and resolved_at:
                delta = (resolved_at - created).total_seconds() / 3600  # hours
                resolution_times.append(delta)
        
        avg_time = sum(resolution_times) / len(resolution_times) if resolution_times else 0
        avg_hours = int(avg_time)
        avg_minutes = int((avg_time - avg_hours) * 60)
    else:
        avg_hours = avg_minutes = 0
    
    response = f"📈 **SUPPORT STATISTICS**\n\n"
    response += f"📊 **Overall:**\n"
    response += f"📋 Total Tickets: {total}\n"
    response += f"⏳ Open: {open_count}\n"
    response += f"✅ Resolved: {resolved}\n"
    response += f"📊 Resolution Rate: {(resolved/total*100):.1f}%\n\n"
    
    response += f"📅 **Today:**\n"
    response += f"🆕 New Tickets: {today_tickets}\n"
    response += f"✅ Resolved: {today_resolved}\n\n"
    
    response += f"⏱️ **Performance:**\n"
    response += f"Avg Resolution Time: {avg_hours}h {avg_minutes}m\n\n"
    
    response += f"👥 **Top 5 Users:**\n"
    for i, user in enumerate(top_users, 1):
        response += f"{i}. {user['user_name']} - {user['count']} tickets\n"
    
    await message.answer(
        response,
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# 🚨 PRIORITY SYSTEM
# ==========================================

@dp.message(F.text == "🚨 PRIORITY")
async def priority_prompt(message: types.Message, state: FSMContext):
    """Set ticket priority"""
    await state.set_state(SupportStates.waiting_for_priority_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚨 **SET PRIORITY**\n\n"
        "Send **MSA+ ID** or **Telegram ID** to set priority for their open ticket.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_priority_id)
async def process_priority_id(message: types.Message, state: FSMContext):
    """Get ticket for priority setting"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_support_more_menu(), parse_mode="Markdown")
        return
    
    search_id = message.text.strip()
    ticket = col_support_tickets.find_one({"msa_id": search_id.upper(), "status": "open"})
    
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one({"user_id": int(search_id), "status": "open"})
    
    if not ticket:
        await message.answer(
            f"❌ **No open ticket found for:** `{search_id}`",
            parse_mode="Markdown"
        )
        return
    
    await state.update_data(priority_ticket_id=str(ticket["_id"]))
    await state.set_state(SupportStates.waiting_for_priority_level)
    
    priority_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔴 URGENT"), KeyboardButton(text="🟠 HIGH")],
            [KeyboardButton(text="🟡 NORMAL"), KeyboardButton(text="🟢 LOW")],
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🚨 **Set priority for {ticket.get('user_name')}**\n\n"
        f"Select priority level:",
        reply_markup=priority_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_priority_level)
async def process_priority_level(message: types.Message, state: FSMContext):
    """Set the priority level"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_support_more_menu(), parse_mode="Markdown")
        return
    
    priority_map = {
        "🔴 URGENT": "urgent",
        "🟠 HIGH": "high",
        "🟡 NORMAL": "normal",
        "🟢 LOW": "low"
    }
    
    priority = priority_map.get(message.text)
    if not priority:
        await message.answer("⚠️ **Invalid priority!** Select from buttons.", parse_mode="Markdown")
        return
    
    data = await state.get_data()
    ticket_id = data.get('priority_ticket_id')
    
    result = col_support_tickets.update_one(
        {"_id": ObjectId(ticket_id)},
        {"$set": {"priority": priority}}
    )
    
    await state.clear()
    
    if result.modified_count > 0:
        await message.answer(
            f"✅ **Priority set to {message.text}**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "❌ **Failed to set priority.**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# ⏰ AUTO-CLOSE OLD TICKETS
# ==========================================

@dp.message(F.text == "⏰ AUTO-CLOSE")
async def auto_close_handler(message: types.Message, state: FSMContext):
    """Auto-close tickets older than 7 days"""
    await state.clear()
    
    # Find tickets older than 7 days
    seven_days_ago = now_local() - timedelta(days=7)
    old_tickets = list(col_support_tickets.find({
        "status": "open",
        "created_at": {"$lt": seven_days_ago}
    }))
    
    if not old_tickets:
        await message.answer(
            "✅ **No old tickets to close!**\n\n"
            "All open tickets are less than 7 days old.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Auto-close them
    closed_count = 0
    for ticket in old_tickets:
        user_id = ticket.get('user_id')
        user_name = ticket.get('user_name', 'User')
        
        # Update database
        col_support_tickets.update_one(
            {"_id": ticket["_id"]},
            {"$set": {"status": "resolved", "resolved_at": now_local(), "auto_closed": True}}
        )
        
        # Notify user
        try:
            await bot_1.send_message(
                user_id,
                f"⏰ **Ticket Auto-Closed**\n\n"
                f"Hi {user_name},\n\n"
                f"Your support ticket has been automatically closed after 7 days.\n\n"
                f"If you still need help, please submit a new ticket using **📞 SUPPORT**.\n\n"
                f"💎 **MSA NODE Team**",
                parse_mode="Markdown"
            )
        except:
            pass
        
        closed_count += 1
    
    await message.answer(
        f"✅ **Auto-closed {closed_count} old tickets!**\n\n"
        f"All tickets older than 7 days have been resolved and users notified.",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )
    print(f"⏰ Auto-closed {closed_count} tickets older than 7 days")

# ==========================================
# 📤 EXPORT REPORT
# ==========================================

@dp.message(F.text == "📤 EXPORT")
async def export_handler(message: types.Message, state: FSMContext):
    """Export tickets to CSV file"""
    await state.clear()
    
    import csv
    import io
    
    # Get all tickets
    tickets = list(col_support_tickets.find({}))
    
    if not tickets:
        await message.answer(
            "❌ **No tickets to export!**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Headers
    writer.writerow([
        'User ID', 'Name', 'Username', 'MSA+ ID', 'Issue', 
        'Status', 'Created', 'Resolved', 'Priority', 'Support Count'
    ])
    
    # Data
    for ticket in tickets:
        writer.writerow([
            ticket.get('user_id', ''),
            ticket.get('user_name', ''),
            ticket.get('username', ''),
            ticket.get('msa_id', ''),
            ticket.get('issue_text', '')[:100],
            ticket.get('status', ''),
            ticket.get('created_at', ''),
            ticket.get('resolved_at', ''),
            ticket.get('priority', 'normal'),
            ticket.get('support_count', 1)
        ])
    
    # Convert to bytes
    csv_bytes = output.getvalue().encode('utf-8')
    
    # Create filename with timestamp
    filename = f"support_tickets_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Send as document
    from aiogram.types import BufferedInputFile
    file = BufferedInputFile(csv_bytes, filename=filename)
    
    await message.answer_document(
        file,
        caption=f"📤 **Support Tickets Export**\n\n"
                f"📋 Total Tickets: {len(tickets)}\n"
                f"📅 Generated: {now_local().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="Markdown"
    )
    
    await message.answer(
        "✅ **Export complete!**",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )
    print(f"📤 Exported {len(tickets)} tickets to CSV")


# =============================================================================
# BACKUP SYSTEM — COMPLETE IMPLEMENTATION (v4)
# Menu: Download, Upload, GDrive, Status, History, Activate TTL, Reset, Main
# All data from/to MSANodeBackups cluster — Main DB (MSANodeDB) NEVER touched.
# =============================================================================

# ─── Pagination helper ───────────────────────────────────────────────────────

_PAGE_CHAR_LIMIT = 3600  # stay safely under Telegram's 4096 limit

def _paginate_text(text: str, page: int = 0, limit: int = _PAGE_CHAR_LIMIT) -> tuple:
    """
    Split long text into pages of ≤ limit chars.
    Returns (page_text: str, total_pages: int, current_page: int).
    page is 0-indexed.
    """
    if len(text) <= limit:
        return text, 1, 0

    # Split by line to avoid cutting mid-line
    lines  = text.splitlines(keepends=True)
    pages  = []
    chunk  = ""
    for line in lines:
        if len(chunk) + len(line) > limit and chunk:
            pages.append(chunk)
            chunk = line
        else:
            chunk += line
    if chunk:
        pages.append(chunk)

    total  = len(pages)
    page   = max(0, min(page, total - 1))
    return pages[page], total, page


def _pager_keyboard(page: int, total: int, extra_rows=None) -> ReplyKeyboardMarkup:
    """Build a keyboard with Prev/Next page buttons + standard Back/Main Menu nav."""
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text=f"⬅️ PREV PAGE"))
    if page < total - 1:
        nav_row.append(KeyboardButton(text=f"NEXT PAGE ➡️"))

    rows = []
    if nav_row:
        rows.append(nav_row)
    if extra_rows:
        rows.extend(extra_rows)
    rows.append([KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def _send_paged(
    message,
    full_text: str,
    page: int = 0,
    state=None,
    page_key: str = "page",
    extra_rows=None,
    parse_mode: str = "HTML",
):
    """
    Send or edit a paginated message.
    Stores current page in FSM state if state provided.
    Returns (page_content, total_pages, current_page).
    """
    content, total, cur = _paginate_text(full_text, page)
    if total > 1:
        header = f"<i>Page {cur+1} of {total}</i>\n\n"
        content = header + content
    kb = _pager_keyboard(cur, total, extra_rows)
    await message.answer(content, reply_markup=kb, parse_mode=parse_mode)
    if state:
        await state.update_data(**{page_key: cur, f"{page_key}_total": total,
                                   f"{page_key}_text": full_text})
    return content, total, cur

# ─── Backup helper keyboards ──────────────────────────────────────────────────

def _bk_nav(extra_rows=None):
    """Standard inline nav row for backup sub-flows."""
    rows = (extra_rows or []) + [[KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _bk_cancel():
    """Cancel-only keyboard (for awaiting-file or typed-input states)."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )


def _bk_bot_select(title: str = "Select bot:") -> tuple:
    """Bot1/Bot2 selection keyboard + standard nav. Returns (text, keyboard)."""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 BOT 1"), KeyboardButton(text="🤖 BOT 2")],
            [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
        ],
        resize_keyboard=True
    )
    return title, kb


def _fetch_day_list(bot_name: str) -> list:
    """
    Fetch all daily cluster snapshots for bot_name from MSANodeBackups.
    Returns list sorted newest-first, each entry:
      { window_key, week_label, month_label, year, month, docs, display }
    """
    bkp_uri     = BACKUP_MONGO_URI or MONGO_URI
    bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
    try:
        bkp_client = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[bkp_db_name]
        col        = bkp_db[f"bot{bot_name[-1]}_backups"]
        cursor     = col.find(
            {"bot": bot_name},
            {"window_key": 1, "week_label": 1, "month_label": 1,
             "year": 1, "month": 1, "docs": 1, "_id": 0}
        ).sort("window_key", -1)
        results = []
        for doc in cursor:
            wk          = doc.get("window_key", "")
            week_label  = doc.get("week_label", "")
            month_label = doc.get("month_label", "")
            year        = doc.get("year", "")
            month_n     = doc.get("month", 0)
            # Back-fill from window_key for legacy docs (no week/month fields)
            if not week_label or not month_label:
                try:
                    from datetime import datetime as _dt
                    dt          = _dt.strptime(wk, "%Y-%m-%d")
                    week_num    = (dt.day - 1) // 7 + 1
                    week_label  = f"Week {week_num}"
                    month_label = dt.strftime("%B %Y")
                    year        = dt.year
                    month_n     = dt.month
                except Exception:
                    week_label  = "Week ?"
                    month_label = "Unknown"
                    year        = "?"
            results.append({
                "window_key":  wk,
                "week_label":  week_label,
                "month_label": month_label,
                "year":        str(year),
                "month":       month_n,
                "docs":        doc.get("docs", 0),
                "display":     f"{year} — {month_label} — {week_label} — {wk}",
            })
        bkp_client.close()
        return results
    except Exception as e:
        logger.error(f"[BACKUP] _fetch_day_list failed for {bot_name}: {e}")
        return []


def _build_cumulative_zip(bot_name: str, from_key: str, to_key: str) -> tuple:
    """
    Build ZIP of all cluster snapshots for bot_name from from_key to to_key (both inclusive).
    Downloads all matching docs, merges & deduplicates by _id, packages as ZIP.
    Returns (zip_bytes: bytes, filename: str) or (None, None).
    """
    bkp_uri     = BACKUP_MONGO_URI or MONGO_URI
    bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
    try:
        bkp_client = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=15000)
        bkp_db     = bkp_client[bkp_db_name]
        col        = bkp_db[f"bot{bot_name[-1]}_backups"]
        records    = list(col.find(
            {"bot": bot_name, "window_key": {"$gte": from_key, "$lte": to_key}},
        ).sort("window_key", 1))  # oldest first → newest overwrites on merge
        bkp_client.close()

        if not records:
            return None, None

        # Merge & deduplicate (newest snapshot wins per _id)
        merged: dict = {}
        for rec in records:
            for col_name, col_data in rec.get("data", {}).items():
                docs = col_data.get("documents", [])
                if col_name not in merged:
                    merged[col_name] = {}
                for doc in docs:
                    if not isinstance(doc, dict):
                        continue
                    doc_id = str(doc.get("_id", id(doc)))
                    merged[col_name][doc_id] = doc

        # Package into ZIP
        buf = io.BytesIO()
        total_docs = 0
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for col_name_out, id_map in merged.items():
                docs_list = list(id_map.values())
                total_docs += len(docs_list)
                payload = json.dumps(docs_list, default=str, indent=2, ensure_ascii=False)
                zf.writestr(f"{col_name_out}.json", payload)
            meta = {
                "bot":         bot_name,
                "from":        from_key,
                "to":          to_key,
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "total_docs":  total_docs,
                "source":      "MSANodeBackups cluster — cumulative snapshot merge",
            }
            zf.writestr("_metadata.json", json.dumps(meta, indent=2))
        zip_bytes = buf.getvalue()
        filename  = f"{bot_name}_backup_{from_key}_to_{to_key}.zip"
        return zip_bytes, filename
    except Exception as e:
        logger.error(f"[BACKUP] _build_cumulative_zip failed: {e}")
        return None, None


def _format_day_list_text(bot_name: str, days: list, title: str) -> str:
    """Format indexed day list for display in chat."""
    hdr  = f"{title} — {bot_name.upper()}\n\n"
    if not days:
        return hdr + "📭 No backup snapshots found in MSANodeBackups.\n\nRun 🔥 FORCE BACKUP NOW to create the first snapshot."
    lines = ""
    for i, d in enumerate(days, 1):
        lines += f"  <b>{i}.</b>  {d['display']}  <i>({d['docs']:,} docs)</i>\n"
    hdr += lines
    hdr += "\n💬 Type an <b>index number</b> to select."
    return hdr


# =============================================================================
# MAIN BACKUP MENU HANDLER
# =============================================================================

@dp.message(F.text == "💾 BACKUP")
async def backup_handler(message: types.Message, state: FSMContext):
    """Backup system main menu."""
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.clear()
    log_action("BACKUP SYSTEM", message.from_user.id, "Accessed backup management")

    # Live prod counts (graceful degrade)
    try:
        live_msa     = col_msa_ids.count_documents({})
        live_verif   = col_user_verification.count_documents({})
        live_track   = col_user_tracking.count_documents({})
        live_bcast   = col_broadcasts.count_documents({})
        live_tickets = col_support_tickets.count_documents({})
    except Exception:
        live_msa = live_verif = live_track = live_bcast = live_tickets = 0

    # Backup cluster snapshot counts (graceful degrade)
    bk_b1 = bk_b2 = "N/A"
    last_b1 = last_b2 = "Never"
    try:
        bk_b1   = col_bot1_backups.count_documents({})
        bk_b2   = col_bot2_backups.count_documents({})
        lb1     = col_bot1_backups.find_one({}, sort=[("backup_date", -1)])
        lb2     = col_bot2_backups.find_one({}, sort=[("backup_date", -1)])
        last_b1 = format_datetime(lb1["backup_date"]) if lb1 else "Never"
        last_b2 = format_datetime(lb2["backup_date"]) if lb2 else "Never"
    except Exception:
        bk_b1 = bk_b2 = "⚠️ Offline"
        last_b1 = last_b2 = "Unavailable"

    msg = (
        "<b>BACKUP MANAGEMENT</b>\n\n"
        "<b>Bot 1 — Live:</b>\n"
        f"  MSA IDs: {live_msa:,}  |  Verified: {live_verif:,}  |  Tracked: {live_track:,}\n"
        f"  Cluster snapshots: {bk_b1}  |  Last: {last_b1}\n\n"
        "<b>Bot 2 — Live:</b>\n"
        f"  Broadcasts: {live_bcast:,}  |  Tickets: {live_tickets:,}\n"
        f"  Cluster snapshots: {bk_b2}  |  Last: {last_b2}\n\n"
        "<b>Storage:</b> MSANodeBackups cluster\n"
        "<b>Schedule:</b> Daily at 23:59 UTC  |  Auto ZIP last-day of month\n"
        "<b>GDrive:</b> Manual via ☁️ GDRIVE SYSTEM\n"
        "<b>TTL:</b> Manual via ⏳ ACTIVATE TTL\n"
    )
    await message.answer(msg, reply_markup=get_backup_menu(), parse_mode="HTML")


# =============================================================================
# FORCE BACKUP NOW
# =============================================================================

@dp.message(F.text == "🔥 FORCE BACKUP NOW")
async def force_backup_now(message: types.Message, state: FSMContext):
    """Immediately snapshot Bot 1 + Bot 2 live data → MSANodeBackups cluster."""
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.clear()
    status_msg = await message.answer(
        "<b>FORCE BACKUP STARTED</b>\n\nSnapshotting Bot 1 + Bot 2 live data...",
        parse_mode="HTML", reply_markup=get_backup_menu()
    )
    try:
        loop = asyncio.get_event_loop()

        def _run_bot1():
            return force_backup_to_cluster(
                "bot1", MONGO_URI, MONGO_DB_NAME,
                backup_mongo_uri=BACKUP_MONGO_URI, backup_db_name=BACKUP_MONGO_DB_NAME or "MSANodeBackups"
            )

        def _run_bot2():
            return force_backup_to_cluster(
                "bot2", MONGO_URI, MONGO_DB_NAME,
                backup_mongo_uri=BACKUP_MONGO_URI, backup_db_name=BACKUP_MONGO_DB_NAME or "MSANodeBackups"
            )

        res1 = await loop.run_in_executor(None, _run_bot1)
        res2 = await loop.run_in_executor(None, _run_bot2)

        # Also write restore snapshots
        def _write_restore_snapshots():
            from datetime import datetime as _dt2, timezone as _tz2
            now2 = _dt2.now(_tz2.utc)
            try:
                prod_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
                prod_db2    = prod_client[MONGO_DB_NAME]
                for bname, rcol in [("bot1", col_bot1_restore_data), ("bot2", col_bot2_restore_data)]:
                    cols  = _export_bot_collections(prod_db2, bname)
                    total = sum(v.get("count", 0) for v in cols.values())
                    rcol.replace_one(
                        {"_id": f"{bname}_latest"},
                        {"_id": f"{bname}_latest", "backup_date": now2,
                         "total_records": total, "collections": cols},
                        upsert=True
                    )
                prod_client.close()
            except Exception as _e:
                logger.warning(f"[BACKUP] Restore snapshot write failed: {_e}")

        await loop.run_in_executor(None, _write_restore_snapshots)

        def s(r): return r.get("status", "error")
        def d(r): return r.get("docs", 0)

        await status_msg.edit_text(
            f"<b>FORCE BACKUP COMPLETE</b>\n\n"
            f"Bot 1: {'OK' if s(res1)=='ok' else 'FAIL'}  ({d(res1):,} docs)\n"
            f"Bot 2: {'OK' if s(res2)=='ok' else 'FAIL'}  ({d(res2):,} docs)\n\n"
            f"Restore snapshots updated.\n"
            f"Cluster: <code>MSANodeBackups</code>",
            parse_mode="HTML"
        )
        log_action("FORCE BACKUP", message.from_user.id,
                   f"Bot1:{s(res1)}({d(res1)}docs) Bot2:{s(res2)}({d(res2)}docs)")
        try:
            col_backup_history.insert_one({
                "bot": "all", "action": "Force Backup",
                "details": f"Bot1:{s(res1)} {d(res1)}docs | Bot2:{s(res2)} {d(res2)}docs",
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception:
            pass

    except Exception as e:
        await status_msg.edit_text(
            f"<b>FORCE BACKUP FAILED</b>\n\n<code>{str(e)[:300]}</code>",
            parse_mode="HTML"
        )


# =============================================================================
# DOWNLOAD BACKUP — Indexed daily list, cumulative ZIP (day1 → selected day)
# =============================================================================

class _DLStates(StatesGroup):
    bot_select  = State()
    day_index   = State()

@dp.message(F.text == "💾 DOWNLOAD BACKUP")
async def download_backup_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_DLStates.bot_select)
    title, kb = _bk_bot_select("💾 <b>DOWNLOAD BACKUP</b>\n\nSelect bot:")
    await message.answer(title, reply_markup=kb, parse_mode="HTML")

@dp.message(_DLStates.bot_select)
async def dl_bot_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return
    if "BOT 1" not in txt and "BOT 2" not in txt:
        await message.answer("Please select BOT 1 or BOT 2."); return

    bot_name = "bot1" if "BOT 1" in txt else "bot2"
    wait_msg = await message.answer(
        f"Loading backup list for {bot_name.upper()}...", reply_markup=_bk_cancel()
    )

    loop = asyncio.get_event_loop()
    days = await loop.run_in_executor(None, _fetch_day_list, bot_name)

    try: await wait_msg.delete()
    except Exception: pass

    await state.update_data(dl_bot=bot_name, dl_days=days)
    await state.set_state(_DLStates.day_index)

    list_text = _format_day_list_text(bot_name, days, "💾 DOWNLOAD BACKUP")
    await _send_paged(message, list_text, page=0, state=state, page_key="dl_page")

@dp.message(_DLStates.day_index)
async def dl_index_entered(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip()
    tu  = txt.upper()
    if "MAIN MENU" in tu:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in tu or "CANCEL" in tu:
        await state.clear()
        return await download_backup_start(message, state)

    data     = await state.get_data()
    bot_name = data.get("dl_bot", "bot1")
    days     = data.get("dl_days", [])
    dl_text  = data.get("dl_page_text", "")
    dl_page  = data.get("dl_page", 0)
    dl_total = data.get("dl_page_total", 1)

    # Pagination nav
    if txt == "⬅️ PREV PAGE" and dl_page > 0:
        await _send_paged(message, dl_text, dl_page - 1, state=state, page_key="dl_page"); return
    if txt == "NEXT PAGE ➡️" and dl_page < dl_total - 1:
        await _send_paged(message, dl_text, dl_page + 1, state=state, page_key="dl_page"); return

    if not txt.isdigit():
        await message.answer("Type a <b>number</b> from the list.", parse_mode="HTML"); return
    idx = int(txt) - 1
    if idx < 0 or idx >= len(days):
        await message.answer(f"Enter a number between 1 and {len(days)}."); return

    selected = days[idx]
    to_key   = selected["window_key"]
    # Find from_key = first day of the same month
    month_n  = selected.get("month", 0)
    year_s   = selected.get("year", "")
    try:
        from_key = f"{year_s}-{int(month_n):02d}-01"
    except Exception:
        from_key = to_key

    await state.clear()
    status_msg = await message.answer(
        f"Building ZIP: <code>{bot_name} {from_key} → {to_key}</code>\n"
        "Fetching from MSANodeBackups cluster...",
        parse_mode="HTML", reply_markup=get_backup_menu()
    )

    loop = asyncio.get_event_loop()

    def _build():
        return _build_cumulative_zip(bot_name, from_key, to_key)

    try:
        zip_bytes, filename = await loop.run_in_executor(None, _build)
        if not zip_bytes:
            await status_msg.edit_text(
                f"No data found for <code>{bot_name}</code> in range "
                f"<code>{from_key}</code> → <code>{to_key}</code>.\n\n"
                "Run 🔥 FORCE BACKUP NOW to create snapshots first.",
                parse_mode="HTML"
            )
            return
        size_kb = len(zip_bytes) / 1024
        from aiogram.types import BufferedInputFile
        await message.answer_document(
            BufferedInputFile(zip_bytes, filename=filename),
            caption=(
                f"<b>BACKUP — {bot_name.upper()}</b>\n"
                f"Range: <code>{from_key}</code> to <code>{to_key}</code>\n"
                f"Size: {size_kb:.1f} KB\n"
                f"JSON files named by collection inside ZIP.\n"
                f"Source: MSANodeBackups cluster (cumulative, deduplicated)"
            ),
            parse_mode="HTML"
        )
        await status_msg.delete()
        log_action("DOWNLOAD BACKUP", message.from_user.id,
                   f"{bot_name} {from_key}→{to_key} ({size_kb:.0f}KB)")
        try:
            col_backup_history.insert_one({
                "bot": bot_name, "action": "Download Backup (Cumulative ZIP)",
                "details": f"{from_key} → {to_key} | {size_kb:.1f} KB",
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception: pass
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu())
    except Exception as e:
        await status_msg.edit_text(
            f"<b>Download failed:</b>\n<code>{str(e)[:300]}</code>", parse_mode="HTML"
        )
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# UPLOAD BACKUP — Send ZIP → choose Main DB or Backup DB → upsert all docs
# =============================================================================

class _ULStates(StatesGroup):
    db_choice    = State()
    awaiting_zip = State()

@dp.message(F.text == "📤 UPLOAD BACKUP")
async def upload_backup_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_ULStates.db_choice)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📦 MAIN DB (MSANodeDB)"), KeyboardButton(text="💾 BACKUP DB (MSANodeBackups)")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await message.answer(
        "<b>UPLOAD BACKUP</b>\n\nChoose target database:\n\n"
        "• <b>MAIN DB</b> — writes to MSANodeDB (live production)\n"
        "• <b>BACKUP DB</b> — writes to MSANodeBackups (backup cluster)\n\n"
        "Accepts ZIP files from 💾 DOWNLOAD BACKUP.\n"
        "Duplicates are <b>overwritten</b> (upsert by <code>_id</code>).",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_ULStates.db_choice)
async def upload_db_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return
    if "MAIN DB" in txt:
        target_db = "main"
    elif "BACKUP DB" in txt:
        target_db = "backup"
    else:
        await message.answer("Please select MAIN DB or BACKUP DB."); return

    await state.update_data(ul_target_db=target_db)
    await state.set_state(_ULStates.awaiting_zip)
    db_label = "MSANodeDB (production)" if target_db == "main" else "MSANodeBackups (backup cluster)"
    await message.answer(
        f"Target DB: <b>{db_label}</b>\n\n"
        "Send a <b>.zip</b> file (from 💾 DOWNLOAD BACKUP).\n"
        "All collections found inside will be upserted.\n"
        "Press ❌ CANCEL to abort.",
        reply_markup=_bk_cancel(), parse_mode="HTML"
    )

@dp.message(_ULStates.awaiting_zip)
async def upload_zip_received(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if txt in ("❌ CANCEL", "CANCEL"):
        await state.clear()
        await message.answer("Upload cancelled.", reply_markup=get_backup_menu()); return

    if not message.document:
        await message.answer("Please send a <b>.zip</b> file.", parse_mode="HTML"); return
    fname = message.document.file_name or ""
    if not fname.lower().endswith(".zip"):
        await message.answer("Only <b>.zip</b> files are accepted.", parse_mode="HTML"); return

    data      = await state.get_data()
    target_db = data.get("ul_target_db", "main")
    await state.clear()

    status_msg = await message.answer(
        "Downloading file...", reply_markup=get_backup_menu()
    )

    try:
        # Download the file
        file_info  = await message.bot.get_file(message.document.file_id)
        file_bytes = await message.bot.download_file(file_info.file_path)
        raw_bytes  = file_bytes.read() if hasattr(file_bytes, "read") else bytes(file_bytes)

        await status_msg.edit_text("Parsing ZIP contents...")

        # Parse ZIP
        buf = io.BytesIO(raw_bytes)
        extracted: dict = {}  # col_name → [docs]
        with zipfile.ZipFile(buf, "r") as zf:
            for zname in zf.namelist():
                if not zname.endswith(".json") or zname == "_metadata.json":
                    continue
                col_name = zname.rstrip("/").split("/")[-1].replace(".json", "")
                try:
                    data_raw = json.loads(zf.read(zname).decode("utf-8"))
                    if isinstance(data_raw, list):
                        extracted[col_name] = data_raw
                    elif isinstance(data_raw, dict) and "documents" in data_raw:
                        extracted[col_name] = data_raw["documents"]
                except Exception:
                    continue

        if not extracted:
            await status_msg.edit_text(
                "No valid collection JSON files found in ZIP.\n"
                "Make sure the ZIP was created by this bot's backup system.",
                parse_mode="HTML"
            ); return

        await status_msg.edit_text(f"Upserting {len(extracted)} collection(s)...")

        # Target database
        def _do_upsert():
            from bson import ObjectId as _ObjId
            if target_db == "main":
                target = db  # MSANodeDB
            else:
                bkp_client = _backup_mongo_client(BACKUP_MONGO_URI or MONGO_URI, serverSelectionTimeoutMS=15000)
                target = bkp_client[BACKUP_MONGO_DB_NAME or "MSANodeBackups"]

            results = {}
            total_upserted = total_matched = 0
            for col_name, docs in extracted.items():
                target_col = target[col_name]
                ins = mat = 0
                for doc in docs:
                    if not isinstance(doc, dict):
                        continue
                    raw_id = doc.get("_id")
                    if raw_id is not None:
                        try:
                            doc["_id"] = _ObjId(raw_id)
                        except Exception:
                            pass
                    try:
                        r = target_col.replace_one(
                            {"_id": doc["_id"]} if "_id" in doc else doc,
                            doc, upsert=True
                        )
                        if r.upserted_id: ins += 1
                        else:             mat += 1
                    except Exception:
                        pass
                results[col_name] = (ins, mat)
                total_upserted += ins
                total_matched  += mat

            if target_db == "backup":
                try: bkp_client.close()
                except Exception: pass

            return total_upserted, total_matched, results

        loop = asyncio.get_event_loop()
        total_ins, total_mat, col_results = await loop.run_in_executor(None, _do_upsert)

        db_label = "MSANodeDB" if target_db == "main" else "MSANodeBackups"
        col_lines = "\n".join(
            f"  • <code>{c}</code>: +{ins} new, ~{mat} updated"
            for c, (ins, mat) in col_results.items()
        )
        await status_msg.edit_text(
            f"<b>UPLOAD COMPLETE</b>\n\n"
            f"Target: <code>{db_label}</code>\n"
            f"File: <code>{fname}</code>\n"
            f"Collections: {len(col_results)}\n"
            f"New records: +{total_ins:,}\n"
            f"Updated records: ~{total_mat:,}\n\n"
            f"<b>Details:</b>\n{col_lines}",
            parse_mode="HTML"
        )
        log_action("UPLOAD BACKUP", message.from_user.id,
                   f"→{db_label} | +{total_ins} ins ~{total_mat} upd | {fname}")
        try:
            col_backup_history.insert_one({
                "bot": "all", "action": f"Upload Backup → {db_label}",
                "details": f"{fname} | +{total_ins} inserted ~{total_mat} updated",
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception: pass

    except Exception as e:
        await status_msg.edit_text(
            f"<b>Upload failed:</b>\n<code>{str(e)[:300]}</code>", parse_mode="HTML"
        )
    await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# GDRIVE SYSTEM — Indexed daily list → select index → upload full month till yesterday
# =============================================================================

class _GDStates(StatesGroup):
    bot_select  = State()
    day_index   = State()
    confirm     = State()

@dp.message(F.text == "☁️ GDRIVE SYSTEM")
async def gdrive_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_GDStates.bot_select)
    title, kb = _bk_bot_select("☁️ <b>GDRIVE SYSTEM</b>\n\nSelect bot:")
    await message.answer(title, reply_markup=kb, parse_mode="HTML")

@dp.message(_GDStates.bot_select)
async def gdrive_bot_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return
    if "BOT 1" not in txt and "BOT 2" not in txt:
        await message.answer("Please select BOT 1 or BOT 2."); return

    bot_name = "bot1" if "BOT 1" in txt else "bot2"
    wait_msg = await message.answer(f"Loading backup list for {bot_name.upper()}...", reply_markup=_bk_cancel())
    loop = asyncio.get_event_loop()
    days = await loop.run_in_executor(None, _fetch_day_list, bot_name)
    try: await wait_msg.delete()
    except Exception: pass

    await state.update_data(gd_bot=bot_name, gd_days=days)
    await state.set_state(_GDStates.day_index)

    # Find yesterday's key (last backup date)
    yesterday_key = (datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - __import__("datetime").timedelta(days=1)).strftime("%Y-%m-%d")

    list_text = _format_day_list_text(bot_name, days, "☁️ GDRIVE SYSTEM")
    list_text += (
        f"\n\n<i>Selecting any index uploads the <b>full month</b> (day 1 → yesterday "
        f"<code>{yesterday_key}</code>) to GDrive.</i>"
    )
    await _send_paged(message, list_text, page=0, state=state, page_key="gd_page")

@dp.message(_GDStates.day_index)
async def gdrive_index_entered(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip()
    tu  = txt.upper()
    if "MAIN MENU" in tu:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in tu or "CANCEL" in tu:
        await state.clear()
        return await gdrive_start(message, state)

    data     = await state.get_data()
    bot_name = data.get("gd_bot", "bot1")
    days     = data.get("gd_days", [])

    if not txt.isdigit():
        await message.answer("Type a <b>number</b> from the list.", parse_mode="HTML"); return
    idx = int(txt) - 1
    if idx < 0 or idx >= len(days):
        await message.answer(f"Enter a number between 1 and {len(days)}."); return

    selected    = days[idx]
    month_label = selected.get("month_label", "")
    month_n     = selected.get("month", 0)
    year_s      = selected.get("year", "")

    # Full month range: from_key = YYYY-MM-01, to_key = yesterday
    try:
        from_key     = f"{year_s}-{int(month_n):02d}-01"
        from datetime import timedelta as _td2
        yesterday_dt = datetime.now(timezone.utc) - _td2(days=1)
        yesterday_key = yesterday_dt.strftime("%Y-%m-%d")
        # Cap to_key at last day of the selected month if yesterday is in a later month
        import calendar as _cal
        _, last_day = _cal.monthrange(int(year_s), int(month_n))
        last_day_key = f"{year_s}-{int(month_n):02d}-{last_day:02d}"
        to_key = min(yesterday_key, last_day_key)
    except Exception:
        from_key = to_key = selected["window_key"]
        yesterday_key = to_key

    await state.update_data(
        gd_from_key=from_key, gd_to_key=to_key,
        gd_month_label=month_label, gd_year=year_s, gd_month_n=month_n
    )
    await state.set_state(_GDStates.confirm)

    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM UPLOAD")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await message.answer(
        f"☁️ <b>GDRIVE UPLOAD CONFIRMATION</b>\n\n"
        f"Bot: <code>{bot_name.upper()}</code>\n"
        f"Month: <b>{month_label}</b>\n"
        f"Range: <code>{from_key}</code> → <code>{to_key}</code>\n\n"
        f"This will ZIP all daily snapshots in the above range from "
        f"<code>MSANodeBackups</code> and upload to GDrive.\n\n"
        f"JSON files inside ZIP are named by collection.\n"
        f"Main DB is never touched.",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_GDStates.confirm)
async def gdrive_confirm(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "CANCEL" in txt or "❌" in txt:
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if "CONFIRM" not in txt and "✅" not in txt:
        await message.answer("Tap ✅ CONFIRM UPLOAD or ❌ CANCEL."); return

    data        = await state.get_data()
    bot_name    = data.get("gd_bot", "bot1")
    from_key    = data.get("gd_from_key", "")
    to_key      = data.get("gd_to_key", "")
    month_label = data.get("gd_month_label", "")
    await state.clear()

    status_msg = await message.answer(
        f"Building ZIP for <code>{bot_name.upper()} — {month_label}</code>...",
        parse_mode="HTML"
        # NO reply_markup here — ReplyKeyboardMarkup makes the message un-editable
    )

    loop = asyncio.get_event_loop()

    def _build():
        return _build_cumulative_zip(bot_name, from_key, to_key)

    try:
        zip_bytes, filename = await loop.run_in_executor(None, _build)
        if not zip_bytes:
            no_data_text = (
                f"No data found for <code>{bot_name}</code> "
                f"in range <code>{from_key}</code> → <code>{to_key}</code>.\n\n"
                "Run 🔥 FORCE BACKUP NOW first."
            )
            try:
                await status_msg.edit_text(no_data_text, parse_mode="HTML")
            except Exception:
                await message.answer(no_data_text, parse_mode="HTML")
            await message.answer("❌ No data. Returned to Backup Menu.", reply_markup=get_backup_menu())
            return
        size_mb = len(zip_bytes) / (1024 * 1024)

        def _upload():
            service   = _get_gdrive_service()
            folder_id = _BOT1_GDRIVE_FOLDER_ID if bot_name == "bot1" else _BOT2_GDRIVE_FOLDER_ID
            if not folder_id:
                raise ValueError(
                    f"GDrive folder ID not configured for {bot_name}. "
                    f"Set {'BOT1_GDRIVE_FOLDER_ID' if bot_name == 'bot1' else 'BOT2_GDRIVE_FOLDER_ID'} "
                    f"in your env file."
                )
            if _gdrive_file_exists(service, filename, folder_id):
                return "exists", ""
            fid = _gdrive_upload_bytes(service, zip_bytes, filename, folder_id)
            return "ok", fid

        gd_status, file_id = await loop.run_in_executor(None, _upload)

        if gd_status == "exists":
            exists_text = (
                f"File <code>{filename}</code> already exists on GDrive.\n"
                "No duplicate uploaded. GDrive data is safe."
            )
            try:
                await status_msg.edit_text(exists_text, parse_mode="HTML")
            except Exception:
                await message.answer(exists_text, parse_mode="HTML")
            await message.answer("✅ Returned to Backup Menu.", reply_markup=get_backup_menu())
            return

        success_text = (
            f"<b>✅ GDRIVE UPLOAD SUCCESS</b>\n\n"
            f"Bot: <code>{bot_name.upper()}</code>\n"
            f"Month: <b>{month_label}</b>\n"
            f"Range: <code>{from_key}</code> → <code>{to_key}</code>\n"
            f"File: <code>{filename}</code>\n"
            f"Size: {size_mb:.2f} MB\n"
            f"GDrive ID: <code>{file_id}</code>\n\n"
            f"TTL not applied. Use ⏳ ACTIVATE TTL to enable auto-deletion."
        )
        try:
            await status_msg.edit_text(success_text, parse_mode="HTML")
        except Exception:
            await message.answer(success_text, parse_mode="HTML")

        log_action("GDRIVE UPLOAD", message.from_user.id,
                   f"{bot_name} {month_label} {from_key}→{to_key} ({size_mb:.2f}MB)")
        try:
            col_backup_history.insert_one({
                "bot": bot_name, "action": "GDrive Upload",
                "details": f"{month_label} | {from_key}→{to_key} | {size_mb:.2f}MB | GDrive ID:{file_id}",
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception as hist_err:
            logger.warning(f"[HISTORY] Failed to write GDrive upload history: {hist_err}")

    except Exception as e:
        # Escape angle brackets so HTML parser doesn't choke on <HttpError ...> etc.
        safe_err = str(e)[:300].replace("<", "&lt;").replace(">", "&gt;")
        fail_text = f"<b>❌ GDrive upload failed:</b>\n<code>{safe_err}</code>"
        try:
            await status_msg.edit_text(fail_text, parse_mode="HTML")
        except Exception:
            await message.answer(fail_text, parse_mode="HTML")
    await message.answer("✅ Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# BACKUP STATUS — Real-time full details from cluster
# =============================================================================

class _STStates(StatesGroup):
    bot_select = State()

@dp.message(F.text == "📊 BACKUP STATUS")
async def backup_status_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_STStates.bot_select)
    title, kb = _bk_bot_select("📊 <b>BACKUP STATUS</b>\n\nSelect bot:")
    await message.answer(title, reply_markup=kb, parse_mode="HTML")

@dp.message(_STStates.bot_select)
async def backup_status_show(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return
    if "BOT 1" not in txt and "BOT 2" not in txt:
        await message.answer("Please select BOT 1 or BOT 2."); return

    bot_name  = "bot1" if "BOT 1" in txt else "bot2"
    await state.clear()
    wait_msg  = await message.answer(
        f"Fetching status for <code>{bot_name.upper()}</code>...",
        parse_mode="HTML", reply_markup=get_backup_menu()
    )

    loop = asyncio.get_event_loop()
    days = await loop.run_in_executor(None, _fetch_day_list, bot_name)

    try: await wait_msg.delete()
    except Exception: pass

    if not days:
        await message.answer(
            f"<b>BACKUP STATUS — {bot_name.upper()}</b>\n\n"
            "No snapshots found in MSANodeBackups cluster.\n"
            "Run 🔥 FORCE BACKUP NOW to create the first snapshot.",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        )
        return

    total_snaps = len(days)
    latest      = days[0]
    oldest      = days[-1]
    total_docs  = sum(d.get("docs", 0) for d in days)

    # Group by month
    month_groups: dict = {}
    for d in days:
        ml = d.get("month_label", "Unknown")
        month_groups.setdefault(ml, []).append(d)

    month_lines = ""
    for ml, ds in sorted(month_groups.items(), reverse=True):
        month_lines += f"  • <b>{ml}</b>: {len(ds)} snapshot(s)\n"

    # Check TTL status for the latest month
    latest_month_docs = month_groups.get(latest.get("month_label", ""), [])
    ttl_active = False
    try:
        bkp_uri = BACKUP_MONGO_URI or MONGO_URI
        bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
        bkp_client = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=8000)
        bkp_db     = bkp_client[bkp_db_name]
        col        = bkp_db[f"bot{bot_name[-1]}_backups"]
        ttl_count  = col.count_documents({"bot": bot_name, "gdrive_uploaded": True})
        ttl_active = ttl_count > 0
        bkp_client.close()
    except Exception:
        pass

    now_str = now_local().strftime("%d %b %Y %I:%M %p")
    await message.answer(
        f"<b>BACKUP STATUS — {bot_name.upper()}</b>\n"
        f"<i>As of {now_str}</i>\n\n"
        f"Total snapshots: <b>{total_snaps}</b>\n"
        f"Total docs: <b>{total_docs:,}</b>\n"
        f"Latest: <code>{latest['window_key']}</code> ({latest['docs']:,} docs)\n"
        f"Oldest: <code>{oldest['window_key']}</code>\n\n"
        f"<b>By Month:</b>\n{month_lines}\n"
        f"TTL (90-day auto-deletion): <b>{'Active on some records' if ttl_active else 'Not active'}</b>\n"
        f"GDrive: use ☁️ GDRIVE SYSTEM to check/upload",
        reply_markup=get_backup_menu(), parse_mode="HTML"
    )


# =============================================================================
# HISTORY — Timestamped backup action log
# =============================================================================

class _HSTStates(StatesGroup):
    bot_select = State()

_HIST_PAGE_SIZE = 8

@dp.message(F.text == "📜 HISTORY")
async def history_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_HSTStates.bot_select)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 BOT 1"), KeyboardButton(text="🤖 BOT 2"), KeyboardButton(text="🤖 BOT 3")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await message.answer(
        "<b>BACKUP HISTORY</b>\n\nSelect scope:",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_HSTStates.bot_select)
async def history_show(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return

    bot_filter = None
    if "BOT 1" in txt:   bot_filter = "bot1"
    elif "BOT 2" in txt: bot_filter = "bot2"
    elif "BOT 3" in txt: bot_filter = "bot3"
    elif "ALL" in txt:   bot_filter = None
    else:
        await message.answer("Select BOT 1, BOT 2, BOT 3, or ALL."); return

    await state.clear()
    try:
        query  = {"bot": bot_filter} if bot_filter else {}
        cursor = col_backup_history.find(query).sort("timestamp", -1).limit(50)
        entries = list(cursor)
    except Exception as e:
        await message.answer(
            f"Could not fetch history: <code>{str(e)[:200]}</code>",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        ); return

    if not entries:
        await message.answer(
            "<b>BACKUP HISTORY</b>\n\nNo history entries found yet.",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        ); return

    scope_label = bot_filter.upper() if bot_filter else "ALL BOTS"
    lines = f"<b>BACKUP HISTORY — {scope_label}</b>\n<i>(Last {len(entries)} entries)</i>\n\n"
    for e in entries:
        ts    = e.get("timestamp")
        ts_s  = format_datetime(ts) if ts else "Unknown"
        bot_s = e.get("bot", "?").upper()
        act   = e.get("action", "?")
        det   = e.get("details", "")
        lines += f"<code>{ts_s}</code>  [{bot_s}]  {act}\n"
        if det:
            lines += f"  <i>{det[:80]}</i>\n"
        lines += "\n"
        if len(lines) > 3800:
            lines += "<i>...truncated (showing newest)</i>"
            break

    await message.answer(lines, reply_markup=get_backup_menu(), parse_mode="HTML")


# =============================================================================
# ACTIVATE TTL — Month list → show GDrive & TTL status → toggle 90-day deletion
# =============================================================================

class _TTLStates(StatesGroup):
    bot_select    = State()
    month_index   = State()
    toggle_confirm = State()

@dp.message(F.text == "⏳ ACTIVATE TTL")
async def ttl_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_TTLStates.bot_select)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 BOT 1"), KeyboardButton(text="🤖 BOT 2"), KeyboardButton(text="🤖 BOT 3")],
        [KeyboardButton(text="🔙 BACK"), KeyboardButton(text="⬅️ MAIN MENU")]
    ], resize_keyboard=True)
    await message.answer("⏳ <b>ACTIVATE TTL</b>\n\nSelect bot:", reply_markup=kb, parse_mode="HTML")

@dp.message(_TTLStates.bot_select)
async def ttl_bot_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "MAIN MENU" in txt:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in txt:
        await state.clear()
        await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu()); return
    if "BOT 1" not in txt and "BOT 2" not in txt and "BOT 3" not in txt:
        await message.answer("Please select BOT 1, BOT 2, or BOT 3."); return

    bot_name = "bot1" if "BOT 1" in txt else ("bot2" if "BOT 2" in txt else "bot3")
    wait_msg = await message.answer(f"Loading months for {bot_name.upper()}...", reply_markup=_bk_cancel())
    loop = asyncio.get_event_loop()
    days = await loop.run_in_executor(None, _fetch_day_list, bot_name)
    try: await wait_msg.delete()
    except Exception: pass

    if not days:
        await state.clear()
        await message.answer(
            f"No snapshots found for {bot_name.upper()}.\nRun 🔥 FORCE BACKUP NOW first.",
            reply_markup=get_backup_menu()
        ); return

    # Build month-groups
    month_map: dict = {}  # "April 2026" → {year, month, count, keys:[...], ttl_count, gdrive_count}
    for d in days:
        ml = d.get("month_label", "Unknown")
        if ml not in month_map:
            month_map[ml] = {
                "month_label": ml, "year": d.get("year", ""),
                "month_n": d.get("month", 0), "count": 0, "keys": []
            }
        month_map[ml]["count"] += 1
        month_map[ml]["keys"].append(d["window_key"])

    # Fetch TTL and GDrive status for each month from cluster
    def _fetch_flags():
        bkp_uri     = BACKUP_MONGO_URI or MONGO_URI
        bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
        bkp_client  = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=10000)
        bkp_db      = bkp_client[bkp_db_name]
        col         = bkp_db[f"bot{bot_name[-1]}_backups"]
        for ml, info in month_map.items():
            keys = info["keys"]
            info["ttl_count"] = col.count_documents(
                {"bot": bot_name, "window_key": {"$in": keys}, "gdrive_uploaded": True})
            info["gdrive_count"] = info["ttl_count"]  # same flag tracks both
        bkp_client.close()
        return month_map

    month_map = await loop.run_in_executor(None, _fetch_flags)
    months    = sorted(month_map.values(), key=lambda x: (x["year"], x["month_n"]), reverse=True)

    lines = f"<b>ACTIVATE TTL — {bot_name.upper()}</b>\n\n"
    for i, m in enumerate(months, 1):
        total     = m["count"]
        ttl_c     = m.get("ttl_count", 0)
        ttl_st    = "Active" if ttl_c > 0 else "Inactive"
        gdr_st    = "Sent" if ttl_c > 0 else "No"
        lines += (
            f"  <b>{i}.</b>  <b>{m['month_label']}</b>  ({total} snapshots)\n"
            f"       GDrive: {gdr_st}  |  TTL 90-day: {ttl_st}\n"
        )
    lines += "\nType an <b>index number</b> to toggle TTL for that month."
    await state.update_data(ttl_bot=bot_name, ttl_months=months)
    await state.set_state(_TTLStates.month_index)
    await message.answer(lines, reply_markup=_bk_nav(), parse_mode="HTML")

@dp.message(_TTLStates.month_index)
async def ttl_month_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip()
    tu  = txt.upper()
    if "MAIN MENU" in tu:
        await state.clear(); return await back_to_main(message, state)
    if "BACK" in tu or "CANCEL" in tu:
        await state.clear()
        return await ttl_start(message, state)

    data     = await state.get_data()
    bot_name = data.get("ttl_bot", "bot1")
    months   = data.get("ttl_months", [])

    if not txt.isdigit():
        await message.answer("Type a number from the list."); return
    idx = int(txt) - 1
    if idx < 0 or idx >= len(months):
        await message.answer(f"Enter a number between 1 and {len(months)}."); return

    selected  = months[idx]
    ml        = selected["month_label"]
    ttl_count = selected.get("ttl_count", 0)
    total     = selected["count"]
    is_active = ttl_count > 0

    action_text = "🛑 DEACTIVATE TTL" if is_active else "✅ ACTIVATE TTL"
    action_desc = "stop 90-day auto-deletion" if is_active else "enable 90-day auto-deletion"
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=action_text)],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)

    await state.update_data(ttl_selected=selected, ttl_is_active=is_active)
    await state.set_state(_TTLStates.toggle_confirm)
    await message.answer(
        f"<b>TTL CONTROL — {bot_name.upper()} — {ml}</b>\n\n"
        f"Snapshots: {total}\n"
        f"TTL Active on: {ttl_count}/{total} records\n"
        f"Status: <b>{'ACTIVE' if is_active else 'INACTIVE'}</b>\n\n"
        f"Tap <b>{action_text}</b> to {action_desc}.\n"
        "(Only affects MSANodeBackups — main DB untouched.)",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_TTLStates.toggle_confirm)
async def ttl_toggle_execute(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "CANCEL" in txt or "❌" in txt:
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return

    data      = await state.get_data()
    bot_name  = data.get("ttl_bot", "bot1")
    selected  = data.get("ttl_selected", {})
    is_active = data.get("ttl_is_active", False)
    ml        = selected.get("month_label", "")
    keys      = selected.get("keys", [])
    await state.clear()

    if "ACTIVATE" not in txt and "DEACTIVATE" not in txt:
        await message.answer("Please use the keyboard button."); return

    activating = "ACTIVATE" in txt and "DEACTIVATE" not in txt

    status_msg = await message.answer(
        f"{'Activating' if activating else 'Deactivating'} TTL for "
        f"<code>{bot_name} — {ml}</code>...",
        parse_mode="HTML"
        # NO reply_markup here — messages with ReplyKeyboardMarkup cannot be edited
    )

    def _toggle():
        bkp_uri     = BACKUP_MONGO_URI or MONGO_URI
        bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
        bkp_client  = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=10000)
        bkp_db      = bkp_client[bkp_db_name]
        col         = bkp_db[f"bot{bot_name[-1]}_backups"]
        filter_q    = {"bot": bot_name, "window_key": {"$in": keys}}
        if activating:
            res = col.update_many(filter_q, {"$set": {"gdrive_uploaded": True,
                "gdrive_uploaded_at": datetime.now(timezone.utc)}})
        else:
            res = col.update_many(filter_q, {"$unset": {"gdrive_uploaded": "",
                "gdrive_uploaded_at": ""}})
        bkp_client.close()
        return res.modified_count

    try:
        loop = asyncio.get_event_loop()
        modified = await loop.run_in_executor(None, _toggle)
        action_done = "ACTIVATED" if activating else "DEACTIVATED"
        effect      = "will auto-expire after 90 days" if activating else "auto-deletion stopped"
        result_text = (
            f"<b>TTL {action_done} — {bot_name.upper()} — {ml}</b>\n\n"
            f"Records updated: <b>{modified}</b>\n"
            f"Effect: {effect}\n\n"
            f"Main DB (MSANodeDB) untouched."
        )
        try:
            await status_msg.edit_text(result_text, parse_mode="HTML")
        except Exception:
            await message.answer(result_text, parse_mode="HTML")
        log_action(f"TTL {action_done}", message.from_user.id,
                   f"{bot_name} {ml} — {modified} records")
        try:
            col_backup_history.insert_one({
                "bot": bot_name, "action": f"TTL {action_done}",
                "details": f"{ml} | {modified} records",
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception: pass
    except Exception as e:
        safe_err = str(e)[:300].replace("<", "&lt;").replace(">", "&gt;")
        fail_text = f"<b>TTL toggle failed:</b>\n<code>{safe_err}</code>"
        try:
            await status_msg.edit_text(fail_text, parse_mode="HTML")
        except Exception:
            await message.answer(fail_text, parse_mode="HTML")
    await message.answer("✅ TTL operation complete. Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# RESET BACKUP DATA — Clear backup cluster records for specific bot
# =============================================================================

class _RSBStates(StatesGroup):
    bot_select = State()
    confirm1   = State()
    confirm2   = State()

@dp.message(F.text == "🗑️ RESET BACKUP DATA")
async def reset_backup_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_RSBStates.bot_select)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 RESET BOT 1"), KeyboardButton(text="🤖 RESET BOT 2")],
        [KeyboardButton(text="🔥 RESET ALL")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await message.answer(
        "<b>RESET BACKUP DATA</b>\n\n"
        "This deletes snapshots from <code>MSANodeBackups</code>.\n"
        "<b>Main DB (MSANodeDB) is never touched.</b>\n\n"
        "Select target:",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_RSBStates.bot_select)
async def reset_backup_bot_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip().upper()
    if "CANCEL" in txt or "❌" in txt:
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return

    if "BOT 1" in txt:   target = "bot1"
    elif "BOT 2" in txt: target = "bot2"
    elif "ALL" in txt:   target = "all"
    else:
        await message.answer("Select a valid target."); return

    label = {"bot1": "Bot 1", "bot2": "Bot 2", "all": "ALL BOTS"}[target]
    await state.update_data(reset_target=target, reset_label=label)
    await state.set_state(_RSBStates.confirm1)
    await message.answer(
        f"<b>RESET BACKUP — {label}</b>\n\n"
        f"This will delete all snapshots for <b>{label}</b> from MSANodeBackups.\n\n"
        f"Type <b>CONFIRM</b> to proceed or ❌ CANCEL to abort.",
        reply_markup=_bk_cancel(), parse_mode="HTML"
    )

@dp.message(_RSBStates.confirm1)
async def reset_backup_confirm1(message: types.Message, state: FSMContext):
    if (message.text or "").strip().upper() == "❌ CANCEL":
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if (message.text or "").strip() != "CONFIRM":
        await message.answer("Type exactly <b>CONFIRM</b> or ❌ CANCEL.", parse_mode="HTML"); return
    await state.set_state(_RSBStates.confirm2)
    await message.answer(
        "<b>FINAL WARNING</b>\n\nType <b>DELETE</b> to permanently remove all selected backup snapshots.",
        reply_markup=_bk_cancel(), parse_mode="HTML"
    )

@dp.message(_RSBStates.confirm2)
async def reset_backup_confirm2(message: types.Message, state: FSMContext):
    if (message.text or "").strip().upper() == "❌ CANCEL":
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if (message.text or "").strip() != "DELETE":
        await message.answer("Type exactly <b>DELETE</b> or ❌ CANCEL.", parse_mode="HTML"); return

    data   = await state.get_data()
    target = data.get("reset_target", "bot1")
    label  = data.get("reset_label", "Bot")
    await state.clear()

    status_msg = await message.answer(
        f"Deleting {label} snapshots from MSANodeBackups...",
        reply_markup=get_backup_menu()
    )

    def _do_reset():
        bkp_uri     = BACKUP_MONGO_URI or MONGO_URI
        bkp_db_name = BACKUP_MONGO_DB_NAME or "MSANodeBackups"
        bkp_client  = _backup_mongo_client(bkp_uri, serverSelectionTimeoutMS=10000)
        bkp_db      = bkp_client[bkp_db_name]
        deleted = {}
        if target in ("bot1", "all"):
            r = bkp_db["bot1_backups"].delete_many({"bot": "bot1"})
            deleted["bot1"] = r.deleted_count
        if target in ("bot2", "all"):
            r = bkp_db["bot2_backups"].delete_many({"bot": "bot2"})
            deleted["bot2"] = r.deleted_count
        bkp_client.close()
        return deleted

    try:
        loop    = asyncio.get_event_loop()
        deleted = await loop.run_in_executor(None, _do_reset)
        det     = " | ".join(f"{b}: {c} deleted" for b, c in deleted.items())
        await status_msg.edit_text(
            f"<b>RESET COMPLETE — {label}</b>\n\n{det}\n\nMain DB untouched.",
            parse_mode="HTML"
        )
        log_action("RESET BACKUP", message.from_user.id, f"{label}: {det}")
    except Exception as e:
        await status_msg.edit_text(
            f"<b>Reset failed:</b>\n<code>{str(e)[:300]}</code>", parse_mode="HTML"
        )
    await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# RESTORE DATA — From MSANodeBackups latest snapshot → MSANodeDB
# =============================================================================

class _RSTStates(StatesGroup):
    bot_select  = State()
    confirm1    = State()
    confirm2    = State()

@dp.message(F.text == "🔄 RESTORE DATA")
async def restore_data_start(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        return
    await state.set_state(_RSTStates.bot_select)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🤖 RESTORE BOT 1"), KeyboardButton(text="🤖 RESTORE BOT 2")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await message.answer(
        "<b>RESTORE DATA</b>\n\n"
        "Restores from latest snapshot in <code>MSANodeBackups</code> → <code>MSANodeDB</code>.\n\n"
        "Existing records are updated. New records are added. Nothing is deleted.",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.message(_RSTStates.bot_select)
async def restore_bot_selected(message: types.Message, state: FSMContext):
    if not await has_permission(message.from_user.id, "backup"):
        await state.clear(); return
    txt = (message.text or "").strip()
    if txt.upper() == "❌ CANCEL":
        await state.clear()
        await message.answer("Cancelled.", reply_markup=get_backup_menu()); return
    if "RESTORE BOT 1" in txt.upper():
        bot_target, restore_col, label = "bot1", col_bot1_restore_data, "Bot 1"
    elif "RESTORE BOT 2" in txt.upper():
        bot_target, restore_col, label = "bot2", col_bot2_restore_data, "Bot 2"
    else:
        await message.answer("Please choose Bot 1 or Bot 2."); return

    try:
        snap = restore_col.find_one({"_id": f"{bot_target}_latest"})
    except Exception as err:
        await state.clear()
        await message.answer(
            f"Backup cluster unreachable:\n<code>{type(err).__name__}: {str(err)[:150]}</code>",
            reply_markup=get_backup_menu(), parse_mode="HTML"
        ); return

    if not snap:
        await state.clear()
        await message.answer(
            f"No restore snapshot for {label}.\nRun 🔥 FORCE BACKUP NOW first.",
            reply_markup=get_backup_menu()
        ); return

    date_str   = format_datetime(snap.get("backup_date")) if snap.get("backup_date") else "Unknown"
    col_counts = snap.get("collection_counts", snap.get("collections", {}))
    if isinstance(col_counts, dict) and col_counts:
        if isinstance(next(iter(col_counts.values())), dict):
            # nested structure from _export_bot_collections
            col_lines = "\n".join(
                f"  • <code>{k}</code>: {v.get('count', 0):,}" for k, v in col_counts.items()
            )
        else:
            col_lines = "\n".join(f"  • <code>{k}</code>: {v:,}" for k, v in col_counts.items())
    else:
        col_lines = "  No collection info"
    total = snap.get("total_records", 0)

    await state.update_data(restore_target=bot_target, restore_label=label)
    await state.set_state(_RSTStates.confirm1)
    await message.answer(
        f"<b>RESTORE PREVIEW — {label}</b>\n\n"
        f"Snapshot date: {date_str}\n"
        f"Total records: {total:,}\n\n"
        f"<b>Collections:</b>\n{col_lines}\n\n"
        f"Type <b>CONFIRM</b> to proceed or ❌ CANCEL:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(_RSTStates.confirm1)
async def restore_confirm1(message: types.Message, state: FSMContext):
    if (message.text or "").upper() == "❌ CANCEL":
        await state.clear()
        await message.answer("Restore cancelled.", reply_markup=get_backup_menu()); return
    if (message.text or "").strip() != "CONFIRM":
        await message.answer("Type exactly <b>CONFIRM</b> or ❌ CANCEL.", parse_mode="HTML"); return
    data  = await state.get_data()
    label = data.get("restore_label", "Bot")
    await state.set_state(_RSTStates.confirm2)
    await message.answer(
        f"<b>FINAL WARNING — {label} Restore</b>\n\nType <b>RESTORE</b> to execute:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(_RSTStates.confirm2)
async def restore_confirm2(message: types.Message, state: FSMContext):
    if (message.text or "").upper() == "❌ CANCEL":
        await state.clear()
        await message.answer("Restore cancelled.", reply_markup=get_backup_menu()); return
    if (message.text or "").strip() != "RESTORE":
        await message.answer("Type exactly <b>RESTORE</b> or ❌ CANCEL.", parse_mode="HTML"); return

    data        = await state.get_data()
    bot_target  = data.get("restore_target", "bot1")
    label       = data.get("restore_label", "Bot")
    restore_col = col_bot1_restore_data if bot_target == "bot1" else col_bot2_restore_data
    await state.clear()

    status_msg = await message.answer(
        f"Restoring {label}...", parse_mode="HTML", reply_markup=get_backup_menu()
    )

    def _do_restore():
        from bson import ObjectId
        snap = restore_col.find_one({"_id": f"{bot_target}_latest"})
        if not snap:
            return {"success": False, "error": "Restore snapshot not found."}
        cols = snap.get("collections", {})
        total_ins = total_mat = 0
        results   = {}
        for col_name, col_data in cols.items():
            docs = col_data.get("documents", col_data) if isinstance(col_data, dict) else col_data
            if not isinstance(docs, list):
                continue
            prod_col = db[col_name]
            ins = mat = 0
            for doc in docs:
                if not isinstance(doc, dict): continue
                raw_id = doc.get("_id")
                try:
                    doc["_id"] = ObjectId(raw_id)
                except Exception: pass
                try:
                    r = prod_col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                    if r.upserted_id: ins += 1
                    else:             mat += 1
                except Exception: pass
            results[col_name] = (ins, mat)
            total_ins += ins; total_mat += mat
        return {"success": True, "total_ins": total_ins, "total_mat": total_mat, "cols": results}

    try:
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _do_restore)
        if not result.get("success"):
            await status_msg.edit_text(
                f"<b>Restore failed:</b>\n<code>{result.get('error', 'Unknown')}</code>",
                parse_mode="HTML"
            ); return
        col_lines = "\n".join(
            f"  • <code>{c}</code>: +{ins} new, ~{mat} updated"
            for c, (ins, mat) in result.get("cols", {}).items()
        )
        await status_msg.edit_text(
            f"<b>RESTORE COMPLETE — {label}</b>\n\n"
            f"Inserted: +{result['total_ins']:,}\n"
            f"Updated: ~{result['total_mat']:,}\n\n"
            f"<b>Details:</b>\n{col_lines}",
            parse_mode="HTML"
        )
        log_action(f"RESTORE — {label}", message.from_user.id,
                   f"+{result['total_ins']} ins ~{result['total_mat']} upd")
    except Exception as e:
        await status_msg.edit_text(
            f"<b>Restore error:</b>\n<code>{str(e)[:300]}</code>", parse_mode="HTML"
        )
    await message.answer("Returned to Backup Menu.", reply_markup=get_backup_menu())


# =============================================================================
# UPLOAD DATA (legacy text trigger kept for compat)
# =============================================================================

@dp.message(F.text == "📤 UPLOAD DATA")
async def upload_data_legacy(message: types.Message, state: FSMContext):
    """Alias for 📤 UPLOAD BACKUP."""
    return await upload_backup_start(message, state)


@dp.message(F.text == "🖥️ TERMINAL")
async def terminal_handler(message: types.Message, state: FSMContext):
    """Terminal - Shows live logs with Bot 1/10 selection"""
    # Log to console and memory
    log_action("🖥️ TERMINAL ACCESS", message.from_user.id, "Admin opened live terminal", "bot2")
    
    try:
        # Show view selection with reply keyboard
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📱 BOT 1 LOGS"), KeyboardButton(text="🎛️ BOT 2 LOGS")],
                [KeyboardButton(text="⬅️ MAIN MENU")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "<b>🖥️ LIVE TERMINAL</b>\n\n"
            "Select which bot logs to view:\n\n"
            "📱 <b>Bot 1 Logs</b> - User interactions & content\n"
            "🎛️ <b>Bot 2 Logs</b> - Admin actions & management\n\n"
            f"<i>💡 Tracking last {MAX_LOGS} actions per bot</i>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )

@dp.message(F.text.in_({"📱 BOT 1 LOGS", "🔄 REFRESH BOT 1"}))
async def view_bot1_logs(message: types.Message, state: FSMContext):
    """Show Bot 1 live logs in raw terminal format"""
    # Simply log strictly (no stats query)
    log_action("CMD", message.from_user.id, "Opened Bot 1 Terminal", "bot1")
    
    try:
        logs_text = get_terminal_logs(bot="bot1", limit=50)
        
        # Specific keyboard for Bot 1 view
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔄 REFRESH BOT 1"), KeyboardButton(text="⬅️ RETURN TO MENU")]
            ],
            resize_keyboard=True
        )
        
        # Raw terminal appearance
        await message.answer(
            f"<b>📱 BOT 1 TERMINAL VIEW</b>\n"
            f"<pre language='bash'>{logs_text}</pre>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.answer(f"Error: {e}")

@dp.message(F.text.in_({"🎛️ BOT 2 LOGS", "🔄 REFRESH BOT 2"}))
async def view_bot2_logs(message: types.Message, state: FSMContext):
    """Show Bot 2 live logs in raw terminal format"""
    log_action("CMD", message.from_user.id, "Opened Bot 2 Terminal", "bot2")
    
    try:
        logs_text = get_terminal_logs(bot="bot2", limit=50)
        
        # Specific keyboard for Bot 2 view
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔄 REFRESH BOT 2"), KeyboardButton(text="⬅️ RETURN TO MENU")]
            ],
            resize_keyboard=True
        )
        
        # Raw terminal appearance  
        await message.answer(
            f"<b>🎛️ BOT 2 TERMINAL VIEW</b>\n"
            f"<pre language='bash'>{logs_text}</pre>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(F.text == "⬅️ RETURN TO MENU")
async def back_to_terminal_menu(message: types.Message, state: FSMContext):
    """Return to main terminal menu"""
    # Call the original terminal handler
    await terminal_handler(message, state)

@dp.callback_query(F.data == "terminal_bot1")
async def terminal_bot1_view(callback: types.CallbackQuery, state: FSMContext):
    """Show Bot 1 terminal view"""
    log_action("📱 BOT 1 TERMINAL", callback.from_user.id, "Viewing Bot 1 statistics")
    
    try:
        await callback.message.edit_text(
            "<b>📱 BOT 1 TERMINAL</b>\n\n"
            "⏳ Fetching live Bot 1 data...\n"
            "📊 Analyzing collections...",
            parse_mode="HTML"
        )
        
        # Get counts from all Bot 1 collections
        user_verification_count = col_user_verification.count_documents({})
        msa_ids_count = col_msa_ids.count_documents({})
        support_tickets_count = col_support_tickets.count_documents({})
        banned_users_count = col_banned_users.count_documents({})
        suspended_features_count = col_suspended_features.count_documents({})

        # MongoDB storage stats for both terminal views
        _st = get_mongo_storage_stats()
        if _st["ok"]:
            _storage_line = (
                f"$ mongodb_storage --live\n"
                f"Atlas Storage        : [{_st['bar']}]\n"
                f"Used / Cap           : {_st['used_mb']:.1f}MB / {_st['cap_mb']:.0f}MB  ({_st['pct']:.1f}%)\n"
                f"Status               : {_st['risk_icon']} {_st['risk_label']}\n"
                f"Breakdown            : data={_st['data_mb']:.1f}MB  idx={_st['index_mb']:.1f}MB\n"
            )
        else:
            _storage_line = f"$ mongodb_storage --live\nStorage check: unavailable ({_st.get('error','')})\n"
        suspended_features_count = col_suspended_features.count_documents({})
        
        # Calculate total
        total_records = (
            banned_users_count + suspended_features_count
        )
        
        # Get Bot 2 collections stats
        bot2_broadcasts_count = col_broadcasts.count_documents({})
        bot2_user_tracking_count = col_user_tracking.count_documents({})
        bot2_backups_count = col_bot2_backups.count_documents({})
        cleanup_backups_count = col_cleanup_backups.count_documents({})
        cleanup_logs_count = col_cleanup_logs.count_documents({})
        
        # Get support ticket stats
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        
        # Build terminal-style output
        terminal_output = (
            "<b>🖥️ MSA NODE - SYSTEM TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ system_info --status\n"
            f"System: {MONGO_DB_NAME}\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Admin: Bot 2 Control Panel\n\n"
            
            f"$ bot2_features --list\n\n"
            f"BOT 2 AVAILABLE ACTIONS:\n"
            f"├─ 📢 BROADCAST         : Send messages to all Bot 1 users\n"
            f"│  ├─ Send Broadcast    : Create & send new broadcast\n"
            f"│  ├─ Delete Broadcast  : Remove broadcast by ID\n"
            f"│  ├─ Edit Broadcast    : Modify existing broadcast\n"
            f"│  └─ List Broadcasts   : View all broadcasts\n"
            f"│\n"
            f"├─  FIND              : Search user by ID/username\n"
            f"│  └─ User lookup       : Get detailed user info\n"
            f"│\n"
            f"├─ 📊 TRAFFIC           : User traffic sources\n"
            f"│  └─ Analytics         : See how users found Bot 1\n"
            f"│\n"
            f"├─ 🩺 DIAGNOSIS         : User management tools\n"
            f"│  ├─ Ban User          : Permanent ban with reason\n"
            f"│  ├─ Temporary Ban     : Time-limited ban (hours/days)\n"
            f"│  ├─ Unban User        : Remove ban\n"
            f"│  ├─ Delete User       : Remove from database\n"
            f"│  ├─ Suspend Features  : Limit specific features\n"
            f"│  ├─ Unsuspend         : Restore all features\n"
            f"│  └─ Reset User        : Clear user verification\n"
            f"│\n"
            f"├─ 📸 SHOOT             : User control panel\n"
            f"│  └─ Actions           : Ban / Unban / Suspend / Delete\n"
            f"│\n"
            f"├─ 💬 SUPPORT           : Support ticket system\n"
            f"│  ├─ Reply to ticket   : Respond to user tickets\n"
            f"│  ├─ Mark resolved     : Close ticket\n"
            f"│  └─ View all tickets  : Browse open/resolved\n"
            f"│\n"
            f"├─ 💾 BACKUP            : Enterprise backup system\n"
            f"│  ├─ Backup Now        : Manual backup (MongoDB + JSON)\n"
            f"│  ├─ View Backups      : List all backups\n"
            f"│  ├─ Monthly Status    : Backup statistics\n"
            f"│  ├─ Auto-Backup       : Schedule info\n"
            f"│  └─ Scalability       : Handles 10M+ users\n"
            f"│\n"
            f"├─ 🖥️ TERMINAL          : System statistics (current)\n"
            f"│  ├─ Database stats    : Collection counts\n"
            f"│  ├─ Bot 1 data        : User verification, MSA IDs\n"
            f"│  ├─ Bot 2 data       : Broadcasts, backups\n"
            f"│  └─ Security status   : Bans, suspensions\n"
            f"│\n"
            f"├─ 👥 ADMINS            : Admin management [COMING SOON]\n"
            f"│  └─ Multi-admin       : Add/remove admin access\n"
            f"│\n"
            f"└─ ⚠️ RESET DATA        : Delete ALL Bot 1 data\n"
            f"   └─ Double confirm    : RESET → DELETE ALL\n\n"
            
            f"$ bot1_stats --collections\n\n"
            f"BOT 1 DATA COLLECTIONS:\n"
            f"├─ user_verification     : {user_verification_count:,} records\n"
            f"├─ msa_ids              : {msa_ids_count:,} records\n"
            f"├─ support_tickets      : {support_tickets_count:,} records\n"
            f"│  ├─ Open              : {open_tickets:,} tickets\n"
            f"│  └─ Resolved          : {resolved_tickets:,} tickets\n"
            f"├─ banned_users         : {banned_users_count:,} records\n"
            f"└─ suspended_features   : {suspended_features_count:,} records\n\n"
            f"TOTAL BOT 1 RECORDS     : {total_records:,}\n\n"
            
            f"$ bot2_stats --collections\n\n"
            f"BOT 2 DATA COLLECTIONS:\n"
            f"├─ bot2_broadcasts     : {bot2_broadcasts_count:,} records\n"
            f"├─ bot2_user_tracking  : {bot2_user_tracking_count:,} records\n"
            f"├─ bot2_backups        : {bot2_backups_count:,} records\n"
            f"├─ cleanup_backups      : {cleanup_backups_count:,} records\n"
            f"└─ cleanup_logs         : {cleanup_logs_count:,} records\n\n"
            
            f"$ disk_usage --total\n"
            f"Total Database Records  : {total_records + bot2_broadcasts_count + bot2_user_tracking_count + bot2_backups_count + cleanup_backups_count + cleanup_logs_count:,}\n\n"
            f"{_storage_line}\n"
            f"$ security_status\n"
            f"Banned Users           : {banned_users_count:,}\n"
            f"Suspended Features     : {suspended_features_count:,}\n"
            f"Open Support Tickets   : {open_tickets:,}\n\n"
            f"$ automation_status\n"
            f"Daily Cleanup          : ACTIVE ✅ (3 AM daily)\n"
            f"Monthly Backup         : ACTIVE ✅ (1st of month, 3 AM)\n"
            f"Backup Retention       : Last 30 backups\n"
            f"Cleanup History        : Last 30 logs</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>System Status:</b> All systems operational ✅\n"
            f"<b>Features:</b> 10 Core Actions + Auto-Cleanup + Auto-Backup\n"
            f"<b>Memory:</b> MongoDB Cloud Atlas\n"
            f"<b>Hosting:</b> Cloud-Safe (Render/Heroku Compatible)\n"
            f"<b>Scalability:</b> Enterprise-grade (10M+ users)\n\n"
            "<i>💡 Terminal displays all Bot 2 features & system stats</i>"
        )
        bot1_terminal = (
            "<b>📱 BOT 1 LIVE TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ bot1_info --status\n"
            f"Bot: MSA Node Bot (Bot 1)\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Live Updates: ENABLED ✅\n\n"
            
            f"$ user_data --collections\n\n"
            f"USER DATA COLLECTIONS:\n"
            f"├─ user_verification     : {user_verification_count:,} users\n"
            f"├─ msa_ids              : {msa_ids_count:,} MSA+ IDs\n"
            
            f"$ support_system --status\n\n"
            f"SUPPORT TICKETS:\n"
            f"├─ Total Tickets        : {support_tickets_count:,}\n"
            f"├─ Open                 : {open_tickets:,} 🟢\n"
            f"└─ Resolved             : {resolved_tickets:,} ✅\n\n"
            
            f"$ security_status\n\n"
            f"SECURITY & MODERATION:\n"
            f"├─ Banned Users         : {banned_users_count:,} 🚫\n"
            f"└─ Suspended Features   : {suspended_features_count:,} ⚠️\n\n"
            
            f"$ total_bot1_records\n"
            f"Total Bot 1 Records     : {total_records:,}\n\n"
            f"{_storage_line}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Live Monitoring:</b> Active ✅\n"
            f"<b>Console Logging:</b> All actions logged\n"
            f"<b>Last Updated:</b> {now_local().strftime('%H:%M:%S')}\n\n"
            "<i>💡 Bot 1 serves end users with content & support</i>"
        )
        
        # Add buttons
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎛️ BOT 2 TERMINAL", callback_data="terminal_bot2")],
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="terminal_bot1")]
        ])
        
        await callback.message.edit_text(bot1_terminal, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("📱 Bot 1 Terminal loaded")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await callback.message.edit_text(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )
        await callback.answer("Error loading terminal", show_alert=True)

@dp.callback_query(F.data == "terminal_bot2")
async def terminal_bot2_view(callback: types.CallbackQuery, state: FSMContext):
    """Show Bot 2 terminal view"""
    log_action("🎛️ BOT 2 TERMINAL", callback.from_user.id, "Viewing Bot 2 admin actions")
    
    try:
        # Get counts
        user_verification_count = col_user_verification.count_documents({})
        msa_ids_count = col_msa_ids.count_documents({})
        support_tickets_count = col_support_tickets.count_documents({})
        banned_users_count = col_banned_users.count_documents({})
        suspended_features_count = col_suspended_features.count_documents({})
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})

        # MongoDB storage stats
        _st = get_mongo_storage_stats()
        if _st["ok"]:
            _storage_line = (
                f"$ mongodb_storage --live\n"
                f"Atlas Storage        : [{_st['bar']}]\n"
                f"Used / Cap           : {_st['used_mb']:.1f}MB / {_st['cap_mb']:.0f}MB  ({_st['pct']:.1f}%)\n"
                f"Status               : {_st['risk_icon']} {_st['risk_label']}\n"
                f"Breakdown            : data={_st['data_mb']:.1f}MB  idx={_st['index_mb']:.1f}MB\n"
            )
        else:
            _storage_line = f"$ mongodb_storage --live\nStorage check: unavailable ({_st.get('error','')})\n"

        bot2_broadcasts_count = col_broadcasts.count_documents({})
        bot2_user_tracking_count = col_user_tracking.count_documents({})
        bot2_backups_count = col_bot2_backups.count_documents({})
        cleanup_backups_count = col_cleanup_backups.count_documents({})
        cleanup_logs_count = col_cleanup_logs.count_documents({})
        
        total_records = (
            banned_users_count + suspended_features_count
        )
        
        # Build Bot 2 terminal output
        bot2_terminal = (
            "<b>🎛️ BOT 2 LIVE TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ bot2_info --status\n"
            f"Bot: Admin Control Panel (Bot 2)\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Live Updates: ENABLED ✅\n"
            f"Console Logging: ACTIVE ✅\n\n"
            
            f"$ admin_actions --available\n\n"
            f"AVAILABLE ADMIN ACTIONS:\n"
            f"├─ 📢 BROADCAST         : {bot2_broadcasts_count:,} sent\n"
            f"├─ 🔍 FIND              : Search users\n"
            f"├─ 📊 TRAFFIC           : {bot2_user_tracking_count:,} tracked\n"
            f"├─ 🩺 DIAGNOSIS         : System health checks\n"
            f"├─ 📸 SHOOT             : User management\n"
            f"├─ 💬 SUPPORT           : {support_tickets_count:,} tickets\n"
            f"├─ 💾 BACKUP            : {bot2_backups_count:,} backups\n"
            f"├─ 🖥️ TERMINAL          : Live view (current)\n"
            f"└─ ⚠️ RESET DATA        : Dangerous operation\n\n"
            
            f"$ bot2_collections --stats\n\n"
            f"BOT 2 DATA:\n"
            f"├─ bot2_broadcasts     : {bot2_broadcasts_count:,} records\n"
            f"├─ bot2_user_tracking  : {bot2_user_tracking_count:,} records\n"
            f"├─ bot2_backups        : {bot2_backups_count:,} records\n"
            f"├─ cleanup_backups      : {cleanup_backups_count:,} records\n"
            f"└─ cleanup_logs         : {cleanup_logs_count:,} records\n\n"
            
            f"$ automation_systems\n\n"
            f"AUTOMATED PROCESSES:\n"
            f"├─ Daily Cleanup        : ACTIVE ✅ (3 AM)\n"
            f"├─ Monthly Backup       : ACTIVE ✅ (1st, 3 AM)\n"
            f"├─ Backup Retention     : Last 30 backups\n"
            f"└─ Log Retention        : Last 30 logs\n\n"
            
            f"$ security_overview\n\n"
            f"SECURITY STATUS:\n"
            f"├─ Banned Users         : {banned_users_count:,}\n"
            f"├─ Suspended Features   : {suspended_features_count:,}\n"
            f"└─ Open Tickets         : {open_tickets:,}\n\n"
            
            f"$ total_database_records\n"
            f"Total Records           : {total_records + bot2_broadcasts_count + bot2_user_tracking_count + bot2_backups_count + cleanup_backups_count + cleanup_logs_count:,}\n\n"
            f"{_st['ok'] and _storage_line or 'Storage check unavailable'}\n</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Admin Panel:</b> Fully operational ✅\n"
            f"<b>Live Logging:</b> All actions → Console\n"
            f"<b>Last Updated:</b> {now_local().strftime('%H:%M:%S')}\n\n"
            "<i>💡 Bot 2 manages Bot 1 with admin tools</i>"
        )
        
        # Add buttons
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 BOT 1 TERMINAL", callback_data="terminal_bot1")],
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="terminal_bot2")]
        ])
        
        await callback.message.edit_text(bot2_terminal, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("🎛️ Bot 2 Terminal loaded")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await callback.message.edit_text(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )
        await callback.answer("Error loading terminal", show_alert=True)

@dp.callback_query(F.data == "terminal_refresh")
async def terminal_refresh(callback: types.CallbackQuery):
    """Refresh terminal view"""
    await callback.answer("🔄 Refreshing terminal...")
    await terminal_handler(callback.message, None)

@dp.message(F.text == "👥 ADMINS")
async def admins_handler(message: types.Message, state: FSMContext):
    """Show admin management menu"""
    if not await has_permission(message.from_user.id, "admins"):
        log_action("🚫 UNAUTHORIZED ACCESS", message.from_user.id, f"{message.from_user.full_name} tried to access ADMINS")
        await message.answer("⛔ **ACCESS DENIED**\n\nYou don't have permission to manage admins.", reply_markup=await get_main_menu(message.from_user.id))
        return

    await state.clear()
    log_action("👥 ADMINS MENU", message.from_user.id, "Opened admin management")

    await _send_admin_overview_page(message, state, 0)


@dp.message(F.text == _ADMIN_OV_PREV)
async def admins_overview_prev(message: types.Message, state: FSMContext):
    """Previous page for admin overview."""
    if not await has_permission(message.from_user.id, "admins"):
        return
    data = await state.get_data()
    pages = data.get("admin_overview_pages") or []
    if not pages:
        await _send_admin_overview_page(message, state, 0)
        return
    page = max(0, int(data.get("admin_overview_page", 0)) - 1)
    await _send_admin_overview_page(message, state, page)


@dp.message(F.text == _ADMIN_OV_NEXT)
async def admins_overview_next(message: types.Message, state: FSMContext):
    """Next page for admin overview."""
    if not await has_permission(message.from_user.id, "admins"):
        return
    data = await state.get_data()
    pages = data.get("admin_overview_pages") or []
    if not pages:
        await _send_admin_overview_page(message, state, 0)
        return
    total_pages = len(pages)
    page = min(total_pages - 1, int(data.get("admin_overview_page", 0)) + 1)
    await _send_admin_overview_page(message, state, page)

# ==========================================
# ADMIN MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "➕ NEW ADMIN")
async def new_admin_handler(message: types.Message, state: FSMContext):
    """Add new admin"""
    log_action("➕ NEW ADMIN", message.from_user.id, "Starting new admin creation")
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "➕ **ADD NEW ADMIN**\n\n"
        "Please send the **User ID** of the new admin:\n\n"
        "💡 Tip: Ask the user to send /start to any bot to get their ID",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_new_admin_id)

@dp.message(AdminStates.waiting_for_new_admin_id)
async def process_new_admin_id(message: types.Message, state: FSMContext):
    """Process new admin user ID"""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Validate user ID
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer(
            "⚠️ Invalid User ID. Please send a valid numeric User ID.\n\n"
            "Example: `123456789`",
            parse_mode="Markdown"
        )
        return
    
    # Check if already admin
    existing = col_admins.find_one({"user_id": user_id})
    if existing:
        await message.answer(
            f"⚠️ User `{user_id}` is already an admin!\n\n"
            f"👔 Current Role: **{existing.get('role', 'Admin')}**\n"
            f"📅 Added: {format_datetime(existing.get('added_at'))}",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    # ─── Ban check: banned users cannot be added as admins ───
    ban_doc = col_banned_users.find_one({"user_id": user_id})
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        banned_by = ban_doc.get("banned_by", "Unknown")
        banned_at_raw = ban_doc.get("banned_at")
        scope = ban_doc.get("scope", "")
        is_auto = banned_by == "SYSTEM"
        banned_at_str = banned_at_raw.strftime('%b %d, %Y at %I:%M %p') if banned_at_raw else "Unknown"
        scope_label = " (Bot 2 auto-ban only)" if scope == "bot2" else ""
        await message.answer(
            f"🚫 **CANNOT ADD AS ADMIN — USER IS BANNED**\n\n"
            f"👤 User ID: `{user_id}`\n\n"
            f"⚠️ **Ban Status:**\n"
            f"  • Source: {'SYSTEM (Auto spam-detection)' if is_auto else f'Manually by Admin ({banned_by})'}{scope_label}\n"
            f"  • Type: {ban_type.capitalize()}\n"
            f"  • Banned At: {banned_at_str}\n\n"
            f"❌ Banned users **cannot** be added as admins.\n\n"
            f"💡 To add this user as admin:\n"
            f"  1️⃣ Go to **🔫 SHOOT → ✅ UNBAN USER** and unban them\n"
            f"  2️⃣ Then return here to add them as admin",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    
    # Prefer @username, then full_name so the admin list shows readable labels
    try:
        chat = await bot.get_chat(user_id)
        uname = getattr(chat, 'username', None)
        if uname:
            admin_name = f"@{uname}"
        elif getattr(chat, 'full_name', None):
            admin_name = chat.full_name
        else:
            admin_name = str(user_id)
    except Exception:
        admin_name = str(user_id)

    # Create admin record with default Admin role (LOCKED by default)
    admin_doc = {
        "user_id": user_id,
        "name": admin_name,
        "role": "Admin",
        "permissions": ["broadcast", "support"],  # Safe defaults - use PERMISSIONS menu to add more
        "added_by": message.from_user.id,
        "added_at": now_local(),
        "status": "active",
        "locked": True  # LOCKED by default - must be unlocked to activate
    }
    
    try:
        col_admins.insert_one(admin_doc)
        log_action("➕ ADMIN ADDED", message.from_user.id, 
                  f"New Admin: {user_id}")
        
        await message.answer(
            f"✅ ADMIN ADDED SUCCESSFULLY!\n\n"
            f"👤 Name: {admin_name}\n"
            f"🆔 User ID: `{user_id}`\n"
            f"👔 Role: Admin\n"
            f"🔐 Default Permissions: Broadcast, Support\n"
            f"🔒 Status: LOCKED (Inactive)\n"
            f"📅 Added: {now_local().strftime('%b %d, %Y %I:%M %p')}\n\n"
            f"⚠️ This admin is LOCKED and cannot access Bot 2 yet!\n"
            f"💡 Use 🔒 LOCK/UNLOCK USER to activate them\n"
            f"💡 Use 🔐 PERMISSIONS to add more permissions\n"
            f"💡 Use 👔 MANAGE ROLES to change role",
            reply_markup=get_admin_menu()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ **ERROR ADDING ADMIN**\n\n"
            f"Error: {str(e)}",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()

@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_handler(message: types.Message, state: FSMContext):
    """Remove an admin"""
    log_action("➖ REMOVE ADMIN", message.from_user.id, "Starting admin removal")
    
    # List current admins excluding MASTER_ADMIN_ID and anyone with "Owner" role
    admins = list(col_admins.find({
        "user_id": {"$ne": MASTER_ADMIN_ID},
        "role": {"$ne": "Owner"}
    }))
    if not admins:
        await message.answer(
            "⚠️ No other admins found in the system.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Store page in state (default to page 0)
    page = 0
    await state.update_data(admin_remove_page=page)
    
    # Pagination: 10 admins per page
    ITEMS_PER_PAGE = 10
    total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create buttons for current page
    admin_buttons = []
    for admin in page_admins:
        admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

    # Add navigation buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

    if nav_buttons:
        admin_buttons.append(nav_buttons)

    # Add back button
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])

    select_kb = ReplyKeyboardMarkup(
        keyboard=admin_buttons,
        resize_keyboard=True
    )

    await message.answer(
        f"➖ **REMOVE ADMIN**\n\n"
        f"📋 **Select admin to remove:**\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_remove_admin_id)

@dp.message(AdminStates.waiting_for_remove_admin_id)
async def process_remove_admin_id(message: types.Message, state: FSMContext):
    """Process admin removal ID"""
    # Handle special buttons
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Handle pagination
    if message.text in ["⬅️ PREV ADMINS", "➡️ NEXT ADMINS"]:
        data = await state.get_data()
        current_page = data.get("admin_remove_page", 0)
        
        if message.text == "⬅️ PREV ADMINS":
            new_page = max(0, current_page - 1)
        else:  # NEXT
            new_page = current_page + 1
        
        await state.update_data(admin_remove_page=new_page)
        
        # Reload admin list with new page, excluding Owner / MASTER_ADMIN_ID
        admins = list(col_admins.find({
            "user_id": {"$ne": MASTER_ADMIN_ID},
            "role": {"$ne": "Owner"}
        }))
        ITEMS_PER_PAGE = 10
        total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        
        # Cap new_page just in case
        new_page = min(new_page, max(0, total_pages - 1))
        
        start_idx = new_page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
        page_admins = admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

        # Navigation
        nav_buttons = []
        if new_page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
        if new_page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])

        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)

        await message.answer(
            f"➖ **REMOVE ADMIN**\n\n"
            f"📋 **Select admin to remove:**\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb,
            parse_mode="Markdown"
        )
        return
    
    # Parse user ID from button text
    try:
        user_id = _parse_admin_uid(message.text)
    except (ValueError, IndexError):
        await message.answer(
            "⚠️ Invalid selection. Please select an admin from the buttons.",
            parse_mode="Markdown"
        )
        return
    
    # Check if admin exists
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(
            f"⚠️ User `{user_id}` is not an admin.",
            parse_mode="Markdown"
        )
        return
    
    # Prevent removing master admin
    if user_id == MASTER_ADMIN_ID:
        await message.answer(
            "🚫 **CANNOT REMOVE MASTER ADMIN**\n\n"
            "The master admin cannot be removed from the system.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Store for confirmation
    await state.update_data(remove_admin_id=user_id)
    
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ YES, REMOVE"), KeyboardButton(text="❌ NO, CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"⚠️ **CONFIRM REMOVAL**\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"👔 Role: **{admin_doc.get('role', 'Admin')}**\n"
        f"📅 Added: {format_datetime(admin_doc.get('added_at'))}\n\n"
        "Are you sure you want to remove this admin?",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_remove_confirm)

@dp.message(AdminStates.waiting_for_remove_confirm)
async def process_remove_confirm(message: types.Message, state: FSMContext):
    """Process admin removal confirmation"""
    if message.text not in ["✅ YES, REMOVE", "❌ NO, CANCEL"]:
        await message.answer("⚠️ Please select YES or NO from the buttons.")
        return
    
    if message.text == "❌ NO, CANCEL":
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    user_id = data.get("remove_admin_id")
    
    try:
        result = col_admins.delete_one({"user_id": user_id})
        
        if result.deleted_count > 0:
            log_action("➖ ADMIN REMOVED", message.from_user.id, f"Removed admin: {user_id}")
            
            await message.answer(
                f"✅ **ADMIN REMOVED**\n\n"
                f"👤 User ID: `{user_id}`\n"
                f"📅 Removed: {now_local().strftime('%b %d, %Y %I:%M %p')}",
                reply_markup=get_admin_menu(),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                "⚠️ Admin not found or already removed.",
                reply_markup=get_admin_menu(),
                parse_mode="Markdown"
            )
        
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ **ERROR REMOVING ADMIN**\n\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )

@dp.message(F.text == "🔐 PERMISSIONS")
async def permissions_handler(message: types.Message, state: FSMContext):
    """Manage admin permissions - show admin list"""
    log_action("🔐 PERMISSIONS", message.from_user.id, "Managing admin permissions")
    
    # Get all admins excluding Master Admin
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
    if not admins:
        await message.answer(
            "⚠️ No other admins found.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Pagination: 5 admins per page
    page = 0
    await state.update_data(permission_page=page)
    
    ITEMS_PER_PAGE = 5
    total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create buttons for current page
    admin_buttons = []
    for admin in page_admins:
        admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

    # Add navigation buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

    if nav_buttons:
        admin_buttons.append(nav_buttons)

    # Add back button
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])

    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)

    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"Select admin to manage:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.waiting_for_permission_admin_id)

@dp.message(AdminStates.waiting_for_permission_admin_id)
async def process_permission_admin_id(message: types.Message, state: FSMContext):
    """Process permission admin ID"""
    # Handle special buttons
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Handle pagination
    if message.text in ["⬅️ PREV ADMINS", "➡️ NEXT ADMINS"]:
        data = await state.get_data()
        current_page = data.get("permission_page", 0)
        
        if message.text == "⬅️ PREV ADMINS":
            new_page = max(0, current_page - 1)
        else:  # NEXT
            new_page = current_page + 1
        
        await state.update_data(permission_page=new_page)
        
        # Reload admin list with new page
        admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
        ITEMS_PER_PAGE = 5
        total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        
        # Cap new_page just in case
        new_page = min(new_page, max(0, total_pages - 1))
        
        start_idx = new_page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
        page_admins = admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

        # Navigation
        nav_buttons = []
        if new_page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
        if new_page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])

        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)

        await message.answer(
            f"🔐 MANAGE PERMISSIONS\n\n"
            f"Select admin to manage:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        return
    
    # Parse user ID from button text
    try:
        user_id = _parse_admin_uid(message.text)
    except (ValueError, IndexError):
        await message.answer("⚠️ Invalid User ID.")
        return

    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return

    await state.update_data(
        permission_admin_id=user_id,
        permission_admin_name=admin_doc.get('name', str(user_id))
    )
    
    # Get current permissions
    current_perms = admin_doc.get('permissions', [])
    
    # Store initial permissions in state
    await state.update_data(current_permissions=current_perms.copy())
    
    # Define all available permissions (10 Bot 2 features)
    all_permissions = {
        'broadcast': '📢 BROADCAST',
        'find': '🔍 FIND',
        'traffic': '📊 TRAFFIC',
        'diagnosis': '🩺 DIAGNOSIS',
        'shoot': '📸 SHOOT',
        'support': '💬 SUPPORT',
        'backup': '💾 BACKUP',
        'terminal': '🖥️ TERMINAL',
        'admins': '👥 ADMINS',
        'bot1': '🤖 BOT 1 SETTINGS'
    }
    
    # Create toggle buttons for each permission
    perm_buttons = []
    for perm_key, perm_label in all_permissions.items():
        # Check if this permission is currently enabled
        if 'all' in current_perms or perm_key in current_perms:
            button_text = f"✅ {perm_label}"
        else:
            button_text = f"❌ {perm_label}"
        perm_buttons.append([KeyboardButton(text=button_text)])
    
    # Add quick action buttons
    perm_buttons.append([
        KeyboardButton(text="✅ GRANT ALL"),
        KeyboardButton(text="❌ REVOKE ALL")
    ])
    
    # Add Save and Cancel buttons
    perm_buttons.append([KeyboardButton(text="💾 SAVE CHANGES")])
    perm_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    perm_kb = ReplyKeyboardMarkup(keyboard=perm_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"👤 Admin: {admin_doc.get('name', str(user_id))} (`{user_id}`)\n"
        f"👔 Role: {admin_doc.get('role', 'Admin')}\n\n"
        f"Toggle permissions below:\n"
        f"✅ = Enabled | ❌ = Disabled\n\n"
        f"Click permissions to toggle, then SAVE CHANGES",
        reply_markup=perm_kb
    )
    await state.set_state(AdminStates.toggling_permissions)

@dp.message(AdminStates.toggling_permissions)
async def process_permission_toggle(message: types.Message, state: FSMContext):
    """Process permission toggle actions"""
    # Handle cancel/back
    if message.text in ["❌ CANCEL", "🔙 BACK"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Get current data
    data = await state.get_data()
    user_id = data.get("permission_admin_id")
    admin_name = data.get("permission_admin_name", str(user_id))
    current_perms = data.get("current_permissions", [])
    
    # Permission mapping
    perm_map = {
        '📢 BROADCAST': 'broadcast',
        '🔍 FIND': 'find',
        '📊 TRAFFIC': 'traffic',
        '🩺 DIAGNOSIS': 'diagnosis',
        '📸 SHOOT': 'shoot',
        '💬 SUPPORT': 'support',
        '💾 BACKUP': 'backup',
        '🖥️ TERMINAL': 'terminal',
        '👥 ADMINS': 'admins',
        '🤖 BOT 1 SETTINGS': 'bot1'
    }
    
    # Handle SAVE CHANGES — permissions can always be saved, even while locked.
    # Locked admins won't have these active until unlocked.
    if message.text == "💾 SAVE CHANGES":
        admin_doc = col_admins.find_one({"user_id": user_id})
        is_locked = admin_doc.get("locked", False) if admin_doc else False
        try:
            col_admins.update_one(
                {"user_id": user_id},
                {"$set": {"permissions": current_perms, "updated_at": now_local()}}
            )
            log_action("🔐 PERMISSIONS UPDATED", message.from_user.id,
                      f"Updated permissions for {user_id} (locked={is_locked})")
            _perm_labels = {
                'broadcast': '📢 Broadcast', 'find': '🔍 Find',
                'traffic': '📊 Traffic', 'diagnosis': '🩺 Diagnosis',
                'shoot': '📸 Shoot', 'support': '💬 Support',
                'backup': '💾 Backup', 'terminal': '🖥️ Terminal',
                'admins': '👥 Admins', 'bot1': '🤖 Bot 1 Settings'
            }
            perm_display = ", ".join(_perm_labels.get(p, p) for p in current_perms) if current_perms else "None"
            _lock_note = (
                "\n\n⚠️ **Admin is currently LOCKED.**\n"
                "These permissions are saved and **will activate automatically once unlocked**."
            ) if is_locked else ""
            await message.answer(
                f"✅ **PERMISSIONS SAVED**\n\n"
                f"👤 Admin: {admin_name} (`{user_id}`)\n"
                f"🔐 Permissions set: {perm_display}{_lock_note}",
                reply_markup=get_admin_menu(),
                parse_mode="Markdown"
            )

            # Instant button/feature refresh for active admins
            if not is_locked:
                await _push_instant_user_menu_refresh(user_id, context="permissions updated")

            await state.clear()
        except Exception as e:
            await message.answer(
                f"❌ Error saving permissions: {str(e)}",
                reply_markup=get_admin_menu()
            )
            await state.clear()
        return

    # Handle GRANT ALL
    if message.text == "✅ GRANT ALL":
        current_perms = list(perm_map.values())
        await state.update_data(current_permissions=current_perms)

    # Handle REVOKE ALL
    elif message.text == "❌ REVOKE ALL":
        current_perms = []
        await state.update_data(current_permissions=current_perms)

    # Handle individual permission toggle
    else:
        button_text = message.text.replace("✅ ", "").replace("❌ ", "")
        if button_text in perm_map:
            perm_key = perm_map[button_text]
            if perm_key in current_perms:
                current_perms.remove(perm_key)
            else:
                current_perms.append(perm_key)
            if 'all' in current_perms:
                current_perms.remove('all')
            await state.update_data(current_permissions=current_perms)

    # Rebuild permission UI with updated state
    all_permissions = {
        'broadcast': '📢 BROADCAST',
        'find': '🔍 FIND',
        'traffic': '📊 TRAFFIC',
        'diagnosis': '🩺 DIAGNOSIS',
        'shoot': '📸 SHOOT',
        'support': '💬 SUPPORT',
        'backup': '💾 BACKUP',
        'terminal': '🖥️ TERMINAL',
        'admins': '👥 ADMINS',
        'bot1': '🤖 BOT 1 SETTINGS'
    }
    
    perm_buttons = []
    for perm_key, perm_label in all_permissions.items():
        if perm_key in current_perms:
            button_text = f"✅ {perm_label}"
        else:
            button_text = f"❌ {perm_label}"
        perm_buttons.append([KeyboardButton(text=button_text)])
    
    perm_buttons.append([
        KeyboardButton(text="✅ GRANT ALL"),
        KeyboardButton(text="❌ REVOKE ALL")
    ])
    perm_buttons.append([KeyboardButton(text="💾 SAVE CHANGES")])
    perm_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    perm_kb = ReplyKeyboardMarkup(keyboard=perm_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"👤 Admin: {admin_name} (`{user_id}`)\n\n"
        f"Toggle permissions below:\n"
        f"✅ = Enabled | ❌ = Disabled\n\n"
        f"Click permissions to toggle, then SAVE CHANGES\n\n"
        f"Current: {', '.join(current_perms) if current_perms else 'None'}",
        reply_markup=perm_kb
    )

@dp.message(F.text == "👔 MANAGE ROLES")
async def manage_roles_handler(message: types.Message, state: FSMContext):
    """Change admin roles - with pagination"""
    log_action("👔 MANAGE ROLES", message.from_user.id, "Managing admin roles")
    
    # Exclude Master Admin and Owners from the list
    admins = list(col_admins.find({
        "user_id": {"$ne": MASTER_ADMIN_ID},
        "role": {"$ne": "Owner"}
    }))
    if not admins:
        await message.answer(
            "⚠️ No other admins found.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Pagination: 10 admins per page
    page = 0
    await state.update_data(role_page=page, admins_list=admins)
    
    ITEMS_PER_PAGE = 10
    total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create admin buttons — show lock status + current role
    admin_buttons = []
    for admin in page_admins:
        uid  = admin['user_id']
        name = admin.get('name', str(uid))
        role = admin.get('role', 'Admin')
        is_locked = admin.get('locked', False)
        lock_icon = "🔒" if is_locked else "🔓"
        role_icon = _ROLE_DESCRIPTIONS.get(role, ("", ""))[0]
        if name != str(uid):
            label = f"{lock_icon} {name} ({uid}) — {role_icon}{role}"
        else:
            label = f"{lock_icon} ({uid}) — {role_icon}{role}"
        admin_buttons.append([KeyboardButton(text=label)])
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))
    
    if nav_buttons:
        admin_buttons.append(nav_buttons)
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
    
    await message.answer(
        f"👔 MANAGE ROLES\n\n"
        f"🔒 = LOCKED (Inactive)  🔓 = UNLOCKED (Active)\n\n"
        f"Select admin to change role:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.waiting_for_role_admin_id)

@dp.message(AdminStates.waiting_for_role_admin_id)
async def process_role_admin_id(message: types.Message, state: FSMContext):
    """Process role change admin ID - with pagination and role selection.
    Also handles BANNED LIST pagination (⬅️ PREV PAGE / NEXT PAGE ➡️)."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    data = await state.get_data()

    # ── Banned list pagination (uses different nav buttons to avoid conflict) ──
    if message.text in ["⬅️ PREV PAGE", "NEXT PAGE ➡️"]:
        current_page = data.get("banned_list_page", 0)
        new_page = max(0, current_page - 1) if message.text == "⬅️ PREV PAGE" else current_page + 1
        await state.update_data(banned_list_page=new_page)
        
        all_admins = list(col_admins.find({}))
        banned_admins = []
        for admin in all_admins:
            if col_banned_users.find_one({"user_id": admin['user_id']}):
                ban_doc = col_banned_users.find_one({"user_id": admin['user_id']})
                admin['ban_info'] = ban_doc
                banned_admins.append(admin)
        
        per_page = 10
        total_pages = (len(banned_admins) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(banned_admins))
        page_admins = banned_admins[start_idx:end_idx]
        
        msg = f"📋 BANNED ADMINS LIST\n\n"
        msg += f"Total Banned: {len(banned_admins)}\n"
        msg += f"Showing {start_idx + 1}-{end_idx}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for admin in page_admins:
            uid = admin['user_id']
            name = admin.get('name', str(uid))
            role = admin.get('role', 'Admin')
            ban_info = admin.get('ban_info', {})
            
            if name != str(uid):
                msg += f"👤 **{name}** (`{uid}`)\n"
            else:
                msg += f"👤 **{uid}**\n"
                
            msg += f"👔 Role: {role}\n"
            msg += f"📅 Banned: {format_datetime(ban_info.get('banned_at'))}\n"
            msg += f"👨‍💼 By: {ban_info.get('banned_by', 'Unknown')}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV PAGE"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT PAGE ➡️"))
        list_kb_buttons = [nav_buttons] if nav_buttons else []
        list_kb_buttons.append([KeyboardButton(text="🔙 BACK")])
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=list_kb_buttons, resize_keyboard=True))
        return

    # ── Role selection pagination (uses ⬅️ PREV ADMINS / ➡️ NEXT ADMINS) ──
    admins_list = data.get('admins_list', [])
    
    if message.text in ["⬅️ PREV ADMINS", "➡️ NEXT ADMINS"]:
        current_page = data.get("role_page", 0)
        new_page = max(0, current_page - 1) if message.text == "⬅️ PREV ADMINS" else current_page + 1
        await state.update_data(role_page=new_page)
        
        ITEMS_PER_PAGE = 10
        total_pages = max(1, (len(admins_list) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        
        # Cap new_page just in case
        new_page = min(new_page, max(0, total_pages - 1))
        
        start_idx = new_page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins_list))
        page_admins = admins_list[start_idx:end_idx]
        
        admin_buttons = []
        for admin in page_admins:
            uid  = admin['user_id']
            name = admin.get('name', str(uid))
            role = admin.get('role', 'Admin')
            is_locked = admin.get('locked', False)
            lock_icon = "🔒" if is_locked else "🔓"
            role_icon = _ROLE_DESCRIPTIONS.get(role, ("", ""))[0]
            if name != str(uid):
                lbl = f"{lock_icon} {name} ({uid}) — {role_icon}{role}"
            else:
                lbl = f"{lock_icon} ({uid}) — {role_icon}{role}"
            admin_buttons.append([KeyboardButton(text=lbl)])
        
        nav_buttons = []
        if new_page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
        if new_page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        await message.answer(
            f"👔 MANAGE ROLES\n\n"
            f"🔒 = LOCKED  🔓 = UNLOCKED\n\n"
            f"Select admin to change role:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins_list)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        )
        return
    
    # ── Parse user ID from button text ──
    try:
        user_id = _parse_admin_uid(message.text)
    except (ValueError, IndexError):
        await message.answer("⚠️ Invalid selection.")
        return

    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return

    await state.update_data(role_admin_id=user_id)

    # Build profile snapshot for the master admin to review
    current_role = admin_doc.get('role', 'Admin')
    current_perms = admin_doc.get('permissions', [])
    is_locked = admin_doc.get('locked', False)
    role_icon, role_desc = _ROLE_DESCRIPTIONS.get(current_role, ("", ""))
    lock_badge = "🔒 LOCKED — changes save as PENDING" if is_locked else "🔓 UNLOCKED — changes activate immediately"
    perms_display = ", ".join(_PERM_LABELS.get(p, p) for p in current_perms) if current_perms else "None"

    type_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔵 AUTO ROLES")],
            [KeyboardButton(text="⚙️ CUSTOM ROLE")],
            [KeyboardButton(text="🔙 BACK")]
        ],
        resize_keyboard=True
    )

    await message.answer(
        f"👔 **CHANGE ROLE**\n\n"
        f"👤 Admin: {admin_doc.get('name', str(user_id))} (`{user_id}`)\n"
        f"📋 Current Role: {role_icon} **{current_role}** — _{role_desc}_\n"
        f"🔑 Permissions: {perms_display}\n"
        f"Status: {lock_badge}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**🔵 AUTO ROLES** — Select a role template.\n"
        f"Permissions are automatically assigned based on the role.\n\n"
        f"**⚙️ CUSTOM ROLE** — Select a role label only.\n"
        f"Permissions remain unchanged (edit manually via 🔐 PERMISSIONS).",
        reply_markup=type_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_role_type)



@dp.message(AdminStates.waiting_for_role_type)
async def process_role_type(message: types.Message, state: FSMContext):
    """Handle AUTO ROLES vs CUSTOM ROLE choice, then show role buttons."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_admin_menu())
        return

    if message.text not in ["🔵 AUTO ROLES", "⚙️ CUSTOM ROLE"]:
        await message.answer("⚠️ Please select 🔵 AUTO ROLES or ⚙️ CUSTOM ROLE.")
        return

    is_auto = message.text == "🔵 AUTO ROLES"
    await state.update_data(role_type="auto" if is_auto else "custom")

    role_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 OWNER")],
            [KeyboardButton(text="🔴 MANAGER"), KeyboardButton(text="🟡 ADMIN")],
            [KeyboardButton(text="🟢 MODERATOR"), KeyboardButton(text="🔵 SUPPORT")],
            [KeyboardButton(text="🔙 BACK")]
        ],
        resize_keyboard=True
    )

    if is_auto:
        info = (
            "👔 **AUTO ROLE ASSIGNMENT**\n\n"
            "Select a role — permissions will be auto-applied:\n\n"
            "👑 **OWNER** — All 10 permissions\n"
            "🔴 **MANAGER** — All 10 permissions\n"
            "🟡 **ADMIN** — Broadcast · Find · Traffic · Support · Backup\n"
            "🟢 **MODERATOR** — Support · Find · Traffic\n"
            "🔵 **SUPPORT** — Support only\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Role + permissions applied automatically on selection."
        )
    else:
        info = (
            "⚙️ **CUSTOM ROLE ASSIGNMENT**\n\n"
            "Select a role label — permissions will **NOT** change.\n"
            "Manage permissions manually via 🔐 PERMISSIONS.\n\n"
            "👑 **OWNER**  🔴 **MANAGER**\n"
            "🟡 **ADMIN**  🟢 **MODERATOR**\n"
            "🔵 **SUPPORT**"
        )

    await message.answer(info, reply_markup=role_kb, parse_mode="Markdown")
    await state.set_state(AdminStates.selecting_role)


@dp.message(AdminStates.selecting_role)
async def process_role_selection(message: types.Message, state: FSMContext):
    """Process role selection OR ban/unban admin selection (shared state)"""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return

    data = await state.get_data()
    ban_action = data.get("ban_action")  # Set only when coming from BAN CONFIG flow

    # ── BAN/UNBAN FLOW ──
    if ban_action:
        admins_list = data.get("admins_list", [])

        # Handle pagination
        if message.text in ["⬅️ PREV", "NEXT ➡️"]:
            current_page = data.get("ban_page", 0)
            new_page = max(0, current_page - 1) if message.text == "⬅️ PREV" else current_page + 1
            await state.update_data(ban_page=new_page)

            per_page = 10
            total_pages = (len(admins_list) + per_page - 1) // per_page
            start_idx = new_page * per_page
            end_idx = min(start_idx + per_page, len(admins_list))
            page_admins = admins_list[start_idx:end_idx]

            admin_buttons = []
            for admin in page_admins:
                admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

            nav_buttons = []
            if total_pages > 1:
                if new_page > 0:
                    nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
                if new_page < total_pages - 1:
                    nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
            if nav_buttons:
                admin_buttons.append(nav_buttons)
            admin_buttons.append([KeyboardButton(text="🔙 BACK")])

            action_text = "BAN" if ban_action == "ban" else "UNBAN"
            target_label = "admin" if ban_action == "ban" else "user"
            status_text = "unbanned admins" if ban_action == "ban" else "banned users"
            await message.answer(
                f"{'🚫' if ban_action == 'ban' else '✅'} {action_text} {target_label.upper()}\n\n"
                f"Select {target_label} to {action_text}:\n"
                f"Showing {start_idx + 1}-{end_idx} of {len(admins_list)} {status_text}"
                f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
                reply_markup=ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
            )
            return

        # Parse user ID from button text
        try:
            user_id = _parse_admin_uid(message.text)
        except (ValueError, IndexError):
            await message.answer("⚠️ Invalid selection.")
            return

        if ban_action == "ban":
            admin_doc = col_admins.find_one({"user_id": user_id})
            if not admin_doc:
                await message.answer(f"⚠️ User {user_id} is not an admin.")
                return

            # ── BLOCK: must remove admin first ──
            is_still_admin = col_admins.find_one({"user_id": user_id}) is not None
            if is_still_admin and user_id != MASTER_ADMIN_ID:
                await message.answer(
                    f"🚫 **CANNOT BAN AN ACTIVE ADMIN**\n\n"
                    f"👤 User ID: `{user_id}`\n"
                    f"👔 Role: **{admin_doc.get('role', 'Admin')}**\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Protocol requires:**\n"
                    f"1️⃣ First use **➖ REMOVE ADMIN** to strip their admin status\n"
                    f"2️⃣ Then use **🚫 BAN ADMIN** to ban them\n\n"
                    f"This prevents partial-access vulnerabilities.\n\n"
                    f"_Remove admin role first, then proceed with ban._",
                    reply_markup=get_admin_menu(),
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            ban_doc = {
                "user_id": user_id,
                "banned_by": message.from_user.id,
                "banned_at": now_local(),
                "reason": "Banned by master admin",
                "status": "banned",
                "scope": "bot2"  # Only blocks Bot 2 admin access, NOT Bot 1
            }
            try:
                col_banned_users.update_one(
                    {"user_id": user_id, "scope": "bot2"},
                    {"$setOnInsert": ban_doc},
                    upsert=True
                )
                log_action("🚫 ADMIN BANNED (BOT2)", message.from_user.id, f"Banned admin from Bot 2: {user_id}")
                await message.answer(
                    f"🚫 **ADMIN BANNED FROM BOT 2**\n\n"
                    f"👤 User ID: `{user_id}`\n"
                    f"📅 Banned: {now_local().strftime('%B %d, %Y — %I:%M %p')}\n\n"
                    f"This user can no longer access Bot 2 admin panel.\n"
                    f"Their Bot 1 access is **NOT affected**.",
                    reply_markup=get_admin_menu(),
                    parse_mode="Markdown"
                )
            except Exception as e:
                await message.answer(f"❌ Error banning: {str(e)}", reply_markup=get_admin_menu())

        elif ban_action == "unban":
            try:
                result = col_banned_users.delete_many({"user_id": user_id, "scope": "bot2"})
                if result.deleted_count == 0:
                    await message.answer(
                        f"⚠️ User `{user_id}` is not currently banned in Bot 2.",
                        reply_markup=get_admin_menu(),
                        parse_mode="Markdown"
                    )
                else:
                    extra = f"\n🧹 Removed duplicate ban records: {result.deleted_count - 1}" if result.deleted_count > 1 else ""
                    log_action("✅ USER UNBANNED (BOT2)", message.from_user.id, f"Unbanned user from Bot 2: {user_id}")
                    await message.answer(
                        f"✅ **USER UNBANNED (BOT 2)**\n\n"
                        f"👤 User ID: `{user_id}`\n"
                        f"📅 Unbanned: {now_local().strftime('%B %d, %Y — %I:%M %p')}"
                        f"{extra}\n\n"
                        f"This user can now access Bot 2 admin panel again.",
                        reply_markup=get_admin_menu(),
                        parse_mode="Markdown"
                    )
            except Exception as e:
                await message.answer(f"❌ Error unbanning: {str(e)}", reply_markup=get_admin_menu())
        await state.clear()
        return

    # ── ROLE CHANGE FLOW ──
    role_map = {
        "👑 OWNER":    "Owner",
        "🔴 MANAGER":  "Manager",
        "🟡 ADMIN":    "Admin",
        "🟢 MODERATOR": "Moderator",
        "🔵 SUPPORT":  "Support",
    }

    if message.text not in role_map:
        await message.answer("⚠️ Please select a valid role from the buttons.")
        return

    new_role = role_map[message.text]
    user_id = data.get("role_admin_id")

    if not user_id:
        await message.answer("⚠️ Session expired. Please try again.")
        await state.clear()
        return

    # ── OWNER TRANSFER: requires triple confirmation + password ──
    if new_role == "Owner":
        await state.update_data(owner_transfer_target=user_id)
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True
        )
        await message.answer(
            "👑 **OWNERSHIP TRANSFER — STEP 1 OF 3**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ **CRITICAL ACTION: PERMANENT**\n\n"
            "Transferring ownership is **irreversible**.\n"
            "The target user will receive full Owner-level authority.\n\n"
            "To proceed, type exactly:\n"
            "`CONFIRM`",
            reply_markup=cancel_kb,
            parse_mode="Markdown"
        )
        await state.set_state(AdminStates.owner_transfer_first_confirm)
        return

    # ── REGULAR ROLE UPDATE ──
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer("⚠️ Admin not found. Session expired.", reply_markup=get_admin_menu())
        await state.clear()
        return

    is_locked  = admin_doc.get('locked', False)
    admin_name = admin_doc.get('name', str(user_id))
    role_type  = data.get("role_type", "custom")  # "auto" or "custom"

    # Build the DB update dict
    update_dict = {"role": new_role, "updated_at": now_local()}
    if role_type == "auto":
        update_dict["permissions"] = _ROLE_PERMISSION_TEMPLATES.get(new_role, [])

    col_admins.update_one({"user_id": user_id}, {"$set": update_dict})
    log_action("👔 ROLE CHANGED", message.from_user.id, f"Changed {user_id} to {new_role} (mode={role_type})")

    # ── If admin is LOCKED — save silently, show pending note ──
    if is_locked:
        if role_type == "auto":
            saved_perms = _ROLE_PERMISSION_TEMPLATES.get(new_role, [])
            perm_str = ", ".join(_PERM_LABELS.get(p, p) for p in saved_perms) if saved_perms else "None"
            perm_note = f"\n🔑 Permissions saved: {perm_str}"
        else:
            perm_note = "\n🔑 Permissions: unchanged (custom mode)"

        await message.answer(
            f"📋 **ROLE SAVED (PENDING)**\n\n"
            f"👤 Admin: {admin_name} (`{user_id}`)\n"
            f"👔 New Role: **{new_role}** ({'AUTO' if role_type == 'auto' else 'CUSTOM'}){perm_note}\n\n"
            f"⚠️ This admin is currently **LOCKED**.\n"
            f"The role{'and permissions' if role_type == 'auto' else ''} will activate once unlocked.\n\n"
            f"Use 🔒 LOCK/UNLOCK USER to activate.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    # ── Admin is UNLOCKED — apply immediately + notify ──

    # ── NOTIFY UNLOCKED ADMIN OF NEW ROLE ──
    _ROLE_NOTIFY = {
        "Manager": (
            "🔴 **ROLE ASSIGNMENT: MANAGER**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Manager** of the MSA NODE system.\n\n"
            "**Your Authority:**\n"
            "• Full oversight of administrative operations\n"
            "• Management of broadcasts, support teams & junior admins\n"
            "• Enforcement of system integrity and security protocols\n"
            "• Access to all Bot 2 management features\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚡ This is a position of significant trust.\n"
            "Execute your responsibilities with precision and discipline.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Admin": (
            "🟡 **ROLE ASSIGNMENT: ADMIN**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Admin** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Execute broadcasts and manage user traffic\n"
            "• Handle escalated support tickets\n"
            "• Monitor system diagnostics and report anomalies\n"
            "• Uphold community standards and guidelines\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📋 Adhere to operational protocols at all times.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Moderator": (
            "🟢 **ROLE ASSIGNMENT: MODERATOR**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Moderator** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Verify user authenticity and content compliance\n"
            "• Assist with support ticket resolution\n"
            "• Monitor community interactions\n"
            "• Escalate issues to Admin tier when required\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🎯 Maintain professional standards in all interactions.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Support": (
            "🔵 **ROLE ASSIGNMENT: SUPPORT**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Support Staff** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Provide timely assistance to user inquiries\n"
            "• Resolve routine support tickets efficiently\n"
            "• Escalate complex issues to Moderators/Admins\n"
            "• Maintain a helpful, professional tone at all times\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💬 User satisfaction is your top priority.\n\n"
            "_— MSA NODE Systems_"
        ),
    }

    notification = _ROLE_NOTIFY.get(new_role)
    if notification:
        try:
            await bot.send_message(user_id, notification, parse_mode="Markdown")
            log_action("📨 ROLE NOTIFICATION SENT", user_id, f"Notified: {new_role}")
        except Exception as e:
            log_action("⚠️ ROLE NOTIFY FAILED", user_id, str(e))

    # Push updated keyboard instantly so features/buttons match new effective permissions
    await _push_instant_user_menu_refresh(user_id, context="role/permissions updated")

    mode_badge = "AUTO (permissions applied)" if role_type == "auto" else "CUSTOM (permissions unchanged)"
    if role_type == "auto":
        saved_perms = _ROLE_PERMISSION_TEMPLATES.get(new_role, [])
        mode_detail = f"\n🔑 Permissions set: {', '.join(_PERM_LABELS.get(p, p) for p in saved_perms) or 'None'}"
    else:
        mode_detail = ""

    await message.answer(
        f"✅ **ROLE UPDATED**\n\n"
        f"👤 Admin: {admin_name} (`{user_id}`)\n"
        f"👔 New Role: **{new_role}**\n"
        f"⚙️ Mode: {mode_badge}{mode_detail}\n\n"
        f"📨 Notification sent to admin.",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown"
    )
    await state.clear()


# ==========================================
# 👑 OWNER TRANSFER FLOW (triple confirm + password)
# ==========================================
_OWNER_TRANSFER_PASSWORD = os.getenv("OWNER_TRANSFER_PW", "")  # Set OWNER_TRANSFER_PW on Render; never hardcode here

@dp.message(AdminStates.owner_transfer_first_confirm)
async def owner_transfer_step1(message: types.Message, state: FSMContext):
    """Ownership transfer — step 1: type CONFIRM"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != "CONFIRM":
        await message.answer(
            "⚠️ Incorrect. Type exactly: `CONFIRM`\n\nOr press ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await message.answer(
        "👑 **OWNERSHIP TRANSFER — STEP 2 OF 3**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "This action cannot be undone.\n\n"
        "To proceed, type exactly:\n"
        "`TRANSFER`",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.owner_transfer_second_confirm)


@dp.message(AdminStates.owner_transfer_second_confirm)
async def owner_transfer_step2(message: types.Message, state: FSMContext):
    """Ownership transfer — step 2: type TRANSFER"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != "TRANSFER":
        await message.answer(
            "⚠️ Incorrect. Type exactly: `TRANSFER`\n\nOr press ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await message.answer(
        "👑 **OWNERSHIP TRANSFER — STEP 3 OF 3**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔐 Enter the **transfer password** to finalise:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.owner_transfer_password)


@dp.message(AdminStates.owner_transfer_password)
async def owner_transfer_step3(message: types.Message, state: FSMContext):
    """Ownership transfer — step 3: enter password"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != _OWNER_TRANSFER_PASSWORD:
        await message.answer(
            "🚫 **INCORRECT PASSWORD**\n\nOwnership transfer aborted for security.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    data = await state.get_data()
    target_id = data.get("owner_transfer_target")

    col_admins.update_one(
        {"user_id": target_id},
        {"$set": {"role": "Owner", "updated_at": now_local()}}
    )
    log_action("👑 OWNERSHIP TRANSFERRED", message.from_user.id, f"Transferred ownership to {target_id}")

    try:
        await bot.send_message(
            target_id,
            "👑 **OWNERSHIP TRANSFERRED TO YOU**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You are now the **Owner** of the MSA NODE system.\n\n"
            "**Full authority has been granted:**\n"
            "• Complete control over all system operations\n"
            "• Management of all admin tiers\n"
            "• Unrestricted access to every feature\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚡ This transfer is **permanent and irreversible**.\n\n"
            "_— MSA NODE Systems_",
            parse_mode="Markdown"
        )
    except Exception as e:
        log_action("⚠️ OWNER NOTIFY FAILED", target_id, str(e))

    await message.answer(
        f"👑 **OWNERSHIP TRANSFERRED**\n\n"
        f"👤 New Owner: `{target_id}`\n"
        f"📅 {now_local().strftime('%B %d, %Y — %I:%M %p')}\n\n"
        f"This action is permanent.",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown"
    )
    await state.clear()


async def _send_lock_unlock_page(message: types.Message, state: FSMContext, page: int = 0):
    """Helper to send the lock/unlock paginated keyboard"""
    # Exclude Master Admin and Owners from the list
    admins = list(col_admins.find({
        "user_id": {"$ne": MASTER_ADMIN_ID},
        "role": {"$ne": "Owner"}
    }))
    if not admins:
        await message.answer("⚠️ No other admins found.", reply_markup=get_admin_menu())
        return

    ITEMS_PER_PAGE = 10
    total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    # Keep page within bounds
    page = max(0, min(page, total_pages - 1))
    await state.update_data(lock_page=page, lock_admins_list=admins)

    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
    page_admins = admins[start_idx:end_idx]

    admin_buttons = []
    for admin in page_admins:
        uid = admin['user_id']
        name = admin.get('name', str(uid))
        is_locked = admin.get('locked', False)
        lock_icon = "🔒" if is_locked else "🔓"
        if name != str(uid):
            admin_buttons.append([KeyboardButton(text=f"{lock_icon} {name} ({uid})")])
        else:
            admin_buttons.append([KeyboardButton(text=f"{lock_icon} ({uid})")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

    if nav_buttons:
        admin_buttons.append(nav_buttons)
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])

    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)

    await message.answer(
        f"🔒 LOCK/UNLOCK ADMIN\n\n"
        f"🔒 = LOCKED (Inactive - Cannot access Bot 2)\n"
        f"🔓 = UNLOCKED (Active - Full access)\n\n"
        f"Select admin to toggle lock status:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.waiting_for_lock_user_id)

@dp.message(F.text == "🔒 LOCK/UNLOCK USER")
async def lock_unlock_user_handler(message: types.Message, state: FSMContext):
    """Lock/unlock admin activation - with pagination"""
    log_action("🔒 LOCK/UNLOCK USER", message.from_user.id, "Managing admin lock status")
    await _send_lock_unlock_page(message, state, 0)

@dp.message(AdminStates.waiting_for_lock_user_id)
async def process_lock_admin_selection(message: types.Message, state: FSMContext):
    """Admin selected from pagination for lock/unlock. Show the action menu."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_admin_menu())
        return
    
    data = await state.get_data()
    current_page = data.get("lock_page", 0)
    
    # Handle pagination
    if message.text in ["⬅️ PREV ADMINS", "➡️ NEXT ADMINS"]:
        new_page = max(0, current_page - 1) if message.text == "⬅️ PREV ADMINS" else current_page + 1
        await _send_lock_unlock_page(message, state, new_page)
        return
    
    # Parse user ID from lock button text
    try:
        user_id = _parse_admin_uid(message.text)
    except (ValueError, IndexError):
        await message.answer("⚠️ Invalid selection.")
        return
    
    # Prevent modifying Master Admin
    if user_id == MASTER_ADMIN_ID:
        await message.answer("🚫 You cannot lock or unlock the Master Admin.")
        return
    
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return
    
    admin_name = admin_doc.get('name', str(user_id))
    is_locked = admin_doc.get('locked', False)
    
    # Store target
    await state.update_data(target_lock_admin_id=user_id, target_lock_admin_name=admin_name)
    await state.set_state(AdminStates.waiting_for_lock_action)
    
    status_text = "🔒 LOCKED" if is_locked else "🔓 UNLOCKED"
    toggle_text = "🔓 UNLOCK" if is_locked else "🔒 LOCK"
    
    action_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=toggle_text)],
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🔒 **LOCK MANAGEMENT**\n\n"
        f"👤 Admin: {admin_name} (`{user_id}`)\n"
        f"Current Status: **{status_text}**\n\n"
        f"Select action below:",
        reply_markup=action_kb,
        parse_mode="Markdown"
    )

@dp.message(AdminStates.waiting_for_lock_action)
async def execute_lock_action(message: types.Message, state: FSMContext):
    """Execute the lock/unlock action and return to pagination list."""
    if message.text == "❌ CANCEL":
        data = await state.get_data()
        current_page = data.get("lock_page", 0)
        # Return to the paginated lock/unlock view
        await _send_lock_unlock_page(message, state, current_page)
        return
    
    data = await state.get_data()
    user_id = data.get("target_lock_admin_id")
    admin_name = data.get("target_lock_admin_name", str(user_id))
    
    if not user_id:
        await message.answer("⚠️ Session expired.", reply_markup=get_admin_menu())
        await state.clear()
        return
        
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is no longer an admin.")
        return
        
    current_lock = admin_doc.get('locked', False)
    
    if message.text == "🔒 LOCK":
        if current_lock:
            await message.answer("⚠️ Admin is already locked.")
            await _send_lock_unlock_page(message, state, data.get("lock_page", 0))
            return
        new_lock = True
    elif message.text == "🔓 UNLOCK":
        if not current_lock:
            await message.answer("⚠️ Admin is already unlocked.")
            await _send_lock_unlock_page(message, state, data.get("lock_page", 0))
            return
        new_lock = False
    else:
        await message.answer("⚠️ Invalid action. Use 🔒 LOCK or 🔓 UNLOCK.")
        return
    
    # Toggle lock status in DB
    col_admins.update_one(
        {"user_id": user_id},
        {"$set": {"locked": new_lock, "updated_at": now_local()}}
    )
    
    status_text = "LOCKED (Inactive)" if new_lock else "UNLOCKED (Active)"
    icon = "🔒" if new_lock else "🔓"
    
    log_action(f"{icon} ADMIN STATUS CHANGED", message.from_user.id, 
              f"Set {user_id} to {status_text}")

    # ── LOCKED: immediately strip menu from the target user ──
    if new_lock:
        try:
            await bot.send_message(
                user_id,
                "🔒 **YOUR ACCOUNT HAS BEEN LOCKED**\n\n"
                "Your access to Bot 2 has been suspended by the master admin.\n"
                "Your menu has been removed. Contact the owner to be unlocked.\n\n"
                "_— MSA NODE Systems_",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode="Markdown"
            )
            log_action("📨 LOCK NOTIFICATION", user_id, "Sent lock notification + removed menu")
        except Exception as e:
            log_action("⚠️ LOCK NOTIFY FAILED", user_id, str(e))

    # ── UNLOCKED: restore role notification + menu ──
    elif not new_lock:
        admin_role = admin_doc.get('role', 'Admin')
        _ROLE_NOTIFY_LOCK = {
            "Owner": (
                "👑 **WELCOME BACK, OWNER**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Owner** account has been unlocked.\n"
                "You have full, unrestricted authority over the MSA NODE system.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Manager": (
                "🔴 **ACCOUNT UNLOCKED — MANAGER**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Manager** account has been restored to active status.\n\n"
                "**Your Authority:**\n"
                "• Full oversight of administrative operations\n"
                "• Management of broadcasts, support teams & junior admins\n"
                "• Access to all Bot 2 management features\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Admin": (
                "🟡 **ACCOUNT UNLOCKED — ADMIN**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Admin** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Execute broadcasts and manage user traffic\n"
                "• Handle escalated support tickets\n"
                "• Monitor system diagnostics\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Moderator": (
                "🟢 **ACCOUNT UNLOCKED — MODERATOR**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Moderator** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Verify user authenticity and content compliance\n"
                "• Assist with support ticket resolution\n"
                "• Escalate issues to Admin tier when required\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Support": (
                "🔵 **ACCOUNT UNLOCKED — SUPPORT**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Support** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Respond to first-tier user inquiries\n"
                "• Process and route support tickets\n"
                "• Maintain professional communication standards\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
        }
        notify_text = _ROLE_NOTIFY_LOCK.get(
            admin_role,
            f"🔓 **ACCOUNT UNLOCKED**\n\nYour admin account is now active.\nRole: **{admin_role}**\n\nUse /start to access your menu.\n\n_— MSA NODE Systems_"
        )
        try:
            await bot.send_message(user_id, notify_text, parse_mode="Markdown")
            # Send personal dynamic menu immediately after notification
            admin_menu_kb = await get_main_menu(user_id)
            await bot.send_message(
                user_id,
                "📋 Your menu has been restored:",
                reply_markup=admin_menu_kb
            )
            log_action("📨 UNLOCK NOTIFICATION", user_id, f"Sent unlock notification (role: {admin_role})")
        except Exception as e:
            log_action("⚠️ UNLOCK NOTIFY FAILED", user_id, str(e))
    
    await message.answer(
        f"✅ STATUS UPDATED\n\n"
        f"👤 User: {user_id}\n"
        f"{icon} Status: {status_text}\n\n"
        f"{'⚠️ This admin CANNOT access Bot 2 until unlocked!' if new_lock else '✅ This admin can now access Bot 2!'}"
    )
    
    # Stay on the same paginated keyboard to allow continuous toggling
    await _send_lock_unlock_page(message, state, data.get("lock_page", 0))


@dp.message(F.text == "🚫 BAN CONFIG")
async def ban_config_handler(message: types.Message, state: FSMContext):
    """Ban/Unban configuration - show choice"""
    log_action("🚫 BAN CONFIG", message.from_user.id, "Opened ban configuration")
    
    choice_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚫 BAN ADMIN")],
            [KeyboardButton(text="✅ UNBAN ADMIN")],
            [KeyboardButton(text="📋 BANNED LIST")],
            [KeyboardButton(text="🔙 BACK")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚫 BAN/UNBAN CONFIGURATION\n\n"
        "Choose an action:\n"
        "• 🚫 BAN ADMIN - Ban any user by User ID (Bot 2 scope)\n"
        "• ✅ UNBAN ADMIN - Remove Bot 2 ban by selecting user\n"
        "• 📋 BANNED LIST - View all Bot 2 banned users",
        reply_markup=choice_kb
    )
    await state.set_state(AdminStates.waiting_for_ban_user_id)

@dp.message(AdminStates.waiting_for_ban_user_id)
async def process_ban_choice(message: types.Message, state: FSMContext):
    """Process BAN or UNBAN choice"""
    # Handle back/cancel
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return

    def _get_unique_bot2_banned_user_ids() -> list[int]:
        """Return bot2-scoped banned user IDs (deduplicated, newest first)."""
        seen = set()
        ordered = []
        for d in col_banned_users.find({"scope": "bot2"}).sort("banned_at", -1):
            uid = d.get("user_id")
            if not isinstance(uid, int):
                continue
            if uid == MASTER_ADMIN_ID:
                continue
            if uid in seen:
                continue
            seen.add(uid)
            ordered.append(uid)
        return ordered

    # BANNED LIST pagination
    if message.text in ["⬅️ PREV PAGE", "NEXT PAGE ➡️"]:
        data = await state.get_data()
        banned_user_ids = data.get("bot2_banned_user_ids", [])
        if not banned_user_ids:
            await message.answer("⚠️ No Bot 2 banned users found.", reply_markup=get_admin_menu())
            await state.clear()
            return

        current_page = data.get("banned_list_page", 0)
        page = max(0, current_page - 1) if message.text == "⬅️ PREV PAGE" else current_page + 1

        per_page = 10
        total_pages = max(1, (len(banned_user_ids) + per_page - 1) // per_page)
        page = min(page, total_pages - 1)
        await state.update_data(banned_list_page=page)

        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(banned_user_ids))
        page_user_ids = banned_user_ids[start_idx:end_idx]

        msg = "📋 BANNED USERS LIST (BOT 2)\n\n"
        msg += f"Total Banned: {len(banned_user_ids)}\n"
        msg += f"Showing {start_idx + 1}-{end_idx} (Page {page + 1}/{total_pages})\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for uid in page_user_ids:
            ban_doc = col_banned_users.find_one(
                {"user_id": uid, "scope": "bot2"},
                sort=[("banned_at", -1)]
            ) or {}
            admin_doc = col_admins.find_one({"user_id": uid}) or {}
            name = admin_doc.get("name", str(uid))
            role = admin_doc.get("role", "User")
            banned_at = ban_doc.get("banned_at")
            banned_by = ban_doc.get("banned_by", "Unknown")

            if name != str(uid):
                msg += f"👤 {name} (`{uid}`)\n"
            else:
                msg += f"👤 ID: `{uid}`\n"
            msg += f"👔 Role: {role}\n"
            msg += f"📅 Banned: {format_datetime(banned_at)}\n"
            msg += f"👨💼 By: {banned_by}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV PAGE"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT PAGE ➡️"))

        list_kb_buttons = []
        if nav_buttons:
            list_kb_buttons.append(nav_buttons)
        list_kb_buttons.append([KeyboardButton(text="🔙 BACK")])

        list_kb = ReplyKeyboardMarkup(keyboard=list_kb_buttons, resize_keyboard=True)
        await message.answer(msg, reply_markup=list_kb, parse_mode="Markdown")
        await state.set_state(AdminStates.waiting_for_ban_user_id)
        return
    
    # Store choice in state
    if message.text == "🚫 BAN ADMIN":
        back_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True
        )
        await message.answer(
            "🚫 **BAN USER**\n\n"
            "Enter the **User ID** of the person you want to ban from Bot 2:\n\n"
            "💡 This bans any user by ID.\n"
            "  • If the user is an active admin, you must remove them from admin first.\n"
            "  • If they are not an admin, ban will proceed after confirmation.",
            reply_markup=back_kb,
            parse_mode="Markdown"
        )
        await state.set_state(AdminStates.waiting_for_ban_config_id)
        
    elif message.text == "✅ UNBAN ADMIN":
        await state.update_data(ban_action="unban")

        # Get unique bot2-scoped banned users (not only admins)
        banned_user_ids = _get_unique_bot2_banned_user_ids()
        if not banned_user_ids:
            await message.answer(
                "⚠️ No Bot 2 banned users to unban!",
                reply_markup=get_admin_menu()
            )
            await state.clear()
            return

        # Build selection list (name from admin record when available)
        unban_items = []
        for uid in banned_user_ids:
            admin_doc = col_admins.find_one({"user_id": uid})
            name = admin_doc.get("name", str(uid)) if admin_doc else str(uid)
            unban_items.append({"user_id": uid, "name": name})

        # Show first page
        page = 0
        await state.update_data(ban_page=page, admins_list=unban_items)
        
        per_page = 10
        total_pages = max(1, (len(unban_items) + per_page - 1) // per_page)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(unban_items))
        page_admins = unban_items[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            admin_buttons.append([KeyboardButton(text=_admin_btn(admin))])

        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))

        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])

        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)

        await message.answer(
            f"✅ UNBAN USER\n\n"
            f"Select user to UNBAN from Bot 2:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(unban_items)} banned users"
            f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        await state.set_state(AdminStates.selecting_role)  # Reuse state
    
    elif message.text == "📋 BANNED LIST":
        # Show list of bot2-scoped banned users (deduplicated)
        banned_user_ids = _get_unique_bot2_banned_user_ids()

        if not banned_user_ids:
            await message.answer(
                "✅ No Bot 2 banned users found!",
                reply_markup=get_admin_menu()
            )
            await state.clear()
            return
        
        # Pagination: 10 per page
        page = 0
        await state.update_data(banned_list_page=page, bot2_banned_user_ids=banned_user_ids)
        
        per_page = 10
        total_pages = max(1, (len(banned_user_ids) + per_page - 1) // per_page)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(banned_user_ids))
        page_user_ids = banned_user_ids[start_idx:end_idx]
        
        # Build message
        msg = f"📋 BANNED USERS LIST (BOT 2)\n\n"
        msg += f"Total Banned: {len(banned_user_ids)}\n"
        msg += f"Showing {start_idx + 1}-{end_idx} (Page {page + 1}/{total_pages})\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for user_id in page_user_ids:
            ban_doc = col_banned_users.find_one(
                {"user_id": user_id, "scope": "bot2"},
                sort=[("banned_at", -1)]
            ) or {}
            admin_doc = col_admins.find_one({"user_id": user_id}) or {}
            name = admin_doc.get("name", str(user_id))
            role = admin_doc.get("role", "User")
            banned_at = ban_doc.get("banned_at")
            banned_by = ban_doc.get("banned_by", "Unknown")

            if name != str(user_id):
                msg += f"👤 {name} (`{user_id}`)\n"
            else:
                msg += f"👤 ID: `{user_id}`\n"
            msg += f"👔 Role: {role}\n"
            msg += f"📅 Banned: {format_datetime(banned_at)}\n"
            msg += f"👨💼 By: {banned_by}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Navigation buttons
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV PAGE"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT PAGE ➡️"))
        
        list_kb_buttons = []
        if nav_buttons:
            list_kb_buttons.append(nav_buttons)
        list_kb_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        list_kb = ReplyKeyboardMarkup(keyboard=list_kb_buttons, resize_keyboard=True)
        
        await message.answer(msg, reply_markup=list_kb, parse_mode="Markdown")
        await state.set_state(AdminStates.waiting_for_ban_user_id)  # Keep in ban flow for pagination
    
    else:
        await message.answer("⚠️ Please select from the buttons.")


@dp.message(AdminStates.waiting_for_ban_config_id)
async def process_ban_config_id(message: types.Message, state: FSMContext):
    """Accept a user ID for ban-config ban, validate, and ask for confirmation."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_admin_menu())
        return

    text = (message.text or "").strip()
    try:
        user_id = int(text)
    except ValueError:
        await message.answer(
            "⚠️ Invalid input. Please enter a numeric **User ID** only.\n\nExample: `987654321`",
            parse_mode="Markdown"
        )
        return

    # Master admin cannot be banned
    if user_id == MASTER_ADMIN_ID:
        await message.answer(
            "⛔ The master admin (MSA) cannot be banned.",
            reply_markup=get_admin_menu()
        )
        await state.clear()
        return

    # Already banned in Bot 2 scope?
    existing_ban = col_banned_users.find_one({"user_id": user_id, "scope": "bot2"})
    if existing_ban:
        banned_at = existing_ban.get("banned_at")
        banned_at_str = banned_at.strftime("%b %d, %Y at %I:%M %p") if banned_at else "Unknown"
        await message.answer(
            f"⚠️ **ALREADY BANNED**\n\n"
            f"👤 User ID: `{user_id}`\n"
            f"📅 Banned since: {banned_at_str}\n\n"
            f"This user is already banned.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    # Is this user an active admin?
    admin_doc = col_admins.find_one({"user_id": user_id})
    if admin_doc:
        admin_role = admin_doc.get("role", "Admin")
        admin_locked = admin_doc.get("locked", False)
        await message.answer(
            f"⚠️ **CANNOT BAN — USER IS AN ACTIVE ADMIN**\n\n"
            f"👤 User ID: `{user_id}`\n"
            f"👔 Role: **{admin_role}**\n"
            f"{'🔒' if admin_locked else '🔓'} Status: **{'Locked' if admin_locked else 'Unlocked'}**\n\n"
            f"❌ You must remove this user from admin first.\n\n"
            f"💡 Steps:\n"
            f"  1️⃣ Go to **👥 ADMINS → ➖ REMOVE ADMIN**\n"
            f"  2️⃣ Remove user `{user_id}`\n"
            f"  3️⃣ Then return here to ban them",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    # Not an admin, not banned — ask for confirmation
    await state.update_data(ban_config_user_id=user_id)
    await state.set_state(AdminStates.waiting_for_ban_config_confirm)

    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM BAN"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        f"🚫 **CONFIRM BAN**\n\n"
        f"👤 User ID: `{user_id}`\n\n"
        f"⚠️ This will ban the user from Bot 2 admin panel access.\n\n"
        f"Type **✅ CONFIRM BAN** to proceed or **❌ CANCEL** to abort.",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )


@dp.message(AdminStates.waiting_for_ban_config_confirm)
async def process_ban_config_confirm(message: types.Message, state: FSMContext):
    """Perform the ban after confirmation."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer("✅ Ban cancelled.", reply_markup=get_admin_menu())
        return

    if message.text != "✅ CONFIRM BAN":
        await message.answer("⚠️ Please click **✅ CONFIRM BAN** or **❌ CANCEL**.", parse_mode="Markdown")
        return

    data = await state.get_data()
    user_id = data.get("ban_config_user_id")

    # Double-check not already banned in Bot 2 scope (race guard)
    if col_banned_users.find_one({"user_id": user_id, "scope": "bot2"}):
        await message.answer("⚠️ This user is already banned in Bot 2.", reply_markup=get_admin_menu())
        await state.clear()
        return

    try:
        col_banned_users.update_one(
            {"user_id": user_id, "scope": "bot2"},
            {"$set": {
                "user_id": user_id,
                "banned_by": message.from_user.id,
                "banned_at": now_local(),
                "reason": "Banned via Ban Config by admin",
                "status": "banned",
                "scope": "bot2"
            }},
            upsert=True
        )
        log_action("🚫 USER BANNED (BAN CONFIG)", message.from_user.id, f"Banned user from Bot 2: {user_id}")
        await message.answer(
            f"✅ **USER BANNED**\n\n"
            f"👤 User ID: `{user_id}`\n"
            f"📅 Banned: {now_local().strftime('%b %d, %Y at %I:%M %p')}\n\n"
            f"This user can no longer access Bot 2 admin panel.\n"
            f"Their Bot 1 access is **NOT affected**.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
    except Exception as e:
        await message.answer(f"❌ Error banning: {str(e)}", reply_markup=get_admin_menu())
    await state.clear()


@dp.message(F.text == "📋 LIST ADMINS")
async def list_admins_handler(message: types.Message, state: FSMContext):
    """Paginated admin list using ReplyKeyboardMarkup."""
    log_action("📋 LIST ADMINS", message.from_user.id, "Viewing admin list")
    
    # Store page in state (default to page 0)
    await state.update_data(admin_list_page=0)
    await _send_admin_list_page(message, state, 0)


async def _send_admin_list_page(message: types.Message, state: FSMContext, page: int):
    """Build and send a paginated admin list page with ReplyKeyboardMarkup."""
    # List current admins excluding anyone with "Owner" role
    admins = list(col_admins.find({
        "role": {"$ne": "Owner"}
    }).sort("added_at", -1))
    
    if not admins:
        await message.answer(
            "⚠️ No admins found in the system.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return

    # Pagination: 10 admins per page
    ITEMS_PER_PAGE = 10
    total_pages = max(1, (len(admins) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    # Cap page just in case
    page = min(page, max(0, total_pages - 1))
    await state.update_data(admin_list_page=page)
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(admins))
    page_admins = admins[start_idx:end_idx]

    # ── Text header ──────────────────────────────────────────────────
    role_icons = {"Super Admin": "🔴", "Manager": "🟣",
                  "Admin": "🟡", "Moderator": "🟢", "Support": "🔵"}
    lines = [
        f"👥 **ADMIN MANAGEMENT**",
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"📊 Total Admins: {len(admins)}  |  Page {page+1}/{total_pages}\n"
    ]
    
    for a in page_admins:
        uid    = a['user_id']
        name   = a.get('name', str(uid))
        role   = a.get('role', 'Admin')
        locked = a.get('locked', False)
        perms  = a.get('permissions', [])
        added_raw = a.get('added_at')
        
        icon   = role_icons.get(role, "👤")
        lock_status = "🔒 **LOCKED** (Inactive)" if locked else "🔓 **UNLOCKED** (Active)"
        # Permissions format
        perm_text = ", ".join(perms) if perms else "None"

        # Date format (12-hour AM/PM)
        if added_raw:
            try:
                date_text = added_raw.strftime('%b %d, %Y — %I:%M %p')
            except AttributeError:
                # Fallback if it's already a string
                date_text = str(added_raw)
        else:
            date_text = "Unknown"
            
        if name != str(uid):
            lines.append(f"{icon} **{name}** ({uid})")
        else:
            lines.append(f"{icon} **{uid}**")
        lines.append(f"👔 Role: **{role}**")
        lines.append(f"⚡ Status: {lock_status}")
        lines.append(f"🔐 Perms: {perm_text}")
        lines.append(f"📅 Added: {date_text}")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")

    text = "\n".join(lines)

    # ── ReplyKeyboard Pagination ────────────────────────────────────
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV LIST"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="NEXT LIST ➡️"))
        
    kb_buttons = []
    if nav_buttons:
        kb_buttons.append(nav_buttons)
    kb_buttons.append([KeyboardButton(text="🔙 BACK")])

    list_kb = ReplyKeyboardMarkup(keyboard=kb_buttons, resize_keyboard=True)

    await message.answer(text, reply_markup=list_kb, parse_mode="Markdown")
    await state.set_state(AdminStates.waiting_for_admin_search)

@dp.message(AdminStates.waiting_for_admin_search)
async def process_admin_list_nav(message: types.Message, state: FSMContext):
    """Handle pagination for the admin list."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer("✅ Returned to menu.", reply_markup=get_admin_menu())
        return
        
    data = await state.get_data()
    current_page = data.get("admin_list_page", 0)
    
    if message.text == "⬅️ PREV LIST":
        await _send_admin_list_page(message, state, current_page - 1)
    elif message.text == "NEXT LIST ➡️":
        await _send_admin_list_page(message, state, current_page + 1)
    else:
        await message.answer("⚠️ Please use the buttons provided.")

# ──────────────────────────────────────────────────────────────
# 📖 GUIDE SYSTEM — two-choice selector + paginated admin guide
# ──────────────────────────────────────────────────────────────

_BOT2_GUIDE_PAGES = [
    # Page 1 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 2 ADMIN GUIDE  ·  <b>Page 1 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📢  <b>BROADCAST</b>\n"
        "Compose and deliver messages to Bot 1 users.\n\n"
        "  ├─ 📤 <b>SEND BROADCAST</b>\n"
        "  │    Select by ID (brd1) or index (1).\n"
        "  │    Category: ALL · YT · IG · IGCC · YTCODE\n"
        "  │    Sent via Bot 1 · real-time progress shown.\n"
        "  │\n"
        "  ├─ ✏️ <b>EDIT BROADCAST</b>\n"
        "  │    Update text or media of any stored broadcast.\n"
        "  │\n"
        "  ├─ 🗑️ <b>DELETE BROADCAST</b>\n"
        "  │    Permanently remove a broadcast from the DB.\n"
        "  │\n"
        "  ├─ 📋 <b>LIST BROADCASTS</b>\n"
        "  │    Paginated view: ID · Category · Media · Date.\n"
        "  │\n"
        "  └─ 🔗 <b>BROADCAST WITH BUTTONS</b>\n"
        "       Adds inline URL buttons (text/photo/video).\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔍  <b>FIND</b>\n"
        "Search any Bot 1 user by:\n"
        "Telegram ID · MSA+ ID · Username\n"
        "Returns: name, join date, verification, MSA+ ID.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊  <b>TRAFFIC</b>\n"
        "Source-tracking stats — how users arrived via links.\n"
        "Breakdown: YT · IG · IGCC · YTCODE · Total."
    ),
    # Page 2 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 2 ADMIN GUIDE  ·  <b>Page 2 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🩺  <b>DIAGNOSIS</b>\n"
        "Full system health check — DB status, bot uptime,\n"
        "backup integrity, error counts, auto-healer stats.\n\n"
        "📸  <b>SHOOT</b>\n"
        "Send a photo, video, or document directly to a\n"
        "specific user by Telegram ID (delivered via Bot 1).\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💬  <b>SUPPORT</b>  (Ticket Management)\n\n"
        "  ├─ 🎫 <b>PENDING TICKETS</b>   Open, unresolved tickets\n"
        "  ├─ 📋 <b>ALL TICKETS</b>       Paginated full list\n"
        "  ├─ ✅ <b>RESOLVE TICKET</b>    Mark ticket resolved\n"
        "  ├─ 📨 <b>REPLY</b>             Message ticket owner\n"
        "  ├─ 🔍 <b>SEARCH TICKETS</b>    Filter by user/keyword\n"
        "  └─ 🗑️ <b>DELETE</b>            Remove from DB\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚫  <b>BAN CONFIG</b>\n"
        "Ban or unban any Bot 1 user.\n"
        "  ├─ Permanent or timed ban.\n"
        "  ├─ Scope = bot2 — does NOT affect normal\n"
        "  │    Bot 1 user experience outside admin context.\n"
        "  └─ Unban restores full Bot 1 access instantly.\n\n"
        "📋  <b>FEATURE SUSPEND</b>\n"
        "Disable individual Bot 1 features per user:\n"
        "SEARCH_CODE · DASHBOARD · RULES · GUIDE\n"
        "User sees 'Feature Suspended' when accessing them."
    ),
    # Page 3 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 2 ADMIN GUIDE  ·  <b>Page 3 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💾  <b>BACKUP</b>\n\n"
        "  ├─ 📥 <b>BACKUP NOW</b>\n"
        "  │    Manual full backup → JSON files sent to admin.\n"
        "  │    Batch-cursor processing (handles 10M+ records).\n"
        "  │    Auto-compresses files above 40 MB.\n"
        "  │\n"
        "  ├─ 📊 <b>VIEW BACKUPS</b>\n"
        "  │    Paginated list sorted newest-first.\n"
        "  │\n"
        "  ├─ 🗓️ <b>MONTHLY STATUS</b>\n"
        "  │    Backup count grouped by Month &amp; Year.\n"
        "  │\n"
        "  └─ ⚙️ <b>AUTO-BACKUP</b>\n"
        "       Runs every 12 h (AM &amp; PM) automatically.\n"
        "       MongoDB-stored — cloud-safe, no disk needed.\n"
        "       Keeps last 60 backups (30 days × 2/day).\n"
        "       Dedup: same AM/PM window stored only once.\n\n"
        "🖥️  <b>TERMINAL</b>\n"
        "Stream live system log lines in real time.\n"
        "Last 50 entries, refreshed on each view.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👥  <b>ADMINS</b>  (Owner-only)\n"
        "Add / remove admin roles for Bot 2.\n"
        "Roles: viewer (read-only) · admin (full access).\n"
        "All admin actions are audit-logged.\n\n"
        "⚠️  <b>RESET DATA</b>  (Owner-only — IRREVERSIBLE)\n"
        "Permanently wipe Bot 1 or Bot 2 collections.\n"
        "Requires double confirmation + typed CONFIRM.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌐  <b>MSA NODE Ecosystem</b>\n"
        "Bot 2 = admin control center.\n"
        "Bot 1  = user-facing delivery bot.\n"
        "Broadcasts, bans &amp; backups managed here flow\n"
        "through to Bot 1 automatically."
    ),
]

_BOT1_GUIDE_FOR_BOT2 = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "  📱  BOT 1 USER GUIDE  (Reference)\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "📊 <b>DASHBOARD</b> — MSA+ ID, member since, status,\n"
    "       live announcements from Bot 2 broadcasts.\n\n"
    "🔍 <b>SEARCH CODE</b> — Enter an MSA CODE to unlock\n"
    "       exclusive content from YouTube/Instagram.\n\n"
    "📜 <b>RULES</b>  — Community guidelines &amp; policies.\n\n"
    "📚 <b>GUIDE</b>  — User manual (this reference + personal).\n\n"
    "📞 <b>SUPPORT</b> — Open a support ticket to contact admin.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "🔐  <b>OWNER-ONLY COMMANDS</b>  (via Bot 1 directly)\n\n"
    "  /start          — Launch bot &amp; regenerate main menu\n"
    "  /menu           — Show the reply keyboard\n"
    "  /resolve &lt;uid&gt;  — Resolve a user's support ticket\n"
    "  /delete  &lt;uid&gt;  — Delete user's verification data\n"
    "  /ticket_stats   — View full ticket statistics\n"
    "  /health         — Bot health &amp; uptime report\n\n"
    "  ⚡ Regular users get no response — owner-exclusive.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "<i>For full user guide details, check Bot 1's 📚 GUIDE.</i>"
)

def _guide_selector_kb() -> ReplyKeyboardMarkup:
    """Keyboard shown on guide selector screen."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 BOT 1 USER GUIDE")],
            [KeyboardButton(text="🖥️ BOT 2 ADMIN GUIDE")],
            [KeyboardButton(text="⬅️ MAIN MENU")],
        ],
        resize_keyboard=True,
    )

def _guide_bot2_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for the paginated Bot 2 guide."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="📖 GUIDE MENU"), KeyboardButton(text="⬅️ MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(F.text == "📖 GUIDE")
async def guide_handler(message: types.Message, state: FSMContext):
    """Show guide selector — Bot 1 Guide or Bot 2 Admin Guide."""
    log_action("📖 GUIDE", message.from_user.id, "Accessed guide selector")
    await state.set_state(GuideStates.selecting)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  <b>📖 GUIDE — SELECT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Which guide would you like to view?\n\n"
        "📱 <b>BOT 1 USER GUIDE</b>\n"
        "   Full user manual for Bot 1 — features,\n"
        "   MSA CODE search, owner commands &amp; more.\n\n"
        "🖥️ <b>BOT 2 ADMIN GUIDE</b>\n"
        "   Complete admin reference — every feature,\n"
        "   button, and system explained (3 pages).",
        parse_mode="HTML",
        reply_markup=_guide_selector_kb(),
    )

@dp.message(GuideStates.selecting, F.text == "📱 BOT 1 USER GUIDE")
async def guide_show_bot1_from_bot2(message: types.Message, state: FSMContext):
    """Show Bot 1 user guide from inside Bot 2."""
    await state.set_state(GuideStates.viewing_bot1)
    await message.answer(
        _BOT1_GUIDE_FOR_BOT2,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📖 GUIDE MENU"), KeyboardButton(text="⬅️ MAIN MENU")]],
            resize_keyboard=True,
        ),
    )

@dp.message(GuideStates.selecting, F.text == "🖥️ BOT 2 ADMIN GUIDE")
async def guide_show_bot2_page1(message: types.Message, state: FSMContext):
    """Start paginated Bot 2 admin guide at page 1."""
    page = 1
    await state.set_state(GuideStates.viewing_bot2)
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT2_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot2_kb(page, len(_BOT2_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot2, F.text == "NEXT ➡️")
async def guide_bot2_next(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 1) + 1
    page = min(page, len(_BOT2_GUIDE_PAGES))
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT2_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot2_kb(page, len(_BOT2_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot2, F.text == "⬅️ PREV")
async def guide_bot2_prev(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(data.get("guide_page", 1) - 1, 1)
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT2_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot2_kb(page, len(_BOT2_GUIDE_PAGES)),
    )

@dp.message(F.text == "📖 GUIDE MENU")
async def guide_back_to_menu(message: types.Message, state: FSMContext):
    """Return to guide selector from any guide page."""
    await state.set_state(GuideStates.selecting)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  <b>📖 GUIDE — SELECT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Which guide would you like to view?\n\n"
        "📱 <b>BOT 1 USER GUIDE</b>\n"
        "   Full user manual for Bot 1 — features,\n"
        "   MSA CODE search, owner commands &amp; more.\n\n"
        "🖥️ <b>BOT 2 ADMIN GUIDE</b>\n"
        "   Complete admin reference — every feature,\n"
        "   button, and system explained (3 pages).",
        parse_mode="HTML",
        reply_markup=_guide_selector_kb(),
    )

@dp.message(F.text == "⚠️ RESET DATA")
async def reset_data_handler(message: types.Message, state: FSMContext):
    """Show reset type selection menu"""
    if message.from_user.id != MASTER_ADMIN_ID:
        log_action("🚫 UNAUTHORIZED ACCESS", message.from_user.id, f"{message.from_user.full_name} tried to access RESET DATA")
        await message.answer("⛔ **ACCESS DENIED**\n\nThis feature is restricted to the Master Admin.", reply_markup=await get_main_menu(message.from_user.id))
        return

    type_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔴 RESET BOT 1"), KeyboardButton(text="🔴 RESET BOT 2")],
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        "<b>⚠️ RESET DATA — SELECT BOT</b>\n\n"
        "Choose which bot's data to permanently erase:\n"
        "<i>(Backups will NOT be affected)</i>\n\n"
        "🔴 <b>RESET BOT 1</b>\n"
        "   bot1_user_verification, bot1_msa_ids,\n"
        "   bot1_support_tickets, bot1_banned_users,\n"
        "   bot1_suspended_features, bot1_settings,\n"
        "   bot1_permanently_banned_msa, bot1_offline_log\n\n"
        "🔴 <b>RESET BOT 2</b>\n"
        "   bot2_broadcasts, bot2_user_tracking,\n"
        "   bot2_cleanup_logs, bot2_access_attempts,\n"
        "   bot2_live_terminal_logs, bot2_admins\n\n"
        "<b>⚠️ ALL DELETIONS ARE PERMANENT AND IRREVERSIBLE!</b>",
        parse_mode="HTML",
        reply_markup=type_kb
    )
    await state.set_state(ResetDataStates.selecting_reset_type)

@dp.message(ResetDataStates.selecting_reset_type)
async def reset_type_selected(message: types.Message, state: FSMContext):
    """Handle reset type selection"""
    choice = message.text.strip()

    if choice == "❌ CANCEL" or choice == "⬅️ BACK":
        await message.answer("✅ Cancelled.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return

    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ CONFIRM RESET")], [KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )

    if choice == "🔴 RESET BOT 1":
        await state.update_data(reset_type="bot1")
        await message.answer(
            "<b>⚠️ RESET BOT 1 DATA</b>\n\n"
            "Will permanently delete:\n"
            "🗑️ bot1_user_verification\n🗑️ bot1_msa_ids\n"
            "🗑️ bot1_support_tickets\n🗑️ bot1_banned_users\n"
            "🗑️ bot1_suspended_features\n🗑️ bot1_settings\n"
            "🗑️ bot1_permanently_banned_msa\n🗑️ bot1_offline_log\n\n"
            "<b>⚠️ IRREVERSIBLE! Press ✅ CONFIRM RESET to proceed.</b>",
            parse_mode="HTML", reply_markup=confirm_kb
        )
        await state.set_state(ResetDataStates.waiting_for_first_confirm)

    elif choice == "🔴 RESET BOT 2":
        await state.update_data(reset_type="bot2")
        await message.answer(
            "<b>⚠️ RESET BOT 2 DATA</b>\n\n"
            "Will permanently delete:\n"
            "🗑️ bot2_broadcasts\n🗑️ bot2_user_tracking\n"
            "🗑️ bot2_cleanup_logs\n🗑️ bot2_access_attempts\n"
            "🗑️ bot2_live_terminal_logs\n🗑️ bot2_admins\n\n"
            "<b>⚠️ IRREVERSIBLE! Press ✅ CONFIRM RESET to proceed.</b>",
            parse_mode="HTML", reply_markup=confirm_kb
        )
        await state.set_state(ResetDataStates.bot2_first_confirm)

    else:
        await message.answer("❌ Invalid choice. Please select from the menu.", parse_mode="HTML")

# ── Bot2 reset first confirm ──
@dp.message(ResetDataStates.bot2_first_confirm)
async def reset_bot2_first_confirm(message: types.Message, state: FSMContext):
    """Bot2 first confirmation"""
    if message.text != "✅ CONFIRM RESET":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)
    await message.answer(
        "<b>🚨 LAST WARNING — BOT 2 DATA</b>\n\n"
        "Type <code>CONFIRM</code> to permanently delete all Bot 2 data.",
        parse_mode="HTML", reply_markup=cancel_kb
    )
    await state.set_state(ResetDataStates.bot2_final_confirm)

@dp.message(ResetDataStates.bot2_final_confirm)
async def reset_bot2_final_confirm(message: types.Message, state: FSMContext):
    """Bot2 final deletion"""
    if message.text.strip() != "CONFIRM":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    status_msg = await message.answer("<b>🗑️ DELETING ALL BOT 2 DATA...</b>\n\n⏳ Please wait...", parse_mode="HTML")
    try:
        # ── Auto-backup BEFORE deletion ──────────────────────────────────────
        # Write snapshot to PRODUCTION MongoDB (MSANodeDB) — backup cluster has SSL issues
        await status_msg.edit_text("<b>🔄 Pre-Reset Backup...</b>\n\n💾 Checking Bot 2 data before deletion...", parse_mode="HTML")

        RESET_BOT2_COLS = [
            (col_broadcasts, "bot2_broadcasts"),
            (col_user_tracking, "bot2_user_tracking"),
            (col_cleanup_logs, "bot2_cleanup_logs"),
            (col_access_attempts, "bot2_access_attempts"),
            (col_admins, "bot2_admins"),
            (db["bot2_live_terminal_logs"], "bot2_live_terminal_logs"),
        ]
        snapshot = {}
        total_docs_found = 0
        for col_obj, col_name in RESET_BOT2_COLS:
            docs = [{**d, "_id": str(d["_id"])} for d in col_obj.find({})]
            snapshot[col_name] = {"count": len(docs), "documents": docs}
            total_docs_found += len(docs)

        if total_docs_found == 0:
            bk_info = "<b>ℹ️ Pre-Reset Backup:</b>\n⚪ All Bot 2 collections already empty — skipped backup."
        else:
            try:
                pre_reset_col = db["bot2_pre_reset_backups"]
                pre_reset_col.insert_one({
                    "bot": "bot2",
                    "backup_date": datetime.now(timezone.utc),
                    "reason": "pre_reset",
                    "total_docs": total_docs_found,
                    "snapshot": snapshot,
                })
                bk_info = f"<b>✅ Pre-Reset Backup Success:</b>\n💾 Saved {len(snapshot)} collections ({total_docs_found:,} docs) to MSANodeDB."
            except Exception as bk_err:
                await status_msg.edit_text(
                    f"<b>❌ PRE-RESET BACKUP FAILED</b>\n\n"
                    f"⚠️ Deletion aborted to prevent data loss.\n"
                    f"<b>Reason:</b> <code>{str(bk_err)[:300]}</code>",
                    parse_mode="HTML"
                )
                await state.clear()
                return

        bk_cols = len(snapshot)
        bk_docs = total_docs_found
        await status_msg.edit_text(f"<b>🗑️ DELETING ALL BOT 2 DATA...</b>\n\n{bk_info}\n\n⏳ Deleting...", parse_mode="HTML")
        # ─────────────────────────────────────────────────────────────────────
        r1 = col_broadcasts.delete_many({})
        r2 = col_user_tracking.delete_many({})
        r4 = col_cleanup_logs.delete_many({})
        r5 = col_access_attempts.delete_many({})
        r6 = col_admins.delete_many({})
        
        # for live terminal logs it uses db[]
        r7 = db["bot2_live_terminal_logs"].delete_many({})
        
        total = r1.deleted_count + r2.deleted_count + r4.deleted_count + r5.deleted_count + r6.deleted_count + r7.deleted_count
        await status_msg.edit_text(
            "<b>✅ ALL BOT 2 DATA DELETED</b>\n\n"
            f"{bk_info}\n\n"
            "<b>🗑️ DELETED FROM PRODUCTION:</b>\n"
            f" ├─ bot2_broadcasts: {r1.deleted_count:,}\n"
            f" ├─ bot2_user_tracking: {r2.deleted_count:,}\n"
            f" ├─ bot2_cleanup_logs: {r4.deleted_count:,}\n"
            f" ├─ bot2_access_attempts: {r5.deleted_count:,}\n"
            f" ├─ bot2_admins: {r6.deleted_count:,}\n"
            f" └─ bot2_live_terminal_logs: {r7.deleted_count:,}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Total Deleted:</b> {total:,}\n\n"
            f"<i>⏰ Completed at {now_local().strftime('%Y-%m-%d %I:%M:%S %p')}</i>",
            parse_mode="HTML"
        )
        await message.answer(
            "<b>🔄 Bot 2 Reset Complete</b>\n\nAll Bot 2 data permanently deleted.",
            parse_mode="HTML", reply_markup=await get_main_menu(message.from_user.id)
        )
        print(f"\n🚨 BOT 2 DATA RESET by {message.from_user.id} — {total:,} records deleted at {now_local()}\n")
    except Exception as e:
        await status_msg.edit_text(f"<b>❌ DELETION ERROR</b>\n\n{str(e)}", parse_mode="HTML")
        await message.answer("⚠️ Error during reset.", reply_markup=await get_main_menu(message.from_user.id))
    await state.clear()

# ── Bot1 reset first confirm ──
@dp.message(ResetDataStates.waiting_for_first_confirm)
async def reset_data_first_confirm(message: types.Message, state: FSMContext):
    """First confirmation for reset data"""
    if message.text != "✅ CONFIRM RESET":
        await message.answer(
            "<b>✅ CANCELLED</b>\n\n"
            "Reset operation cancelled. No data was deleted.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        await state.clear()
        return
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    final_warning = (
        "<b>⚠️ FINAL CONFIRMATION REQUIRED</b>\n\n"
        "<b>🚨 LAST WARNING 🚨</b>\n\n"
        "You are about to permanently delete ALL Bot 1 data.\n\n"
        "<b>⚠️ THIS IS IRREVERSIBLE!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>FINAL STEP:</b>\n"
        "Type <code>CONFIRM</code> below to execute deletion.\n"
        "Type anything else to cancel."
    )
    
    await message.answer(final_warning, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(ResetDataStates.waiting_for_final_confirm)

@dp.message(ResetDataStates.waiting_for_final_confirm)
async def reset_data_final_confirm(message: types.Message, state: FSMContext):
    """Final confirmation - actually delete all Bot 1 data"""
    if message.text.strip() != "CONFIRM":
        await message.answer(
            "<b>✅ CANCELLED</b>\n\n"
            "Reset operation cancelled. No data was deleted.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        await state.clear()
        return
    
    status_msg = await message.answer(
        "<b>🗑️ DELETING ALL BOT 1 DATA...</b>\n\n"
        "⏳ Please wait...",
        parse_mode="HTML"
    )
    
    try:
        # ── Auto-backup BEFORE deletion ──────────────────────────────────────
        # Write snapshot to PRODUCTION MongoDB (MSANodeDB) — backup cluster has SSL issues
        await status_msg.edit_text("<b>🔄 Pre-Reset Backup...</b>\n\n💾 Checking Bot 1 data before deletion...", parse_mode="HTML")

        RESET_BOT1_COLS = [
            (col_user_verification, "bot1_user_verification"),
            (col_msa_ids, "bot1_msa_ids"),
            (col_support_tickets, "bot1_support_tickets"),
            (col_banned_users, "bot1_banned_users"),
            (col_suspended_features, "bot1_suspended_features"),
            (col_bot1_settings, "bot1_settings"),
            (col_permanently_banned_msa, "bot1_permanently_banned_msa"),
            (col_offline_log, "bot1_offline_log"),
        ]
        snapshot = {}
        total_docs_found = 0
        for col_obj, col_name in RESET_BOT1_COLS:
            # Exclude security_lock docs from backup snapshot (they are not user data)
            query = {"type": {"$ne": "security_lock"}} if col_name == "bot1_support_tickets" else {}
            docs = [{**d, "_id": str(d["_id"])} for d in col_obj.find(query)]
            snapshot[col_name] = {"count": len(docs), "documents": docs}
            total_docs_found += len(docs)

        if total_docs_found == 0:
            bk_info = "<b>ℹ️ Pre-Reset Backup:</b>\n⚪ All Bot 1 collections already empty — skipped backup."
        else:
            try:
                pre_reset_col = db["bot1_pre_reset_backups"]
                pre_reset_col.insert_one({
                    "bot": "bot1",
                    "backup_date": datetime.now(timezone.utc),
                    "reason": "pre_reset",
                    "total_docs": total_docs_found,
                    "snapshot": snapshot,
                })
                bk_info = f"<b>✅ Pre-Reset Backup Success:</b>\n💾 Saved {len(snapshot)} collections ({total_docs_found:,} docs) to MSANodeDB."
            except Exception as bk_err:
                await status_msg.edit_text(
                    f"<b>❌ PRE-RESET BACKUP FAILED</b>\n\n"
                    f"⚠️ Deletion aborted to prevent data loss.\n"
                    f"<b>Reason:</b> <code>{str(bk_err)[:300]}</code>",
                    parse_mode="HTML"
                )
                await state.clear()
                return

        bk_cols = len(snapshot)
        bk_docs = total_docs_found
        await status_msg.edit_text(f"<b>🗑️ DELETING ALL BOT 1 DATA...</b>\n\n{bk_info}\n\n⏳ Deleting...", parse_mode="HTML")
        # ─────────────────────────────────────────────────────────────────────
        r1 = col_user_verification.delete_many({})
        r2 = col_msa_ids.delete_many({})
        # ⚠️ Preserve security_lock docs — they survive reset so locked users
        # cannot bypass the 6h support ban by asking admin to reset bot 1.
        r3 = col_support_tickets.delete_many({"type": {"$ne": "security_lock"}})
        r4 = col_banned_users.delete_many({})
        r5 = col_suspended_features.delete_many({})
        r6 = col_bot1_settings.delete_many({})
        r7 = col_permanently_banned_msa.delete_many({})
        r8 = col_offline_log.delete_many({})
        
        total_deleted = sum([r1.deleted_count, r2.deleted_count, r3.deleted_count, 
                            r4.deleted_count, r5.deleted_count, r6.deleted_count,
                            r7.deleted_count, r8.deleted_count])
        
        success_msg = (
            "<b>✅ ALL BOT 1 DATA DELETED</b>\n\n"
            f"{bk_info}\n\n"
            "<b>🗑️ DELETED FROM PRODUCTION:</b>\n"
            f" ├─ bot1_user_verification: {r1.deleted_count:,}\n"
            f" ├─ bot1_msa_ids: {r2.deleted_count:,}\n"
            f" ├─ bot1_support_tickets: {r3.deleted_count:,}\n"
            f" ├─ bot1_banned_users: {r4.deleted_count:,}\n"
            f" ├─ bot1_suspended_features: {r5.deleted_count:,}\n"
            f" ├─ bot1_settings: {r6.deleted_count:,}\n"
            f" ├─ bot1_permanently_banned_msa: {r7.deleted_count:,}\n"
            f" └─ bot1_offline_log: {r8.deleted_count:,}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Total Deleted:</b> {total_deleted:,}\n\n"
            f"<i>⏰ Completed at {now_local().strftime('%Y-%m-%d %I:%M:%S %p')}</i>"
        )
        
        await status_msg.edit_text(success_msg, parse_mode="HTML")
        await message.answer(
            "<b>🔄 Bot 1 Reset Complete</b>\n\n"
            "All Bot 1 data has been permanently deleted.\n"
            "Bot 1 is now in fresh state.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        
        print(f"\n🚨 BOT 1 DATA RESET by {message.from_user.id} — {total_deleted:,} records deleted at {now_local()}\n")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await status_msg.edit_text(
            f"<b>❌ DELETION ERROR</b>\n\n{error_msg}\n\n"
            "Some data may have been partially deleted. Please check database manually.",
            parse_mode="HTML"
        )
        await message.answer(
            "<b>⚠️ Error occurred during reset</b>\n\n"
            "Please check the error message above and contact developer if needed.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
    
    await state.clear()


# ==========================================
# AUTOMATED DATABASE CLEANUP SYSTEM
# ==========================================

async def automated_database_cleanup():
    """
    Automated cleanup that runs daily at 3 AM
    - Cleans broadcasts older than 90 days (backed up before deletion)
    - Support tickets are PERMANENT — never auto-deleted
    - Auto-backup to MongoDB (cloud-safe, works on Render/Heroku/Railway)
    """
    now = now_local()
    print(f"\n🧹 ═══════════════════════════════════════")
    print(f"🧹 AUTOMATED DATABASE CLEANUP")
    print(f"🧹 Started at: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🧹 ═══════════════════════════════════════\n")
    
    cleanup_stats = {
        "cleanup_date": now,
        "tickets_deleted": 0,
        "broadcasts_deleted": 0,
        "backup_created": False,
        "old_backups_deleted": 0
    }
    
    try:
        # === GET BROADCASTS TO BACKUP BEFORE DELETION ===
        # Tickets are permanent records — never backed-up-then-deleted here
        old_broadcasts = list(col_broadcasts.find({
            "created_at": {"$lt": now - timedelta(days=90)}
        }))
        
        # === SAVE BROADCAST BACKUP TO MONGODB (Cloud-Safe!) ===
        if old_broadcasts:
            backup_doc = {
                "backup_date": now,
                "broadcasts_count": len(old_broadcasts),
                "broadcasts": old_broadcasts
            }
            
            col_cleanup_backups.insert_one(backup_doc)
            cleanup_stats['backup_created'] = True
            
            print(f"💾 Broadcast backup saved to MongoDB (cloud-safe)")
            print(f"   📢 Broadcasts backed up: {len(old_broadcasts)}\n")
        else:
            print(f"📦 No broadcasts old enough to delete\n")
        
        # === CLEANUP OLD BACKUPS IN MONGODB (Keep only last 30) ===
        backup_count = col_cleanup_backups.count_documents({})
        
        if backup_count > 30:
            # Get oldest backups to delete
            old_backups = list(col_cleanup_backups.find({}).sort("backup_date", 1).limit(backup_count - 30))
            old_backup_ids = [b['_id'] for b in old_backups]
            
            result = col_cleanup_backups.delete_many({"_id": {"$in": old_backup_ids}})
            cleanup_stats['old_backups_deleted'] = result.deleted_count
            
            print(f"🧹 Deleted {result.deleted_count} old backups from MongoDB")
            print(f"📦 Kept: 30 most recent backups\n")
        else:
            print(f"📦 MongoDB backups: {backup_count}/30 (no cleanup needed)\n")
        
        # === TICKETS ARE PERMANENT — no auto-deletion ever ===
        cleanup_stats['tickets_deleted'] = 0
        print(f"🎫 Support tickets: permanent records — no auto-deletion")
        
        # === CLEANUP OLD BROADCASTS (90+ days) ===
        cutoff_date_broadcasts = now - timedelta(days=90)
        result_broadcasts = col_broadcasts.delete_many({
            "created_at": {"$lt": cutoff_date_broadcasts}
        })
        cleanup_stats['broadcasts_deleted'] = result_broadcasts.deleted_count
        
        if result_broadcasts.deleted_count > 0:
            print(f"📢 Deleted {result_broadcasts.deleted_count} old broadcasts (>90 days)")
        else:
            print(f"📢 No old broadcasts to delete")
        
        # === SAVE CLEANUP LOG TO MONGODB ===
        col_cleanup_logs.insert_one(cleanup_stats)
        
        # Keep only last 30 logs in MongoDB
        log_count = col_cleanup_logs.count_documents({})
        if log_count > 30:
            old_logs = list(col_cleanup_logs.find({}).sort("cleanup_date", 1).limit(log_count - 30))
            old_log_ids = [log['_id'] for log in old_logs]
            col_cleanup_logs.delete_many({"_id": {"$in": old_log_ids}})
            print(f"📋 Cleaned up old logs (kept last 30)")
        
        print(f"\n✅ Cleanup completed successfully!")
        print(f"   🗑️ Total deleted: {cleanup_stats['tickets_deleted'] + cleanup_stats['broadcasts_deleted']} items")
        print(f"   💾 Backup: Stored in MongoDB (cloud-safe)")
        print(f"   📋 Log: Saved to cleanup_logs collection")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {str(e)}")
        cleanup_stats['error'] = str(e)
        cleanup_stats['cleanup_date'] = now
        col_cleanup_logs.insert_one(cleanup_stats)
    
    print(f"\n🧹 ═══════════════════════════════════════")
    print(f"🧹 Cleanup finished at: {now_local().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🧹 ═══════════════════════════════════════\n")
    
    return cleanup_stats

async def schedule_daily_cleanup():
    """Schedule cleanup to run daily at 3 AM"""
    while True:
        now = now_local()
        
        # Calculate next 3 AM
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now.hour >= 3:
            next_run += timedelta(days=1)
        
        # Calculate seconds until next run
        seconds_until_run = (next_run - now).total_seconds()
        
        print(f"🕒 Next automated cleanup scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Time until cleanup: {seconds_until_run / 3600:.1f} hours\n")
        
        # Wait until 3 AM
        await asyncio.sleep(seconds_until_run)
        
        # Run cleanup
        await automated_database_cleanup()
        
        # Wait 1 hour before checking again (prevents multiple runs)
        await asyncio.sleep(3600)



# ==========================================
# ENTERPRISE AUTO-HEALER SYSTEM (BOT 2)
# ==========================================
# (bot2_health dict is defined near top of file, after bot/dp initialization)

# Per-alert cooldown tracker: {"{severity}:{error_type}": last_sent_datetime}
_bot2_last_alert: dict = {}

async def notify_master_admin(error_type: str, error_msg: str, severity: str = "ERROR", auto_healed: bool = False):
    """Instantly notify owner (MASTER_ADMIN_ID) of any error via Telegram — with per-type deduplication"""
    try:
        # --- Cooldown / deduplication to prevent notification spam ---
        _alert_cooldowns = {"CRITICAL": 120, "ERROR": 600, "WARNING": 1800}
        cooldown = _alert_cooldowns.get(severity, 600)
        alert_key = f"{severity}:{error_type}"
        last_sent = _bot2_last_alert.get(alert_key)
        if last_sent:
            elapsed = (now_local() - last_sent).total_seconds()
            if elapsed < cooldown:
                print(f"[BOT2] Suppressing {severity} alert '{error_type}' (cooldown {cooldown - elapsed:.0f}s left)")
                return
        _bot2_last_alert[alert_key] = now_local()
        # --- end cooldown ---

        bot2_health["owner_notified"] += 1
        emoji = {"CRITICAL": "🔴", "ERROR": "🟠", "WARNING": "🟡"}.get(severity, "🟡")
        heal_status = "✅ AUTO-HEALED" if auto_healed else "❌ NEEDS ATTENTION"
        uptime = now_local() - bot2_health["bot_start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)

        msg = (
            f"{emoji} **BOT 2 ALERT — {severity}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**Type:** `{error_type}`\n"
            f"**Status:** {heal_status}\n\n"
            f"**Error:**\n```\n{str(error_msg)[:600]}\n```\n\n"
            f"**Stats:**\n"
            f"• Uptime: {h}h {m}m\n"
            f"• Errors Caught: {bot2_health['errors_caught']}\n"
            f"• Auto-Healed: {bot2_health['auto_healed']}\n"
            f"• Alerts Sent: {bot2_health['owner_notified']}\n\n"
            f"**Time:** {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Bot 2 Enterprise Auto-Healer_"
        )

        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
        print(f"📢 [ALERT] Notified owner: {severity} — {error_type}")
    except Exception as e:
        print(f"❌ Failed to notify owner: {e}")


async def bot2_auto_heal(error_type: str, error: Exception) -> bool:
    """Attempt automatic recovery before escalating to owner"""
    try:
        print(f"🏥 [AUTO-HEAL] Attempting recovery: {error_type}")
        err_str = str(error).lower()

        # MongoDB / DB connection issues
        if any(k in err_str for k in ["mongo", "database", "pymongo", "connection refused"]):
            print("🔌 [AUTO-HEAL] Reconnecting to MongoDB...")
            try:
                client.admin.command('ping')
                print("✅ [AUTO-HEAL] MongoDB reconnected!")
                bot2_health["auto_healed"] += 1
                bot2_health["consecutive_failures"] = 0
                return True
            except Exception:
                print("❌ [AUTO-HEAL] MongoDB reconnect failed")
                return False

        # Timeout / network blips
        elif any(k in err_str for k in ["timeout", "timed out", "temporarily unavailable"]):
            print("⏱️ [AUTO-HEAL] Timeout — waiting 2s and continuing...")
            await asyncio.sleep(2)
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        # Telegram rate limit
        elif "retry after" in err_str or "flood" in err_str or "too many requests" in err_str:
            wait = 5
            try:
                import re
                m = re.search(r'retry after (\d+)', err_str)
                if m:
                    wait = int(m.group(1)) + 1
            except Exception:
                pass
            print(f"⏳ [AUTO-HEAL] Rate limit — waiting {wait}s...")
            await asyncio.sleep(wait)
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        # Generic connection error
        elif any(k in err_str for k in ["connection", "network", "socket", "ssl"]):
            print("🔄 [AUTO-HEAL] Connection issue — waiting 5s...")
            await asyncio.sleep(5)
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        # Telegram bad request — "can't parse entities" (markdown error) → silent suppress
        elif "can't parse entities" in err_str or "parse entities" in err_str or "byte offset" in err_str:
            print("📝 [AUTO-HEAL] Markdown parse error — silently suppressed (no user impact)")
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        # Telegram bad request — message edit failures (too old, deleted, already same content)
        elif any(k in err_str for k in ["message can't be edited", "message is not modified", "message to edit not found"]):
            print("✏️ [AUTO-HEAL] Edit-message error suppressed — message is old/deleted/unchanged")
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        # Telegram bad request — bad request misc (bot blocked, chat not found, etc.)
        elif "bad request" in err_str and any(k in err_str for k in [
            "chat not found", "user not found", "bot was blocked",
            "deactivated", "kicked", "not enough rights", "member list is inaccessible"
        ]):
            print("🤖 [AUTO-HEAL] Telegram user/chat issue suppressed (user-side, not our fault)")
            bot2_health["auto_healed"] += 1
            bot2_health["consecutive_failures"] = 0
            return True

        else:
            print(f"❓ [AUTO-HEAL] Unknown error type, cannot auto-heal: {error_type}")
            return False

    except Exception as ex:
        print(f"❌ [AUTO-HEAL] Healing itself failed: {ex}")
        return False


import traceback
async def bot2_global_error_handler(event: types.ErrorEvent):
    traceback.print_exc()
    """Global error handler — catches ALL unhandled errors in bot2 handlers"""
    update = event.update
    exception = event.exception
    try:
        bot2_health["errors_caught"] += 1
        bot2_health["last_error"] = now_local()
        bot2_health["last_error_type"] = type(exception).__name__
        bot2_health["consecutive_failures"] += 1

        err_type = type(exception).__name__
        err_msg = str(exception)
        print(f"❌ [BOT2 ERROR] {err_type}: {err_msg[:200]}")

        # Try auto-heal first
        healed = await bot2_auto_heal(err_type, exception)

        # Determine severity
        err_lower = err_msg.lower()
        if "critical" in err_lower or "fatal" in err_lower or bot2_health["consecutive_failures"] >= 5:
            severity = "CRITICAL"
        elif healed:
            severity = "WARNING"
        else:
            severity = "ERROR"

        # Suppress noisy Telegram operational errors — never notify owner for these
        _silent_patterns = [
            "can't parse entities", "message can't be edited",
            "message is not modified", "message to edit not found",
            "chat not found", "user not found",
            "bot was blocked", "deactivated", "kicked"
        ]
        is_silent = any(p in err_msg.lower() for p in _silent_patterns)

        # Notify owner if not healed or if critical (but never for silent patterns)
        if (not healed or severity == "CRITICAL") and not is_silent:
            await notify_master_admin(err_type, err_msg, severity, healed)
        elif is_silent:
            print(f"🔕 [BOT2] Silent error suppressed (no owner alert): {err_type}")

        print(f"🏥 [BOT2] Error handled. Auto-healed: {healed}")
        return True

    except Exception as handler_err:
        print(f"💥 CRITICAL: Bot2 error handler crashed: {handler_err}")
        try:
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🔴🔴🔴 **BOT 2 CRITICAL FAILURE**\n\n"
                f"The error handler itself crashed!\n```{str(handler_err)[:300]}```",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return False


async def bot2_health_monitor():
    """Background health monitor — checks every hour, reports issues instantly"""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour

            # Check MongoDB
            try:
                t0 = time.time()
                client.admin.command('ping')
                latency_ms = (time.time() - t0) * 1000
                print(f"✅ [HEALTH] DB OK — {latency_ms:.1f}ms")
                if latency_ms > 2000:
                    await notify_master_admin("DB Latency Warning", f"MongoDB latency {latency_ms:.0f}ms (high)", "WARNING", True)
            except Exception as e:
                print(f"❌ [HEALTH] DB FAILED: {e}")
                healed = await bot2_auto_heal("DB Health Check", e)
                if not healed:
                    await notify_master_admin("DB Health Check", str(e), "CRITICAL", False)

            # Check bot connection
            try:
                me = await bot.get_me()
                print(f"✅ [HEALTH] Bot OK — @{me.username}")
            except Exception as e:
                print(f"❌ [HEALTH] Bot connection FAILED: {e}")
                await notify_master_admin("Bot Connection Check", str(e), "CRITICAL", False)

        except asyncio.CancelledError:
            print("💊 [HEALTH] Bot2 health monitor stopping...")
            break
        except Exception as e:
            print(f"❌ [HEALTH MONITOR ERROR] {e}")


# ==========================================
# 📦 MONTHLY JSON BACKUP DELIVERY — Bot 2
# Runs on the 1st of every month, 09:30–11:59 AM local time (30 min after Bot 1)
# Dumps EVERY collection in MSANodeDB — complete cluster backup
# Each collection → separate gzip-compressed JSON delivered to master admin
# JSON format: {collection, exported_at, total_records, restore_unique_key, records:[...]}
# ==========================================

_MONTHLY_RESTORE_KEYS = {
    "bot1_user_verification":      "user_id",
    "bot1_msa_ids":                "user_id",
    "bot1_support_tickets":        "user_id",
    "bot1_banned_users":           "user_id",
    "bot1_suspended_features":     "user_id",
    "bot1_permanently_banned_msa": "msa_id",
    "bot2_broadcasts":       "broadcast_id",
    "bot2_admins":           "user_id",
    "bot2_user_tracking":    "user_id",
    "bot1_offline_log":       "_id",
    "bot1_state_persistence": "key",
    "bot2_runtime_state":    "state_key",
    "bot2_backups":          "_id",
    "bot2_access_attempts":  "_id",
    "cleanup_backups":        "_id",
    "cleanup_logs":           "_id",
    "bot2_live_terminal_logs":     "_id",
    "bot1_backups":           "_id",
    "bot1_restore_data":      "_id",
    "bot2_restore_data":     "_id",
}


def _mongo_json_encoder_b2(obj):
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


async def _send_col_json_bot2(col_name: str, unique_key: str, now) -> tuple:
    """Dump one collection to gzip JSON and deliver to master admin. Returns (count, bytes)."""
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
    chunks = [records[i:i+CHUNK] for i in range(0, len(records), CHUNK)] if records else [[]]
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
        raw  = json.dumps(payload, default=_mongo_json_encoder_b2, ensure_ascii=False, indent=2).encode()
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
        await bot.send_document(
            MASTER_ADMIN_ID,
            BufferedInputFile(data, filename=fname),
            caption=cap,
            parse_mode="HTML",
        )
        total_bytes += len(data)
        await asyncio.sleep(0.5)

    return len(records), total_bytes


async def monthly_json_delivery_bot2():
    """Background task: 1st of every month, 09:30\u201311:59 AM \u2014 full JSON backup of ALL collections."""
    while True:
        try:
            now = now_local()
            is_window = (
                now.day == 1
                and ((now.hour == 9 and now.minute >= 30) or (10 <= now.hour <= 11))
            )
            if is_window:
                month_key = now.strftime("%Y-%m")
                track_key = f"monthly_json_{month_key}"
                if not db["bot2_runtime_state"].find_one({"state_key": track_key}):
                    db["bot2_runtime_state"].update_one(
                        {"state_key": track_key},
                        {"$set": {"state_key": track_key, "run_at": now.isoformat()}},
                        upsert=True,
                    )
                    # ── Only back up meaningful data collections (not internal/log collections) ──
                    _SKIP_MONTHLY = {
                        "bot2_live_terminal_logs",   # internal logs — not useful in monthly dump
                        "bot2_runtime_state",         # scheduler dedup state — not user data
                        "bot1_pre_reset_backups",     # internal rollback — huge, already on prod
                        "bot2_pre_reset_backups",     # internal rollback — huge, already on prod
                        "bot2_cleanup_backups",       # broadcast rollback snapshots — already on prod
                        "bot2_cleanup_logs",          # internal cleanup event logs
                        "bot1_offline_log",           # bot on/off event log — not critical monthly
                        "bot_backup_history",         # backup event history — on backup cluster
                    }
                    all_db_cols = sorted(db.list_collection_names())
                    all_cols    = [c for c in all_db_cols if c not in _SKIP_MONTHLY]
                    period   = "AM" if now.hour < 12 else "PM"
                    ts_label = now.strftime(f"%B %d, %Y \u2014 %I:%M {period}")
                    await bot.send_message(
                        MASTER_ADMIN_ID,
                        f"\U0001f4e6 <b>BOT 2 \u2014 MONTHLY FULL JSON BACKUP</b>\n\n"
                        f"\U0001f5d3 <b>{now.strftime('%B %Y')}</b>\n"
                        f"\U0001f558 {ts_label}\n\n"
                        f"Delivering <b>{len(all_cols)}</b> data collections \u2014 MSANodeDB snapshot.\n"
                        f"Internal/log collections excluded. Every file is independently restorable.",
                        parse_mode="HTML",
                    )

                    total_records = 0
                    total_bytes   = 0
                    errors: list  = []
                    for col_name in all_cols:
                        unique_key = _MONTHLY_RESTORE_KEYS.get(col_name, "_id")
                        try:
                            cnt, nb = await _send_col_json_bot2(col_name, unique_key, now)
                            total_records += cnt
                            total_bytes   += nb
                        except Exception as e:
                            errors.append(f"{col_name}: {e}")
                            print(f"\u274c Monthly JSON bot2 \u2014 {col_name}: {e}")
                    summary = (
                        f"\u2705 <b>BOT 2 MONTHLY BACKUP COMPLETE</b>\n\n"
                        f"\U0001f5d3 {now.strftime('%B %Y')}\n"
                        f"\U0001f4ca Total records: <b>{total_records:,}</b>\n"
                        f"\U0001f4be Compressed: <b>{total_bytes/1024:.1f} KB</b>\n"
                        f"\U0001f4c1 Collections: <b>{len(all_cols)-len(errors)}/{len(all_cols)}</b>"
                    )
                    if errors:
                        summary += "\n\n\u26a0\ufe0f Errors:\n" + "\n".join(f"\u2022 {e}" for e in errors)
                    await bot.send_message(MASTER_ADMIN_ID, summary, parse_mode="HTML")
                    print(f"\u2705 Bot 2 monthly JSON backup done \u2014 {total_records:,} records, {total_bytes/1024:.1f} KB")
            await asyncio.sleep(1800)   # check every 30 minutes
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"\u274c monthly_json_delivery_bot2: {e}")
            await asyncio.sleep(300)


# ==========================================
# STATE PERSISTENCE (Restart Recovery)
# ==========================================

bot2_STATE_COLLECTION = db["bot2_runtime_state"]

def save_bot2_state():
    """Save runtime state to MongoDB so restarts pick up where they left off"""
    try:
        state_doc = {
            "state_key": "bot2_main",
            "saved_at": now_local(),
            "health_stats": {
                "errors_caught": bot2_health["errors_caught"],
                "auto_healed": bot2_health["auto_healed"],
                "owner_notified": bot2_health["owner_notified"],
                "consecutive_failures": bot2_health["consecutive_failures"],
            },
            "uptime_seconds": (now_local() - bot2_health["bot_start_time"]).total_seconds(),
            "last_shutdown": now_local().isoformat(),
        }
        bot2_STATE_COLLECTION.update_one(
            {"state_key": "bot2_main"},
            {"$set": state_doc},
            upsert=True
        )
        print("💾 [STATE] Runtime state saved to MongoDB")
    except Exception as e:
        print(f"⚠️ [STATE] Failed to save state: {e}")


def load_bot2_state():
    """Load previous runtime state on startup for continuity"""
    try:
        state = bot2_STATE_COLLECTION.find_one({"state_key": "bot2_main"})
        if state:
            last_shutdown = state.get("last_shutdown", "Unknown")
            prev_uptime = state.get("uptime_seconds", 0)
            h = int(prev_uptime // 3600)
            m = int((prev_uptime % 3600) // 60)
            print(f"♻️ [STATE] Previous session found — Last shutdown: {last_shutdown}")
            print(f"♻️ [STATE] Previous uptime was {h}h {m}m")
            print(f"♻️ [STATE] Previous errors caught: {state.get('health_stats', {}).get('errors_caught', 0)}")
            # Restore cumulative health counters from previous session
            prev_stats = state.get("health_stats", {})
            bot2_health["errors_caught"]       += prev_stats.get("errors_caught", 0)
            bot2_health["auto_healed"]         += prev_stats.get("auto_healed", 0)
            bot2_health["owner_notified"]      += prev_stats.get("owner_notified", 0)
            bot2_health["consecutive_failures"] = 0  # Reset on clean restart
            return state
        else:
            print("🆕 [STATE] No previous state found — fresh start")
            return None
    except Exception as e:
        print(f"⚠️ [STATE] Could not load previous state: {e}")
        return None


async def state_auto_save_loop():
    """Auto-save state every 5 minutes for crash recovery"""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            save_bot2_state()
        except asyncio.CancelledError:
            save_bot2_state()  # Save on shutdown
            break
        except Exception as e:
            print(f"⚠️ [STATE SAVE] Error: {e}")



# ==========================================
# DAILY REPORT SYSTEM (8:40 AM & 8:40 PM)
# ==========================================

async def generate_daily_report() -> str:
    """Generate comprehensive daily report for Bot 1 + Bot 2 with real live data."""
    now = now_local()
    uptime = now - bot2_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # === BOT 2 DB HEALTH ===
    try:
        t0 = time.time()
        client.admin.command('ping')
        db_ms = (time.time() - t0) * 1000
        db_status = f"Online ({db_ms:.0f}ms)"
        db_ok = True
    except Exception as _de:
        db_status = f"OFFLINE ({type(_de).__name__})"
        db_ok = False

    # === BACKUP CLUSTER HEALTH ===
    try:
        t0b = time.time()
        _backup_mongo_client(BACKUP_MONGO_URI or MONGO_URI,
                             serverSelectionTimeoutMS=5000).admin.command('ping')
        bkp_ms = (time.time() - t0b) * 1000
        bkp_status = f"Online ({bkp_ms:.0f}ms)"
    except Exception:
        bkp_status = "Offline"

    # === BOT 1 DATA (from shared MSANodeDB) ===
    try:
        b1_msa_total     = col_msa_ids.count_documents({})
        b1_verified      = col_user_verification.count_documents({"verified": True})
        b1_unverified    = col_user_verification.count_documents({"verified": {"$ne": True}})
        b1_perm_banned   = col_permanently_banned_msa.count_documents({})
        b1_open_tickets  = col_support_tickets.count_documents({"status": "open"})
        b1_total_tickets = col_support_tickets.count_documents({})
        b1_new_tickets   = col_support_tickets.count_documents({"created_at": {"$gte": today_start}})
        b1_banned        = col_banned_users.count_documents({})
        b1_suspended     = col_suspended_features.count_documents({})
    except Exception:
        b1_msa_total = b1_verified = b1_unverified = b1_perm_banned = 0
        b1_open_tickets = b1_total_tickets = b1_new_tickets = b1_banned = b1_suspended = 0

    # === BOT 2 DATA ===
    try:
        b2_tracked       = col_user_tracking.count_documents({})
        b2_yt            = col_user_tracking.count_documents({"source": "YT"})
        b2_ig            = col_user_tracking.count_documents({"source": "IG"})
        b2_igcc          = col_user_tracking.count_documents({"source": "IGCC"})
        b2_ytcode        = col_user_tracking.count_documents({"source": "YTCODE"})
        b2_broadcasts    = col_broadcasts.count_documents({})
        last_bcast       = col_broadcasts.find_one({}, sort=[("created_at", -1)])
        last_bcast_str   = last_bcast["created_at"].strftime("%d %b %I:%M %p") if last_bcast and last_bcast.get("created_at") else "Never"
        b2_admins        = col_admins.count_documents({})
        b2_locked_admins = col_admins.count_documents({"locked": True})
        b2_banned_own    = col_banned_users.count_documents({"scope": "bot2"})
    except Exception:
        b2_tracked = b2_yt = b2_ig = b2_igcc = b2_ytcode = b2_broadcasts = 0
        last_bcast_str = "N/A"
        b2_admins = b2_locked_admins = b2_banned_own = 0

    # === BACKUP STATUS ===
    try:
        bk1 = col_bot1_backups.find_one({}, sort=[("backup_date", -1)])
        bk2 = col_bot2_backups.find_one({}, sort=[("backup_date", -1)])
        bk1_str = bk1["backup_date"].strftime("%d %b %I:%M %p") if bk1 and bk1.get("backup_date") else "Never"
        bk2_str = bk2["backup_date"].strftime("%d %b %I:%M %p") if bk2 and bk2.get("backup_date") else "Never"
        bk1_count = col_bot1_backups.count_documents({})
        bk2_count = col_bot2_backups.count_documents({})
    except Exception:
        bk1_str = bk2_str = "Unavailable"
        bk1_count = bk2_count = 0

    # === MONGODB STORAGE ===
    _st = get_mongo_storage_stats()

    period     = "🌅 MORNING" if now.hour < 12 else "🌆 EVENING"
    report_time = now.strftime("%B %d, %Y  %I:%M %p")

    report = (
        f"📊 **DAILY {period} REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓 **{report_time}**\n\n"

        f"⚡ **BOT 2 — SYSTEM STATUS**\n"
        f"• Status: ✅ Online\n"
        f"• Uptime: `{h}h {m}m`\n"
        f"• Main DB (`MSANodeDB`): {'✅' if db_ok else '❌'} {db_status}\n"
        f"• Backup DB (`MSANodeBackups`): {bkp_status}\n"
        f"• Auto-Healer: ✅ Active\n"
        f"• Watchdog Monitor: ✅ Active (hourly DB + bot ping)\n"
        f"• Daily Reports: ✅ 8:40 AM & 8:40 PM (this report)"
        f"\n• Errors Caught: `{bot2_health['errors_caught']}`"
        f" | Auto-Healed: `{bot2_health['auto_healed']}`"
        f" | Alerts Sent: `{bot2_health['owner_notified']}`\n\n"

        f"🤖 **BOT 1 — USER DATA**\n"
        f"• MSA IDs Registered: `{b1_msa_total:,}`\n"
        f"• Verified Users: `{b1_verified:,}` | Unverified: `{b1_unverified:,}`\n"
        f"• Banned Users: `{b1_banned:,}` | Perm Banned MSA: `{b1_perm_banned:,}`\n"
        f"• Feature Suspended: `{b1_suspended:,}`\n"
        f"• Open Tickets: `{b1_open_tickets}` | Total: `{b1_total_tickets:,}` | New Today: `{b1_new_tickets}`\n\n"

        f"🎛 **BOT 2 — ADMIN DATA**\n"
        f"• User Tracking Total: `{b2_tracked:,}`\n"
        f"• By Source — YT: `{b2_yt}` | IG: `{b2_ig}` | IGCC: `{b2_igcc}` | YTCODE: `{b2_ytcode}`\n"
        f"• Broadcasts Stored: `{b2_broadcasts}` | Last: {last_bcast_str}\n"
        f"• Admins: `{b2_admins}` | Locked: `{b2_locked_admins}`\n"
        f"• Bot2-Scoped Bans: `{b2_banned_own}`\n\n"

        f"💾 **BACKUP CLUSTER (MSANodeBackups)**\n"
        f"• Bot 1 — Snapshots: `{bk1_count}` | Last: {bk1_str}\n"
        f"• Bot 2 — Snapshots: `{bk2_count}` | Last: {bk2_str}\n\n"
    )

    if _st["ok"]:
        report += (
            f"🗄 **MONGODB STORAGE**\n"
            f"• Used: `{_st['used_mb']:.1f}MB` / `{_st['cap_mb']:.0f}MB` ({_st['pct']:.1f}%)\n"
            f"• [{_st['bar']}] {_st['risk_icon']} {_st['risk_label']}\n"
        )
        if _st["pct"] >= 75:
            report += f"• ⚠️ Storage at {_st['pct']:.0f}% — plan upgrade soon\n"
        report += "\n"

    report += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Auto-report · Next in 12h_"
    )
    return report


async def schedule_storage_alerts():
    """
    Check MongoDB storage every 6 hours.
    Sends Telegram alert to master admin when storage crosses 60 / 75 / 85 / 95%.
    Alerts reset daily so you get one reminder per threshold per day.
    """
    _alerted: set = set()
    _alerted_date = None

    while True:
        try:
            today = now_local().date()
            if _alerted_date != today:
                _alerted.clear()
                _alerted_date = today

            _st = get_mongo_storage_stats()
            if _st["ok"]:
                pct = _st["pct"]
                for threshold, emoji, action in [
                    (95, "🔴", "UPGRADE NOW — bot may stop storing data soon"),
                    (85, "🟠", "Start cleaning up old records or upgrade MongoDB plan"),
                    (75, "🟡", "Plan ahead — delete old data or upgrade soon"),
                    (60, "📊", "MongoDB is over half full — keep an eye on it"),
                ]:
                    if pct >= threshold and threshold not in _alerted:
                        _alerted.add(threshold)
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"{emoji} *MONGODB STORAGE ALERT — {pct:.0f}% USED*\n\n"
                            f"`[{_st['bar']}]`\n"
                            f"*Used:* `{_st['used_mb']:.1f}MB` / `{_st['cap_mb']:.0f}MB`\n\n"
                            f"*Action:* {action}\n\n"
                            f"*Data breakdown:*\n"
                            f"• Documents: `{_st['data_mb']:.1f}MB`\n"
                            f"• Indexes: `{_st['index_mb']:.1f}MB`\n\n"
                            f"Upgrade at: https://cloud.mongodb.com",
                            parse_mode="Markdown"
                        )
                        break  # Only alert highest triggered threshold once
        except Exception as _e:
            print(f"⚠️ [STORAGE ALERT] Check failed: {_e}")

        await asyncio.sleep(6 * 3600)  # Check every 6 hours


async def schedule_daily_reports():
    """Send daily reports at exactly 8:40 AM and 8:40 PM — strict timing"""
    print("📊 [DAILY REPORT] Scheduler started — reports at 8:40 AM and 8:40 PM")
    sent_times = set()  # Track which slots were already sent today

    while True:
        try:
            now = now_local()
            current_slot = None

            # 8:40 AM slot
            if now.hour == 8 and now.minute >= 40 and now.minute < 55:
                current_slot = f"{now.date()}_AM"
            # 8:40 PM slot
            elif now.hour == 20 and now.minute >= 40 and now.minute < 55:
                current_slot = f"{now.date()}_PM"

            if current_slot and current_slot not in sent_times:
                print(f"📊 [DAILY REPORT] Sending {current_slot} report...")
                try:
                    report_text = await generate_daily_report()
                    await bot.send_message(MASTER_ADMIN_ID, report_text, parse_mode="Markdown")
                    sent_times.add(current_slot)
                    print(f"✅ [DAILY REPORT] {current_slot} report sent to owner")
                    # Clean old slots (keep only today's)
                    today_str = str(now.date())
                    sent_times = {s for s in sent_times if today_str in s}
                except Exception as e:
                    print(f"❌ [DAILY REPORT] Failed to send {current_slot}: {e}")

            await asyncio.sleep(60)  # Check every minute for precision

        except asyncio.CancelledError:
            print("📊 [DAILY REPORT] Scheduler stopping...")
            break
        except Exception as e:
            print(f"❌ [DAILY REPORT SCHEDULER] Error: {e}")
            await asyncio.sleep(60)


# ==========================================
# 🌐 RENDER HEALTH CHECK WEB SERVER
# Render requires a web service to respond on $PORT — this lightweight
# aiohttp server satisfies that requirement alongside the bot polling.
# ==========================================

async def _health_handler_bot2(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """Health check endpoint for Render — confirms Bot 2 is alive."""
    uptime = now_local() - bot2_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    return aiohttp_web.json_response({
        "status": "ok",
        "bot": "MSA NODE Bot 2",
        "uptime": f"{h}h {m}m",
        "errors_caught": bot2_health["errors_caught"],
        "auto_healed": bot2_health["auto_healed"],
    })


async def start_health_server_bot2():
    """Start the lightweight aiohttp web server for Render health checks + webhook."""
    if "PORT" not in os.environ:
        print("🌐 Health server skipped (PORT not set — local dev mode)")
        return None
    app = aiohttp_web.Application()
    app.router.add_get("/health", _health_handler_bot2)
    app.router.add_get("/", _health_handler_bot2)  # Render also checks root

    if _WEBHOOK_URL:
        # Register Telegram webhook route onto the same aiohttp app
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=_WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        print(f"✅ Webhook route registered: {_WEBHOOK_PATH}")

    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", PORT)
    try:
        await site.start()
        print(f"🌐 Web server running on port {PORT}")
        return runner
    except OSError as e:
        _is_render = bool(os.environ.get("RENDER") or os.environ.get("RENDER_EXTERNAL_URL"))
        if _is_render:
            raise  # Fatal on Render — PORT collision should never happen there
        print(
            f"⚠️ Health server could not bind to port {PORT}: {e}\n"
            f"   Bot 2 running WITHOUT health endpoint (local dev only — safe to ignore)."
        )
        await runner.cleanup()
        return None


# =============================================================================
# 🩺 BACKUP CLUSTER HEALTH — 6h ping + storage monitor
# =============================================================================

async def schedule_backup_cluster_ping():
    """Ping MSANodeBackups cluster every 6h — alert owner on failure or storage pressure."""
    _PING_INTERVAL = 6 * 3600
    _last_ok = True
    while True:
        try:
            await asyncio.sleep(_PING_INTERVAL)
            if not BACKUP_MONGO_URI:
                continue
            loop = asyncio.get_event_loop()
            def _ping_sync():
                c = MongoClient(
                    BACKUP_MONGO_URI,
                    serverSelectionTimeoutMS=8000,
                    tlsCAFile=certifi.where(),
                )
                try:
                    c.admin.command("ping")
                    try:
                        st = c[BACKUP_MONGO_DB_NAME or "MSANodeBackups"].command("dbStats")
                        return {"ok": True, "used_mb": round(st.get("dataSize", 0) / 1_048_576, 2)}
                    except Exception:
                        return {"ok": True, "used_mb": None}
                finally:
                    c.close()
            res = await loop.run_in_executor(None, _ping_sync)
            if not _last_ok:
                await bot.send_message(
                    MASTER_ADMIN_ID,
                    "✅ <b>Backup Cluster RECOVERED</b>\n\n"
                    "<code>MSANodeBackups</code> is reachable again.\n"
                    "Backup writes have resumed normally.",
                    parse_mode="HTML"
                )
            _last_ok = True
            used_mb = res.get("used_mb")
            if used_mb and used_mb > 400:
                pct = round(used_mb / 512 * 100, 1)
                await bot.send_message(
                    MASTER_ADMIN_ID,
                    f"⚠️ <b>Backup Cluster Storage Alert</b>\n\n"
                    f"📊 Used: <b>{used_mb:.1f} MB / 512 MB ({pct}%)</b>\n\n"
                    "Consider clearing old snapshots:\n"
                    "💾 BACKUP → 🗑️ RESET BACKUP DATA",
                    parse_mode="HTML"
                )
        except asyncio.CancelledError:
            break
        except Exception as _ping_err:
            if _last_ok:
                try:
                    await bot.send_message(
                        MASTER_ADMIN_ID,
                        f"🚨 <b>Backup Cluster UNREACHABLE (6h check)</b>\n\n"
                        f"⚠️ <code>MSANodeBackups</code> did not respond.\n"
                        f"Error: <code>{str(_ping_err)[:200]}</code>\n\n"
                        "Daily/weekly backup writes will fail until resolved.",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
            _last_ok = False
            await asyncio.sleep(300)


async def _check_gdrive_token_startup():
    """Check GDrive token validity 20s after startup — alert owner if expired/missing."""
    await asyncio.sleep(20)
    try:
        loop = asyncio.get_event_loop()
        def _check():

            _get_gdrive_service()
        await loop.run_in_executor(None, _check)
        print("✅ GDrive token valid — ☁️ GDRIVE SYSTEM ready")
    except Exception as _gdrive_err:
        print(f"⚠️ GDrive token invalid/expired: {_gdrive_err}")
        try:
            await bot.send_message(
                MASTER_ADMIN_ID,
                "⚠️ <b>Google Drive Token Expired / Missing</b>\n\n"
                "☁️ GDRIVE SYSTEM uploads will fail until this is fixed.\n\n"
                "<b>Fix steps:</b>\n"
                "1. Run <code>python bot2.py</code> locally\n"
                "2. Complete Google OAuth in browser\n"
                "3. Fresh <code>token.json</code> is created\n"
                "4. Restart Bot 2",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ==========================================
# MAIN EXECUTION — ENTERPRISE READY
# ==========================================

async def main():
    """Enterprise-grade bot2 startup with full resilience"""
    health_task = None
    state_save_task = None
    daily_report_task = None
    cleanup_task = None
    monthly_backup_task = None
    storage_alert_task = None
    web_runner = None

    print("\n🚀 ═══════════════════════════════════════")
    print("🚀  BOT 2 — ENTERPRISE STARTUP")
    print("🚀 ═══════════════════════════════════════\n")

    # ── 1. Load previous state for continuity ──
    previous_state = load_bot2_state()
    if previous_state:
        print(f"♻️ Resuming from previous session (last seen: {previous_state.get('last_shutdown', 'unknown')})")

    # ── 2. Check backup storage status ──
    pass  # backup storage check retired — new hierarchical system handles this

    # ── 2b. Migrate old bot2-triggered bans to have scope="bot2" ──
    # This ensures auto-bans and admin-panel bans don't block Bot 1 users
    try:
        migrated = col_banned_users.update_many(
            {
                "scope": {"$exists": False},
                "$or": [
                    {"banned_by": "SYSTEM"},
                    {"reason": "Banned by master admin"}
                ]
            },
            {"$set": {"scope": "bot2"}}
        )
        if migrated.modified_count > 0:
            print(f"🔧 Ban migration: {migrated.modified_count} bot2-scoped ban(s) patched (no longer affect Bot 1)")
    except Exception as _e:
        print(f"⚠️ Ban migration skipped: {_e}")

    # ── 3. Register global error handler ──
    dp.errors.register(bot2_global_error_handler)
    print("🏥 Auto-healer registered — all errors will be caught and handled")

    try:
        # ── 3b. Start Render health check web server ──
        web_runner = await start_health_server_bot2()

        # ── 4. Start background tasks ──
        health_task = asyncio.create_task(bot2_health_monitor())
        print("💊 Health monitor started (checks every hour)")

        cleanup_task = asyncio.create_task(schedule_daily_cleanup())
        print("🧹 Daily cleanup scheduler started (runs at 3:00 AM)")

        daily_report_task = asyncio.create_task(schedule_daily_reports())
        print("📊 Daily report scheduler started (8:40 AM & 8:40 PM)")

        storage_alert_task = asyncio.create_task(schedule_storage_alerts())
        print("🗄️ Storage alert scheduler started (checks every 6h — alerts at 60/75/85/95%)")

        state_save_task = asyncio.create_task(state_auto_save_loop())
        print("💾 State auto-save started (every 5 minutes)")

        asyncio.create_task(schedule_backup_cluster_ping())
        print("🩺 Backup cluster health monitor started (pings every 6h — alerts if down or >80% storage)")

        asyncio.create_task(_check_gdrive_token_startup())
        print("☁️ GDrive token check queued (validates in 20s)")

        # ── Unified backup schedulers ─────────────────────────────────────────
        # Reads from PRODUCTION (MONGO_URI) — writes to BACKUP cluster (BACKUP_MONGO_URI).
        # Both bot1 and bot2 data are backed up separately.
        if weekly_backup_scheduler:
            # bot2 weekly backup
            asyncio.create_task(
                weekly_backup_scheduler(
                    bot_instance=bot,
                    bot_name="bot2",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=BACKUP_MONGO_URI,
                    backup_db_name=BACKUP_MONGO_DB_NAME
                ),
                name="weekly_backup_bot2"
            )
            # bot1 weekly backup (reads same prod DB, bot1_ collections)
            asyncio.create_task(
                weekly_backup_scheduler(
                    bot_instance=bot,
                    bot_name="bot1",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=BACKUP_MONGO_URI,
                    backup_db_name=BACKUP_MONGO_DB_NAME
                ),
                name="weekly_backup_bot1"
            )
            print("💾 Weekly backup schedulers started (bot1 + bot2 → Backup Cluster, every Sunday 23:59 UTC, TTL 90 days)")

        if monthly_export_scheduler:
            # bot2 monthly export
            asyncio.create_task(
                monthly_export_scheduler(
                    bot_instance=bot,
                    bot_name="bot2",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=BACKUP_MONGO_URI,
                    backup_db_name=BACKUP_MONGO_DB_NAME
                ),
                name="monthly_export_bot2"
            )
            # bot1 monthly export
            asyncio.create_task(
                monthly_export_scheduler(
                    bot_instance=bot,
                    bot_name="bot1",
                    owner_id=OWNER_ID,
                    mongo_uri=MONGO_URI,
                    db_name=MONGO_DB_NAME,
                    backup_mongo_uri=BACKUP_MONGO_URI,
                    backup_db_name=BACKUP_MONGO_DB_NAME
                ),
                name="monthly_export_bot1"
            )
            print("📦 Monthly export schedulers started (bot1 + bot2 → ZIP to owner, last day of month 23:59 UTC)")

        # ⚠️ Old 12h / monthly schedulers deprecated — replaced above


        # ── 5. Notify owner of successful startup ──
        try:
            prev_info = ""
            if previous_state:
                prev_shutdown = previous_state.get("last_shutdown", "Unknown")
                prev_info = f"\n♻️ <b>Resumed from:</b> {prev_shutdown}"

            await bot.send_message(
                MASTER_ADMIN_ID,
                f"✅ <b>BOT 2 STARTED SUCCESSFULLY</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🏥 Auto-Healer: ✅ Active\n"
                f"💊 Health Monitor: ✅ Running\n"
                f"📊 Daily Reports: ✅ 8:40 AM &amp; 8:40 PM\n"
                f"💾 State Persistence: ✅ Active\n"
                f"🧹 Auto-Cleanup: ✅ 3 AM daily\n"
                f"💿 Auto-Backup: ✅ Daily 23:59 UTC + Weekly Sunday → Backup Cluster\n"
                f"🩺 Backup Cluster Monitor: ✅ Every 6h (alerts on failure/storage)\n"
                f"☁️ GDrive Token: ✅ Validated at startup\n"
                f"🔄 Restore UI: ✅ Live (💾 BACKUP → 🔄 RESTORE DATA)\n"
                f"🗄️ Storage Alerts: ✅ Every 6h (alerts at 60/75/85/95%)\n"
                f"{prev_info}\n\n"
                f"<b>Started:</b> {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Bot 2 Enterprise — All systems operational</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"⚠️ Could not send startup notification: {e}")

        # ── 6. Reindex broadcasts to fix any gaps from previous data ──
        try:
            reindex_broadcasts()
            print("🔄 Broadcasts reindexed on startup — all indices are sequential.")
        except Exception as e:
            print(f"⚠️ Broadcast reindex on startup failed: {e}")

        # ── 7. Start webhook or polling ──────────────────────────────────────────
        print("\n✅ All systems started...\n")
        if _WEBHOOK_URL:
            # ── WEBHOOK MODE (production) ───────────────────────────────────
            print("🔄 Starting in WEBHOOK mode...")
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(_WEBHOOK_URL)
            print(f"✅ Webhook set: {_WEBHOOK_URL}")
            # Webhook handler registered in start_health_server_bot2()
            await asyncio.Event().wait()
        else:
            # ── POLLING MODE (local dev fallback) ───────────────────────────
            print("ℹ️ Using polling (local dev mode)")
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except Exception as e:
        print(f"❌ FATAL ERROR during startup: {e}")
        try:
            await notify_master_admin("Bot Startup Failure", str(e), "CRITICAL", False)
        except Exception:
            pass
        raise

    finally:
        # ── 7. Graceful shutdown ──
        print("\n🛑 Bot 2 shutting down gracefully...")

        # Save final state
        save_bot2_state()

        # Cancel background tasks
        for task_name, task in [
            ("Health Monitor", health_task),
            ("State Save", state_save_task),
            ("Daily Report", daily_report_task),
            ("Cleanup", cleanup_task),
            ("Monthly Backup", monthly_backup_task),
            ("Storage Alerts", storage_alert_task),
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    print(f"✅ {task_name} stopped cleanly")

        # Notify owner of shutdown
        try:
            uptime = now_local() - bot2_health["bot_start_time"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)

            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🛑 **BOT 2 SHUTDOWN**\n\n"
                f"**Uptime:** {h}h {m}m\n"
                f"**Errors Caught:** {bot2_health['errors_caught']}\n"
                f"**Auto-Healed:** {bot2_health['auto_healed']}\n"
                f"**Alerts Sent:** {bot2_health['owner_notified']}\n\n"
                f"**Shutdown:** {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"_State saved. Bot will resume when restarted._",
                parse_mode="Markdown"
            )
        except Exception:
            pass

        try:
            await bot.session.close()
            await bot_1.session.close()
        except Exception:
            pass

        # ── Stop health check web server ──
        if web_runner:
            try:
                await web_runner.cleanup()
                print("🌐 Health check server stopped")
            except Exception:
                pass

        print("✅ Bot 2 shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Bot 2 stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n💥 Critical error: {e}")
        sys.exit(1)
