"""
MSA Node — Unified Backup Scheduler Module (v3 — Standalone)
=============================================================

Imported by bot1.py as:
    from backup_schedulers import weekly_backup_scheduler, monthly_export_scheduler

Provides:
  - weekly_backup_scheduler  : Daily 23:59 UTC — save local JSON + cluster upsert
                               Last day of month → auto GDrive ZIP upload
  - monthly_export_scheduler : No-op compatibility stub (logic is in weekly_backup_scheduler)
  - force_backup_to_cluster  : Manual snapshot trigger
  - list_cluster_backups     : List backup records from MSANodeBackups
  - download_cluster_backup  : Fetch a backup record as ZIP bytes
  - gdrive_upload_cluster_backup : Upload a cluster record to Google Drive

Backup tiers:
  1. Daily auto-scheduler (23:59 UTC) → local MSANode_Local_Backups/ folder hierarchy
  2. Daily auto-scheduler (23:59 UTC) → MSANodeBackups cluster (90-day TTL)
  3. Month-end auto-scheduler (last day 23:59 UTC) → Google Drive ZIP

Local folder hierarchy:
  MSANode_Local_Backups/{bot_name}/{year}/{Month Year}/Week {N}/{YYYY-MM-DD}/{col}.json
"""

import asyncio
import io
import json
import logging
import os
import zipfile
from calendar import monthrange
from datetime import datetime, timedelta

import certifi
from pymongo import ASCENDING, MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# ─── Constants ────────────────────────────────────────────────────────────────

# Root for local daily JSON backups (relative to this file's directory)
_LOCAL_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSANode_Local_Backups")

# 90-day TTL for cluster backup documents
_TTL_SECONDS = 90 * 24 * 3600

# Google Drive folder IDs — resolved after load_dotenv runs in the calling bot
# These are read lazily inside each function so env is already loaded by then.
def _gdrive_folder_for(bot_name: str, override: str = None) -> str:
    """Return the correct GDrive folder ID for the given bot_name."""
    if override:
        return override
    if bot_name == "bot1":
        return os.environ.get("BOT1_GDRIVE_FOLDER_ID", "")
    if bot_name == "bot2":
        return os.environ.get("BOT2_GDRIVE_FOLDER_ID", "")
    return os.environ.get("BOT3_GDRIVE_FOLDER_ID", "")

# Which collections belong to each bot (seed list — extras are auto-discovered)
_BOT_COLLECTIONS = {
    "bot1": [
        "bot1_msa_ids",
        "bot1_user_verification",
        "bot1_support_tickets",
        "bot1_banned_users",
        "bot1_suspended_features",
        "bot1_permanently_banned_msa",
        "bot1_backups",
        "bot1_restore_data",
        "bot1_offline_log",
        "bot1_settings",
        "bot1_state_persistence",
        "bot1_referrals",
        "bot1_reviews",
        "bot1_msa_credits",
        "bot1_pre_reset_backups",
    ],
    "bot2": [
        "bot2_user_tracking",
        "bot2_broadcasts",
        "bot2_admins",
        "bot2_banned_users",
        "bot2_backups",
        "bot2_restore_data",
        "bot2_runtime_state",
        "bot2_cleanup_logs",
        "bot2_backup_history",
        "bot2_live_terminal_logs",
        "bot2_pre_reset_backups",
    ],
    "bot3": [
        "bot3_pdfs",
        "bot3_ig_content",
        "bot3_rewards",
        "bot3_store_items",
        "bot3_milestones",
        "bot3_tutorials",
        "bot3_admins",
        "bot3_settings",
        "bot3_banned_users",
        "bot3_logs",
        "bot3_user_activity",
        "bot3_backups",
    ],
}

# ─── Internal Helpers ─────────────────────────────────────────────────────────

def _ensure_ttl_index(col):
    """Create 90-day TTL index on backup_date field (idempotent)."""
    try:
        col.create_index(
            [("backup_date", ASCENDING)],
            expireAfterSeconds=_TTL_SECONDS,
            name="backup_ttl_90d",
            background=True,
        )
    except Exception:
        pass  # Non-fatal


def _backup_mongo_client(uri: str, **kwargs):
    """Create a MongoClient for the backup cluster using the certifi CA bundle."""
    opts = {
        "serverSelectionTimeoutMS": 10000,
        "tlsCAFile": certifi.where(),
    }
    opts.update(kwargs)
    return MongoClient(uri, **opts)


def _get_gdrive_service():
    """Build and return an authenticated Google Drive service object.
    Will auto-refresh the token if it is expired but the refresh_token is valid.
    Raises RuntimeError if token.json is missing or cannot be refreshed.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(base_dir, "token.json")
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "w") as fh:
                fh.write(creds.to_json())
        else:
            raise RuntimeError(
                "Google Drive token.json missing or invalid. "
                "Re-run the OAuth flow locally to generate a fresh token.json."
            )

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
    """Upload bytes as a file to GDrive and return the file ID."""
    from googleapiclient.http import MediaIoBaseUpload
    meta = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(zip_bytes), mimetype="application/zip", resumable=True)
    f = service.files().create(body=meta, media_body=media, fields="id").execute()
    return f.get("id", "")


def _get_or_create_gdrive_folder(service, folder_name: str, parent_id: str) -> str:
    """Get an existing GDrive folder by name, or create it if it doesn't exist."""
    query = (
        f"name='{folder_name}' and "
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and "
        "trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    
    meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    f = service.files().create(body=meta, fields="id").execute()
    return f.get("id")

def _resolve_gdrive_path(service, base_folder_id: str, ts_dt) -> str:
    """
    Resolve the nested GDrive folder structure:
    Base -> Year -> Month -> Week -> Date
    Creates any missing folders along the way.
    """
    year_str = ts_dt.strftime("%Y")
    month_str = ts_dt.strftime("%B")  # e.g., 'July'
    date_str = ts_dt.strftime("%Y-%m-%d")
    day_of_month = ts_dt.day
    week_num = ((day_of_month - 1) // 7) + 1
    week_str = f"Week {week_num}"

    year_id = _get_or_create_gdrive_folder(service, year_str, base_folder_id)
    month_id = _get_or_create_gdrive_folder(service, month_str, year_id)
    week_id = _get_or_create_gdrive_folder(service, week_str, month_id)
    date_id = _get_or_create_gdrive_folder(service, date_str, week_id)
    return date_id


def _export_bot_collections(prod_db, bot_name: str) -> dict:
    """Export all collections for a bot from PROD DB.
    Returns {col_name: {count: int, documents: list}} for each collection.
    Auto-discovers any extra collections with the bot's prefix.
    """
    result = {}
    cols = list(_BOT_COLLECTIONS.get(bot_name, []))
    all_db_cols = prod_db.list_collection_names()
    prefix = f"{bot_name}_"
    extra = [c for c in all_db_cols if c.startswith(prefix) and c not in cols]
    all_cols = cols + extra

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
    """Save daily JSON export to the local folder hierarchy.

    Structure: MSANode_Local_Backups/{bot_name}/{year}/{Month Year}/Week {N}/{YYYY-MM-DD}/{col}.json
    Skipped automatically on Render (ephemeral disk).
    """
    try:
        year     = str(now.year)
        month    = now.strftime("%B %Y")        # e.g. "May 2026"
        week_num = (now.day - 1) // 7 + 1       # 1-5
        day      = now.strftime("%Y-%m-%d")     # e.g. "2026-05-27"
        day_dir  = os.path.join(_LOCAL_ROOT, bot_name, year, month, f"Week {week_num}", day)
        os.makedirs(day_dir, exist_ok=True)

        collections = _export_bot_collections(prod_db, bot_name)
        for col_name, col_data in collections.items():
            docs = col_data.get("documents", [])
            if docs:
                fpath = os.path.join(day_dir, f"{col_name}.json")
                with open(fpath, "w", encoding="utf-8") as fh:
                    json.dump(docs, fh, default=str, indent=2, ensure_ascii=False)
        logging.info(f"[BACKUP] Daily local export saved: {day_dir}")
    except Exception as e:
        logging.error(f"[BACKUP] _save_local_daily_backup failed for {bot_name}: {e}")


def _upsert_cluster_snapshot(bot_name: str, prod_db, bkp_db, now: datetime):
    """Upsert today's snapshot into MSANodeBackups cluster (one doc per day per bot)."""
    from datetime import timezone as _tz
    try:
        bot_num     = bot_name[-1]
        bkp_col     = bkp_db[f"bot{bot_num}_backups"]
        _ensure_ttl_index(bkp_col)
        day_key     = now.strftime("%Y-%m-%d")
        week_num    = (now.day - 1) // 7 + 1
        week_label  = f"Week {week_num}"
        month_label = now.strftime("%B %Y")
        year        = now.year
        month_n     = now.month
        collections = _export_bot_collections(prod_db, bot_name)
        doc_count   = sum(v.get("count", 0) for v in collections.values())
        bkp_col.update_one(
            {"bot": bot_name, "window_key": day_key},
            {"$set": {
                "bot":         bot_name,
                "window_key":  day_key,
                "backup_date": now,
                "backup_type": "daily",
                "docs":        doc_count,
                "data":        collections,
                "week_num":    week_num,
                "week_label":  week_label,
                "month_label": month_label,
                "year":        year,
                "month":       month_n,
            }},
            upsert=True
        )
        logging.info(f"[BACKUP] Cluster snapshot upserted: {bot_name} {day_key} ({doc_count} docs)")
    except Exception as e:
        logging.error(f"[BACKUP] _upsert_cluster_snapshot failed for {bot_name}: {e}")


# ─── Public API ───────────────────────────────────────────────────────────────

def force_backup_to_cluster(
    bot_name: str,
    mongo_uri: str,
    db_name: str,
    backup_mongo_uri: str = None,
    backup_db_name: str = "MSANodeBackups",
) -> dict:
    """Snapshot all of a bot's live collections into MSANodeBackups cluster.
    Returns: {status: "ok"|"error", collections: int, docs: int, error: str}
    Safe to call with await loop.run_in_executor(None, force_backup_to_cluster, ...)
    """
    from datetime import timezone as _tz
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name

    try:
        prod_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_client  = _backup_mongo_client(_write_uri)
        prod_db     = prod_client[db_name]
        bkp_db      = bkp_client[_write_db]

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
    """List all backup records for bot_name from MSANodeBackups (newest first).
    The heavy 'data' field is excluded for performance.
    """
    try:
        bkp_client = _backup_mongo_client(backup_mongo_uri, serverSelectionTimeoutMS=10000)
        bkp_db     = bkp_client[backup_db_name]
        col_name   = f"bot{bot_name[-1]}_backups" if bot_name.startswith("bot") else "bot_backups"
        bkp_col    = bkp_db[col_name]

        records = list(bkp_col.find({"bot": bot_name}, {"data": 0}).sort("backup_date", -1))
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
    """Fetch one backup record by _id and build an in-memory ZIP.
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
        filename    = f"{bot_name}_backup_{ts_str}.zip"

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
    """Upload one MSANodeBackups record to Google Drive.
    On SUCCESS: marks gdrive_uploaded=True in the record.
    On FAILURE: leaves the record untouched (never deletes on failure).
    Returns: {status, zip_name, size_mb, file_id, message}
    """
    from bson import ObjectId
    from datetime import timezone as _tz

    _folder = _gdrive_folder_for(bot_name, gdrive_folder_id)

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
        
        # Parse timestamp string from record (e.g. 20260724_093000) or use current time
        from datetime import datetime as _dt
        try:
            ts_dt = _dt.strptime(ts_str.split("_")[0], "%Y%m%d")
        except:
            ts_dt = _dt.utcnow()
            
        final_folder_id = _resolve_gdrive_path(service, _folder, ts_dt)

        if _gdrive_file_exists(service, zip_name, final_folder_id):
            bkp_client.close()
            return {"status": "error", "message": f"File '{zip_name}' already exists in GDrive — skipped duplicate."}

        file_id = _gdrive_upload_bytes(service, zip_bytes, zip_name, final_folder_id)

        bkp_col.update_one(
            {"_id": ObjectId(backup_id)},
            {"$set": {
                "gdrive_uploaded":    True,
                "gdrive_file_id":     file_id,
                "gdrive_uploaded_at": datetime.now(_tz.utc),
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


# ─── Auto-Schedulers ──────────────────────────────────────────────────────────

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
    Daily at 23:59 UTC: save local JSON + upsert to MSANodeBackups cluster.
    On the last day of the month: also upload a ZIP to Google Drive.
    Runs forever; auto-restarts on crash with an owner alert via Telegram.
    """
    from datetime import timezone as _tz
    _write_uri = backup_mongo_uri or mongo_uri
    _write_db  = backup_db_name if backup_mongo_uri else db_name
    _is_render = os.environ.get("RENDER", "").lower() in ("true", "1", "yes")

    while True:
        try:
            while True:
                now = datetime.now(_tz.utc)

                # Calculate seconds until 23:59:00 UTC today (or tomorrow if past)
                next_run = now.replace(hour=23, minute=59, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                # ── Daily backup run ──────────────────────────────────────────────
                try:
                    run_now     = datetime.now(_tz.utc)
                    prod_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
                    bkp_client  = _backup_mongo_client(_write_uri)
                    prod_db     = prod_client[db_name]
                    bkp_db      = bkp_client[_write_db]

                    # Local disk write (skipped on Render — ephemeral disk)
                    if not _is_render:
                        _save_local_daily_backup(bot_name, prod_db, run_now)
                    else:
                        logging.info(f"[BACKUP] Render detected — skipping local disk write for {bot_name}")

                    # Cluster upsert (always runs)
                    _upsert_cluster_snapshot(bot_name, prod_db, bkp_db, run_now)
                    prod_client.close()

                    # ── Month-end: GDrive upload ──────────────────────────────────
                    _, last_day = monthrange(run_now.year, run_now.month)
                    if run_now.day == last_day:
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
                                    None,  # folder resolved from env by _gdrive_folder_for()
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

                    # Confirmation message to owner
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

                await asyncio.sleep(60)  # Wait before recalculating next_run

        except asyncio.CancelledError:
            logging.info(f"[BACKUP] weekly_backup_scheduler cancelled for {bot_name}")
            return
        except Exception as crash_err:
            logging.error(f"[BACKUP] weekly_backup_scheduler CRASHED for {bot_name}: {crash_err}")
            try:
                await bot_instance.send_message(
                    chat_id=owner_id,
                    text=(
                        f"⚠️ <b>Backup scheduler CRASHED</b> — <code>{bot_name}</code>\n"
                        f"Auto-restarting in 5 min.\n"
                        f"<code>{str(crash_err)[:200]}</code>"
                    ),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            await asyncio.sleep(300)  # 5-minute back-off before restart


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
    Compatibility stub — monthly logic is embedded in weekly_backup_scheduler.
    Kept so bot1.py can import it without error.
    """
    logging.info(f"[BACKUP] monthly_export_scheduler started for {bot_name} (no-op — handled by weekly_backup_scheduler)")
    while True:
        try:
            await asyncio.sleep(86400)
        except asyncio.CancelledError:
            logging.info(f"[BACKUP] monthly_export_scheduler cancelled for {bot_name}")
            return
        except Exception:
            await asyncio.sleep(3600)
