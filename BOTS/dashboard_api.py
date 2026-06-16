import os
import json
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from bson import ObjectId
import certifi
import os
from dotenv import load_dotenv

if os.path.exists("bot2.env"):
    load_dotenv("bot2.env", override=True)
if os.path.exists("bot1.env"):
    load_dotenv("bot1.env", override=False)

BOT_1_TOKEN = os.getenv("BOT_1_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_1_TOKEN}/sendMessage"

app = FastAPI(title="MSA NODE — Bot 2 Command Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

import os
if os.path.exists("dashboard"):
    app.mount("/static", StaticFiles(directory="dashboard"), name="static")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "MSANodeDB")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
    db = client[MONGO_DB_NAME]
    client.admin.command("ping")
    DB_ONLINE = True
    print("Dashboard DB connected successfully")
except Exception as e:
    print(f"DB Error: {e}")
    DB_ONLINE = False

def _oid(doc):
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

# ── Collections ────────────────────────────────────────────────────────────────
def col(name): return db[name]

@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    import os
    if not os.path.exists("dashboard/index.html"):
        return HTMLResponse("<h1>Dashboard HTML not found on server. But the /tg redirector is ONLINE!</h1>")
    with open("dashboard/index.html", "r", encoding="utf-8") as f:
        return f.read()

# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — OVERVIEW STATS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/stats")
def get_stats():
    if not DB_ONLINE:
        return {"error": "Database Offline"}
    try:
        total_users    = col("bot2_user_tracking").count_documents({})
        vault_members  = col("bot1_user_verification").count_documents({"vault_joined": True})
        open_tickets   = col("bot1_support_tickets").count_documents({"status": "open"})
        total_banned   = col("bot1_banned_users").count_documents({})
        total_broadcasts = col("bot2_broadcasts").count_documents({})
        suspended_count  = col("bot1_suspended_features").count_documents({})

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        chart_labels, chart_data = [], []
        for i in range(6, -1, -1):
            day      = today - timedelta(days=i)
            next_day = day + timedelta(days=1)
            count    = col("bot1_user_verification").count_documents({"first_start": {"$gte": day, "$lt": next_day}})
            chart_labels.append(day.strftime("%b %d"))
            chart_data.append(count)

        ig_count   = col("bot2_user_tracking").count_documents({"source": "IG"})
        yt_count   = col("bot2_user_tracking").count_documents({"source": "YT"})
        igcc_count = col("bot2_user_tracking").count_documents({"source": "IGCC"})
        ytcode_count = col("bot2_user_tracking").count_documents({"source": "YTCODE"})
        unknown_count = col("bot2_user_tracking").count_documents({"source": {"$nin": ["IG","YT","IGCC","YTCODE"]}})

        today_joins     = chart_data[-1]
        yesterday_joins = chart_data[-2] if len(chart_data) > 1 else 0
        growth = round(((today_joins - yesterday_joins) / yesterday_joins) * 100, 1) if yesterday_joins > 0 else 0

        maint_doc  = col("bot1_settings").find_one({"setting": "maintenance_mode"})
        is_maint   = maint_doc.get("value", False) if maint_doc else False

        ref_confirmed = col("bot1_referrals").count_documents({"status": "confirmed"})
        ref_pending   = col("bot1_referrals").count_documents({"status": "pending"})

        return {
            "total_users": total_users,
            "vault_members": vault_members,
            "open_tickets": open_tickets,
            "total_banned": total_banned,
            "total_broadcasts": total_broadcasts,
            "suspended_count": suspended_count,
            "growth_percent": growth,
            "chart_labels": chart_labels,
            "chart_data": chart_data,
            "source_data": {
                "IG": ig_count, "YT": yt_count,
                "IGCC": igcc_count, "YTCODE": ytcode_count, "UNKNOWN": unknown_count
            },
            "bot_online": not is_maint,
            "ref_confirmed": ref_confirmed,
            "ref_pending": ref_pending,
        }
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — TRAFFIC & SOURCE ANALYTICS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/traffic")
def get_traffic():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        sources = ["IG", "YT", "IGCC", "YTCODE"]
        weekly = {}
        for src in sources:
            weekly[src] = []
            for i in range(6, -1, -1):
                day = today - timedelta(days=i)
                nxt = day + timedelta(days=1)
                c = col("bot2_user_tracking").count_documents({"source": src, "joined_at": {"$gte": day, "$lt": nxt}})
                weekly[src].append(c)

        labels = [(today - timedelta(days=i)).strftime("%b %d") for i in range(6, -1, -1)]

        # Daily join trend (all sources)
        daily_all = []
        for i in range(6, -1, -1):
            day = today - timedelta(days=i)
            nxt = day + timedelta(days=1)
            c = col("bot2_user_tracking").count_documents({"joined_at": {"$gte": day, "$lt": nxt}})
            daily_all.append(c)

        # Monthly cumulative
        month_start = today.replace(day=1)
        monthly_joined = col("bot2_user_tracking").count_documents({"joined_at": {"$gte": month_start}})

        top_sources = []
        for src in sources + ["UNKNOWN"]:
            query = {"source": src} if src != "UNKNOWN" else {"source": {"$nin": sources}}
            c = col("bot2_user_tracking").count_documents(query)
            top_sources.append({"source": src, "count": c})
        top_sources.sort(key=lambda x: x["count"], reverse=True)

        return {
            "labels": labels,
            "weekly_by_source": weekly,
            "daily_all": daily_all,
            "monthly_joined": monthly_joined,
            "top_sources": top_sources,
        }
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BROADCASTS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/broadcasts")
def get_broadcasts():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        docs = list(col("bot2_broadcasts").find({}, {"data": 0}).sort("index", -1).limit(30))
        result = []
        for d in docs:
            d["_id"] = str(d["_id"])
            if "created_at" in d and hasattr(d["created_at"], "strftime"):
                d["created_at"] = d["created_at"].strftime("%b %d, %Y %I:%M %p")
            result.append(d)
        total = col("bot2_broadcasts").count_documents({})
        return {"broadcasts": result, "total": total}
    except Exception as e:
        return {"error": str(e)}

from fastapi import BackgroundTasks
import httpx
import asyncio

async def _send_broadcast_task(broadcast_id: str, message_text: str, category: str):
    try:
        active_vault_docs = list(col("bot1_user_verification").find({"vault_joined": True}, {"user_id": 1}))
        active_vault_ids = {u["user_id"] for u in active_vault_docs}
        
        if category == "ALL":
            target_ids = list(active_vault_ids)
        else:
            tracking_docs = list(col("bot2_user_tracking").find({"source": category}, {"user_id": 1}))
            target_ids = [u["user_id"] for u in tracking_docs if u["user_id"] in active_vault_ids]
            
        success_count = 0
        failed_count = 0
        
        dt = datetime.utcnow().strftime("%b %d, %Y  ·  %I:%M %p")
        full_text = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  📢  MSA NODE  ·  BROADCAST\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{message_text}\n\n"
            "──────────────────────────────\n"
            "📢  MSA NODE  ·  Official\n"
            f"🕐  {dt}"
        )

        async with httpx.AsyncClient() as client:
            for uid in target_ids:
                try:
                    payload = {
                        "chat_id": uid,
                        "text": full_text,
                        "parse_mode": "HTML"
                    }
                    res = await client.post(TELEGRAM_API_URL, json=payload, timeout=10.0)
                    if res.status_code == 200:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
                await asyncio.sleep(0.05)
                
        col("bot2_broadcasts").update_one(
            {"broadcast_id": broadcast_id},
            {"$set": {"status": "sent", "sent_count": success_count, "failed_count": failed_count, "last_sent": datetime.utcnow()}}
        )
    except Exception as e:
        print(f"Broadcast error: {e}")

from pydantic import BaseModel
class BroadcastRequest(BaseModel):
    message: str
    category: str

class ShootRequest(BaseModel):
    user_id: int
    message: str

@app.post("/api/users/shoot")
async def shoot_user(req: ShootRequest):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        dt = datetime.utcnow().strftime("%b %d, %Y  ·  %I:%M %p")
        full_text = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  📸  MSA NODE  ·  DIRECT\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{req.message}\n\n"
            "──────────────────────────────\n"
            "📢  MSA NODE  ·  Official\n"
            f"🕐  {dt}"
        )
        async with httpx.AsyncClient() as client:
            payload = {"chat_id": req.user_id, "text": full_text, "parse_mode": "HTML"}
            res = await client.post(TELEGRAM_API_URL, json=payload, timeout=10.0)
            if res.status_code == 200:
                return {"success": True}
            else:
                return {"error": f"Telegram API error: {res.text}"}
    except Exception as e:
        return {"error": str(e)}

class BackupForceRequest(BaseModel):
    bot_name: str

@app.post("/api/backups/force")
def force_backup_api(req: BackupForceRequest):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        import backup_schedulers
        BACKUP_MONGO_URI = os.getenv("BACKUP_MONGO_URI")
        BACKUP_DB_NAME = os.getenv("BACKUP_MONGO_DB_NAME", "MSANodeBackups")
        res = backup_schedulers.force_backup_to_cluster(
            bot_name=req.bot_name,
            mongo_uri=MONGO_URI,
            db_name=MONGO_DB_NAME,
            backup_mongo_uri=BACKUP_MONGO_URI,
            backup_db_name=BACKUP_DB_NAME
        )
        return {"success": res.get("status") == "ok", "details": res}
    except Exception as e:
        return {"error": str(e)}

class AdminActionRequest(BaseModel):
    user_id: int
    action: str
    value: str = None

@app.post("/api/admins/action")
def admin_action(req: AdminActionRequest):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        uid = req.user_id
        admin_col = col("bot2_admins")
        
        if req.action == "add":
            if admin_col.find_one({"user_id": uid}):
                return {"error": "Admin already exists"}
            admin_col.insert_one({
                "user_id": uid,
                "role": "Admin",
                "permissions": [],
                "locked": False,
                "added_at": datetime.utcnow(),
                "added_by": "Web Dashboard"
            })
        elif req.action == "remove":
            admin_col.delete_one({"user_id": uid})
        elif req.action == "toggle_lock":
            doc = admin_col.find_one({"user_id": uid})
            if doc:
                admin_col.update_one({"user_id": uid}, {"$set": {"locked": not doc.get("locked", False)}})
        elif req.action == "set_role":
            admin_col.update_one({"user_id": uid}, {"$set": {"role": req.value}})
            
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/broadcasts/send")
def send_broadcast(req: BroadcastRequest, background_tasks: BackgroundTasks):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        total = col("bot2_broadcasts").count_documents({})
        next_idx = total + 1
        broadcast_id = f"brd_web_{next_idx}"
        
        broadcast_data = {
            "broadcast_id": broadcast_id,
            "index": next_idx,
            "category": req.category,
            "message_text": req.message,
            "message_type": "text",
            "created_by": "Web Dashboard",
            "created_at": datetime.utcnow(),
            "status": "sending",
            "sent_count": 0,
            "broadcast_type": "web"
        }
        col("bot2_broadcasts").insert_one(broadcast_data)
        
        background_tasks.add_task(_send_broadcast_task, broadcast_id, req.message, req.category)
        return {"success": True, "broadcast_id": broadcast_id}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — SUPPORT TICKETS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/tickets")
def get_tickets(status: str = "open", limit: int = 20):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        query = {} if status == "all" else {"status": status}
        docs  = list(col("bot1_support_tickets").find(query).sort("created_at", -1).limit(limit))
        result = []
        for d in docs:
            d["_id"] = str(d["_id"])
            for k in ("created_at", "resolved_at"):
                if k in d and hasattr(d[k], "strftime"):
                    d[k] = d[k].strftime("%b %d, %Y %I:%M %p")
            result.append(d)

        open_count     = col("bot1_support_tickets").count_documents({"status": "open"})
        resolved_count = col("bot1_support_tickets").count_documents({"status": "resolved"})
        return {"tickets": result, "open": open_count, "resolved": resolved_count}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — USER LOOKUP
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/user/{user_id}")
def lookup_user(user_id: int):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        verify = col("bot1_user_verification").find_one({"user_id": user_id}, {"_id": 0})
        track  = col("bot2_user_tracking").find_one({"user_id": user_id}, {"_id": 0})
        msa    = col("bot1_msa_ids").find_one({"user_id": user_id}, {"_id": 0})
        banned = col("bot1_banned_users").find_one({"user_id": user_id}, {"_id": 0})
        susp   = col("bot1_suspended_features").find_one({"user_id": user_id}, {"_id": 0})
        credits= col("bot1_msa_credits").find_one({"user_id": user_id}, {"_id": 0})
        refs   = col("bot1_referrals").count_documents({"referrer_id": user_id, "status": "confirmed"})
        tickets= col("bot1_support_tickets").count_documents({"user_id": user_id})

        def fmt(doc):
            if not doc: return doc
            for k, v in doc.items():
                if hasattr(v, "strftime"):
                    doc[k] = v.strftime("%b %d, %Y %I:%M %p")
            return doc

        return {
            "found": bool(verify or track),
            "verification": fmt(verify),
            "tracking": fmt(track),
            "msa_id": fmt(msa),
            "banned": fmt(banned),
            "suspended": fmt(susp),
            "credits": fmt(credits),
            "referrals_confirmed": refs,
            "tickets_count": tickets,
        }
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — LIVE TERMINAL LOGS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/logs")
def get_logs(bot: str = "bot2", limit: int = 50):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        docs = list(col("bot2_live_terminal_logs").find(
            {"bot": bot}, {"_id": 0}
        ).sort("created_at", -1).limit(limit))
        docs.reverse()
        result = []
        for d in docs:
            if "created_at" in d and hasattr(d["created_at"], "strftime"):
                d["created_at"] = d["created_at"].strftime("%b %d  %I:%M:%S %p")
            result.append(d)
        return {"logs": result}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — BOT 1 STATUS & SETTINGS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/bot1/status")
def get_bot1_status():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        doc = col("bot1_settings").find_one({"setting": "maintenance_mode"})
        is_maint = doc.get("value", False) if doc else False
        updated_at = doc.get("updated_at") if doc else None
        updated_str = updated_at.strftime("%b %d, %Y %I:%M %p") if updated_at and hasattr(updated_at, "strftime") else "Never"

        recent_events = list(col("bot1_offline_log").find({}, {"_id": 0}).sort("triggered_at", -1).limit(10))
        for e in recent_events:
            if "triggered_at" in e and hasattr(e["triggered_at"], "strftime"):
                e["triggered_at"] = e["triggered_at"].strftime("%b %d  %I:%M %p")

        return {
            "maintenance": is_maint,
            "status": "OFFLINE" if is_maint else "ONLINE",
            "last_changed": updated_str,
            "recent_events": recent_events,
        }
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — BACKUP STATUS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/backup/status")
def get_backup_status():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        history = list(col("bot2_backup_history").find({}, {"_id": 0}).sort("timestamp", -1).limit(10))
        for h in history:
            if "timestamp" in h and hasattr(h["timestamp"], "strftime"):
                h["timestamp"] = h["timestamp"].strftime("%b %d, %Y %I:%M %p")
        total_history = col("bot2_backup_history").count_documents({})
        return {"history": history, "total": total_history}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — STORAGE STATS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/storage")
def get_storage():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        stats      = db.command("dbStats")
        data_mb    = stats.get("dataSize", 0) / 1_048_576
        index_mb   = stats.get("indexSize", 0) / 1_048_576
        used_mb    = data_mb + index_mb
        cap_mb     = 512.0
        pct        = min(used_mb / cap_mb * 100, 100)
        col_count  = len(db.list_collection_names())
        return {
            "used_mb": round(used_mb, 2),
            "cap_mb": cap_mb,
            "pct": round(pct, 1),
            "data_mb": round(data_mb, 2),
            "index_mb": round(index_mb, 2),
            "collections": col_count,
        }
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# SECTION 10 — ADMINS
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/admins")
def get_admins():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        docs = list(col("bot2_admins").find({}, {"_id": 0}))
        for d in docs:
            if "added_at" in d and hasattr(d["added_at"], "strftime"):
                d["added_at"] = d["added_at"].strftime("%b %d, %Y")
        return {"admins": docs, "total": len(docs)}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# WEB CONTROLS (TICKETS & USERS)
# ═══════════════════════════════════════════════════════════════════
from pydantic import BaseModel
class TicketAction(BaseModel):
    ticket_id: str

@app.post("/api/tickets/resolve")
def resolve_ticket(action: TicketAction):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        res = col("bot1_support_tickets").update_one(
            {"_id": ObjectId(action.ticket_id)},
            {"$set": {"status": "resolved", "resolved_at": datetime.utcnow()}}
        )
        return {"success": res.modified_count > 0}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/tickets/delete")
def delete_ticket(action: TicketAction):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        res = col("bot1_support_tickets").delete_one({"_id": ObjectId(action.ticket_id)})
        return {"success": res.deleted_count > 0}
    except Exception as e:
        return {"error": str(e)}

class UserAction(BaseModel):
    user_id: int
    reason: str = "Banned via Web Dashboard"

@app.post("/api/users/ban")
def ban_user(action: UserAction):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        col("bot1_banned_users").update_one(
            {"user_id": action.user_id},
            {"$set": {"user_id": action.user_id, "reason": action.reason, "banned_at": datetime.utcnow()}},
            upsert=True
        )
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/users/unban")
def unban_user(action: UserAction):
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        res = col("bot1_banned_users").delete_one({"user_id": action.user_id})
        return {"success": res.deleted_count > 0}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/bot1/toggle")
def toggle_bot1():
    if not DB_ONLINE: return {"error": "Database Offline"}
    try:
        doc = col("bot1_settings").find_one({"setting": "maintenance_mode"})
        is_maint = doc.get("value", False) if doc else False
        new_state = not is_maint
        
        col("bot1_settings").update_one(
            {"setting": "maintenance_mode"},
            {"$set": {"value": new_state, "updated_at": datetime.utcnow(), "updated_by": "Web Dashboard"}},
            upsert=True
        )
        
        col("bot1_offline_log").insert_one({
            "direction": "OFF" if new_state else "ON",
            "message": "Silently toggled via Web Dashboard (No Broadcast)",
            "triggered_by": "Web Dashboard",
            "triggered_at": datetime.utcnow()
        })
        return {"success": True, "maintenance": new_state}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════
# DEEP LINK REDIRECTOR (MSA NODE Premium Branded Middleman)
# ═══════════════════════════════════════════════════════════════════
@app.get("/tg")
def tg_redirect(start: str = None):
    if not start:
        return HTMLResponse("<h1>Invalid Link</h1>")
        
    bot_username = os.getenv("BOT_USERNAME", "msanodebot")
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>MSA NODE | Get Your Link</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800;900&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg-main: #030305;
                --accent-1: #00F0FF;
                --accent-2: #0057FF;
                --glass-bg: rgba(15, 15, 20, 0.6);
                --glass-border: rgba(255, 255, 255, 0.08);
            }}
            
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            
            body {{
                font-family: 'Inter', sans-serif;
                background-color: var(--bg-main);
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                overflow: hidden;
                position: relative;
            }}

            /* Premium Background Effects */
            .bg-glow {{
                position: absolute;
                width: 600px;
                height: 600px;
                background: radial-gradient(circle, rgba(0,87,255,0.15) 0%, rgba(0,0,0,0) 70%);
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                z-index: 0;
                pointer-events: none;
            }}
            
            .grid-overlay {{
                position: absolute;
                top: 0; left: 0; right: 0; bottom: 0;
                background-image: 
                    linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px),
                    linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px);
                background-size: 30px 30px;
                z-index: 0;
                opacity: 0.5;
                pointer-events: none;
            }}

            /* Glassmorphism Card */
            .card {{
                position: relative;
                z-index: 10;
                background: var(--glass-bg);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid var(--glass-border);
                border-radius: 24px;
                padding: 50px 40px;
                width: 90%;
                max-width: 420px;
                text-align: center;
                box-shadow: 0 30px 60px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.1);
                animation: slideUp 0.8s cubic-bezier(0.16, 1, 0.3, 1) forwards;
                opacity: 0;
                transform: translateY(30px);
            }}

            @keyframes slideUp {{
                to {{ opacity: 1; transform: translateY(0); }}
            }}

            /* Brand Element */
            .brand-logo {{
                width: 64px;
                height: 64px;
                margin: 0 auto 20px auto;
                background: linear-gradient(135deg, var(--accent-1), var(--accent-2));
                border-radius: 16px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 0 30px rgba(0, 114, 255, 0.4);
            }}

            .brand-logo svg {{
                width: 32px;
                height: 32px;
                fill: white;
            }}

            .brand-text {{
                font-size: 26px;
                font-weight: 900;
                letter-spacing: 5px;
                margin-bottom: 8px;
                background: linear-gradient(to right, #fff, #a0a0b0);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-transform: uppercase;
            }}

            .subtitle {{
                font-size: 14px;
                color: #888899;
                font-weight: 600;
                letter-spacing: 1.5px;
                text-transform: uppercase;
                margin-bottom: 40px;
            }}

            /* Button Styling */
            .action-btn {{
                display: inline-block;
                width: 100%;
                background: linear-gradient(90deg, var(--accent-2) 0%, var(--accent-1) 100%);
                color: #fff;
                padding: 18px 20px;
                text-decoration: none;
                border-radius: 14px;
                font-weight: 800;
                font-size: 16px;
                letter-spacing: 1px;
                border: none;
                cursor: pointer;
                position: relative;
                overflow: hidden;
                transition: all 0.3s ease;
                box-shadow: 0 10px 25px rgba(0, 87, 255, 0.4);
            }}

            .action-btn::before {{
                content: '';
                position: absolute;
                top: 0; left: -100%;
                width: 100%; height: 100%;
                background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
                transition: all 0.5s ease;
            }}

            .action-btn:hover {{
                transform: translateY(-2px);
                box-shadow: 0 15px 35px rgba(0, 87, 255, 0.6);
            }}

            .action-btn:hover::before {{
                left: 100%;
            }}

            .action-btn:active {{
                transform: translateY(1px);
            }}

            /* Footer */
            .secure-badge {{
                margin-top: 30px;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                font-size: 12px;
                color: #555566;
                font-weight: 600;
                letter-spacing: 1px;
                text-transform: uppercase;
            }}
            
            .secure-badge svg {{
                width: 14px;
                height: 14px;
                fill: #00F0FF;
                opacity: 0.7;
            }}

        </style>
    </head>
    <body>
        <div class="bg-glow"></div>
        <div class="grid-overlay"></div>
        
        <div class="card">
            <div class="brand-logo">
                <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path d="M12 2L3 6v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V6l-9-4zm0 10.99h7c-.53 4.12-3.28 7.79-7 8.94V12H5V8.1l7-3.11v7z"/>
                </svg>
            </div>
            
            <div class="brand-text">MSA NODE</div>
            <div class="subtitle">Your Links Are Ready</div>
            
            <p style="color: #bbbbc5; font-size: 15px; margin-bottom: 35px; line-height: 1.6;">
                Click the button below to get your links directly in the Telegram app.
            </p>
            
            <a href="tg://resolve?domain={bot_username}&start={start}" class="action-btn">
                GET YOUR LINKS
            </a>
            
            <div class="secure-badge">
                <svg viewBox="0 0 24 24"><path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zM9 6c0-1.66 1.34-3 3-3s3 1.34 3 3v2H9V6zm9 14H6V10h12v10zm-6-3c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2z"/></svg>
                Secure Connection
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ═══════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/health")
def health():
    return {"status": "ok", "db": DB_ONLINE, "time": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard_api:app", host="0.0.0.0", port=3002, reload=True)
