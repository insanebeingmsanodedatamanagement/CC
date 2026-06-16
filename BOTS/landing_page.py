import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/tg", response_class=HTMLResponse)
@router.get("/ig", response_class=HTMLResponse)
@router.get("/igc", response_class=HTMLResponse)
@router.get("/igcc", response_class=HTMLResponse)
@router.get("/yt", response_class=HTMLResponse)
@router.get("/ytcode", response_class=HTMLResponse)
@router.get("/start", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def tg_redirect(request: Request, start: str = ""):
    if not start:
        return HTMLResponse("<h1>Invalid Link</h1>")
        
    bot_username = os.getenv("BOT_USERNAME", "msanodebot")
    
    # Safely get db from app state to avoid multiple connections or circular imports
    # If run in standalone mode, these might not exist on the state
    db = getattr(request.app.state, 'db', None)
    DB_ONLINE = getattr(request.app.state, 'DB_ONLINE', False)
    
    try:
        live_count = db.users.count_documents({}) if DB_ONLINE else 0
        if live_count > 0:
            formatted_count = f"{live_count:,}"
        else:
            formatted_count = "18,400+"
    except:
        formatted_count = "18,400+"

    # Fetch Real Reviews Data
    try:
        pipeline = [{"$group": {"_id": None, "avg_stars": {"$avg": "$stars"}}}]
        aggr = list(db.bot1_reviews.aggregate(pipeline)) if DB_ONLINE else []
        rating_score = round(aggr[0]["avg_stars"], 1) if aggr and aggr[0]["avg_stars"] else 4.9
        formatted_rating = f"{rating_score:.1f}"
        
        real_reviews = db.bot1_reviews.count_documents({}) if DB_ONLINE else 0
        ratings_count = f"{real_reviews:,}" if real_reviews > 0 else "12,854"
    except:
        formatted_rating = "4.9"
        ratings_count = "12,854"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>MSA NODE | Get Your Link</title>
        <link rel="icon" href="/static/msa_logo.png" type="image/png">
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800;900&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg-main: #13151a;
                --text-main: #ffffff;
                --text-muted: #8e95a5;
                --accent: #00e5ff;
                --shadow-dark: #0a0b0d;
                --shadow-light: #1c1f27;
                --grid-color: rgba(255, 255, 255, 0.03);
            }}
            
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            
            body {{
                background-color: var(--bg-main);
                color: var(--text-main);
                font-family: 'Inter', sans-serif;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                overflow: hidden;
            }}

            .grid-overlay {{
                position: absolute;
                top: 0; left: 0; right: 0; bottom: 0;
                background-image: 
                    linear-gradient(var(--grid-color) 1px, transparent 1px),
                    linear-gradient(90deg, var(--grid-color) 1px, transparent 1px);
                background-size: 35px 35px;
                z-index: 0;
                pointer-events: none;
                mask-image: radial-gradient(circle at center, black 40%, transparent 90%);
                -webkit-mask-image: radial-gradient(circle at center, black 40%, transparent 90%);
            }}

            .ambient-light-1 {{
                position: absolute;
                top: -10%; left: -10%;
                width: 600px; height: 600px;
                background: radial-gradient(circle, rgba(0,229,255,0.08) 0%, rgba(0,0,0,0) 60%);
                z-index: 0;
                pointer-events: none;
            }}
            .ambient-light-2 {{
                position: absolute;
                bottom: -10%; right: -10%;
                width: 600px; height: 600px;
                background: radial-gradient(circle, rgba(138,43,226,0.08) 0%, rgba(0,0,0,0) 60%);
                z-index: 0;
                pointer-events: none;
            }}

            /* 3D Levitating Card */
            .card {{
                position: relative;
                z-index: 10;
                background: linear-gradient(145deg, #171a21, #101217);
                border-radius: 36px;
                padding: 50px 40px;
                width: 90%;
                max-width: 420px;
                text-align: center;
                /* Realistic Dark Mode Card Shadows */
                box-shadow:  20px 20px 60px rgba(0,0,0,0.8),
                            -20px -20px 60px rgba(255,255,255,0.02),
                            inset 1px 1px 1px rgba(255,255,255,0.06),
                            inset -1px -1px 2px rgba(0,0,0,0.5);
                border: 1px solid rgba(255,255,255,0.02);
                animation: slideUp 0.8s cubic-bezier(0.16, 1, 0.3, 1) forwards;
                opacity: 0;
                transform: translateY(30px);
                transition: transform 0.5s cubic-bezier(0.175, 0.885, 0.32, 1.275), box-shadow 0.5s ease;
            }}

            .card:hover {{
                transform: translateY(-12px);
                box-shadow:  30px 30px 80px rgba(0,0,0,0.9),
                            -30px -30px 80px rgba(255,255,255,0.03),
                            inset 1px 1px 2px rgba(255,255,255,0.08),
                            inset -1px -1px 3px rgba(0,0,0,0.6);
            }}

            @keyframes slideUp {{
                to {{ opacity: 1; transform: translateY(0); }}
            }}

            /* 3D Frame Logo */
            .brand-logo {{
                width: 130px;
                height: 130px;
                margin: 0 auto 30px auto;
                border-radius: 28px;
                display: flex;
                align-items: center;
                justify-content: center;
                overflow: hidden;
                background: #000;
                border: 3px solid var(--accent);
                /* Realistic Dark Mode Logo Shadows */
                box-shadow: 10px 10px 20px rgba(0,0,0,0.6), 
                           -10px -10px 20px rgba(255,255,255,0.02),
                           0 0 35px rgba(0, 229, 255, 0.3),
                           inset 0 0 15px rgba(0, 229, 255, 0.4);
                transition: transform 0.4s ease, box-shadow 0.4s ease;
            }}

            .brand-logo:hover {{
                transform: scale(1.05) translateZ(0);
                box-shadow: 15px 15px 30px var(--shadow-dark), 
                           -15px -15px 30px var(--shadow-light),
                           0 0 60px rgba(0, 229, 255, 0.6),
                           inset 0 0 20px rgba(0, 229, 255, 0.6);
            }}

            .brand-logo img {{
                width: 100%;
                height: 100%;
                object-fit: cover;
            }}

            .brand-text {{
                font-size: 22px;
                font-weight: 900;
                letter-spacing: 2px;
                margin-bottom: 8px;
                background: linear-gradient(to right, #ffffff, #888888);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-transform: uppercase;
                text-align: center;
                width: 100%;
                text-shadow: none;
            }}

            .subtitle {{
                font-size: 14px;
                color: var(--text-muted);
                font-weight: 600;
                letter-spacing: 1.5px;
                text-transform: uppercase;
                margin-bottom: 40px;
            }}

            .desc-text {{ color: #bbbbc5; font-size: 15px; margin-bottom: 35px; line-height: 1.6; }}
            .rating-text {{ font-size: 0.85rem; color: #fbbf24; font-weight: 600; margin-bottom: 2px; }}
            .reviews-text {{ color: rgba(255,255,255,0.4); font-weight: 400; }}
            .join-text {{ font-size: 0.85rem; color: rgba(255,255,255,0.6); font-weight: 500; display: flex; align-items: center; gap: 8px; justify-content: center; }}
            
            .secure-badge {{
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                font-size: 0.65rem;
                color: rgba(255, 255, 255, 0.3);
                margin-top: 18px;
                font-weight: 600;
                letter-spacing: 0.8px;
                text-transform: uppercase;
            }}
            .secure-badge svg {{
                width: 10px;
                height: 10px;
                fill: #10b981;
            }}

            /* 3D Levitating Button */
            .action-btn {{
                display: inline-block;
                width: 100%;
                background: linear-gradient(145deg, #181b22, #111317);
                color: var(--accent);
                padding: 18px 20px;
                text-decoration: none;
                border-radius: 18px;
                font-weight: 800;
                font-size: 16px;
                letter-spacing: 1px;
                border: 1px solid rgba(255,255,255,0.03);
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);
                position: relative;
                overflow: hidden;
                /* Realistic Dark Mode Button Shadows */
                box-shadow: 8px 8px 15px rgba(0,0,0,0.6), 
                           -8px -8px 15px rgba(255,255,255,0.02),
                           inset 1px 1px 1px rgba(255,255,255,0.08),
                           inset -1px -1px 2px rgba(0,0,0,0.5);
                text-shadow: 0 0 10px rgba(0, 229, 255, 0.2);
            }}

            /* Infinite Glossy Sweep */
            .action-btn::after {{
                content: '';
                position: absolute;
                top: 0; left: -100%;
                width: 50%; height: 100%;
                background: linear-gradient(90deg, transparent, rgba(255,255,255,0.15), transparent);
                transform: skewX(-20deg);
                animation: sweep 4s infinite;
            }}

            @keyframes sweep {{
                0% {{ left: -100%; }}
                20% {{ left: 200%; }}
                100% {{ left: 200%; }}
            }}

            .action-btn:hover {{
                transform: translateY(-5px);
                color: #fff;
                box-shadow: 12px 12px 25px rgba(0,0,0,0.7), 
                           -12px -12px 25px rgba(255,255,255,0.03),
                           0 10px 25px rgba(0, 229, 255, 0.2),
                           inset 1px 1px 2px rgba(255,255,255,0.1);
                text-shadow: 0 0 15px rgba(0, 229, 255, 0.6);
            }}

            .action-btn:active {{
                transform: translateY(2px) scale(0.97);
                box-shadow: inset 8px 8px 16px rgba(0,0,0,0.8), 
                            inset -8px -8px 16px rgba(255,255,255,0.02);
                color: var(--accent);
                text-shadow: none;
            }}

            /* Premium Stars & Pulse */
            .stars-container {{
                display: flex;
                gap: 4px;
                color: #fbbf24;
                margin-bottom: 4px;
            }}
            .stars-container svg {{
                width: 20px;
                height: 20px;
                fill: currentColor;
                filter: drop-shadow(0 0 4px rgba(251, 191, 36, 0.4));
            }}
            .pulse-dot {{
                width: 8px;
                height: 8px;
                background-color: #10b981;
                border-radius: 50%;
                box-shadow: 0 0 8px #10b981;
                animation: pulse 2s infinite;
            }}
            @keyframes pulse {{
                0% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                70% {{ box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }}
                100% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
            }}
            
            @media (max-width: 480px) {{
                .card {{ padding: 40px 25px; }}
            }}

            /* Premium Light Mode Class (Ultra-Realistic 3D Neumorphism) */
            body.light-mode {{
                --bg-main: #ebf0f5;
                --text-main: #333944;
                --text-muted: #7a8291;
                --accent: #0066ff;
                --grid-color: rgba(0, 0, 0, 0.04);
            }}
            /* Apple Siri Style Background Glows for Light Mode */
            body.light-mode .ambient-light-1 {{
                display: block;
                background: radial-gradient(circle, rgba(0, 102, 255, 0.08) 0%, rgba(0,0,0,0) 60%);
            }}
            body.light-mode .ambient-light-2 {{
                display: block;
                background: radial-gradient(circle, rgba(138, 43, 226, 0.06) 0%, rgba(0,0,0,0) 60%);
            }}
            
            body.light-mode .card {{ 
                background: linear-gradient(145deg, #f5f8fa, #e1e7ed); 
                border: none;
                /* Realistic Light Mode Card Shadows */
                box-shadow: 20px 20px 60px rgba(163, 177, 198, 0.6), 
                           -20px -20px 60px rgba(255, 255, 255, 0.8),
                           inset 1px 1px 2px rgba(255, 255, 255, 1),
                           inset -1px -1px 2px rgba(163, 177, 198, 0.2);
            }}
            
            body.light-mode .brand-logo {{
                background: #000;
                border: 3px solid var(--accent);
                box-shadow: 8px 8px 16px rgba(163, 177, 198, 0.6), 
                           -8px -8px 16px rgba(255, 255, 255, 0.8),
                           0 0 35px rgba(0, 102, 255, 0.5),
                           inset 0 0 15px rgba(0, 102, 255, 0.6);
            }}
            
            body.light-mode .brand-text {{
                background: linear-gradient(to right, #111827, #4b5563);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: none;
            }}
            
            body.light-mode .desc-text {{ color: var(--text-muted); }}
            body.light-mode .rating-text {{ color: #d97706; text-shadow: none; }}
            body.light-mode .reviews-text {{ color: #9ca3af; }}
            body.light-mode .join-text {{ color: var(--text-muted); }}
            body.light-mode .secure-badge {{ color: #9ca3af; }}
            body.light-mode .secure-badge svg {{ fill: #059669; }}
            
            body.light-mode .action-btn {{
                background: linear-gradient(145deg, #f5f8fa, #e1e7ed);
                color: var(--accent);
                border: none;
                /* Realistic Light Mode Button Shadows */
                box-shadow: 8px 8px 15px rgba(163, 177, 198, 0.6), 
                           -8px -8px 15px rgba(255, 255, 255, 0.8),
                           inset 1px 1px 2px rgba(255, 255, 255, 1),
                           inset -1px -1px 2px rgba(163, 177, 198, 0.2);
                text-shadow: none;
            }}
            
            body.light-mode .action-btn:hover {{
                transform: translateY(-4px);
                box-shadow: 12px 12px 20px rgba(163, 177, 198, 0.7), 
                           -12px -12px 20px rgba(255, 255, 255, 0.9),
                           inset 1px 1px 2px rgba(255, 255, 255, 1);
                color: var(--accent);
            }}

            body.light-mode .action-btn:active {{
                transform: translateY(2px);
                box-shadow: inset 8px 8px 15px rgba(163, 177, 198, 0.6), 
                            inset -8px -8px 15px rgba(255, 255, 255, 0.8);
            }}
            
            body.light-mode .action-btn::after {{
                background: linear-gradient(90deg, transparent, rgba(255,255,255,0.8), transparent);
            }}

            body.light-mode #theme-toggle {{
                color: var(--text-muted);
                border: 1px solid rgba(163, 177, 198, 0.6);
                background: var(--bg-main);
                box-shadow: 4px 4px 8px rgba(163, 177, 198, 0.6), -4px -4px 8px rgba(255, 255, 255, 0.8);
            }}
            body.light-mode #theme-toggle:active {{
                box-shadow: inset 2px 2px 4px rgba(163, 177, 198, 0.6), inset -2px -2px 4px rgba(255, 255, 255, 0.8);
            }}
        </style>
    </head>
    <body>
        <button id="theme-toggle" style="position: absolute; top: 20px; right: 20px; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.2); color: var(--text-muted); padding: 8px 12px; border-radius: 8px; cursor: pointer; z-index: 100; font-family: 'Inter', sans-serif; font-weight: 600; transition: all 0.3s ease;">
            Toggle Theme
        </button>
        <div class="ambient-light-1"></div>
        <div class="ambient-light-2"></div>
        <div class="grid-overlay"></div>
        
        <div class="card">
            <div class="brand-logo">
                <img src="/static/msa_logo.png" alt="MSA NODE">
            </div>
            
            <div class="brand-text">MSA NODE AGENT V2.0</div>
            <div class="subtitle">Your Content Is Ready</div>
            
            <p class="desc-text">
                Click the button below to securely unlock your content directly in the Telegram Vault.
            </p>
            
            <a href="tg://resolve?domain={bot_username}&start={start}" class="action-btn">
                GET YOUR CONTENT
            </a>

            <div style="margin-top: 35px; display: flex; flex-direction: column; align-items: center; gap: 8px;">
                <div class="stars-container">
                    <svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
                    <svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
                    <svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
                    <svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
                    <svg viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>
                </div>
                <div class="rating-text">
                    {formatted_rating} / 5.0 Rating <span class="reviews-text">({ratings_count} Reviews)</span>
                </div>
                <div class="join-text">
                    <div class="pulse-dot"></div>
                    Join {formatted_count} Vault Members
                </div>
                <div class="secure-badge">
                    <svg viewBox="0 0 24 24"><path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zM9 6c0-1.66 1.34-3 3-3s3 1.34 3 3v2H9V6zm9 14H6V10h12v10zm-6-3c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2z"/></svg>
                    Secure Connection
                </div>
            </div>
        </div>
        
        <script>
            document.getElementById('theme-toggle').addEventListener('click', function() {{
                document.body.classList.toggle('light-mode');
            }});
        </script>
    </body>
    </html>
    """
    
    # -------------------------------------------------------------
    # EXTREME ANTI-HACKER SECURITY HEADERS (Bank-Grade)
    # -------------------------------------------------------------
    headers = {
        "Content-Security-Policy": "default-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://fonts.gstatic.com; img-src 'self' data:;",
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Referrer-Policy": "no-referrer-when-downgrade"
    }
    
    return HTMLResponse(content=html_content, headers=headers)

# STANDALONE TEST MODE
if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles
    
    app = FastAPI(title="MSA NODE — Landing Page Standalone")
    
    if os.path.exists("dashboard"):
        app.mount("/static", StaticFiles(directory="dashboard"), name="static")
        
    app.include_router(router)
    
    print("🚀 Starting Standalone Landing Page Server...")
    uvicorn.run(app, host="0.0.0.0", port=3003)
