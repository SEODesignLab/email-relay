import os
import sys
import time
import json
import sqlite3
import logging
import threading
import datetime
import re
import requests as http_requests
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, redirect, Response, g
from flask_cors import CORS
from functools import wraps
from collections import deque
import uuid
from datetime import datetime as dt
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load .env for local dev
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

# --- Shared Config ---
API_KEY = os.environ.get("API_KEY")  # Email relay key
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
FROM_ADDRESS = os.environ.get("FROM_ADDRESS", "milo@seodesignlab.com")
TRACKER_KEY = os.environ.get("TRACKER_KEY", "sdl-email-2026")

# Prospector config
PROSPECTOR_KEY = os.environ.get("PROSPECTOR_KEY", "sdl-prospector-2026")
DATAFORSEO_LOGIN = os.environ.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
POP_API_KEY = os.environ.get("POP_API_KEY", "ADD_ON_0cee5c62d39a7736")
POP_BASE = "https://app.pageoptimizer.pro/api"

# DB paths - use /data on Render (persistent disk), else local
DB_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
TRACKING_DB_PATH = os.environ.get("DB_PATH", os.path.join(DB_DIR, "tracking.db"))
PROSPECTS_DB_PATH = os.path.join(DB_DIR, "prospects.db")


# ============================================================
# EMAIL TRACKING DB
# ============================================================

def get_tracking_db():
    db = sqlite3.connect(TRACKING_DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute('''CREATE TABLE IF NOT EXISTS emails (
        id TEXT PRIMARY KEY,
        subject TEXT,
        recipient TEXT,
        recipient_name TEXT,
        client TEXT,
        sent_at TEXT,
        resend_id TEXT
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email_id TEXT,
        event_type TEXT,
        url TEXT,
        ip TEXT,
        user_agent TEXT,
        timestamp TEXT
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_events_email ON events(email_id)')
    db.commit()
    return db


# ============================================================
# PROSPECTS DB
# ============================================================

def get_prospects_db():
    if "prospects_db" not in g:
        g.prospects_db = sqlite3.connect(PROSPECTS_DB_PATH)
        g.prospects_db.row_factory = sqlite3.Row
    return g.prospects_db

@app.teardown_appcontext
def close_prospects_db(exc):
    db = g.pop("prospects_db", None)
    if db:
        db.close()

def init_prospects_db():
    conn = sqlite3.connect(PROSPECTS_DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS prospects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        business_name TEXT,
        website TEXT UNIQUE,
        phone TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        niche TEXT,
        rating REAL,
        reviews INTEGER,
        seo_score INTEGER DEFAULT 0,
        prospect_score INTEGER DEFAULT 0,
        prospect_status TEXT DEFAULT 'new',
        issues TEXT,
        has_ssl INTEGER DEFAULT 0,
        pitch_subject TEXT,
        pitch_body TEXT,
        pitch_date TEXT,
        sent_date TEXT,
        contact_method TEXT,
        response_date TEXT,
        pop_report_data TEXT,
        pop_audit_date TEXT,
        pop_score INTEGER,
        pop_word_count_current INTEGER DEFAULT 0,
        pop_word_count_target INTEGER DEFAULT 0,
        search_query TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT,
        niche TEXT,
        location TEXT,
        result_count INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    conn.close()

init_prospects_db()


# ============================================================
# SHARED HELPERS
# ============================================================

# 1x1 transparent GIF
PIXEL_GIF = bytes([
    0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,
    0x80,0x00,0x00,0xff,0xff,0xff,0x00,0x00,0x00,0x21,
    0xf9,0x04,0x01,0x00,0x00,0x00,0x00,0x2c,0x00,0x00,
    0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,0x44,
    0x01,0x00,0x3b
])

# Rate limiting - 10 emails per minute
send_times = deque()
RATE_LIMIT = 10
RATE_WINDOW = 60

def now_str():
    return dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def require_api_key(f):
    """Auth for email relay endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if not key or key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def require_prospector_key(f):
    """Auth for prospector endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = (request.args.get("key")
               or request.form.get("key")
               or (request.json.get("key", "") if request.is_json else "")
               or request.headers.get("X-API-Key", ""))
        if key != PROSPECTOR_KEY:
            return jsonify({"error": "Invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated


def check_rate_limit():
    now = time.time()
    while send_times and send_times[0] < now - RATE_WINDOW:
        send_times.popleft()
    if len(send_times) >= RATE_LIMIT:
        return False
    send_times.append(now)
    return True


# ============================================================
# EMAIL RELAY ENDPOINTS
# ============================================================

@app.route("/send", methods=["POST"])
@require_api_key
def send_email():
    if not check_rate_limit():
        return jsonify({"error": "Rate limit exceeded (10/min)"}), 429

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    to = data.get("to")
    subject = data.get("subject", "")
    body = data.get("body", "")
    html = data.get("html", "")
    cc = data.get("cc", "")
    bcc = data.get("bcc", "")
    from_addr = data.get("from", FROM_ADDRESS)

    if not to:
        return jsonify({"error": "'to' field is required"}), 400
    if not body and not html:
        return jsonify({"error": "'body' or 'html' required"}), 400

    try:
        payload = {
            "from": from_addr,
            "to": [a.strip() for a in to.split(",")] if isinstance(to, str) else to,
            "subject": subject,
        }

        if html:
            payload["html"] = html
        if body:
            payload["text"] = body
        if cc:
            payload["cc"] = [a.strip() for a in cc.split(",")] if isinstance(cc, str) else cc
        if bcc:
            payload["bcc"] = [a.strip() for a in bcc.split(",")] if isinstance(bcc, str) else bcc

        resp = http_requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )

        if resp.status_code in (200, 201):
            result = resp.json()
            resend_id = result.get("id", "")
            app.logger.info(f"Email sent to={to} subject={subject} id={resend_id}")

            tracking_id = data.get("tracking_id")
            if tracking_id:
                try:
                    db = get_tracking_db()
                    db.execute(
                        "INSERT OR REPLACE INTO emails (id, subject, recipient, recipient_name, client, sent_at, resend_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (tracking_id, subject, to if isinstance(to, str) else ",".join(to), data.get("recipient_name", ""), data.get("client", ""), dt.utcnow().isoformat(), resend_id)
                    )
                    db.commit()
                    db.close()
                except Exception as e:
                    app.logger.error(f"Auto-register tracking error: {e}")

            return jsonify({"success": True, "message": f"Email sent to {to}", "id": resend_id, "tracking_id": tracking_id})
        else:
            error = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"message": resp.text}
            app.logger.error(f"Resend error: {resp.status_code} {error}")
            return jsonify({"error": error.get("message", str(error)), "status_code": resp.status_code}), 502

    except Exception as e:
        app.logger.error(f"Send failed: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================
# EMAIL TRACKING ENDPOINTS
# ============================================================

@app.route("/t/open", methods=["GET"])
def track_open():
    email_id = request.args.get("id", "")
    if email_id:
        try:
            db = get_tracking_db()
            db.execute(
                "INSERT INTO events (email_id, event_type, ip, user_agent, timestamp) VALUES (?, ?, ?, ?, ?)",
                (email_id, "open", request.remote_addr or "", request.headers.get("User-Agent", ""), dt.utcnow().isoformat())
            )
            db.commit()
            db.close()
        except Exception as e:
            app.logger.error(f"Track open error: {e}")
    return Response(PIXEL_GIF, mimetype="image/gif", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.route("/t/click", methods=["GET"])
def track_click():
    email_id = request.args.get("id", "")
    url = request.args.get("url", "")
    if not url:
        return "Missing url", 400
    if email_id:
        try:
            db = get_tracking_db()
            db.execute(
                "INSERT INTO events (email_id, event_type, url, ip, user_agent, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (email_id, "click", url, request.remote_addr or "", request.headers.get("User-Agent", ""), dt.utcnow().isoformat())
            )
            db.commit()
            db.close()
        except Exception as e:
            app.logger.error(f"Track click error: {e}")
    return redirect(url)


@app.route("/t/register", methods=["POST"])
@require_api_key
def register_email():
    data = request.get_json()
    if not data or not data.get("id"):
        return jsonify({"error": "Missing email id"}), 400
    try:
        db = get_tracking_db()
        db.execute(
            "INSERT OR REPLACE INTO emails (id, subject, recipient, recipient_name, client, sent_at, resend_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (data["id"], data.get("subject", ""), data.get("recipient", ""), data.get("recipient_name", ""), data.get("client", ""), data.get("sent_at", dt.utcnow().isoformat()), data.get("resend_id", ""))
        )
        db.commit()
        db.close()
        return jsonify({"ok": True, "id": data["id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/t/analytics", methods=["GET"])
@require_api_key
def analytics():
    try:
        db = get_tracking_db()
        rows = db.execute('''
            SELECT e.*,
                (SELECT COUNT(*) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as opens,
                (SELECT COUNT(DISTINCT ip) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as unique_opens,
                (SELECT COUNT(*) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'click') as clicks,
                (SELECT MIN(timestamp) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as first_open,
                (SELECT MAX(timestamp) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as last_open
            FROM emails e ORDER BY e.sent_at DESC
        ''').fetchall()
        emails = [dict(row) for row in rows]

        total_sent = db.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        total_opened = db.execute("SELECT COUNT(DISTINCT email_id) FROM events WHERE event_type = 'open'").fetchone()[0]
        total_clicks = db.execute("SELECT COUNT(*) FROM events WHERE event_type = 'click'").fetchone()[0]
        db.close()

        return jsonify({
            "total_sent": total_sent,
            "total_opened": total_opened,
            "open_rate": round((total_opened / total_sent) * 100, 1) if total_sent > 0 else 0,
            "total_clicks": total_clicks,
            "emails": emails
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/t/email/<email_id>", methods=["GET"])
@require_api_key
def email_detail(email_id):
    try:
        db = get_tracking_db()
        email = db.execute("SELECT * FROM emails WHERE id = ?", (email_id,)).fetchone()
        events = db.execute("SELECT * FROM events WHERE email_id = ? ORDER BY timestamp DESC", (email_id,)).fetchall()
        db.close()
        return jsonify({
            "email": dict(email) if email else None,
            "events": [dict(e) for e in events]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# SMART PROSPECTOR ENDPOINTS
# ============================================================

@app.route("/api/search")
@require_prospector_key
def prospect_search():
    niche = request.args.get("niche", "")
    location = request.args.get("location", "")
    limit = int(request.args.get("limit", "20"))
    if not niche or not location:
        return jsonify({"error": "niche and location required"}), 400

    keyword = f"{niche} in {location}"
    payload = [{"keyword": keyword, "language_code": "en", "location_name": "United States", "depth": limit}]

    try:
        resp = http_requests.post(
            "https://api.dataforseo.com/v3/serp/google/maps/live/advanced",
            json=payload,
            auth=(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD),
            timeout=60
        )
        data = resp.json()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    tasks = data.get("tasks", [])
    if not tasks or not tasks[0].get("result"):
        return jsonify({"success": True, "query": keyword, "count": 0, "prospects": [], "raw_status": data.get("status_message", "no results")})

    items = tasks[0]["result"][0].get("items", [])
    db = get_prospects_db()
    prospects = []

    for item in items:
        if item.get("type") != "maps_search":
            continue
        website = item.get("url") or item.get("domain")
        if not website:
            continue
        website = website.rstrip("/")
        biz = item.get("title", "")
        phone = item.get("phone", "")
        address = item.get("address", "")
        rating = item.get("rating", {}).get("value", 0) if isinstance(item.get("rating"), dict) else item.get("rating", 0)
        reviews = item.get("rating", {}).get("votes_count", 0) if isinstance(item.get("rating"), dict) else item.get("reviews_count", 0)

        city = location.split(",")[0].strip() if "," in location else location
        state = location.split(",")[1].strip() if "," in location else ""

        try:
            db.execute("""INSERT OR IGNORE INTO prospects 
                (business_name, website, phone, address, city, state, niche, rating, reviews, search_query, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (biz, website, phone, address, city, state, niche, rating, reviews, keyword, now_str(), now_str()))
        except Exception:
            pass

        row = db.execute("SELECT * FROM prospects WHERE website = ?", (website,)).fetchone()
        if row:
            prospects.append(row_to_dict(row))

    db.commit()
    db.execute("INSERT INTO searches (query, niche, location, result_count, created_at) VALUES (?, ?, ?, ?, ?)",
               (keyword, niche, location, len(prospects), now_str()))
    db.commit()

    return jsonify({"success": True, "query": keyword, "count": len(prospects), "prospects": prospects})


@app.route("/api/analyze")
@require_prospector_key
def prospect_analyze():
    pid = request.args.get("prospect_id")
    if not pid:
        return jsonify({"error": "prospect_id required"}), 400

    db = get_prospects_db()
    prospect = row_to_dict(db.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone())
    if not prospect:
        return jsonify({"error": "Prospect not found"}), 404

    url = prospect["website"]
    if not url.startswith("http"):
        url = "https://" + url

    issues = []
    has_ssl = False
    seo_score = 100

    try:
        resp = http_requests.get(url, timeout=15, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        final_url = resp.url
        has_ssl = final_url.startswith("https://")
        if not has_ssl:
            issues.append("No SSL/HTTPS")
            seo_score -= 15

        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.find("title")
        if not title or not title.string or len(title.string.strip()) < 5:
            issues.append("Missing or poor title tag")
            seo_score -= 15

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if not meta_desc or not meta_desc.get("content"):
            issues.append("Missing meta description")
            seo_score -= 15

        h1 = soup.find("h1")
        if not h1:
            issues.append("No H1 tag")
            seo_score -= 10

        viewport = soup.find("meta", attrs={"name": "viewport"})
        if not viewport:
            issues.append("No viewport meta (not mobile-friendly)")
            seo_score -= 10

        canonical = soup.find("link", attrs={"rel": "canonical"})
        if not canonical:
            issues.append("No canonical tag")
            seo_score -= 5

        text_len = len(soup.get_text())
        if text_len < 1000:
            issues.append("Very thin content")
            seo_score -= 15
        elif text_len < 3000:
            issues.append("Thin content")
            seo_score -= 10

    except Exception as e:
        issues.append(f"Could not fetch site: {str(e)[:100]}")
        seo_score = 10

    seo_score = max(0, min(100, seo_score))
    prospect_score = max(0, 100 - seo_score)
    if prospect_score >= 70:
        status = "hot"
    elif prospect_score >= 40:
        status = "warm"
    else:
        status = "cold"

    db.execute("""UPDATE prospects SET seo_score=?, prospect_score=?, prospect_status=?, 
        issues=?, has_ssl=?, updated_at=? WHERE id=?""",
        (seo_score, prospect_score, status, json.dumps(issues), int(has_ssl), now_str(), pid))
    db.commit()

    return jsonify({
        "url": url, "seo_score": seo_score, "prospect_score": prospect_score,
        "prospect_status": status, "issues": issues, "has_ssl": has_ssl
    })


# --- Async POP Audit Job System ---
pop_jobs = {}  # job_id -> {"status": "running"|"complete"|"error", "result": {...}, "started": timestamp, "progress": str}

def _poll_pop_task(task_id, step_name="task", max_attempts=240, poll_interval=3):
    """
    Poll POP API task until complete.
    Returns the final result or raises exception on timeout/failure.
    
    POP API status values:
    - "PROGRESS" = still running
    - "SUCCESS" = complete
    - "FAILURE" = failed
    """
    for attempt in range(max_attempts):
        try:
            r = http_requests.get(f"{POP_BASE}/task/{task_id}/results/", timeout=30)
            r.raise_for_status()
            data = r.json()
            
            status = data.get("status", "")
            value = data.get("value", 0)
            msg = data.get("msg", "")
            
            app.logger.info(f"POP {step_name} poll (attempt {attempt+1}): status={status}, value={value}, msg={msg}")
            
            if status == "SUCCESS":
                return data
            # value==100 with PROGRESS means "calculating done" but data not ready yet
            # Only return if we actually have useful data
            if value == 100 and (data.get("prepareId") or data.get("data", {}).get("prepareId") or data.get("report")):
                return data
            elif status == "FAILURE":
                raise Exception(f"POP {step_name} failed: {msg}")
            elif status == "PROGRESS":
                time.sleep(poll_interval)
                continue
            else:
                # Unknown status, check if we have data anyway
                if data.get("prepareId") or data.get("report") or data.get("data"):
                    return data
                time.sleep(poll_interval)
                
        except http_requests.exceptions.RequestException as e:
            app.logger.warning(f"POP {step_name} poll error: {e}, retrying...")
            time.sleep(poll_interval)
    
    raise TimeoutError(f"POP {step_name} timed out after {max_attempts * poll_interval} seconds")


def _run_pop_audit_job(job_id, pid):
    """Background worker for POP audit - runs full 3-step flow"""
    app.logger.info(f"Starting POP audit job {job_id} for prospect {pid}")
    
    try:
        # Update job status with progress
        pop_jobs[job_id]["progress"] = "Fetching prospect data..."
        
        db = sqlite3.connect(PROSPECTS_DB_PATH)
        db.row_factory = sqlite3.Row
        prospect = dict(db.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone())
        db.close()

        url = prospect["website"]
        if not url.startswith("http"):
            url = "https://" + url

        keyword = f"{prospect.get('niche', '')} {prospect.get('city', '')}".strip()
        if not keyword:
            keyword = prospect.get("business_name", "business")

        app.logger.info(f"POP audit {job_id}: keyword='{keyword}', url='{url}'")

        # ==================== STEP 1: Get Terms ====================
        pop_jobs[job_id]["progress"] = "Step 1/3: Getting search terms from POP..."
        
        terms_resp = http_requests.post(f"{POP_BASE}/expose/get-terms/", json={
            "apiKey": POP_API_KEY,
            "keyword": keyword,
            "locationName": "United States",
            "targetLanguage": "english",
            "targetUrl": url
        }, timeout=120)
        terms_resp.raise_for_status()
        terms_data = terms_resp.json()
        
        app.logger.info(f"POP audit {job_id}: get-terms response: {json.dumps(terms_data)[:500]}")

        if terms_data.get("status") == "FAILURE":
            raise Exception(f"POP get-terms failed: {terms_data.get('msg', 'Unknown error')}")

        task_id = terms_data.get("taskId") or terms_data.get("task_id")
        if not task_id:
            # If no taskId, maybe it's a direct response
            if terms_data.get("prepareId"):
                terms_result = terms_data
            else:
                raise Exception(f"No taskId from POP get-terms: {terms_data}")
        else:
            # Poll for terms results (can take ~3 minutes)
            pop_jobs[job_id]["progress"] = f"Step 1/3: Polling for terms (task {task_id[:8]}...)"
            terms_result = _poll_pop_task(task_id, "get-terms", max_attempts=200, poll_interval=3)

        app.logger.info(f"POP audit {job_id}: terms result received")
        
        # Extract data from terms response (handle nested structure)
        result_data = terms_result.get("data", terms_result)
        prepare_id = result_data.get("prepareId")
        variations = result_data.get("variations", [])
        lsa_phrases = result_data.get("lsaPhrases", [])
        
        if not prepare_id:
            raise Exception(f"No prepareId in terms response: {terms_result}")

        # ==================== STEP 2: Create Report ====================
        pop_jobs[job_id]["progress"] = "Step 2/3: Creating optimization report..."
        
        report_payload = {
            "apiKey": POP_API_KEY,
            "prepareId": prepare_id,
            "variations": variations,
            "lsaPhrases": lsa_phrases,
            "strategy": "target",
            "approach": "regular",
            "eeatCalculation": 0,
            "googleNlpCalculation": 0
        }
        
        report_resp = http_requests.post(f"{POP_BASE}/expose/create-report/", json=report_payload, timeout=120)
        report_resp.raise_for_status()
        report_data = report_resp.json()
        
        app.logger.info(f"POP audit {job_id}: create-report response: {json.dumps(report_data)[:500]}")

        if report_data.get("status") == "FAILURE":
            raise Exception(f"POP create-report failed: {report_data.get('msg', 'Unknown error')}")

        report_task_id = report_data.get("taskId") or report_data.get("task_id")
        
        if report_task_id:
            # Poll for report results (can take another ~3 minutes)
            pop_jobs[job_id]["progress"] = f"Step 3/3: Polling for report (task {report_task_id[:8]}...)"
            final_report = _poll_pop_task(report_task_id, "create-report", max_attempts=200, poll_interval=3)
        else:
            # Direct response
            final_report = report_data

        app.logger.info(f"POP audit {job_id}: final report received")
        
        # ==================== Extract Metrics ====================
        # Navigate the nested structure
        report_wrapper = final_report.get("data", final_report)
        report = report_wrapper.get("report", report_wrapper)
        
        # Word counts (handle both nested and flat structures)
        word_count = report.get("wordCount", {})
        if isinstance(word_count, dict):
            word_count_current = word_count.get("current", 0)
            word_count_target = word_count.get("target", word_count.get("recommendation", 0))
            word_count_avg = word_count.get("competitorAvg", word_count.get("average", word_count.get("avg", 0)))
        else:
            word_count_current = word_count
            word_count_target = report.get("recommendedWordCount", 0)
            word_count_avg = report.get("averageWordCount", 0)

        # Competitors
        competitor_info = report.get("competitorInfo", {})
        competitors = competitor_info.get("competitors", [])
        competitor_count = len(competitors)

        # Tag counts (POP returns a list)
        tag_counts = report.get("tagCounts", [])
        if isinstance(tag_counts, dict):
            tag_counts = list(tag_counts.values()) if tag_counts else []

        # Terms and missing terms
        terms = report.get("terms", [])
        missing_terms = [t.get("term", t.get("phrase", "")) for t in terms if t.get("count", 0) == 0][:20]

        # Page score from cleanedContentBrief
        cleaned_brief = report.get("cleanedContentBrief", {})
        page_score_data = cleaned_brief.get("pageScore", {})
        page_score = 0
        if isinstance(page_score_data, dict):
            page_score = page_score_data.get("pageScore", 0)
        elif isinstance(page_score_data, (int, float)):
            page_score = page_score_data

        # Calculate prospect score
        pop_score = 50  # Base score
        reasons = []
        
        if word_count_target > 0 and word_count_current > 0:
            wc_percent = (word_count_current / word_count_target) * 100
            if wc_percent < 30:
                pop_score += 30
                reasons.append(f"Severe content gap ({word_count_current} vs {word_count_target} words)")
            elif wc_percent < 50:
                pop_score += 20
                reasons.append(f"Major content gap ({word_count_current} vs {word_count_target} words)")
            elif wc_percent < 70:
                pop_score += 10
                reasons.append(f"Content below target ({word_count_current} vs {word_count_target} words)")

        if len(missing_terms) >= 15:
            pop_score += 15
            reasons.append(f"Missing {len(missing_terms)}+ LSI terms")
        elif len(missing_terms) >= 10:
            pop_score += 10
            reasons.append(f"Missing {len(missing_terms)} LSI terms")
        elif len(missing_terms) >= 5:
            pop_score += 5
            reasons.append(f"Missing {len(missing_terms)} LSI terms")

        pop_score = min(100, pop_score)

        if pop_score >= 80:
            status = "hot"
        elif pop_score >= 60:
            status = "warm"
        else:
            status = "cold"

        metrics = {
            "word_count_current": word_count_current,
            "word_count_target": word_count_target,
            "word_count_avg": word_count_avg,
            "page_score": round(page_score, 1),
            "competitor_count": competitor_count,
            "tag_counts": tag_counts,
            "missing_terms": missing_terms,
            "missing_terms_count": len(missing_terms),
            "reasons": reasons
        }

        app.logger.info(f"POP audit {job_id}: metrics extracted - score={pop_score}, status={status}")

        # ==================== Save to DB ====================
        pop_jobs[job_id]["progress"] = "Saving results..."
        
        db2 = sqlite3.connect(PROSPECTS_DB_PATH)
        db2.execute("""UPDATE prospects SET pop_report_data=?, pop_audit_date=?, pop_score=?,
            prospect_score=?, prospect_status=?, pop_word_count_current=?, pop_word_count_target=?, updated_at=? WHERE id=?""",
            (json.dumps({"metrics": metrics, "report_data": final_report}), now_str(), pop_score, pop_score, status, 
             word_count_current, word_count_target, now_str(), pid))
        db2.commit()
        db2.close()

        app.logger.info(f"POP audit {job_id}: completed successfully")
        
        pop_jobs[job_id] = {
            "status": "complete", 
            "result": {
                "success": True, 
                "metrics": metrics, 
                "scoring": {"pop_score": pop_score, "status": status}
            }
        }

    except Exception as e:
        app.logger.error(f"POP audit {job_id} failed: {e}")
        pop_jobs[job_id] = {"status": "error", "error": str(e), "progress": "Failed"}


@app.route("/api/pop_audit_start", methods=["POST", "GET"])
@require_prospector_key
def pop_audit_start():
    """Start async POP audit - returns job_id immediately"""
    pid = request.args.get("prospect_id") or (request.json.get("prospect_id") if request.is_json else None)
    if not pid:
        return jsonify({"error": "prospect_id required"}), 400

    db = get_prospects_db()
    prospect = row_to_dict(db.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone())
    if not prospect:
        return jsonify({"error": "Prospect not found"}), 404

    job_id = str(uuid.uuid4())[:8]
    pop_jobs[job_id] = {
        "status": "running", 
        "started": time.time(),
        "progress": "Initializing POP audit...",
        "prospect_id": int(pid)
    }
    t = threading.Thread(target=_run_pop_audit_job, args=(job_id, int(pid)), daemon=True)
    t.start()

    return jsonify({
        "success": True, 
        "job_id": job_id, 
        "status": "running", 
        "message": "POP audit started. Poll /api/pop_audit_status?job_id=X for results.",
        "estimated_time": "3-6 minutes"
    })


@app.route("/api/pop_audit_status", methods=["GET"])
@require_prospector_key
def pop_audit_status():
    """Poll for async POP audit result"""
    job_id = request.args.get("job_id")
    if not job_id or job_id not in pop_jobs:
        return jsonify({"error": "Invalid or unknown job_id"}), 404

    job = pop_jobs[job_id]
    if job["status"] == "running":
        elapsed = int(time.time() - job.get("started", 0))
        progress = job.get("progress", "Processing...")
        return jsonify({
            "status": "running", 
            "elapsed_seconds": elapsed,
            "progress": progress
        })
    elif job["status"] == "complete":
        result = job["result"]
        # Keep job for a bit to allow polling to get the result
        # Cleanup happens on next poll or can be done manually
        return jsonify(result)
    else:
        error = job.get("error", "Unknown error")
        return jsonify({"status": "error", "error": error}), 500


@app.route("/api/pop_audit", methods=["POST", "GET"])
@require_prospector_key
def pop_audit():
    pid = request.args.get("prospect_id") or (request.json.get("prospect_id") if request.is_json else None)
    if not pid:
        return jsonify({"error": "prospect_id required"}), 400

    db = get_prospects_db()
    prospect = row_to_dict(db.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone())
    if not prospect:
        return jsonify({"error": "Prospect not found"}), 404

    url = prospect["website"]
    if not url.startswith("http"):
        url = "https://" + url

    keyword = f"{prospect.get('niche', '')} {prospect.get('city', '')}".strip()
    if not keyword:
        keyword = prospect.get("business_name", "business")

    # Step 1: Get terms
    try:
        terms_resp = http_requests.post(f"{POP_BASE}/expose/get-terms/", json={
            "apiKey": POP_API_KEY,
            "keyword": keyword,
            "locationName": "United States",
            "targetLanguage": "english",
            "targetUrl": url
        }, timeout=60)
        terms_data = terms_resp.json()
    except Exception as e:
        return jsonify({"error": f"POP get-terms failed: {e}"}), 500

    task_id = terms_data.get("taskId") or terms_data.get("task_id")
    if not task_id:
        return jsonify({"error": "No taskId from POP", "raw": terms_data}), 500

    # Poll for terms results
    terms_result = None
    for _ in range(200):
        time.sleep(3)
        try:
            r = http_requests.get(f"{POP_BASE}/task/{task_id}/results/", timeout=30)
            rd = r.json()
            if rd.get("status") in ("complete", "SUCCESS") or rd.get("data"):
                terms_result = rd
                break
        except Exception:
            continue

    if not terms_result:
        return jsonify({"error": "POP terms timed out"}), 504

    # Step 2: Create report
    prepare_id = terms_result.get("prepareId") or terms_result.get("data", {}).get("prepareId") or task_id
    variations = terms_result.get("variations") or terms_result.get("data", {}).get("variations", [])
    lsa_phrases = terms_result.get("lsaPhrases") or terms_result.get("data", {}).get("lsaPhrases", [])

    try:
        report_resp = http_requests.post(f"{POP_BASE}/expose/create-report/", json={
            "apiKey": POP_API_KEY,
            "prepareId": prepare_id,
            "variations": variations,
            "lsaPhrases": lsa_phrases
        }, timeout=60)
        report_data = report_resp.json()
    except Exception as e:
        return jsonify({"error": f"POP create-report failed: {e}"}), 500

    report_task_id = report_data.get("taskId") or report_data.get("task_id")
    if report_task_id:
        for _ in range(200):
            time.sleep(3)
            try:
                r = http_requests.get(f"{POP_BASE}/task/{report_task_id}/results/", timeout=30)
                rd = r.json()
                if rd.get("status") in ("complete", "SUCCESS") or rd.get("report"):
                    report_data = rd
                    break
            except Exception:
                continue

    # Extract metrics
    report = report_data.get("report", report_data.get("data", {}).get("report", report_data))
    word_count_current = report.get("wordCount", 0)
    word_count_target = report.get("recommendedWordCount", 0)
    word_count_avg = report.get("averageWordCount", 0)
    competitor_count = len(report.get("competitorInfo", {}).get("competitors", []))
    tag_counts = report.get("tagCounts", [])
    if isinstance(tag_counts, dict):
        tag_counts = list(tag_counts.values()) if tag_counts else []
    terms = report.get("terms", [])
    missing_terms = [t["term"] for t in terms if t.get("count", 0) == 0][:10]

    pop_score = 50
    if word_count_target > 0:
        if word_count_current < word_count_target * 0.5:
            pop_score += 20
        elif word_count_current < word_count_target:
            pop_score += 10
    if len(missing_terms) > 10:
        pop_score += 25
    elif len(missing_terms) > 5:
        pop_score += 15
    pop_score = min(100, pop_score)

    if pop_score >= 80:
        status = "hot"
    elif pop_score >= 60:
        status = "warm"
    else:
        status = "cold"

    metrics = {
        "word_count_current": word_count_current,
        "word_count_target": word_count_target,
        "word_count_avg": word_count_avg,
        "competitor_count": competitor_count,
        "tag_counts": tag_counts,
        "missing_terms": missing_terms,
        "missing_terms_count": len(missing_terms)
    }

    # Save (use separate connection to avoid Flask g issues in long-running request)
    db2 = sqlite3.connect(PROSPECTS_DB_PATH)
    db2.execute("""UPDATE prospects SET pop_report_data=?, pop_audit_date=?, pop_score=?,
        prospect_score=?, prospect_status=?, updated_at=? WHERE id=?""",
        (json.dumps({"metrics": metrics, "report_data": report_data}), now_str(), pop_score, pop_score, status, now_str(), pid))
    db2.commit()
    db2.close()

    return jsonify({"success": True, "metrics": metrics, "scoring": {"pop_score": pop_score, "status": status}})


@app.route("/api/pitch")
@require_prospector_key
def prospect_pitch():
    pid = request.args.get("prospect_id")
    if not pid:
        return jsonify({"error": "prospect_id required"}), 400

    db = get_prospects_db()
    prospect = row_to_dict(db.execute("SELECT * FROM prospects WHERE id = ?", (pid,)).fetchone())
    if not prospect:
        return jsonify({"error": "Prospect not found"}), 404

    used_pop = False
    pop_context = ""
    if prospect.get("pop_report_data"):
        try:
            pop = json.loads(prospect["pop_report_data"])
            m = pop.get("metrics", {})
            pop_context = f"""
Their website analysis shows:
- Current word count: {m.get('word_count_current', 'N/A')} (recommended: {m.get('word_count_target', 'N/A')})
- They are missing {m.get('missing_terms_count', 0)} important keyword variations
- {m.get('competitor_count', 0)} competitors are ranking for this term
"""
            used_pop = True
        except Exception:
            pass

    issues_ctx = ""
    if prospect.get("issues"):
        try:
            issues = json.loads(prospect["issues"])
            if issues:
                issues_ctx = "Website issues found: " + ", ".join(issues)
        except Exception:
            pass

    prompt = f"""Write a short, personalized cold outreach email for an SEO agency pitching services to a local business.

Business: {prospect.get('business_name', 'Unknown')}
Industry: {prospect.get('niche', 'Unknown')}
Location: {prospect.get('city', '')}, {prospect.get('state', '')}
Website: {prospect.get('website', '')}
SEO Score: {prospect.get('seo_score', 'Not analyzed')}/100
{issues_ctx}
{pop_context}

Rules:
- NEVER mention any specific tools, software, or analysis platforms by name
- Be conversational and genuine, not salesy
- Reference specific issues you found on their site
- Keep it under 150 words for the body
- Format: First line is the subject line, then a blank line, then the body
- Sign off as the SEO Design Lab team
"""

    try:
        resp = http_requests.post("https://openrouter.ai/api/v1/chat/completions", json={
            "model": "anthropic/claude-3-haiku",
            "messages": [{"role": "user", "content": prompt}]
        }, headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json"
        }, timeout=30)
        ai = resp.json()
        content = ai["choices"][0]["message"]["content"]
    except Exception as e:
        return jsonify({"error": f"AI pitch failed: {e}"}), 500

    lines = content.strip().split("\n", 1)
    subject = lines[0].replace("Subject:", "").replace("Subject Line:", "").strip()
    body = lines[1].strip() if len(lines) > 1 else content

    db.execute("UPDATE prospects SET pitch_subject=?, pitch_body=?, pitch_date=?, updated_at=? WHERE id=?",
               (subject, body, now_str(), now_str(), pid))
    db.commit()

    return jsonify({"success": True, "subject": subject, "pitch": body, "used_pop_data": used_pop})


@app.route("/api/list")
@require_prospector_key
def list_prospects():
    status = request.args.get("status", "all")
    db = get_prospects_db()
    if status and status != "all":
        rows = db.execute("""SELECT * FROM prospects WHERE prospect_status = ? 
            ORDER BY pop_audit_date DESC, prospect_score DESC""", (status,)).fetchall()
    else:
        rows = db.execute("""SELECT * FROM prospects 
            ORDER BY CASE WHEN pop_audit_date IS NULL THEN 1 ELSE 0 END, pop_audit_date DESC, prospect_score DESC""").fetchall()
    return jsonify({"success": True, "count": len(rows), "prospects": [row_to_dict(r) for r in rows]})


@app.route("/api/stats")
@require_prospector_key
def prospect_stats():
    db = get_prospects_db()
    total = db.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
    by_status = {}
    for row in db.execute("SELECT prospect_status, COUNT(*) as c FROM prospects GROUP BY prospect_status"):
        by_status[row["prospect_status"]] = row["c"]
    by_niche = {}
    for row in db.execute("SELECT niche, COUNT(*) as c FROM prospects WHERE niche IS NOT NULL GROUP BY niche ORDER BY c DESC LIMIT 10"):
        by_niche[row["niche"]] = row["c"]
    recent = [row_to_dict(r) for r in db.execute("SELECT * FROM searches ORDER BY created_at DESC LIMIT 5").fetchall()]
    sent = db.execute("SELECT COUNT(*) FROM prospects WHERE sent_date IS NOT NULL").fetchone()[0]
    responded = db.execute("SELECT COUNT(*) FROM prospects WHERE response_date IS NOT NULL").fetchone()[0]
    pop_count = db.execute("SELECT COUNT(*) FROM prospects WHERE pop_audit_date IS NOT NULL").fetchone()[0]
    rate = round(responded / sent * 100, 1) if sent > 0 else 0

    return jsonify({
        "success": True, "total": total, "by_status": by_status, "by_niche": by_niche,
        "recent_searches": recent, "sent": sent, "responded": responded,
        "response_rate": rate, "pop_audits": pop_count
    })


@app.route("/api/mark_sent", methods=["POST", "GET"])
@require_prospector_key
def mark_sent():
    pid = request.args.get("prospect_id")
    method = request.args.get("contact_method", "email")
    db = get_prospects_db()
    db.execute("UPDATE prospects SET sent_date=?, contact_method=?, updated_at=? WHERE id=?", (now_str(), method, now_str(), pid))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/mark_response", methods=["POST", "GET"])
@require_prospector_key
def mark_response():
    pid = request.args.get("prospect_id")
    db = get_prospects_db()
    db.execute("UPDATE prospects SET response_date=?, updated_at=? WHERE id=?", (now_str(), now_str(), pid))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/undo_sent", methods=["POST", "GET"])
@require_prospector_key
def undo_sent():
    pid = request.args.get("prospect_id")
    db = get_prospects_db()
    db.execute("UPDATE prospects SET sent_date=NULL, contact_method=NULL, updated_at=? WHERE id=?", (now_str(), pid))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/undo_response", methods=["POST", "GET"])
@require_prospector_key
def undo_response():
    pid = request.args.get("prospect_id")
    db = get_prospects_db()
    db.execute("UPDATE prospects SET response_date=NULL, updated_at=? WHERE id=?", (now_str(), pid))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/text")
@require_prospector_key
def text_summary():
    db = get_prospects_db()
    total = db.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
    hot = db.execute("SELECT COUNT(*) FROM prospects WHERE prospect_status='hot'").fetchone()[0]
    warm = db.execute("SELECT COUNT(*) FROM prospects WHERE prospect_status='warm'").fetchone()[0]
    sent = db.execute("SELECT COUNT(*) FROM prospects WHERE sent_date IS NOT NULL").fetchone()[0]
    responded = db.execute("SELECT COUNT(*) FROM prospects WHERE response_date IS NOT NULL").fetchone()[0]
    pop = db.execute("SELECT COUNT(*) FROM prospects WHERE pop_audit_date IS NOT NULL").fetchone()[0]

    text = f"""📊 Smart Prospector Pipeline
━━━━━━━━━━━━━━━━━━━━
Total Prospects: {total}
🔥 Hot: {hot} | 🟡 Warm: {warm}
📧 Sent: {sent} | 💬 Responded: {responded}
🔍 POP Audits: {pop}
━━━━━━━━━━━━━━━━━━━━"""

    return jsonify({"success": True, "text": text})


@app.route("/api/get_pop_report")
@require_prospector_key
def get_pop_report():
    pid = request.args.get("prospect_id")
    if not pid:
        return jsonify({"error": "prospect_id required"}), 400
    db = get_prospects_db()
    prospect = row_to_dict(db.execute("SELECT id, business_name, website, pop_report_data, pop_audit_date, pop_score, pop_word_count_current, pop_word_count_target FROM prospects WHERE id = ?", (pid,)).fetchone())
    if not prospect:
        return jsonify({"error": "Prospect not found"}), 404
    
    pop_data = None
    if prospect.get("pop_report_data"):
        try:
            pop_data = json.loads(prospect["pop_report_data"])
        except Exception:
            pop_data = prospect["pop_report_data"]
    
    # Extract processed metrics for the frontend modal
    metrics = None
    keyword = prospect.get("business_name", "")
    website = prospect.get("website", "")
    
    if pop_data and isinstance(pop_data, dict):
        # First check for pre-extracted metrics from the async job
        pre_metrics = pop_data.get("metrics", {})
        
        # Get the report data - could be nested in different ways
        report_data = pop_data.get("report_data", pop_data)
        if isinstance(report_data, dict):
            report = report_data.get("report", report_data.get("data", {}).get("report", report_data))
        else:
            report = {}
        
        if report:
            word_count = report.get("wordCount", {})
            tag_counts = report.get("tagCounts", [])
            cb = report.get("cleanedContentBrief", {})
            p_total = cb.get("pTotal", {})
            page_score_data = cb.get("pageScore", {})
            page_score = pre_metrics.get("page_score", 0)
            
            if not page_score and isinstance(page_score_data, dict):
                page_score = page_score_data.get("pageScore", 0)
            elif isinstance(page_score_data, (int, float)):
                page_score = page_score_data

            keyword = report.get("keyword", keyword)
            website = report.get("url", website)

            # Use pre-extracted word counts or fall back to extracting from report
            wc_current = pre_metrics.get("word_count_current", 0) or word_count.get("current", 0) or prospect.get("pop_word_count_current", 0)
            wc_target = pre_metrics.get("word_count_target", 0) or word_count.get("target", 0) or prospect.get("pop_word_count_target", 0)
            wc_avg = pre_metrics.get("word_count_avg", 0) or word_count.get("competitorAvg", word_count.get("avg", 0))

            metrics = {
                "page_score": round(page_score, 1) if page_score else 0,
                "word_count_current": wc_current,
                "word_count_target": wc_target,
                "word_count_avg": wc_avg,
                "competitor_count": pre_metrics.get("competitor_count", len(report.get("competitors", []))),
                "tag_counts": tag_counts if isinstance(tag_counts, list) else [],
                "terms_current": p_total.get("current", 0),
                "terms_target_min": p_total.get("min", 0),
                "terms_target_max": p_total.get("max", 0),
                "terms": [],
                "related_questions": report.get("relatedQuestions", []),
                "lsa_variations": [v.get("phrase", v) if isinstance(v, dict) else v for v in report.get("lsaVariations", [])[:10]],
                "related_searches": [v.get("phrase", v) if isinstance(v, dict) else v for v in report.get("relatedSearches", [])[:8]],
                "competitors": report.get("competitors", []),
                "schema_types": report.get("schemaTypes", []),
                "ai_schema_types": report.get("aiGenSchemaTypes", []),
                "missing_terms": pre_metrics.get("missing_terms", []),
                "target_schema": report.get("schemaTypes", []) or report.get("aiGenSchemaTypes", [])
            }
            
            # Extract all content brief terms
            if cb and cb.get("p"):
                for item in cb["p"]:
                    t = item.get("term", {})
                    brief = item.get("contentBrief", {})
                    current = brief.get("current", 0)
                    target_min = brief.get("targetMin", brief.get("target", 0))
                    target_max = brief.get("targetMax", target_min)
                    metrics["terms"].append({
                        "phrase": t.get("phrase", ""),
                        "current": current,
                        "target_min": target_min,
                        "target_max": target_max,
                        "type": t.get("type", ""),
                        "weight": t.get("weight", 0),
                        "met": current >= target_min if target_min > 0 else (current > 0)
                    })
                    if current < target_min and target_min > 0 and t.get("phrase") not in metrics["missing_terms"]:
                        metrics["missing_terms"].append(t.get("phrase", ""))
    
    return jsonify({
        "success": True, "prospect_id": pid,
        "pop_audit_date": prospect.get("pop_audit_date"), "pop_score": prospect.get("pop_score"),
        "metrics": metrics, "keyword": keyword, "website": website,
        "audit_date": prospect.get("pop_audit_date")
    })


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/api/bulk_import", methods=["POST"])
@require_prospector_key
def bulk_import():
    """Import prospects in bulk (for backfilling from WPX SQLite)."""
    data = request.get_json()
    if not data or not data.get("prospects"):
        return jsonify({"error": "prospects array required"}), 400

    prospects = data["prospects"]
    db = sqlite3.connect(PROSPECTS_DB_PATH)
    imported = 0
    skipped = 0

    for p in prospects:
        try:
            db.execute("""INSERT OR REPLACE INTO prospects 
                (id, business_name, website, phone, address, city, state, niche, rating, reviews,
                 seo_score, prospect_score, prospect_status, issues, has_ssl,
                 pitch_subject, pitch_body, pitch_date, sent_date, contact_method,
                 response_date, pop_report_data, pop_audit_date, pop_score,
                 search_query, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (p.get("id"), p.get("business_name"), p.get("website"), p.get("phone"),
                 p.get("address"), p.get("city"), p.get("state"), p.get("niche"),
                 p.get("rating"), p.get("reviews"), p.get("seo_score", 0),
                 p.get("prospect_score", 0), p.get("prospect_status", "new"),
                 p.get("issues"), p.get("has_ssl", 0),
                 p.get("pitch_subject"), p.get("pitch_body"), p.get("pitch_date"),
                 p.get("sent_date"), p.get("contact_method"), p.get("response_date"),
                 p.get("pop_report_data"), p.get("pop_audit_date"), p.get("pop_score"),
                 p.get("search_query"), p.get("created_at"), p.get("updated_at")))
            imported += 1
        except Exception as e:
            skipped += 1

    db.commit()
    db.close()
    return jsonify({"success": True, "imported": imported, "skipped": skipped})


# Also import searches
@app.route("/api/bulk_import_searches", methods=["POST"])
@require_prospector_key
def bulk_import_searches():
    data = request.get_json()
    if not data or not data.get("searches"):
        return jsonify({"error": "searches array required"}), 400

    db = sqlite3.connect(PROSPECTS_DB_PATH)
    imported = 0
    for s in data["searches"]:
        try:
            db.execute("INSERT OR REPLACE INTO searches (id, query, niche, location, result_count, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (s.get("id"), s.get("query"), s.get("niche"), s.get("location"), s.get("result_count"), s.get("created_at")))
            imported += 1
        except Exception:
            pass
    db.commit()
    db.close()
    return jsonify({"success": True, "imported": imported})


# ============================================================
# PROPOSAL GENERATION
# ============================================================

NICHE_COLORS = {
    "plumbing": ("#1a3d5c", "#2a6496"), "pool": ("#1a3d5c", "#2a6496"),
    "pool service": ("#1a3d5c", "#2a6496"), "pool care": ("#1a3d5c", "#2a6496"),
    "roofing": ("#5c1a1a", "#963a2a"), "painting": ("#1a4d2e", "#2d7a4a"),
    "tree service": ("#2d4a1a", "#4a7a2d"), "landscaping": ("#2d4a1a", "#4a7a2d"),
    "pressure washing": ("#1a3d4d", "#2a6a7a"), "cleaning": ("#1a3d4d", "#2a6a7a"),
    "dental": ("#1a2d4d", "#2d4a7a"), "dentist": ("#1a2d4d", "#2d4a7a"),
    "medical": ("#1a2d4d", "#2d4a7a"), "legal": ("#1a1a3d", "#2d2d5a"),
    "attorney": ("#1a1a3d", "#2d2d5a"), "gym": ("#4d1a4d", "#7a2d7a"),
    "med spa": ("#3d1a3d", "#5a2d5a"), "garage door": ("#3d2d1a", "#5a4a2d"),
    "hvac": ("#1a3d5c", "#2a6496"), "auto": ("#3d3d3d", "#5a5a5a"),
    "restaurant": ("#4d1a1a", "#7a2d2d"),
}

def get_niche_colors(niche):
    n = niche.lower().strip()
    for k, v in NICHE_COLORS.items():
        if k in n:
            return v
    return ("#1a1a2e", "#2d2d5a")

def dataforseo_api(endpoint, payload):
    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        return {}
    try:
        r = http_requests.post(
            f"https://api.dataforseo.com/v3/{endpoint}",
            json=payload, auth=(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD), timeout=60
        )
        d = r.json()
        if d.get("tasks") and d["tasks"][0].get("result"):
            return d["tasks"][0]["result"][0] if d["tasks"][0]["result"] else {}
    except Exception as e:
        app.logger.error(f"DataForSEO error: {e}")
    return {}

def fetch_prospect_seo_data(domain):
    clean = domain.replace("https://","").replace("http://","").rstrip("/")
    overview = dataforseo_api("dataforseo_labs/google/domain_rank_overview/live",
        [{"target": clean, "language_code": "en", "location_code": 2840}])
    ranked = dataforseo_api("dataforseo_labs/google/ranked_keywords/live",
        [{"target": clean, "language_code": "en", "location_code": 2840, "limit": 50}])
    competitors = dataforseo_api("dataforseo_labs/google/competitors_domain/live",
        [{"target": clean, "language_code": "en", "location_code": 2840, "limit": 10}])

    metrics = overview.get("metrics", {}).get("organic", {}) if overview else {}
    result = {
        "organic_traffic": metrics.get("etv", 0),
        "keywords_count": metrics.get("count", 0),
        "domain_rank": overview.get("rank", 0) if overview else 0,
        "ranked_keywords": [], "keyword_opportunities": [], "competitors": [],
    }
    if ranked and ranked.get("items"):
        for item in ranked["items"][:30]:
            kw = item.get("keyword_data", {})
            result["ranked_keywords"].append({
                "keyword": kw.get("keyword",""), "position": item.get("rank_group",0),
                "volume": kw.get("keyword_info",{}).get("search_volume",0),
                "cpc": kw.get("keyword_info",{}).get("cpc",0),
            })
        for item in ranked["items"]:
            pos = item.get("rank_group",0)
            kw = item.get("keyword_data",{})
            vol = kw.get("keyword_info",{}).get("search_volume",0)
            if 11 <= pos <= 50 and vol >= 100:
                result["keyword_opportunities"].append({
                    "keyword": kw.get("keyword",""), "position": pos,
                    "volume": vol, "cpc": kw.get("keyword_info",{}).get("cpc",0),
                })
    if competitors and competitors.get("items"):
        for item in competitors["items"][:8]:
            result["competitors"].append({
                "domain": item.get("domain",""),
                "organic_traffic": item.get("metrics",{}).get("organic",{}).get("etv",0),
                "keywords": item.get("metrics",{}).get("organic",{}).get("count",0),
            })
    return result


@app.route("/api/generate_proposal", methods=["POST"])
@require_prospector_key
def api_generate_proposal():
    """Generate proposal HTML for a prospect. Returns dashboard + proposal HTML."""
    data = request.get_json() or {}

    # Get prospect data - either from DB or from request body
    prospect_id = data.get("prospect_id")
    if prospect_id:
        db = get_prospects_db()
        row = row_to_dict(db.execute("SELECT * FROM prospects WHERE id = ?", (prospect_id,)).fetchone())
        if not row:
            return jsonify({"error": "Prospect not found"}), 404
        prospect = {
            "prospect_name": data.get("prospect_name") or row.get("business_name", ""),
            "prospect_domain": data.get("prospect_domain") or row.get("website", ""),
            "niche": data.get("niche") or row.get("niche", ""),
            "location": data.get("location") or f"{row.get('city','')}, {row.get('state','')}".strip(", "),
            "contact_name": data.get("contact_name", ""),
            "contact_email": data.get("contact_email", ""),
            "contact_phone": data.get("contact_phone", ""),
            "package": data.get("package", "premium"),
        }
    else:
        prospect = {
            "prospect_name": data.get("prospect_name", ""),
            "prospect_domain": data.get("prospect_domain", ""),
            "niche": data.get("niche", ""),
            "location": data.get("location", ""),
            "contact_name": data.get("contact_name", ""),
            "contact_email": data.get("contact_email", ""),
            "contact_phone": data.get("contact_phone", ""),
            "package": data.get("package", "premium"),
        }

    if not prospect["prospect_name"] or not prospect["prospect_domain"]:
        return jsonify({"error": "prospect_name and prospect_domain required"}), 400

    # Fetch SEO data
    seo_data = fetch_prospect_seo_data(prospect["prospect_domain"])

    # Read templates from embedded or fallback
    # On Render we won't have the template files, so we return seo_data
    # and let the local script do the HTML generation
    return jsonify({
        "success": True,
        "prospect": prospect,
        "seo_data": seo_data,
        "message": "SEO data fetched. Use local script for HTML generation.",
    })


@app.route("/api/backfill_word_counts", methods=["POST"])
@require_prospector_key
def backfill_word_counts():
    """Extract word counts from existing POP report JSON into dedicated columns."""
    db = get_prospects_db()
    # Add columns if missing
    cols = [c[1] for c in db.execute("PRAGMA table_info(prospects)").fetchall()]
    if "pop_word_count_current" not in cols:
        db.execute("ALTER TABLE prospects ADD COLUMN pop_word_count_current INTEGER DEFAULT 0")
    if "pop_word_count_target" not in cols:
        db.execute("ALTER TABLE prospects ADD COLUMN pop_word_count_target INTEGER DEFAULT 0")
    db.commit()
    
    rows = db.execute("SELECT id, pop_report_data FROM prospects WHERE pop_report_data IS NOT NULL AND pop_report_data != ''").fetchall()
    updated = 0
    for row in rows:
        pid, raw = row
        try:
            data = json.loads(raw)
            report = data.get("report_data", {}).get("report", data.get("report", {}))
            wc = report.get("wordCount", {})
            current = wc.get("current", 0)
            target = wc.get("target", 0)
            if current > 0 or target > 0:
                db.execute("UPDATE prospects SET pop_word_count_current=?, pop_word_count_target=? WHERE id=?", (current, target, pid))
                updated += 1
        except Exception:
            continue
    db.commit()
    return jsonify({"success": True, "updated": updated, "total_with_pop": len(rows)})


@app.route("/health", methods=["GET"])
def health():
    try:
        conn = sqlite3.connect(PROSPECTS_DB_PATH)
        prospects_count = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
        conn.close()
    except Exception:
        prospects_count = 0
    return jsonify({
        "status": "ok",
        "services": ["email-relay", "email-tracking", "smart-prospector"],
        "provider": "resend",
        "tracking": True,
        "prospects_count": prospects_count
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
