import os
import time
import json
import sqlite3
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, redirect, Response
from functools import wraps
from collections import deque
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_KEY")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
FROM_ADDRESS = os.environ.get("FROM_ADDRESS", "milo@seodesignlab.com")
TRACKER_KEY = os.environ.get("TRACKER_KEY", "sdl-email-2026")

# SQLite for email tracking (persistent on Render with disk, in-memory fallback)
DB_PATH = os.environ.get("DB_PATH", "/opt/render/project/data/tracking.db") if os.environ.get("RENDER") else "tracking.db"


def get_db():
    db = sqlite3.connect(DB_PATH)
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


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if not key or key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
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
        # Build Resend payload
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

        resp = requests.post(
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

            # Auto-register for tracking if tracking_id provided
            tracking_id = data.get("tracking_id")
            if tracking_id:
                try:
                    db = get_db()
                    db.execute(
                        "INSERT OR REPLACE INTO emails (id, subject, recipient, recipient_name, client, sent_at, resend_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (tracking_id, subject, to if isinstance(to, str) else ",".join(to), data.get("recipient_name", ""), data.get("client", ""), datetime.utcnow().isoformat(), resend_id)
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


# --- Email Tracking Endpoints ---

@app.route("/t/open", methods=["GET"])
def track_open():
    """1x1 pixel - tracks email opens."""
    email_id = request.args.get("id", "")
    if email_id:
        try:
            db = get_db()
            db.execute(
                "INSERT INTO events (email_id, event_type, ip, user_agent, timestamp) VALUES (?, ?, ?, ?, ?)",
                (email_id, "open", request.remote_addr or "", request.headers.get("User-Agent", ""), datetime.utcnow().isoformat())
            )
            db.commit()
            db.close()
        except Exception as e:
            app.logger.error(f"Track open error: {e}")
    return Response(PIXEL_GIF, mimetype="image/gif", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.route("/t/click", methods=["GET"])
def track_click():
    """Click redirect - tracks link clicks."""
    email_id = request.args.get("id", "")
    url = request.args.get("url", "")
    if not url:
        return "Missing url", 400
    if email_id:
        try:
            db = get_db()
            db.execute(
                "INSERT INTO events (email_id, event_type, url, ip, user_agent, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (email_id, "click", url, request.remote_addr or "", request.headers.get("User-Agent", ""), datetime.utcnow().isoformat())
            )
            db.commit()
            db.close()
        except Exception as e:
            app.logger.error(f"Track click error: {e}")
    return redirect(url)


@app.route("/t/register", methods=["POST"])
@require_api_key
def register_email():
    """Register an email send for tracking."""
    data = request.get_json()
    if not data or not data.get("id"):
        return jsonify({"error": "Missing email id"}), 400
    try:
        db = get_db()
        db.execute(
            "INSERT OR REPLACE INTO emails (id, subject, recipient, recipient_name, client, sent_at, resend_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (data["id"], data.get("subject", ""), data.get("recipient", ""), data.get("recipient_name", ""), data.get("client", ""), data.get("sent_at", datetime.utcnow().isoformat()), data.get("resend_id", ""))
        )
        db.commit()
        db.close()
        return jsonify({"ok": True, "id": data["id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/t/analytics", methods=["GET"])
@require_api_key
def analytics():
    """Email analytics dashboard - open rates, click rates, per-email detail."""
    try:
        db = get_db()
        emails = []
        rows = db.execute('''
            SELECT e.*,
                (SELECT COUNT(*) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as opens,
                (SELECT COUNT(DISTINCT ip) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as unique_opens,
                (SELECT COUNT(*) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'click') as clicks,
                (SELECT MIN(timestamp) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as first_open,
                (SELECT MAX(timestamp) FROM events ev WHERE ev.email_id = e.id AND ev.event_type = 'open') as last_open
            FROM emails e ORDER BY e.sent_at DESC
        ''').fetchall()
        for row in rows:
            emails.append(dict(row))

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
    """Per-email event detail."""
    try:
        db = get_db()
        email = db.execute("SELECT * FROM emails WHERE id = ?", (email_id,)).fetchone()
        events = db.execute("SELECT * FROM events WHERE email_id = ? ORDER BY timestamp DESC", (email_id,)).fetchall()
        db.close()
        return jsonify({
            "email": dict(email) if email else None,
            "events": [dict(e) for e in events]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "provider": "resend", "tracking": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
