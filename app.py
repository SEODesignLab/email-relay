import os
import time
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify
from functools import wraps
from collections import deque

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_KEY")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
FROM_ADDRESS = os.environ.get("FROM_ADDRESS", "milo@seodesignlab.com")

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
            app.logger.info(f"Email sent to={to} subject={subject} id={result.get('id')}")
            return jsonify({"success": True, "message": f"Email sent to {to}", "id": result.get("id")})
        else:
            error = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"message": resp.text}
            app.logger.error(f"Resend error: {resp.status_code} {error}")
            return jsonify({"error": error.get("message", str(error)), "status_code": resp.status_code}), 502

    except Exception as e:
        app.logger.error(f"Send failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "provider": "resend"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
