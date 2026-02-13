import os
import smtplib
import ssl
import time
import socket
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify
from functools import wraps
from collections import deque

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_KEY")
SMTP_HOST = os.environ.get("SMTP_HOST", "s18.wpxhosting.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "milo@seodesignlab.com")
SMTP_PASS = os.environ.get("SMTP_PASS")
FROM_ADDRESS = "milo@seodesignlab.com"

# SOCKS5 proxy for SMTP (WPX blocks direct connections from cloud IPs)
PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "0"))
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

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


def create_smtp_connection():
    """Create SMTP_SSL connection, optionally through SOCKS5 proxy."""
    context = ssl.create_default_context()

    # Try direct connection first (no proxy - SOCKS5 blocks SMTP)
    server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=45)

    server.login(SMTP_USER, SMTP_PASS)
    return server


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

    if not to:
        return jsonify({"error": "'to' field is required"}), 400

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = FROM_ADDRESS
        msg["To"] = to
        msg["Subject"] = subject
        if cc:
            msg["Cc"] = cc

        if body:
            msg.attach(MIMEText(body, "plain"))
        if html:
            msg.attach(MIMEText(html, "html"))
        elif not body:
            return jsonify({"error": "'body' or 'html' required"}), 400

        recipients = [to]
        if cc:
            recipients += [a.strip() for a in cc.split(",")]
        if bcc:
            recipients += [a.strip() for a in bcc.split(",")]

        server = create_smtp_connection()
        try:
            server.sendmail(FROM_ADDRESS, recipients, msg.as_string())
        finally:
            server.quit()

        app.logger.info(f"Email sent to={to} subject={subject}")
        return jsonify({"success": True, "message": f"Email sent to {to}"})

    except Exception as e:
        app.logger.error(f"Send failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "proxy": bool(PROXY_HOST)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
