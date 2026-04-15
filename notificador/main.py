"""
Cloud Function que escucha Pub/Sub y envía email + Telegram.
"""
import base64
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests

DESTINATARIO = os.environ.get("EMAIL_DESTINO", "")
REMITENTE    = os.environ.get("EMAIL_ORIGEN",  "")
SMTP_HOST    = "smtp.gmail.com"
SMTP_PORT    = 587
SMTP_PASS    = os.environ.get("SMTP_PASS", "")
TG_TOKEN     = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID", "")


def _enviar_email(asunto: str, cuerpo: str):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = asunto
        msg["From"]    = REMITENTE
        msg["To"]      = DESTINATARIO
        msg.attach(MIMEText(cuerpo, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(REMITENTE, SMTP_PASS)
            s.sendmail(REMITENTE, DESTINATARIO, msg.as_string())
        print(f"Email enviado a {DESTINATARIO}")
    except Exception as e:
        print(f"Error enviando email: {e}")


def _enviar_telegram(mensaje: str):
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       mensaje,
            "parse_mode": "HTML"
        }, timeout=10)
        print(f"Telegram response: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Error enviando Telegram: {e}")


def sesion_detectada(event, context):
    d = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    asunto = f"📋 Nueva sesión — {d['fuente']} — {d['titulo'][:50]}"
    cuerpo = f"""
    <h2>Nueva sesión legislativa detectada</h2>
    <table>
      <tr><td><b>Título</b></td><td>{d['titulo']}</td></tr>
      <tr><td><b>Fuente</b></td><td>{d['fuente']}</td></tr>
      <tr><td><b>Fecha</b></td><td>{d['fecha']}</td></tr>
    </table>
    <p>La descarga y transcripción han comenzado.</p>
    """
    tg_msg = (
        f"📋 <b>Nueva sesión detectada</b>\n\n"
        f"<b>Título:</b> {d['titulo']}\n"
        f"<b>Fuente:</b> {d['fuente']}\n"
        f"<b>Fecha:</b> {d['fecha']}\n\n"
        f"Transcripción iniciada..."
    )
    _enviar_email(asunto, cuerpo)
    _enviar_telegram(tg_msg)


def transcripcion_lista(event, context):
    d = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    asunto = f"✅ Transcripción lista — {d['fuente']} — {d['titulo'][:50]}"
    cuerpo = f"""
    <h2>Transcripción completada</h2>
    <table>
      <tr><td><b>Título</b></td><td>{d['titulo']}</td></tr>
      <tr><td><b>Fuente</b></td><td>{d['fuente']}</td></tr>
      <tr><td><b>Fecha</b></td><td>{d['fecha']}</td></tr>
      <tr><td><b>TXT</b></td><td>{d['uri_txt']}</td></tr>
    </table>
    <p><a href="https://console.cloud.google.com/storage/browser/komv1/transcripciones">
    Ver en Cloud Storage</a></p>
    """
    tg_msg = (
        f"✅ <b>Transcripción lista</b>\n\n"
        f"<b>Título:</b> {d['titulo']}\n"
        f"<b>Fuente:</b> {d['fuente']}\n"
        f"<b>Fecha:</b> {d['fecha']}\n\n"
        f"<b>Archivo:</b> <code>{d['uri_txt']}</code>"
    )
    _enviar_email(asunto, cuerpo)
    _enviar_telegram(tg_msg)
