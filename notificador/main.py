"""
Cloud Function que escucha Pub/Sub y envía emails.
"""
import base64
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

DESTINATARIO = os.environ.get("EMAIL_DESTINO", "tu@gmail.com")
REMITENTE    = os.environ.get("EMAIL_ORIGEN",  "tu@gmail.com")
SMTP_HOST    = "smtp.gmail.com"
SMTP_PORT    = 587
SMTP_PASS    = os.environ.get("SMTP_PASS", "")


def _enviar(asunto: str, cuerpo: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = REMITENTE
    msg["To"]      = DESTINATARIO
    msg.attach(MIMEText(cuerpo, "html", "utf-8"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(REMITENTE, SMTP_PASS)
        s.sendmail(REMITENTE, DESTINATARIO, msg.as_string())


def sesion_detectada(event, context):
    """Trigger: topic sesion-detectada"""
    d = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    _enviar(
        f"📋 Nueva sesión — {d['fuente']} — {d['titulo'][:50]}",
        f"""<h2>Nueva sesión legislativa detectada</h2>
        <table>
          <tr><td><b>Título</b></td><td>{d['titulo']}</td></tr>
          <tr><td><b>Fuente</b></td><td>{d['fuente']}</td></tr>
          <tr><td><b>Fecha</b></td><td>{d['fecha']}</td></tr>
        </table>
        <p>La descarga y transcripción han comenzado.<br>
        Te avisaremos cuando terminen.</p>"""
    )


def transcripcion_lista(event, context):
    """Trigger: topic transcripcion-lista"""
    d = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    _enviar(
        f"✅ Transcripción lista — {d['fuente']} — {d['titulo'][:50]}",
        f"""<h2>Transcripción completada</h2>
        <table>
          <tr><td><b>Título</b></td><td>{d['titulo']}</td></tr>
          <tr><td><b>Fuente</b></td><td>{d['fuente']}</td></tr>
          <tr><td><b>Fecha</b></td><td>{d['fecha']}</td></tr>
          <tr><td><b>TXT</b></td><td>{d['uri_txt']}</td></tr>
          <tr><td><b>Audio</b></td><td>{d['uri_mp3']}</td></tr>
        </table>
        <p><a href="https://console.cloud.google.com/storage/browser/komv1/transcripciones">
        Ver en Cloud Storage</a></p>"""
    )
