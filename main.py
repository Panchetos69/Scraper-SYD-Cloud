import os
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request
from google.cloud import firestore
import scraper

app = Flask(__name__)
_lock = threading.Lock()

GCP_PROJECT = os.environ.get("GCP_PROJECT", "crack-map-317501")
TG_TOKEN = os.environ.get("TG_TOKEN", "8526676401:AAESmMiVjf7fKUi9bzcq0mMz2CJ0nzIIxxY")

def _tg_responder(chat_id: str, texto: str):
    """Envía un mensaje directo a un usuario en Telegram."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": texto, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Error respondiendo en Telegram: {e}")

@app.route("/webhook", methods=["POST"])
def webhook():
    """Recibe los mensajes de los usuarios desde Telegram."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    msg = data.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id", ""))
    texto   = msg.get("text", "").strip()
    nombre  = msg.get("from", {}).get("first_name", "")

    if not chat_id or not texto:
        return jsonify({"ok": True})

    db = firestore.Client(project=GCP_PROJECT)

    if texto == "/start":
        # Registrar usuario en la base de datos
        db.collection("bot_usuarios").document(chat_id).set({
            "chat_id":  chat_id,
            "nombre":   nombre,
            "activo":   True,
            "registro": datetime.now(timezone.utc),
        }, merge=True)
        
        _tg_responder(chat_id,
            f"Bienvenido/a <b>{nombre}</b> al bot de Transcripciones.\n\n"
            f"Desde ahora recibirás alertas automáticas cuando:\n"
            f"🎥 Se detecte una nueva sesión del Senado o Cámara.\n"
            f"✅ Se complete una transcripción de audio a texto.\n\n"
            f"Comandos:\n"
            f"/stop  — Dejar de recibir notificaciones"
        )

    elif texto == "/stop":
        db.collection("bot_usuarios").document(chat_id).update({"activo": False})
        _tg_responder(chat_id, "Has sido dado de baja. Escribe /start para volver a activarte.")

    else:
        _tg_responder(chat_id,
            "Comandos disponibles:\n"
            "/start  — Activar notificaciones\n"
            "/stop   — Desactivar notificaciones"
        )

    return jsonify({"ok": True})

@app.route("/run", methods=["POST", "GET"])
def run():
    # Ejecuta el scraper directamente
    try:
        scraper.main()
        return jsonify({"status": "completado"}), 200
    except Exception as e:
        return jsonify({"status": "error", "detalle": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)