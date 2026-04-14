"""
Servidor HTTP mínimo para Cloud Run.
Cloud Scheduler hace POST a /run → ejecuta el scraper → responde OK.
"""
import threading
from flask import Flask, jsonify
import scraper

app = Flask(__name__)

@app.route("/run", methods=["POST", "GET"])
def run():
    # Corre el scraper en un thread para no bloquear la respuesta HTTP
    t = threading.Thread(target=scraper.main)
    t.start()
    return jsonify({"status": "iniciado"}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)