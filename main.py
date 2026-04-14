import threading
from flask import Flask, jsonify
import scraper

app = Flask(__name__)
_lock = threading.Lock()

@app.route("/run", methods=["POST", "GET"])
def run():
    # Ejecuta el scraper directamente (no en thread)
    # Cloud Run tiene timeout de 3600s configurado
    try:
        scraper.main()
        return jsonify({"status": "completado"}), 200
    except Exception as e:
        return jsonify({"status": "error", "detalle": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
