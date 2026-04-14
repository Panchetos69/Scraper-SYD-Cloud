import threading
from flask import Flask, jsonify
import scraper

app = Flask(__name__)

@app.route("/run", methods=["POST", "GET"])
def run():
    t = threading.Thread(target=scraper.main)
    t.start()
    return jsonify({"status": "iniciado"}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
