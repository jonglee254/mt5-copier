"""
MT5 copier relay hub — runs on PythonAnywhere as a Flask web app.

The master client POSTs its full list of open positions to /push every ~0.5s.
Slave clients GET /state and reconcile against it. The hub never touches MT5.

Set COPIER_TOKEN (WSGI file env var) or edit TOKEN below. Use a long random string.
"""
import hmac
import json
import os
import threading
import time

from flask import Flask, abort, jsonify, request

app = Flask(__name__)

TOKEN = os.environ.get("COPIER_TOKEN", "CHANGE_ME_LONG_RANDOM_STRING")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshot.json")
_lock = threading.Lock()


def _auth():
    sent = request.headers.get("X-Token", "")
    if not hmac.compare_digest(sent.encode(), TOKEN.encode()):
        abort(401)


def _load():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


@app.get("/")
def index():
    return "copier hub ok"


@app.post("/push")
def push():
    _auth()
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not isinstance(data.get("positions"), list):
        abort(400)
    snap = {
        "ts": time.time(),
        "balance": float(data.get("balance", 0)),
        "equity": float(data.get("equity", 0)),
        "positions": data["positions"],
    }
    with _lock:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        os.replace(tmp, STATE_FILE)
    return jsonify(ok=True, n=len(snap["positions"]))


@app.get("/state")
def state():
    _auth()
    snap = _load()
    if snap is None:
        return jsonify(snapshot_age=1e9, balance=0, equity=0, positions=[])
    snap["snapshot_age"] = time.time() - snap["ts"]  # computed on hub clock; avoids clock-skew issues
    return jsonify(snap)
