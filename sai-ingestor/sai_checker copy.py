import os
import time
import threading
import requests
from flask import Flask, jsonify

# Mejor usar localhost si Flask corre en la misma máquina que ViewPower:
# VIEWPOWER_BASE = os.getenv("VIEWPOWER_BASE", "http://127.0.0.1:15178")
VIEWPOWER_BASE = os.getenv("VIEWPOWER_BASE", "http://192.168.3.40:15178")

HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))

app = Flask(__name__)
sess = requests.Session()

_last_raw = None
_last_norm = {"ok": False, "error": "not polled yet"}

def fetch_req_monitor_data():
    url = f"{VIEWPOWER_BASE}/ViewPower/workstatus/reqMonitorData"
    r = sess.post(url, timeout=5)
    r.raise_for_status()
    return r.json()

def pick(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d.get(k) is not None:
            return d.get(k)
    return default

def to_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s in ("----", "---", "", "N/A", "na", "null"):
            return None
        try:
            return float(s.replace(",", "."))
        except ValueError:
            return None
    return None

def guess_live_block(raw: dict) -> dict:
    # En tu ViewPower el “live” viene en workInfo
    if isinstance(raw, dict) and isinstance(raw.get("workInfo"), dict):
        return raw["workInfo"]
    return raw if isinstance(raw, dict) else {}

def normalize(raw: dict) -> dict:
    live = guess_live_block(raw)

    input_v = to_number(pick(live, "inputVoltage"))
    input_f = to_number(pick(live, "inputFrequency"))
    output_v = to_number(pick(live, "outputVoltage"))
    output_f = to_number(pick(live, "outputFrequency"))

    load_pct = to_number(pick(live, "load", "loadPercent", "upsLoad"))
    batt_pct = to_number(pick(live, "batteryCapacity"))
    remain_min = to_number(pick(live, "batteryRemainTime"))
    ups_type = pick(live, "upsType")

    work_mode = pick(live, "workMode")  # <-- clave real (ej: "Line mode")

    # source a partir de workMode
    source = "unknown"
    wm = str(work_mode or "").lower()
    if "line" in wm:
        source = "mains"
    elif "battery" in wm:
        source = "battery"
    elif "bypass" in wm:
        source = "mains"

    return {
        "ok": True,
        "source": source,
        "status_raw": work_mode,
        "battery_percent": batt_pct,
        "battery_remain_min": remain_min,
        "load_percent": load_pct,
        "input_voltage": input_v,
        "input_frequency": input_f,
        "output_voltage": output_v,
        "output_frequency": output_f,
        "ups_type": ups_type,
    }

def poll_loop():
    global _last_raw, _last_norm
    while True:
        try:
            raw = fetch_req_monitor_data()
            _last_raw = raw
            _last_norm = normalize(raw)
            print(_last_norm, flush=True)
        except Exception as e:
            _last_norm = {"ok": False, "error": str(e)}
            print(_last_norm, flush=True)
        time.sleep(POLL_SECONDS)

@app.get("/status")
def status():
    return jsonify(_last_norm)

@app.get("/raw")
def raw():
    return jsonify(_last_raw if _last_raw is not None else {"ok": False, "error": "no data yet"})

@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    threading.Thread(target=poll_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=HTTP_PORT)