import os
import time
import json
import threading
import requests
from flask import Flask, jsonify
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Gauge, Counter

# =========================
# CONFIGURACIÓN
# =========================
VIEWPOWER_BASE = os.getenv("VIEWPOWER_BASE", "http://192.168.3.40:15178")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))

# Configuración MQTT
MQTT_HOST = os.getenv("MQTT_HOST", "explotaciones.koiote.es")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USERNAME", "")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.getenv("MQTT_UPS_TOPIC", "vpower/ups01/status")
MQTT_CLIENT_ID = os.getenv("MQTT_SAI_CLIENT_ID", "ups-monitor-bridge")

# Define las métricas al inicio
SAI_STATUS = Gauge("sai_online", "1 si el bridge conecta con la API del SAI, 0 si no")
SAI_BATTERY = Gauge("sai_battery_percent", "Porcentaje de batería")
SAI_LOAD = Gauge("sai_load_percent", "Carga del SAI")
SAI_MQTT_OK = Gauge("sai_mqtt_connected", "Estado conexión MQTT")
SAI_POLLS = Counter("sai_poll_total", "Número de peticiones a la API del SAI")


SCRIPT_HEARTBEAT = Gauge("script_last_run_timestamp_seconds", "Timestamp de la última iteración exitosa")


app = Flask(__name__)
sess = requests.Session()

_last_norm = {"ok": False, "error": "not polled yet"}

# =========================
# CLIENTE MQTT
# =========================
def get_mqtt_client():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)
    
    if MQTT_USER or MQTT_PASS:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] ✅ Conectado a {MQTT_HOST}")
        else:
            print(f"[MQTT] ❌ Error de conexión: {rc}")

    client.on_connect = on_connect
    client.reconnect_delay_set(1, 30)
    
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start() # Hilo separado para mantener la conexión viva
        return client
    except Exception as e:
        print(f"[MQTT] ❌ No se pudo conectar: {e}")
        return None

mqttc = get_mqtt_client()

# =========================
# LÓGICA DE PROCESAMIENTO (Tus funciones originales)
# =========================
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
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s in ("----", "---", "", "N/A", "na", "null"): return None
        try: return float(s.replace(",", "."))
        except ValueError: return None
    return None

def normalize(raw: dict) -> dict:
    # (Mantenemos tu lógica de normalización intacta)
    live = raw.get("workInfo", {}) if isinstance(raw, dict) else {}
    
    work_mode = pick(live, "workMode")
    source = "unknown"
    wm = str(work_mode or "").lower()
    if "line" in wm: source = "mains"
    elif "battery" in wm: source = "battery"
    elif "bypass" in wm: source = "mains"

    return {
        "ok": True,
        "ts": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "source": source,
        "status_raw": work_mode,
        "battery_percent": to_number(pick(live, "batteryCapacity")),
        "battery_remain_min": to_number(pick(live, "batteryRemainTime")),
        "load_percent": to_number(pick(live, "load", "loadPercent")),
        "input_voltage": to_number(pick(live, "inputVoltage")),
        "output_voltage": to_number(pick(live, "outputVoltage")),
        "output_frequency": to_number(pick(live, "outputFrequency")),
        "ups_type": pick(live, "upsType"),
    }

# =========================
# BUCLE DE POLLING Y ENVÍO
# =========================
def poll_loop():
    # Iniciamos el servidor de métricas (mejor si lo mueves al __main__ como comentamos)
    start_http_server(9107) 
    global _last_norm
    
    while True:
        try:
            raw = fetch_req_monitor_data()
            _last_norm = normalize(raw)
            
            if _last_norm.get("ok"):
                # 1. Lógica de Batería vs Línea (Tu propuesta)
                # Extraemos el status_raw (ej: "Line Mode" o "Battery Mode")
                status_raw = str(_last_norm.get("status_raw", "")).upper()
                
                if "LINE" in status_raw or "BYPASS" in status_raw:
                    SAI_STATUS.set(1)  # 1 = RED ELÉCTRICA
                elif "BATTERY" in status_raw:
                    SAI_STATUS.set(0)  # 0 = MODO BATERÍA
                else:
                    SAI_STATUS.set(2)  # 2 = DESCONOCIDO / OTRO
                
                # 2. Actualizar el resto de métricas numéricas
                SAI_BATTERY.set(_last_norm.get("battery_percent") or 0)
                SAI_LOAD.set(_last_norm.get("load_percent") or 0)
            else:
                # Si la API del SAI no responde, marcamos como desconocido
                SAI_STATUS.set(2)

            # 3. Métricas de control del script
            SAI_POLLS.inc() 
            SAI_MQTT_OK.set(1 if mqttc and mqttc.is_connected() else 0)
            SCRIPT_HEARTBEAT.set(time.time()) 
            
            # 🚀 Envío MQTT original
            if mqttc and mqttc.is_connected():
                payload = json.dumps(_last_norm)
                mqttc.publish(MQTT_TOPIC, payload, qos=1, retain=True)
                print(f"[POLL] Datos enviados a {MQTT_TOPIC} | Status: {status_raw}", flush=True)
            
        except Exception as e:
            SAI_STATUS.set(2) # Error de conexión = Desconocido
            _last_norm = {"ok": False, "error": str(e)}
            print(f"[ERROR] {e}", flush=True)
            
        time.sleep(POLL_SECONDS)
# =========================
# ENDPOINTS FLASK
# =========================
@app.get("/status")
def status():
    return jsonify(_last_norm)

@app.get("/health")
def health():
    return jsonify({"ok": True, "mqtt_connected": mqttc.is_connected() if mqttc else False})

if __name__ == "__main__":
    # Lanzar el poller en segundo plano
    threading.Thread(target=poll_loop, daemon=True).start()
    # Lanzar servidor API
    app.run(host="0.0.0.0", port=HTTP_PORT)