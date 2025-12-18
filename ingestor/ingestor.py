import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import redis
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ... (Métricas se mantienen igual) ...
REDIS_CONSUMED = Counter("redis_messages_consumed_total", "Total mensajes consumidos")
REDIS_PENDING = Gauge("redis_stream_pending", "Mensajes pendientes")
MQTT_PUBLISHED = Counter("mqtt_messages_published_total", "Mensajes publicados")
MQTT_BATCH_PUBLISHED = Counter("mqtt_batches_published_total", "Batches publicados")
MQTT_PUBLISH_ERRORS = Counter("mqtt_publish_errors_total", "Errores MQTT", ["type"])
PUBLISH_SKIPPED = Counter("publish_skipped_total", "Skipped por intervalo")
PIPELINE_LAG_SECONDS = Histogram("pipeline_lag_seconds", "Lag PLC-Ingestor", buckets=(0.1, 0.5, 1, 5, 10))
MQTT_PUBLISH_DURATION = Histogram("mqtt_publish_duration_seconds", "Duración publish")
MQTT_PUBLISHED_PER_EVENT = Histogram("mqtt_published_per_event", "Mensajes por evento")

def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
def env_bool(k, d=False): return os.getenv(k, str(d)).lower() in ("1", "true", "yes")
def iso_to_epoch(s): return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

# =========================
# MQTT CON DIAGNÓSTICO
# =========================
def mqtt_client():
    host = os.getenv("MQTT_HOST")
    port = env_int("MQTT_PORT", 1883)
    user = os.getenv("MQTT_USERNAME", "")
    pwd = os.getenv("MQTT_PASSWORD", "")
    cid = os.getenv("MQTT_CLIENT_ID", "edge-ingestor-1")

    print(f"[MQTT] Configurando cliente para {host}:{port}...")
    c = mqtt.Client(client_id=cid, clean_session=True)
    
    def on_connect(client, userdata, flags, rc):
        codes = {0: "Éxito", 1: "Protocolo incorrecto", 2: "ID inválido", 3: "Servidor offline", 4: "User/Pass mal", 5: "No autorizado"}
        if rc == 0:
            print(f"[MQTT] ✅ CONECTADO con éxito a {host}")
        else:
            print(f"[MQTT] ❌ ERROR de conexión: {codes.get(rc, f'Código {rc}')}")

    def on_disconnect(client, userdata, rc):
        print(f"[MQTT] ⚠️ Desconectado. Reintentando automáticamente...")

    c.on_connect = on_connect
    c.on_disconnect = on_disconnect
    
    if user or pwd:
        c.username_pw_set(user, pwd)

    c.reconnect_delay_set(1, 30)
    
    try:
        # connect_async no bloquea el inicio del script
        print(f"[MQTT] Iniciando conexión asíncrona a {host}...")
        c.connect_async(host, port, keepalive=30)
        c.loop_start()
    except Exception as e:
        print(f"[MQTT] ❌ Error crítico al configurar cliente: {e}")
        
    return c

def should_publish_by_rules(addr: int, value: int, bits: list[int]) -> bool:
    if addr == 1100: return True
    return value == 1

def main():
    load_dotenv()

    print("[METRICS] Iniciando servidor de métricas en puerto 9105")
    start_http_server(9105)

    print("[REDIS] Conectando a la base de datos...")
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=env_int("REDIS_PORT", 6379),
        decode_responses=True,
    )

    stream = os.getenv("REDIS_STREAM_KEY", "plc:m:stream")
    group = "ingestors"
    consumer = os.getenv("MQTT_CLIENT_ID", "edge-ingestor-1")

    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
        print(f"[REDIS] Consumer group '{group}' verificado/creado.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            print(f"[REDIS] Error al crear grupo: {e}")

    mqttc = mqtt_client()

    # Variables de estado
    topic_base = os.getenv("MQTT_TOPIC_BASE", "plc/m").rstrip("/")
    json_topic = os.getenv("JSON_TOPIC", f"{topic_base}/batch")
    qos = env_int("MQTT_QOS", 0)
    retain = env_bool("MQTT_RETAIN", False)
    publish_mode = os.getenv("PUBLISH_MODE", "changed").strip().lower()
    min_interval = env_float("PUBLISH_MIN_INTERVAL", 1.0)
    always_batch = env_bool("ALWAYS_PUBLISH_BATCH", True)

    last_bits = None
    last_publish_t = 0.0
    last_pending_check = 0.0

    print(f"[INGESTOR] 🚀 TODO LISTO. Escuchando stream: {stream}")

    while True:
        try:
            resp = r.xreadgroup(group, consumer, {stream: ">"}, count=1, block=5000)
            if not resp:
                continue

            _, messages = resp[0]
            msg_id, fields = messages[0]
            data = json.loads(fields["data"])
            bits, start_m, ts = data["bits"], int(data["start_m"]), data.get("ts")

            if ts: PIPELINE_LAG_SECONDS.observe(time.time() - iso_to_epoch(ts))

            now = time.time()
            can_publish = (now - last_publish_t) >= min_interval
            
            # Lógica de detección de cambios e índices
            indices = []
            if publish_mode == "all":
                indices = list(range(len(bits)))
            else:
                if last_bits is None:
                    indices = list(range(len(bits)))
                else:
                    indices = [i for i in range(len(bits)) if bits[i] != last_bits[i]]
                if publish_mode == "rules":
                    indices = [i for i in indices if should_publish_by_rules(start_m + i, bits[i], bits)]

            if indices and can_publish:
                for i in indices:
                    addr = start_m + i
                    with MQTT_PUBLISH_DURATION.time():
                        info = mqttc.publish(f"{topic_base}/{addr}", str(int(bits[i])), qos=qos, retain=retain)
                        if info.rc != mqtt.MQTT_ERR_SUCCESS: MQTT_PUBLISH_ERRORS.labels(type="single").inc()
                        else: MQTT_PUBLISHED.inc()
                last_publish_t = now

            if always_batch and can_publish:
                payload = [{"memoria": f"M{start_m+i}", "valor": int(bits[i]), "ts": ts} for i in range(len(bits))]
                mqttc.publish(json_topic, json.dumps(payload), qos=qos, retain=retain)
                MQTT_BATCH_PUBLISHED.inc()
                last_publish_t = now

            last_bits = bits
            r.xack(stream, group, msg_id)
            REDIS_CONSUMED.inc()

            if now - last_pending_check > 5:
                try:
                    p = r.xpending(stream, group)
                    REDIS_PENDING.set(p["pending"])
                except: pass
                last_pending_check = now
        except Exception as e:
            print(f"[ERROR] Error en bucle principal: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()