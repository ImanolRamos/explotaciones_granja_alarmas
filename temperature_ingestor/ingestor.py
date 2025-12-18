import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import redis
import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# =========================
# MÉTRICAS PROMETHEUS
# =========================
REDIS_CONSUMED = Counter("temp_ingestor_redis_consumed_total", "Mensajes consumidos")
REDIS_PENDING = Gauge("temp_ingestor_pending", "Mensajes pendientes")
MQTT_PUBLISHED = Counter("temp_ingestor_mqtt_published_total", "Publicaciones OK")
MQTT_ERRORS = Counter("temp_ingestor_mqtt_errors_total", "Errores MQTT", ["type"])
MQTT_PUBLISH_DURATION = Histogram("temp_ingestor_mqtt_publish_duration_seconds", "Duración publish")
PIPELINE_LAG_SECONDS = Histogram("temp_ingestor_pipeline_lag_seconds", "Lag desde PLC", buckets=(0.1, 0.5, 1, 2, 5))

def iso_to_epoch(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except:
        return time.time()

# =========================
# MQTT CON DIAGNÓSTICO
# =========================
def mqtt_client():
    host = os.getenv("MQTT_HOST", "explotaciones.koiote.es")
    port = int(os.getenv("MQTT_PORT", 1883))
    user = os.getenv("MQTT_USERNAME", "")
    pwd = os.getenv("MQTT_PASSWORD", "")
    cid = os.getenv("MQTT_TEMPERATURE_CLIENT_ID", "temp-ingestor")
    
    # IMPORTANTE: No usamos CallbackAPIVersion para mantener compatibilidad simple
    c = mqtt.Client(client_id=cid, clean_session=True)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] ✅ CONECTADO con éxito a {host}")
        else:
            print(f"[MQTT] ❌ ERROR de conexión: Código {rc} (4:Pass mal, 5:No autorizado)")

    def on_publish(client, userdata, mid):
        print(f"[MQTT] 📤 Publicación confirmada por el broker (ID: {mid})")

    def on_disconnect(client, userdata, rc):
        print(f"[MQTT] ⚠️ Desconectado. Código: {rc}. Reintentando...")

    c.on_connect = on_connect
    c.on_publish = on_publish
    c.on_disconnect = on_disconnect

    if user or pwd:
        c.username_pw_set(user, pwd)

    c.reconnect_delay_set(1, 30)
    
    try:
        print(f"[MQTT] Intentando conectar a {host}...")
        c.connect(host, port, keepalive=30)
        c.loop_start()
    except Exception as e:
        print(f"[MQTT] ❌ No se pudo iniciar conexión: {e}")
        
    return c

# =========================
# MAIN
# =========================
def main():
    load_dotenv()

    # CAMBIO: Puerto 9106
    print("[TEMP-INGESTOR] Iniciando métricas en puerto 9106")
    start_http_server(9106)

    # Redis usa el nombre del servicio 'redis'
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    stream = os.getenv("REDIS_TEMP_STREAM", "plc:temp:stream")
    group = "temp_ingestors"
    consumer = os.getenv("MQTT_CLIENT_ID", "temp-ingestor")

    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except:
        pass

    mqttc = mqtt_client()
    
    topic_base = os.getenv("MQTT_TEMPERATURE_TOPIC", "temperatures").rstrip("/")
    qos = int(os.getenv("MQTT_QOS", 0))
    retain = os.getenv("MQTT_RETAIN", "false").lower() == "true"

    print(f"[TEMP-INGESTOR] Procesando {stream}...")

    last_pending_check = 0

    while True:
        resp = r.xreadgroup(group, consumer, {stream: ">"}, count=10, block=5000)
        if not resp:
            continue

        _, messages = resp[0]

        for msg_id, fields in messages:
            try:
                data = json.loads(fields["data"])
                ts = data.get("ts")
                if ts:
                    PIPELINE_LAG_SECONDS.observe(time.time() - iso_to_epoch(ts))

                start_d = int(data["start_d"])
                values = data["values"]

                # 1. Publicar cada sensor por separado
                for i, val in enumerate(values):
                    sensor = start_d + i
                    payload = {"sensor": sensor, "value": val, "ts": ts}
                    topic = f"{topic_base}/{sensor}"
                    
                    with MQTT_PUBLISH_DURATION.time():
                        info = mqttc.publish(topic, json.dumps(payload), qos=qos, retain=retain)
                        if info.rc != mqtt.MQTT_ERR_SUCCESS:
                            MQTT_ERRORS.labels(type="single").inc()
                        else:
                            MQTT_PUBLISHED.inc()

                # 2. Publicar el Batch completo
                batch_topic = f"{topic_base}/batch"
                mqttc.publish(batch_topic, fields["data"], qos=qos, retain=retain)

                r.xack(stream, group, msg_id)
                REDIS_CONSUMED.inc()

            except Exception as e:
                print(f"⚠️ Error en bucle: {e}")

        # Actualizar métricas pendientes
        if time.time() - last_pending_check > 5:
            try:
                p = r.xpending(stream, group)
                REDIS_PENDING.set(p["pending"])
                last_pending_check = time.time()
            except: pass

if __name__ == "__main__":
    main()