import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
import redis
import paho.mqtt.client as mqtt

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram,
)

# =========================
# MÉTRICAS PROMETHEUS
# =========================

REDIS_CONSUMED = Counter(
    "temp_ingestor_redis_consumed_total",
    "Mensajes de temperatura consumidos del Redis Stream",
)

REDIS_PENDING = Gauge(
    "temp_ingestor_pending",
    "Mensajes pendientes en el consumer group de temperaturas",
)

MQTT_PUBLISHED = Counter(
    "temp_ingestor_mqtt_published_total",
    "Mensajes individuales de temperatura publicados a MQTT",
)

MQTT_BATCH_PUBLISHED = Counter(
    "temp_ingestor_mqtt_batch_published_total",
    "Mensajes batch de temperatura publicados a MQTT",
)

MQTT_ERRORS = Counter(
    "temp_ingestor_mqtt_errors_total",
    "Errores publicando temperaturas a MQTT",
    ["type"],
)

PIPELINE_LAG_SECONDS = Histogram(
    "temp_ingestor_pipeline_lag_seconds",
    "Lag desde ts del PLC reader hasta procesar en ingestor",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10),
)

MQTT_PUBLISH_DURATION = Histogram(
    "temp_ingestor_mqtt_publish_duration_seconds",
    "Duración del publish MQTT",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5),
)

def iso_to_epoch(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

# =========================
# MQTT
# =========================

def mqtt_client():
    host = os.getenv("MQTT_HOST")
    port = int(os.getenv("MQTT_PORT", 1883))
    user = os.getenv("MQTT_USERNAME", "")
    pwd = os.getenv("MQTT_PASSWORD", "")
    cid = os.getenv("MQTT_CLIENT_ID", "temp-ingestor")

    c = mqtt.Client(client_id=cid, clean_session=True)
    if user or pwd:
        c.username_pw_set(user, pwd)

    c.reconnect_delay_set(1, 30)
    c.connect(host, port, keepalive=30)
    c.loop_start()
    return c

# =========================
# MAIN
# =========================

def main():
    load_dotenv()

    print("[TEMP-INGESTOR] Iniciando servidor de métricas en puerto 9106")
    start_http_server(9106)

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    stream = os.getenv("REDIS_TEMP_STREAM", "plc:temp:stream")
    group = "temp_ingestors"
    consumer = os.getenv("MQTT_CLIENT_ID", "temp-ingestor")

    # Crear consumer group si no existe
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except redis.exceptions.ResponseError:
        pass

    mqttc = mqtt_client()

    topic_base = os.getenv("MQTT_TEMPERATURE_TOPIC", "temperatures").rstrip("/")
    qos = int(os.getenv("MQTT_QOS", 0))
    retain = os.getenv("MQTT_RETAIN", "false").lower() == "true"

    print(f"[TEMP-INGESTOR] stream={stream} group={group} consumer={consumer}")

    last_pending_check = 0

    while True:
        resp = r.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=10,
            block=5000,
        )

        if not resp:
            continue

        _, messages = resp[0]

        for msg_id, fields in messages:
            try:
                data = json.loads(fields["data"])
            except Exception:
                print("⚠️ [TEMP-INGESTOR] JSON inválido en stream")
                r.xack(stream, group, msg_id)
                continue

            # Validación estricta
            if "start_d" not in data or "values" not in data:
                print(f"⚠️ [TEMP-INGESTOR] Payload inválido: {data}")
                r.xack(stream, group, msg_id)
                continue

            ts = data.get("ts")
            if ts:
                PIPELINE_LAG_SECONDS.observe(time.time() - iso_to_epoch(ts))

            start_d = int(data["start_d"])
            values = data["values"]

            # Publicación individual por sensor
            for i, val in enumerate(values):
                sensor = start_d + i
                payload = {
                    "sensor": sensor,
                    "value": val,
                    "ts": ts,
                }

                print(f"[TEMP-INGESTOR] Publicando D{sensor}: {val}")

                topic = f"{topic_base}/{sensor}"

                with MQTT_PUBLISH_DURATION.time():
                    info = mqttc.publish(topic, json.dumps(payload), qos=qos, retain=retain)

                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    MQTT_ERRORS.labels(type="single").inc()
                else:
                    MQTT_PUBLISHED.inc()

            # Publicación batch
            batch_topic = f"{topic_base}/batch"
            with MQTT_PUBLISH_DURATION.time():
                info = mqttc.publish(batch_topic, json.dumps(data), qos=qos, retain=retain)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                MQTT_ERRORS.labels(type="batch").inc()
            else:
                MQTT_BATCH_PUBLISHED.inc()

            r.xack(stream, group, msg_id)
            REDIS_CONSUMED.inc()

        # Actualizar pending cada 5s
        now = time.time()
        if now - last_pending_check > 5:
            pending = r.xpending(stream, group)["pending"]
            REDIS_PENDING.set(pending)
            last_pending_check = now


if __name__ == "__main__":
    main()
