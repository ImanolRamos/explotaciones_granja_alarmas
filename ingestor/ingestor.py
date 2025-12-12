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
    "redis_messages_consumed_total",
    "Total mensajes consumidos de Redis Stream",
)

REDIS_PENDING = Gauge(
    "redis_stream_pending",
    "Mensajes pendientes en el consumer group de Redis",
)

MQTT_PUBLISHED = Counter(
    "mqtt_messages_published_total",
    "Total mensajes individuales MQTT publicados",
)

MQTT_BATCH_PUBLISHED = Counter(
    "mqtt_batches_published_total",
    "Total mensajes batch MQTT publicados",
)

MQTT_PUBLISH_ERRORS = Counter(
    "mqtt_publish_errors_total",
    "Errores publicando a MQTT",
    ["type"],
)

PUBLISH_SKIPPED = Counter(
    "publish_skipped_total",
    "Eventos donde no se publicó por min_interval",
)

PIPELINE_LAG_SECONDS = Histogram(
    "pipeline_lag_seconds",
    "Lag desde ts del PLC reader hasta procesar en ingestor",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60),
)

MQTT_PUBLISH_DURATION = Histogram(
    "mqtt_publish_duration_seconds",
    "Duración del publish MQTT",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1),
)

MQTT_PUBLISHED_PER_EVENT = Histogram(
    "mqtt_published_per_event",
    "Mensajes MQTT publicados por evento",
    buckets=(0, 1, 2, 5, 10, 20, 50, 100, 500, 1000),
)

# =========================
# HELPERS
# =========================

def env_int(k, d): 
    return int(os.getenv(k, d))

def env_float(k, d): 
    return float(os.getenv(k, d))

def env_bool(k, d=False):
    v = os.getenv(k, str(d)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def iso_to_epoch(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

# =========================
# MQTT
# =========================

def mqtt_client():
    host = os.getenv("MQTT_HOST")
    port = env_int("MQTT_PORT", 1883)
    user = os.getenv("MQTT_USERNAME", "")
    pwd = os.getenv("MQTT_PASSWORD", "")
    cid = os.getenv("MQTT_CLIENT_ID", "edge-ingestor-1")

    c = mqtt.Client(client_id=cid, clean_session=True)
    if user or pwd:
        c.username_pw_set(user, pwd)

    c.reconnect_delay_set(1, 30)
    c.connect(host, port, keepalive=30)
    c.loop_start()
    return c

# =========================
# BUSINESS RULES
# =========================

def should_publish_by_rules(addr: int, value: int, bits: list[int]) -> bool:
    if addr == 1100:
        return True
    return value == 1

# =========================
# MAIN
# =========================

def main():
    load_dotenv()

    print("[METRICS] Iniciando servidor de métricas en puerto 8000")
    start_http_server(8000)

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
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    topic_base = os.getenv("MQTT_TOPIC_BASE", "plc/m").rstrip("/")
    json_topic = os.getenv("JSON_TOPIC", f"{topic_base}/batch")
    qos = env_int("MQTT_QOS", 0)
    retain = env_bool("MQTT_RETAIN", False)

    publish_mode = os.getenv("PUBLISH_MODE", "changed").strip().lower()
    min_interval = env_float("PUBLISH_MIN_INTERVAL", 1.0)
    always_batch = env_bool("ALWAYS_PUBLISH_BATCH", True)

    mqttc = mqtt_client()

    last_bits = None
    last_publish_t = 0.0
    last_pending_check = 0.0

    print(f"[INGESTOR] stream={stream} group={group} consumer={consumer}")

    while True:
        resp = r.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=1,
            block=5000,
        )

        if not resp:
            continue

        _, messages = resp[0]
        msg_id, fields = messages[0]

        data = json.loads(fields["data"])
        bits = data["bits"]
        start_m = int(data["start_m"])
        ts = data.get("ts")

        if ts:
            PIPELINE_LAG_SECONDS.observe(time.time() - iso_to_epoch(ts))

        count = len(bits)
        payload_array = [
            {
                "memoria": f"M{start_m + i}",
                "valor": int(bits[i]),
                "ts": ts,
            }
            for i in range(count)
        ]

        now = time.time()
        can_publish = (now - last_publish_t) >= min_interval

        indices = []
        if publish_mode == "all":
            indices = list(range(count))
        else:
            if last_bits is None:
                indices = list(range(count))
            else:
                indices = [i for i in range(count) if bits[i] != last_bits[i]]

            if publish_mode == "rules":
                indices = [
                    i for i in indices
                    if should_publish_by_rules(start_m + i, bits[i], bits)
                ]

        if indices and can_publish:
            for i in indices:
                addr = start_m + i
                with MQTT_PUBLISH_DURATION.time():
                    info = mqttc.publish(
                        f"{topic_base}/{addr}",
                        str(int(bits[i])),
                        qos=qos,
                        retain=retain,
                    )
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    MQTT_PUBLISH_ERRORS.labels(type="single").inc()
                else:
                    MQTT_PUBLISHED.inc()

            MQTT_PUBLISHED_PER_EVENT.observe(len(indices))
            last_publish_t = now

        elif indices and not can_publish:
            PUBLISH_SKIPPED.inc()

        if always_batch and can_publish:
            with MQTT_PUBLISH_DURATION.time():
                info = mqttc.publish(
                    json_topic,
                    json.dumps(payload_array),
                    qos=qos,
                    retain=retain,
                )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                MQTT_PUBLISH_ERRORS.labels(type="batch").inc()
            else:
                MQTT_BATCH_PUBLISHED.inc()

            last_publish_t = now

        last_bits = bits

        r.xack(stream, group, msg_id)
        REDIS_CONSUMED.inc()

        # actualizar pending cada 5s
        if now - last_pending_check > 5:
            pending = r.xpending(stream, group)["pending"]
            REDIS_PENDING.set(pending)
            last_pending_check = now


if __name__ == "__main__":
    main()
