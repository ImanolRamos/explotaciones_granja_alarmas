import os
import json
import time
from dotenv import load_dotenv

import redis
import paho.mqtt.client as mqtt

# Añadir la importación
from prometheus_client import start_http_server, Gauge, Counter

# 1. Definir las métricas globalmente
REDIS_CONSUMED = Counter('redis_messages_consumed_total', 'Total mensajes consumidos de Redis Stream')
MQTT_PUBLISHED = Counter('mqtt_messages_published_total', 'Total mensajes individuales MQTT publicados')
MQTT_BATCH_PUBLISHED = Counter('mqtt_batches_published_total', 'Total mensajes batch MQTT publicados')


def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
def env_bool(k, d=False):
    v = os.getenv(k, str(d)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


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
    c.on_connect = lambda client, userdata, flags, rc: print(f"[MQTT] connected rc={rc}")
    c.on_disconnect = lambda client, userdata, rc: print(f"[MQTT] disconnected rc={rc}")
    c.connect(host, port, keepalive=30)
    c.loop_start()
    return c


def should_publish_by_rules(addr: int, value: int, bits: list[int]) -> bool:
    # PLANTILLA: aquí metes tu lógica de negocio real
    # ejemplo: publicar siempre M1100 y cualquier bit a 1.
    if addr == 1100:
        return True
    return value == 1


def main():
    load_dotenv()
    
    # 2. Iniciar el servidor HTTP de métricas antes del bucle principal
    print("[METRICS] Iniciando servidor de metricas en puerto 8000")
    start_http_server(8000)

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=env_int("REDIS_PORT", 6379),
        decode_responses=True,
    )

    stream = os.getenv("REDIS_STREAM_KEY", "plc:m:stream")
    group = "ingestors"
    consumer = os.getenv("MQTT_CLIENT_ID", "edge-ingestor-1")  # nombre consumer

    # consumer group (idempotente)
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    topic_base = os.getenv("MQTT_TOPIC_BASE", "plc/m").rstrip("/")
    json_topic = os.getenv("JSON_TOPIC", f"{topic_base}/batch")
    qos = env_int("MQTT_QOS", 0)
    retain = env_bool("MQTT_RETAIN", False)

    publish_mode = os.getenv("PUBLISH_MODE", "changed").strip().lower()  # changed|all|rules
    min_interval = env_float("PUBLISH_MIN_INTERVAL", 1.0)
    always_batch = env_bool("ALWAYS_PUBLISH_BATCH", True)

    mqttc = mqtt_client()

    last_bits = None
    last_publish_t = 0.0

    print(f"[INGESTOR] consuming stream={stream} group={group} consumer={consumer} mode={publish_mode}")

    while True:
        # lee 1 evento cada vez (puedes subir COUNT si quieres)
        resp = r.xreadgroup(group, consumer, {stream: ">"}, count=1, block=5000)
        if not resp:
            continue

        _, messages = resp[0]
        msg_id, fields = messages[0]

        data = json.loads(fields["data"])
        bits = data["bits"]
        start_m = int(data["start_m"])
        count = len(bits)
        ts = data.get("ts")  # ya viene del reader
        payload_array = [
            {"memoria": f"M{start_m + i}", "valor": int(bits[i]), "ts": ts}
            for i in range(count)
        ]
        # anti-spam global
        now = time.time()
        can_publish = (now - last_publish_t) >= min_interval

        # decide qué publicar
        indices = []
        if publish_mode == "all":
            indices = list(range(count))
        else:
            # changed o rules parten de cambios si hay last_bits
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
                mqttc.publish(f"{topic_base}/{addr}", str(int(bits[i])), qos=qos, retain=retain)
                MQTT_PUBLISHED.inc() # <- Métrica: publicación individual

            last_publish_t = now

        if always_batch and can_publish:
            mqttc.publish(json_topic, json.dumps(payload_array), qos=qos, retain=retain)
            MQTT_BATCH_PUBLISHED.inc() # <- Métrica: publicación batch
            last_publish_t = now

        last_bits = bits

        # ack
        r.xack(stream, group, msg_id)
        REDIS_CONSUMED.inc() # <- Métrica: mensaje consumido


if __name__ == "__main__":
    main()
