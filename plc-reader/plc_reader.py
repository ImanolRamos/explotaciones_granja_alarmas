import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from pymcprotocol import Type3E
import redis

from prometheus_client import (
    start_http_server,
    Gauge,
    Counter,
    Histogram,
)

# =========================
# MÉTRICAS PROMETHEUS
# =========================

PLC_CONNECTED = Gauge(
    "plc_connection_status",
    "Status de la conexion con el PLC (1=up, 0=down)",
)

PLC_READS = Counter(
    "plc_read_total",
    "Total de lecturas exitosas del PLC",
)

PLC_BITS_READ = Counter(
    "plc_bits_read_total",
    "Total de bits leídos del PLC",
)

REDIS_WRITES = Counter(
    "redis_stream_writes_total",
    "Total de eventos escritos en Redis Stream",
)

READ_ERRORS = Counter(
    "plc_read_errors_total",
    "Errores de conexion o lectura del PLC",
    ["stage"],
)

PLC_POLL_DURATION = Histogram(
    "plc_poll_duration_seconds",
    "Duración del ciclo completo de lectura y push a Redis",
    buckets=(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)

PLC_LAST_SUCCESS_UNIX = Gauge(
    "plc_last_success_unix",
    "Timestamp unix de la última lectura OK",
)

PLC_SECONDS_SINCE_SUCCESS = Gauge(
    "plc_seconds_since_success",
    "Segundos desde la última lectura OK",
)

# =========================
# HELPERS
# =========================

def env_int(k, d): 
    return int(os.getenv(k, d))

def env_float(k, d): 
    return float(os.getenv(k, d))

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# =========================
# MAIN
# =========================

def main():
    load_dotenv()

    print("[METRICS] Iniciando servidor de métricas en puerto 8000")
    start_http_server(8000)

    plc_ip = os.getenv("PLC_IP", "192.168.3.39")
    plc_port = env_int("PLC_PORT", 9000)

    start_m = env_int("PLC_START_M", 410)
    end_m = env_int("PLC_END_M", 1143)
    count = end_m - start_m + 1
    poll = env_float("PLC_POLL_SECONDS", 0.2)

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=env_int("REDIS_PORT", 6379),
        decode_responses=True,
    )

    key_last = os.getenv("REDIS_KEY_LAST_BITS", "plc:m:last_bits")
    stream = os.getenv("REDIS_STREAM_KEY", "plc:m:stream")
    maxlen = env_int("REDIS_MAXLEN", 5000)

    plc = Type3E(plctype="Q")

    last_success_ts = 0.0

    while True:
        try:
            plc.connect(plc_ip, plc_port)
            print(f"[PLC] connected {plc_ip}:{plc_port} reading M{start_m}..M{end_m}")
            PLC_CONNECTED.set(1)

            while True:
                cycle_start = time.time()

                with PLC_POLL_DURATION.time():
                    bits = plc.batchread_bitunits(f"M{start_m}", count)
                    bits = [int(b) for b in bits]

                    payload = {
                        "ts": now_iso(),
                        "start_m": start_m,
                        "end_m": end_m,
                        "bits": bits,
                    }

                    # Guardar último estado
                    r.set(key_last, json.dumps(payload))

                    # Push a Redis Stream
                    r.xadd(
                        stream,
                        {"data": json.dumps(payload)},
                        maxlen=maxlen,
                        approximate=True,
                    )

                PLC_READS.inc()
                PLC_BITS_READ.inc(count)
                REDIS_WRITES.inc()

                last_success_ts = time.time()
                PLC_LAST_SUCCESS_UNIX.set(last_success_ts)
                PLC_SECONDS_SINCE_SUCCESS.set(0)

                time.sleep(poll)

        except Exception as e:
            print(f"[ERROR] {e} -> reconnect in 2s")
            PLC_CONNECTED.set(0)
            READ_ERRORS.labels(stage="connect_or_read").inc()

            try:
                plc.close()
            except Exception:
                pass

            time.sleep(2)

        finally:
            if last_success_ts > 0:
                PLC_SECONDS_SINCE_SUCCESS.set(time.time() - last_success_ts)


if __name__ == "__main__":
    main()
