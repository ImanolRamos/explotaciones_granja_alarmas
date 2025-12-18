# temperatures/temperature_reader.py
import os
import json
import time
import logging
import traceback
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

PLC_CONNECTED = Gauge("plc_temp_connected", "Estado de conexión PLC para temperaturas")
PLC_READS = Counter("plc_temp_read_total", "Total de lecturas OK de temperaturas")
PLC_VALUES_READ = Counter("plc_temp_values_read_total", "Total de valores de temperatura leídos")
REDIS_WRITES = Counter("redis_temp_stream_writes_total", "Eventos de temperatura escritos en Redis Stream")
PLC_ERRORS = Counter("plc_temp_errors_total", "Errores del lector de temperaturas", ["stage", "exc"])
PLC_RECONNECTS = Counter("plc_temp_reconnects_total", "Reconexiones del lector de temperaturas")
PLC_POLL_DURATION = Histogram("plc_temp_poll_duration_seconds", "Duración del ciclo de lectura de temperaturas")
PLC_LAST_SUCCESS_UNIX = Gauge("plc_temp_last_success_unix", "Timestamp unix de última lectura OK")
PLC_SECONDS_SINCE_SUCCESS = Gauge("plc_temp_seconds_since_success", "Segundos desde última lectura OK")
PLC_LAST_ERROR_UNIX = Gauge("plc_temp_last_error_unix", "Timestamp unix del último error")

# =========================
# HELPERS
# =========================

def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
def now_iso(): return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def setup_logger():
    logger = logging.getLogger("plc_temp_reader")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger

def push_error_to_redis(r, stream, maxlen, stage, exc):
    r.xadd(
        stream,
        {
            "ts": now_iso(),
            "stage": stage,
            "exc": type(exc).__name__,
            "msg": str(exc)[:500],
            "tb": traceback.format_exc()[:1500],
        },
        maxlen=maxlen,
        approximate=True,
    )

# =========================
# MAIN
# =========================

def main():
    load_dotenv()
    logger = setup_logger()

    metrics_port = env_int("TEMP_METRICS_PORT", 8001)
    logger.info(f"[TEMP-METRICS] Iniciando servidor de métricas en puerto {metrics_port}")
    start_http_server(metrics_port)

    plc_ip = os.getenv("PLC_IP", "192.168.3.39")
    plc_port = env_int("PLC_PORT", 9000)

    # Rango de registros de temperatura
    start_d = env_int("PLC_TEMP_START_D", 100)
    end_d = env_int("PLC_TEMP_END_D", 199)
    count = end_d - start_d + 1

    poll = env_float("PLC_TEMP_POLL_SECONDS", 1.0)
    reconnect_sleep = env_float("PLC_TEMP_RECONNECT_SECONDS", 2.0)

    # Redis
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=env_int("REDIS_PORT", 6379),
        decode_responses=True,
    )

    key_last = os.getenv("REDIS_TEMP_LAST_KEY", "plc:temp:last")
    data_stream = os.getenv("REDIS_TEMP_STREAM", "plc:temp:stream")
    maxlen = env_int("REDIS_TEMP_MAXLEN", 5000)
    err_stream = os.getenv("REDIS_TEMP_ERROR_STREAM", "plc:temp:errors")
    err_maxlen = env_int("REDIS_TEMP_ERR_MAXLEN", 5000)

    plc = Type3E(plctype="Q")
    last_success_ts = 0.0

    def set_success():
        nonlocal last_success_ts
        last_success_ts = time.time()
        PLC_LAST_SUCCESS_UNIX.set(last_success_ts)
        PLC_SECONDS_SINCE_SUCCESS.set(0)

    while True:
        connected = False
        try:
            logger.info(f"[PLC-TEMP] Conectando a {plc_ip}:{plc_port} ...")
            plc.connect(plc_ip, plc_port)
            connected = True
            PLC_CONNECTED.set(1)
            PLC_RECONNECTS.inc()
            logger.info(f"[PLC-TEMP] Conectado. Leyendo D{start_d}..D{end_d} ({count} registros)")

            while True:
                with PLC_POLL_DURATION.time():
                    try:
                        values = plc.batchread_wordunits(f"D{start_d}", count)
                        # Convertir a enteros
                        values = [int(v) for v in values]
                    except Exception as e:
                        logger.exception("[PLC-TEMP-READ] Error en lectura")
                        PLC_ERRORS.labels(stage="plc_read", exc=type(e).__name__).inc()
                        PLC_LAST_ERROR_UNIX.set(time.time())
                        push_error_to_redis(r, err_stream, err_maxlen, "plc_read", e)
                        raise

                    # Construir payload
                    payload = {
                        "ts": now_iso(),
                        "start_d": start_d,
                        "end_d": end_d,
                        "values": values,
                    }
                    
                    print(f"[PLC-TEMP-READ] Payload leído: {payload}")

                    try:
                        r.set(key_last, json.dumps(payload))
                        r.xadd(data_stream, {"data": json.dumps(payload)}, maxlen=maxlen, approximate=True)
                    except Exception as e:
                        logger.exception("[REDIS-TEMP-WRITE] Error en escritura")
                        PLC_ERRORS.labels(stage="redis_write", exc=type(e).__name__).inc()
                        PLC_LAST_ERROR_UNIX.set(time.time())
                        push_error_to_redis(r, err_stream, err_maxlen, "redis_write", e)
                        raise

                    PLC_READS.inc()
                    PLC_VALUES_READ.inc(count)
                    REDIS_WRITES.inc()
                    set_success()

                time.sleep(poll)

        except Exception as e:
            stage = "reset" if connected else "connect"
            logger.exception(f"[PLC-TEMP] {stage} error")
            PLC_CONNECTED.set(0)
            PLC_ERRORS.labels(stage=stage, exc=type(e).__name__).inc()
            PLC_LAST_ERROR_UNIX.set(time.time())
            push_error_to_redis(r, err_stream, err_maxlen, stage, e)
            try: plc.close()
            except Exception: pass
            time.sleep(reconnect_sleep)

        finally:
            if last_success_ts > 0:
                PLC_SECONDS_SINCE_SUCCESS.set(time.time() - last_success_ts)

if __name__ == "__main__":
    main()
