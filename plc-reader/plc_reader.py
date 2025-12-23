import os
import json
import time
import logging
import traceback
# from datetime import datetime
from datetime import datetime, timezone


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


# =========================
# MÉTRICAS PROMETHEUS
# =========================

PLC_CONNECTED = Gauge("plc_connected", "Estado de conexión PLC (1=conectado TCP, 0=desconectado)")
PLC_READS = Counter("plc_read_total", "Total de ciclos de lectura OK")
PLC_BITS_READ = Counter("plc_bits_read_total", "Total de bits leídos del PLC")
PLC_VALUES_READ = Counter("plc_values_read_total", "Total de valores D leídos del PLC")

REDIS_WRITES = Counter("redis_stream_writes_total", "Total de eventos escritos en Redis Stream")

PLC_ERRORS = Counter("plc_errors_total", "Errores del PLC reader", ["stage", "exc"])
PLC_RECONNECTS = Counter("plc_reconnects_total", "Número de reconexiones realizadas con el PLC")

PLC_POLL_DURATION = Histogram("plc_poll_duration_seconds", "Duración del ciclo completo")
PLC_LAST_SUCCESS_UNIX = Gauge("plc_last_success_unix", "Timestamp unix de la última lectura OK")
PLC_SECONDS_SINCE_SUCCESS = Gauge("plc_seconds_since_success", "Segundos desde la última lectura OK")
PLC_LAST_ERROR_UNIX = Gauge("plc_last_error_unix", "Timestamp unix del último error detectado")


SCRIPT_HEARTBEAT = Gauge("script_last_run_timestamp_seconds", "Timestamp de la última iteración exitosa")

# =========================
# HELPERS
# =========================

def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
# def now_iso(): return datetime.utcnow().isoformat(timespec="seconds") + "Z"
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")



def setup_logger():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger("plc_reader")
    logger.setLevel(getattr(logging, level, logging.INFO))

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    log_path = os.getenv("LOG_PATH", "").strip()
    if log_path:
        try:
            from logging.handlers import RotatingFileHandler
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            fh = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            logger.warning("[LOG] No se pudo activar RotatingFileHandler; usando stdout")

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

    metrics_port = env_int("METRICS_PORT", 8000)
    logger.info(f"[METRICS] Iniciando servidor de métricas en puerto {metrics_port}")
    start_http_server(metrics_port)

    plc_ip = os.getenv("PLC_IP", "192.168.3.39")
    
    
    plc_port = env_int("PLC_PORT", 9000)
    start_m = env_int("PLC_START_M", 410)
    end_m = env_int("PLC_END_M", 1143)
    
    count_m = end_m - start_m + 1
    
    # --- Rango D ---
    start_d = env_int("PLC_TEMP_START_D", 100)
    end_d = env_int("PLC_TEMP_END_D", 199)
    count_d = end_d - start_d + 1
    
    poll = env_float("PLC_POLL_SECONDS", 1)
    reconnect_sleep = env_float("PLC_RECONNECT_SECONDS", 5.0)

    r = redis.Redis(
        host=os.getenv("PLC_REDIS_HOST", "redis"),
        port=env_int("REDIS_PORT", 6379),
        decode_responses=True,
        socket_timeout=2,
        socket_connect_timeout=2,
        # retry_on_timeout=True,
    )

    # Streams
    stream_bits = os.getenv("REDIS_STREAM_BITS", "plc:m:stream")
    stream_temps = os.getenv("REDIS_STREAM_TEMPS", "plc:temp:stream")

    maxlen = env_int("REDIS_MAXLEN", 5000)
    err_stream = os.getenv("REDIS_ERROR_STREAM_KEY", "plc:errors:stream")
    err_maxlen = env_int("REDIS_ERR_MAXLEN", 5000)

    plc = Type3E(plctype="Q")
    last_success_ts = 0.0

    def set_success():
        nonlocal last_success_ts
        last_success_ts = time.time()
        PLC_LAST_SUCCESS_UNIX.set(last_success_ts)
        PLC_SECONDS_SINCE_SUCCESS.set(0)

    while True:
        SCRIPT_HEARTBEAT.set(time.time())
        PLC_CONNECTED.set(0) # Empezamos en 0
        connected = False
        try:
            logger.info(f"[PLC] conectando a {plc_ip}:{plc_port} ...")
            plc.connect(plc_ip, plc_port)
            connected = True
            # 👇 subir timeout TCP del PLC (configurable por env)
            # try:
            #     plc._sock.settimeout(env_float("PLC_SOCKET_TIMEOUT", 5.0))
            # except Exception:
            #     pass

            PLC_CONNECTED.set(1)
            PLC_RECONNECTS.inc()
           

            while True:
                
                with PLC_POLL_DURATION.time():
                    # --- Leer M ---
                    try:
                        logger.info(f"[PLC] conectado. leyendo M{start_m}..M{end_m} ({count_m} bits)")
                        bits = plc.batchread_bitunits(f"M{start_m}", count_m)
                        bits = [int(b) for b in bits]
                    except Exception as e:
                        logger.exception("[PLC_READ_M] Error en lectura M")
                        PLC_ERRORS.labels(stage="plc_read_m", exc=type(e).__name__).inc()
                        PLC_LAST_ERROR_UNIX.set(time.time())
                        push_error_to_redis(r, err_stream, err_maxlen, "plc_read_m", e)
                        raise

                    # --- Leer D ---
                    try:
                        logger.info(f"[PLC] leyendo D{start_d}..D{end_d} ({count_d} valores)")
                        values = plc.batchread_wordunits(f"D{start_d}", count_d)
                        values = [int(v) for v in values]
                    except Exception as e:
                        logger.exception("[PLC_READ_D] Error en lectura D")
                        PLC_ERRORS.labels(stage="plc_read_d", exc=type(e).__name__).inc()
                        PLC_LAST_ERROR_UNIX.set(time.time())
                        push_error_to_redis(r, err_stream, err_maxlen, "plc_read_d", e)
                        raise
                    
                    # Construir payload
                    ts = now_iso()
                    # --- Payload bits ---
                    payload_bits = {
                        "ts": ts,
                        "start_m": start_m,
                        "end_m": end_m,
                        "bits": bits,
                    }

                    # --- Payload temps ---
                    payload_temps = {
                        "ts": ts,
                        "start_d": start_d,
                        "end_d": end_d,
                        "values": values,
                    }
                    # payload = {"ts": now_iso(), "start_m": start_m, "end_m": end_m, "bits": bits}

                    try:
                        r.xadd(stream_bits, {"data": json.dumps(payload_bits)}, maxlen=maxlen, approximate=True)
                        r.xadd(stream_temps, {"data": json.dumps(payload_temps)}, maxlen=maxlen, approximate=True)
                    except Exception as e:
                        logger.exception("[REDIS_WRITE] Error en escritura")
                        PLC_ERRORS.labels(stage="redis_write", exc=type(e).__name__).inc()
                        PLC_LAST_ERROR_UNIX.set(time.time())
                        push_error_to_redis(r, err_stream, err_maxlen, "redis_write", e)
                        raise

                    PLC_READS.inc()
                    PLC_BITS_READ.inc(count_m)
                    PLC_VALUES_READ.inc(count_d)
                    REDIS_WRITES.inc()
                    set_success()

                time.sleep(poll)

        except Exception as e:
            stage = "reset" if connected else "connect"
            logger.exception(f"[PLC] {stage} error")
            PLC_CONNECTED.set(0)
            PLC_ERRORS.labels(stage=stage, exc=type(e).__name__).inc()
            PLC_LAST_ERROR_UNIX.set(time.time())
            try: push_error_to_redis(r, err_stream, err_maxlen, stage, e)
            except Exception: logger.warning("[ERR_STREAM] fallo al guardar error (connect/reset)")
            try: plc.close()
            except Exception: pass
            time.sleep(reconnect_sleep)

        finally:
            if last_success_ts > 0:
                PLC_SECONDS_SINCE_SUCCESS.set(time.time() - last_success_ts)

if __name__ == "__main__":
    main()
 