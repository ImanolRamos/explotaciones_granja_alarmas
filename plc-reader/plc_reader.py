import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from pymcprotocol import Type3E
import redis

# Añadir la importación
import time
from prometheus_client import start_http_server, Gauge, Counter, Summary 

# 1. Definir las métricas globalmente
PLC_CONNECTED = Gauge('plc_connection_status', 'Status de la conexion con el PLC (1=up, 0=down)')
PLC_READS = Counter('plc_read_total', 'Total de lecturas exitosas del PLC')
REDIS_WRITES = Counter('redis_stream_writes_total', 'Total de eventos escritos en Redis Stream')
READ_ERRORS = Counter('plc_read_errors_total', 'Total de errores de conexion/lectura con el PLC')


def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
def now_iso(): return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def main():
    load_dotenv()
    
    print("[METRICS] Iniciando servidor de metricas en puerto 8000")
    start_http_server(8000)

    plc_ip = os.getenv("PLC_IP", "192.168.3.39")
    plc_port = env_int("PLC_PORT", 9000)
    start_m = env_int("PLC_START_M", 1100)
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

    while True:
        try:
            plc.connect(plc_ip, plc_port)
            print(f"[PLC] connected {plc_ip}:{plc_port} reading M{start_m}..M{end_m}")
            PLC_CONNECTED.set(1) # <- Métrica: conectado

            while True:
                bits = plc.batchread_bitunits(f"M{start_m}", count)
                bits = [int(b) for b in bits]

                payload = {
                    "ts": now_iso(),
                    "start_m": start_m,
                    "end_m": end_m,
                    "bits": bits,
                }

                # Persist “last known”
                r.set(key_last, json.dumps(payload))

                # Push event to stream
                r.xadd(stream, {"data": json.dumps(payload)}, maxlen=maxlen, approximate=True)
                
                PLC_READS.inc()    # <- Métrica: lectura exitosa
                REDIS_WRITES.inc() # <- Métrica: escritura en Redis

                time.sleep(poll)

        except Exception as e:
            print(f"[ERROR] {e} -> reconnect in 2s")
            try:
                plc.close()
            except Exception:
                pass
            time.sleep(2)


if __name__ == "__main__":
    main()
