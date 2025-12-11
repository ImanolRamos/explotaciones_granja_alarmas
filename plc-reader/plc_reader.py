import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from pymcprotocol import Type3E
import redis


def env_int(k, d): return int(os.getenv(k, d))
def env_float(k, d): return float(os.getenv(k, d))
def now_iso(): return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def main():
    load_dotenv()

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
