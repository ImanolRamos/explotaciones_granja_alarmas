import os
import socket
import time
import requests

PUSHOVER_API = "https://api.pushover.net/1/messages.json"

class PushoverError(RuntimeError):
    pass

def _sound_for_priority(priority: int) -> str | None:
    # Ajusta a tu gusto (sonidos válidos dependen de Pushover)
    return {
        -2: "none",     # silencio total
        -1: "none",     # silencioso
         0: None,       # usa el sonido por defecto del usuario/app
         1: "siren",    # más “agresivo”
         2: "siren",    # emergencia (además reintenta)
    }.get(priority, None)

def send_to_group(
    *,
    title: str,
    message: str,
    priority: int = 0,
    client: str | None = None,
    sound: str | None = None,
    # Solo para priority=2:
    retry: int | None = None,     # segundos, mínimo 30
    expire: int | None = None,    # segundos, máximo 10800 (3h)
    callback: str | None = None,  # URL para ACK (opcional)
    timestamp: int | None = None, # epoch seconds (opcional)
) -> dict:
    token = os.environ["PUSHOVER_APP_TOKEN"]
    group_key = os.environ["PUSHOVER_GROUP_KEY"]

    if priority not in (-2, -1, 0, 1, 2):
        raise ValueError("priority must be one of -2,-1,0,1,2")

    # Si quieres incorporar el cliente al título/mensaje:
    if client:
        title = f"[{client}] {title}"

    payload = {
        "token": token,
        "user": group_key,
        "title": title,
        "message": message,
        "priority": str(priority),
    }

    # Sonido: si no te lo pasan, lo calculamos por prioridad
    final_sound = sound if sound is not None else _sound_for_priority(priority)
    if final_sound is not None:
        payload["sound"] = final_sound

    # Timestamp opcional
    if timestamp is None:
        timestamp = int(time.time())
    payload["timestamp"] = str(timestamp)

    # Emergencia: retry/expire obligatorios
    if priority == 2:
        if retry is None or expire is None:
            raise ValueError("For priority=2 you must set retry and expire")
        if retry < 30:
            raise ValueError("retry must be >= 30 seconds")
        if expire > 10800:
            raise ValueError("expire must be <= 10800 seconds (3 hours)")
        payload["retry"] = str(retry)
        payload["expire"] = str(expire)
        if callback:
            payload["callback"] = callback

    headers = {"User-Agent": f"explotaciones-notifier/1.0 ({socket.gethostname()})"}

    r = requests.post(PUSHOVER_API, data=payload, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()

    if data.get("status") != 1:
        raise PushoverError(str(data))

    return data
