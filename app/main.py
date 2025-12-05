import os
import socket
from typing import Optional, Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from pushover import send_to_group, PushoverError

app = FastAPI(title="Pushover Group Notifier", version="1.0.0")

Priority = Literal[-2, -1, 0, 1, 2]

class NotifyRequest(BaseModel):
    client: str = Field(..., min_length=1, max_length=80, description="Identificador del cliente/origen")
    message: str = Field(..., min_length=1, max_length=1024)
    priority: Priority = 0
    title: Optional[str] = Field(None, max_length=100)

    # NUEVO: argumentos opcionales
    sound: Optional[str] = Field(None, description="Nombre de sonido de Pushover (ej: siren, none, ...)")
    retry: Optional[int] = Field(None, description="Solo priority=2: intervalo de reintentos (>=30)")
    expire: Optional[int] = Field(None, description="Solo priority=2: tiempo máximo (<=10800)")
    callback: Optional[str] = Field(None, description="Solo priority=2: URL callback para ACK (opcional)")

class NotifyResponse(BaseModel):
    delivered: bool
    pushover: Optional[dict] = None
    reason: Optional[str] = None

def business_rules(req: NotifyRequest) -> tuple[bool, str, int, str, Optional[str], Optional[int], Optional[int], Optional[str]]:
    """
    Devuelve: (should_send, title, priority, message, sound, retry, expire, callback)
    """
    title = req.title or "Explotaciones"
    title = f"[{req.client}] {title}"

    # Aquí meteremos reglas después (silencios, horarios, filtros, escalados, etc.)
    return True, title, int(req.priority), req.message, req.sound, req.retry, req.expire, req.callback

def check_api_key(x_api_key: Optional[str]) -> None:
    expected = os.getenv("API_KEY")
    if expected:
        if not x_api_key or x_api_key != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/health")
def health():
    return {"ok": True, "host": socket.gethostname()}

@app.post("/notify", response_model=NotifyResponse)
def notify(payload: NotifyRequest, x_api_key: Optional[str] = Header(None)):
    check_api_key(x_api_key)

    should_send, title, priority, message, sound, retry, expire, callback = business_rules(payload)
    if not should_send:
        return NotifyResponse(delivered=False, reason="Blocked by business rules")

    try:
        resp = send_to_group(
            title=title,
            message=message,
            priority=priority,
            client=payload.client,
            sound=sound,
            retry=retry,
            expire=expire,
            callback=callback,
        )
        return NotifyResponse(delivered=True, pushover=resp)
    except PushoverError as e:
        raise HTTPException(status_code=502, detail=f"Pushover rejected: {e}")
    except ValueError as e:
        # Para errores tipo "priority=2 requiere retry/expire"
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")
