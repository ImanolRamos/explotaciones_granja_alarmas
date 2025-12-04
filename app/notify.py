#!/usr/bin/env python3
import os
import socket
import time
import requests

PUSHOVER_API = "https://api.pushover.net/1/messages.json"

def notify_group(message: str, title: str = "Explotaciones", priority: int = 0):
    token = os.environ["PUSHOVER_APP_TOKEN"]
    group_key = os.environ["PUSHOVER_GROUP_KEY"]

    payload = {
        "token": token,
        "user": group_key,
        "message": message,
        "title": title,
        "priority": str(priority),
    }

    headers = {"User-Agent": f"explotaciones-notifier/1.0 ({socket.gethostname()})"}

    r = requests.post(PUSHOVER_API, data=payload, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != 1:
        raise RuntimeError(f"Pushover error: {data}")
    return data

if __name__ == "__main__":
    msg = os.getenv("MESSAGE", f"Ping desde {socket.gethostname()} @ {time.strftime('%F %T')}")
    title = os.getenv("TITLE", "SCADA - Notifier")
    priority = int(os.getenv("PRIORITY", "0"))

    resp = notify_group(msg, title=title, priority=priority)
    print("OK:", resp)
