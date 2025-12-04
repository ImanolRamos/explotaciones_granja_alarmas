FROM python:3.12-slim

WORKDIR /app

# venv dentro del contenedor
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/ /app/

CMD ["python", "notify.py"]
