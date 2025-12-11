FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY plc_reader.py .
CMD ["python", "-u", "plc_reader.py"]
