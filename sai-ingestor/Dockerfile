
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sai_checker.py .
CMD ["python", "-u", "sai_checker.py"]