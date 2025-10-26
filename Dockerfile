# Dockerfile para o serviço app
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

ENV MINIO_ENDPOINT=minio:9000 \
    MINIO_ROOT_USER=minioadmin \
    MINIO_ROOT_PASSWORD=minioadmin \
    DB_HOST=sptrans_db \
    DB_PORT=5432 \
    DB_NAME=sptrans \
    DB_USER=postgres \
    DB_PASSWORD=postgres

ENTRYPOINT ["python", "main.py"]