FROM python:3.11-slim

LABEL maintainer="EMSI-IADATA 2025/2026"
LABEL description="News Big Data Platform - Scraping & Pipeline"

WORKDIR /app

# Dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code source
COPY . .

# Variables d'environnement par défaut
ENV MINIO_ENDPOINT=http://minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV PG_HOST=postgres-dw
ENV PG_DB=newswarehouse
ENV PG_USER=newsuser
ENV PG_PASSWORD=newspass
ENV PYTHONPATH=/app

CMD ["python", "-m", "orchestration.dags"]