"""
Ingestion Module - Batch & Streaming
- Batch : Scraping toutes les heures → MinIO (Bronze)
- Streaming : Kafka Producer → Kafka → Consumer → MinIO
"""

import json
import boto3
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from botocore.exceptions import ClientError
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC      = "news_articles_raw"
BRONZE_BUCKET    = "bronze"


# ─── MinIO Client ────────────────────────────────────────
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(client, bucket: str):
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' créé.")


def save_to_bronze(articles: list[dict], batch_id: str):
    """Sauvegarde les articles bruts dans MinIO (couche Bronze)"""
    client = get_minio_client()
    ensure_bucket(client, BRONZE_BUCKET)

    now = datetime.utcnow()
    key = f"articles/year={now.year}/month={now.month:02d}/day={now.day:02d}/{batch_id}.json"

    payload = json.dumps(articles, ensure_ascii=False).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info(f"✅ {len(articles)} articles sauvegardés → s3://{BRONZE_BUCKET}/{key}")
    return key


# ─── Batch Ingestion ─────────────────────────────────────
class BatchIngestion:
    """Ingestion par lot : scrape → MinIO Bronze"""

    def run(self, articles: list[dict]):
        if not articles:
            logger.warning("Aucun article à ingérer.")
            return
        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        save_to_bronze(articles, batch_id)
        logger.info(f"Batch '{batch_id}' ingéré : {len(articles)} articles.")


# ─── Streaming Ingestion ──────────────────────────────────
class StreamingProducer:
    """Publie chaque article comme événement Kafka"""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def publish(self, article: dict):
        future = self.producer.send(KAFKA_TOPIC, value=article)
        future.get(timeout=10)
        logger.info(f"📤 Article publié → Kafka: {article.get('title', '')[:60]}")

    def publish_many(self, articles: list[dict]):
        for art in articles:
            self.publish(art)
        self.producer.flush()
        logger.info(f"✅ {len(articles)} articles publiés sur Kafka.")

    def close(self):
        self.producer.close()


class StreamingConsumer:
    """Consomme les événements Kafka → MinIO Bronze"""

    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="news_consumer_group",
        )
        self.buffer = []
        self.batch_size = 10  # Micro-batch

    def run(self):
        logger.info(f"🎧 Consumer en écoute sur '{KAFKA_TOPIC}'...")
        for msg in self.consumer:
            article = msg.value
            self.buffer.append(article)
            logger.info(f"📥 Reçu: {article.get('title', '')[:60]}")

            if len(self.buffer) >= self.batch_size:
                self._flush()

    def _flush(self):
        if self.buffer:
            batch_id = f"stream_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
            save_to_bronze(self.buffer, batch_id)
            self.buffer.clear()


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"

    if mode == "producer":
        # Simuler publication streaming
        from scrapers.scraper import ArticleScraper
        scraper = ArticleScraper()
        articles = scraper.scrape_all()
        producer = StreamingProducer()
        producer.publish_many(articles)
        producer.close()

    elif mode == "consumer":
        consumer = StreamingConsumer()
        consumer.run()

    else:
        # Mode batch par défaut
        from scrapers.scraper import ArticleScraper
        scraper = ArticleScraper()
        articles = scraper.scrape_all()
        bi = BatchIngestion()
        bi.run(articles)