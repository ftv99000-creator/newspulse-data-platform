"""
Module Gouvernance des Données
- Catalogue de données (métadonnées)
- Traçabilité du pipeline (lineage)
- Documentation automatique
"""

import json
import uuid
import logging
import boto3
import os
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
GOV_BUCKET       = "governance"


def get_client():
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# ─── Catalogue de données ─────────────────────────────────

@dataclass
class FieldMeta:
    name: str
    dtype: str
    description: str
    nullable: bool = True
    example: str = ""

@dataclass
class DatasetMeta:
    name: str
    layer: str           # bronze | silver | gold
    description: str
    source: str
    fields: list[FieldMeta] = field(default_factory=list)
    owner: str = "IADATA Team"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    tags: list[str] = field(default_factory=list)
    record_count: int = 0
    storage_path: str = ""


CATALOG = {
    "bronze_articles": DatasetMeta(
        name="bronze_articles",
        layer="bronze",
        description="Articles bruts collectés par le scraper, sans nettoyage. Conserve le HTML original.",
        source="Web Scraping (Hespress, Al Jazeera, BBC, CNN, Reuters...)",
        storage_path="s3://bronze/articles/year=*/month=*/day=*/*.json",
        tags=["raw", "news", "scraping"],
        fields=[
            FieldMeta("id",           "string",    "Hash MD5 de l'URL, identifiant unique",  False, "a3f8d2..."),
            FieldMeta("title",        "string",    "Titre de l'article",                      False, "Le budget 2026 adopté"),
            FieldMeta("author",       "string",    "Auteur ou journaliste",                   True,  "Karim Alaoui"),
            FieldMeta("published_at", "datetime",  "Date de publication ISO8601",             True,  "2026-05-09T08:30:00"),
            FieldMeta("category",     "string",    "Catégorie éditoriale",                    True,  "Politique"),
            FieldMeta("content",      "text",      "Contenu complet (peut contenir HTML)",    True,  "<p>Le gouvernement..."),
            FieldMeta("source",       "string",    "Nom du site source",                      False, "Hespress"),
            FieldMeta("source_country","string",   "Code pays ISO2 de la source",             True,  "MA"),
            FieldMeta("url",          "string",    "URL complète de l'article",               False, "https://..."),
            FieldMeta("language",     "string",    "Langue détectée (ISO 639-1)",             True,  "ar"),
            FieldMeta("scraped_at",   "datetime",  "Horodatage de collecte",                  False, "2026-05-09T09:00:00"),
            FieldMeta("raw_html",     "text",      "HTML brut du bloc article",               True,  "<article>...</article>"),
        ]
    ),
    "silver_articles": DatasetMeta(
        name="silver_articles",
        layer="silver",
        description="Articles nettoyés : HTML supprimé, texte normalisé, langue détectée, qualité évaluée.",
        source="Bronze → Transformation Python",
        storage_path="s3://silver/articles/year=*/month=*/day=*/*.json",
        tags=["cleaned", "normalized", "news"],
        fields=[
            FieldMeta("id",             "string",   "Identifiant unique",                    False),
            FieldMeta("title",          "string",   "Titre nettoyé sans HTML",               False),
            FieldMeta("content",        "text",     "Contenu nettoyé et normalisé",          True),
            FieldMeta("language",       "string",   "Langue re-détectée après nettoyage",    True),
            FieldMeta("quality_flags",  "array",    "Liste des problèmes détectés",          True,  "['content_too_short']"),
            FieldMeta("is_valid",       "boolean",  "Article valide (0 flags critiques)",    False, "true"),
            FieldMeta("processed_at",   "datetime", "Horodatage de traitement Silver",       False),
        ]
    ),
    "gold_analytics": DatasetMeta(
        name="gold_analytics",
        layer="gold",
        description="Tables analytiques agrégées pour visualisation et BI.",
        source="Silver → Agrégations Python/Spark",
        storage_path="s3://gold/analytics/year=*/month=*/day=*/*.json",
        tags=["analytics", "aggregated", "kpi"],
        fields=[
            FieldMeta("articles_by_source",   "array", "Nombre d'articles par source"),
            FieldMeta("articles_by_day",      "array", "Nombre d'articles par jour"),
            FieldMeta("articles_by_category", "array", "Répartition par catégorie"),
            FieldMeta("top_keywords",         "array", "Top 50 mots-clés globaux"),
            FieldMeta("trends_by_hour",       "array", "Tendances par heure (streaming)"),
        ]
    ),
}


# ─── Lineage Tracker ──────────────────────────────────────

class LineageTracker:
    """Enregistre la traçabilité de chaque exécution du pipeline"""

    def __init__(self):
        self.client = get_client()
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=GOV_BUCKET)
        except Exception:
            self.client.create_bucket(Bucket=GOV_BUCKET)

    def log_run(self, run_id: str, step: str, source_layer: str,
                target_layer: str, record_count: int,
                status: str = "success", metadata: dict = None) -> str:
        """Enregistre une étape du pipeline"""
        entry = {
            "run_id": run_id,
            "step": step,
            "source_layer": source_layer,
            "target_layer": target_layer,
            "record_count": record_count,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {},
        }
        key = f"lineage/{run_id}/{step}_{datetime.utcnow().strftime('%H%M%S')}.json"
        self.client.put_object(
            Bucket=GOV_BUCKET,
            Key=key,
            Body=json.dumps(entry, ensure_ascii=False).encode(),
            ContentType="application/json",
        )
        logger.info(f"📝 Lineage [{step}]: {record_count} records | {source_layer}→{target_layer} | {status}")
        return key

    def get_run_history(self, limit: int = 20) -> list[dict]:
        """Récupère l'historique des runs"""
        try:
            resp = self.client.list_objects_v2(Bucket=GOV_BUCKET, Prefix="lineage/")
            keys = sorted([obj['Key'] for obj in resp.get('Contents', [])], reverse=True)[:limit]
            runs = []
            for key in keys:
                obj = self.client.get_object(Bucket=GOV_BUCKET, Key=key)
                runs.append(json.loads(obj['Body'].read()))
            return runs
        except Exception as e:
            logger.error(f"Erreur lecture lineage: {e}")
            return []


# ─── Catalogue Manager ────────────────────────────────────

class DataCatalog:
    """Gère le catalogue de métadonnées"""

    def __init__(self):
        self.client = get_client()
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=GOV_BUCKET)
        except Exception:
            self.client.create_bucket(Bucket=GOV_BUCKET)

    def publish(self):
        """Publie le catalogue complet dans MinIO"""
        catalog_export = {
            "exported_at": datetime.utcnow().isoformat(),
            "datasets": {k: asdict(v) for k, v in CATALOG.items()},
            "pipeline_summary": {
                "layers": ["bronze", "silver", "gold"],
                "sources": ["Hespress", "Akhbarona", "Barlamane", "Al Jazeera", "BBC News", "CNN", "Reuters"],
                "ingestion_modes": ["batch (hourly)", "streaming (kafka)"],
                "storage": "MinIO (S3-compatible)",
                "orchestration": "Apache Airflow",
                "warehouse": "PostgreSQL",
            }
        }
        self.client.put_object(
            Bucket=GOV_BUCKET,
            Key="catalog/data_catalog.json",
            Body=json.dumps(catalog_export, ensure_ascii=False, indent=2).encode(),
            ContentType="application/json",
        )
        logger.info("✅ Catalogue publié → s3://governance/catalog/data_catalog.json")
        return catalog_export

    def get_dataset(self, name: str) -> Optional[DatasetMeta]:
        return CATALOG.get(name)

    def list_datasets(self) -> list[str]:
        return list(CATALOG.keys())


if __name__ == "__main__":
    # Initialiser la gouvernance
    catalog = DataCatalog()
    cat = catalog.publish()
    print(f"✅ Catalogue publié: {len(cat['datasets'])} datasets")

    tracker = LineageTracker()
    run_id = str(uuid.uuid4())[:8]
    tracker.log_run(run_id, "scraping", "web", "bronze", 150, "success", {"sources": ["Hespress", "BBC"]})
    tracker.log_run(run_id, "cleaning", "bronze", "silver", 147, "success")
    tracker.log_run(run_id, "aggregation", "silver", "gold", 147, "success")
    print(f"✅ Lineage enregistré pour run {run_id}")