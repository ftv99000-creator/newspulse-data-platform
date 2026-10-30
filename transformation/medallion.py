"""
Architecture Médaillon
Bronze  → Articles bruts (JSON MinIO)
Silver  → Nettoyage : strip HTML, normalisation, détection langue
Gold    → Tables analytiques : tendances, top sujets, stats par source
"""

import json
import re
import boto3
import logging
from datetime import datetime
from collections import Counter
from langdetect import detect
import unicodedata
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

STOPWORDS_FR = {"le","la","les","un","une","des","de","du","et","en","est","pour","dans","sur","au","aux","il","elle","ils","elles","nous","vous","ce","se","qui","que","à","par","avec","mais","ou","donc","or","ni","car","pas","ne","plus","très","aussi","comme","tout","bien","même","après","avant","lors","quand","si","alors","entre","être","avoir","faire","dire","aller","voir","savoir","pouvoir","vouloir","venir"}
STOPWORDS_AR = {"في","من","إلى","على","عن","مع","هذا","هذه","التي","الذي","وقد","وكان","كان","أن","إن","لا","ما","كل","قد","بعد","قبل","حتى","هو","هي","هم","نحن","أنا","أنت"}
STOPWORDS_EN = {"the","a","an","is","are","was","were","be","been","being","have","has","had","do","does","did","will","would","could","should","may","might","shall","to","of","in","on","at","by","for","with","from","as","into","through","about","after","before","under","over","between","if","but","or","and","not","so","then","than","more","also","just","can","this","that","these","those","it","its","he","she","they","we","you","i","my","your","our","their","his","her"}

ALL_STOPWORDS = STOPWORDS_FR | STOPWORDS_AR | STOPWORDS_EN


def get_client():
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def list_objects(client, bucket, prefix=""):
    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj['Key'] for obj in resp.get('Contents', [])]
    except Exception:
        return []


def read_json_from_bucket(client, bucket, key):
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Erreur lecture {key}: {e}")
        return []


def write_json_to_bucket(client, bucket, key, data):
    ensure_bucket(client, bucket)
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType='application/json')


# ─── Bronze → Silver ─────────────────────────────────────

def strip_html(text: str) -> str:
    """Supprime les balises HTML"""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'&[a-zA-Z]+;', ' ', clean)
    return clean

def normalize_whitespace(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()

def normalize_text(text: str) -> str:
    """Normalise unicode, espaces, casse"""
    text = unicodedata.normalize('NFC', text)
    text = strip_html(text)
    text = normalize_whitespace(text)
    return text

def detect_language(text: str) -> str:
    try:
        return detect(text[:300]) if text.strip() else "unknown"
    except Exception:
        return "unknown"

def clean_article(raw: dict) -> dict:
    """Bronze → Silver : nettoyage d'un article"""
    silver = raw.copy()
    silver['title']   = normalize_text(raw.get('title', ''))
    silver['content'] = normalize_text(raw.get('content', ''))
    silver['author']  = normalize_text(raw.get('author', ''))
    silver['language'] = detect_language(silver['title'] + ' ' + silver['content'][:200])

    # Validation basique
    silver['quality_flags'] = []
    if not silver['title']:
        silver['quality_flags'].append('missing_title')
    if not silver.get('published_at'):
        silver['quality_flags'].append('missing_date')
    if len(silver['content']) < 100:
        silver['quality_flags'].append('content_too_short')

    silver['is_valid'] = len(silver['quality_flags']) == 0
    silver['processed_at'] = datetime.utcnow().isoformat()
    return silver


def bronze_to_silver(client):
    """Transforme tous les articles Bronze en Silver"""
    keys = list_objects(client, 'bronze', 'articles/')
    all_silver = []

    for key in keys:
        raw_articles = read_json_from_bucket(client, 'bronze', key)
        if isinstance(raw_articles, dict):
            raw_articles = [raw_articles]
        cleaned = [clean_article(a) for a in raw_articles]
        all_silver.extend(cleaned)

    now = datetime.utcnow()
    out_key = f"articles/year={now.year}/month={now.month:02d}/day={now.day:02d}/silver_{now.strftime('%H%M%S')}.json"
    write_json_to_bucket(client, 'silver', out_key, all_silver)
    logger.info(f"✅ Silver: {len(all_silver)} articles → s3://silver/{out_key}")
    return all_silver


# ─── Silver → Gold ───────────────────────────────────────

def extract_keywords(text: str, top_n: int = 20) -> list[str]:
    """Extrait les mots-clés les plus fréquents"""
    words = re.findall(r'\b[a-zA-ZÀ-ÿأ-ي]{4,}\b', text.lower())
    words = [w for w in words if w not in ALL_STOPWORDS]
    return [w for w, _ in Counter(words).most_common(top_n)]


def silver_to_gold(client, silver_articles: list[dict]):
    """Construit les tables analytiques Gold"""

    valid = [a for a in silver_articles if a.get('is_valid', True)]

    # ── Table 1 : Articles par source ─────────────────────
    by_source = Counter(a.get('source', 'Unknown') for a in valid)
    gold_by_source = [{"source": s, "count": c} for s, c in by_source.most_common()]

    # ── Table 2 : Articles par jour ───────────────────────
    by_day = Counter()
    for a in valid:
        day = a.get('published_at', '')[:10] or a.get('scraped_at', '')[:10]
        by_day[day] += 1
    gold_by_day = [{"date": d, "count": c} for d, c in sorted(by_day.items(), reverse=True)]

    # ── Table 3 : Articles par catégorie ──────────────────
    by_cat = Counter(a.get('category', 'General') for a in valid)
    gold_by_category = [{"category": c, "count": n} for c, n in by_cat.most_common()]

    # ── Table 4 : Articles par langue ────────────────────
    by_lang = Counter(a.get('language', 'unknown') for a in valid)
    gold_by_language = [{"language": l, "count": c} for l, c in by_lang.most_common()]

    # ── Table 5 : Top mots-clés globaux ──────────────────
    full_text = ' '.join(a.get('title', '') + ' ' + a.get('content', '')[:200] for a in valid)
    top_keywords = extract_keywords(full_text, top_n=50)
    gold_keywords = [{"keyword": kw, "rank": i+1} for i, kw in enumerate(top_keywords)]

    # ── Table 6 : Top mots-clés par source ────────────────
    kw_by_source = {}
    for src in by_source.keys():
        src_text = ' '.join(a.get('title', '') for a in valid if a.get('source') == src)
        kw_by_source[src] = extract_keywords(src_text, top_n=10)

    # ── Table 7 : Tendances (articles récents par heure) ──
    by_hour = Counter()
    for a in valid:
        hour = a.get('scraped_at', '')[:13]
        by_hour[hour] += 1
    gold_trends = [{"hour": h, "count": c} for h, c in sorted(by_hour.items(), reverse=True)[:24]]

    gold = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_articles": len(valid),
        "articles_by_source": gold_by_source,
        "articles_by_day": gold_by_day,
        "articles_by_category": gold_by_category,
        "articles_by_language": gold_by_language,
        "top_keywords": gold_keywords,
        "keywords_by_source": kw_by_source,
        "trends_by_hour": gold_trends,
    }

    now = datetime.utcnow()
    key = f"analytics/year={now.year}/month={now.month:02d}/day={now.day:02d}/gold_{now.strftime('%H%M%S')}.json"
    write_json_to_bucket(client, 'gold', key, gold)
    logger.info(f"✅ Gold: tables analytiques → s3://gold/{key}")
    return gold


# ─── Pipeline complet ─────────────────────────────────────

def run_medallion_pipeline():
    """Bronze → Silver → Gold"""
    client = get_client()
    logger.info("🥉 Bronze → Silver...")
    silver = bronze_to_silver(client)
    logger.info("🥇 Silver → Gold...")
    gold = silver_to_gold(client, silver)
    logger.info(f"✅ Pipeline Médaillon terminé. {gold['total_articles']} articles valides.")
    return gold


if __name__ == "__main__":
    run_medallion_pipeline()