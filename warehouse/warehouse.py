"""
Data Warehouse – PostgreSQL
Schéma en étoile pour analyses décisionnelles
"""

import psycopg2
import psycopg2.extras
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PG_HOST     = os.getenv('PG_HOST', 'localhost')
PG_PORT     = os.getenv('PG_PORT', '5432')
PG_DB       = os.getenv('PG_DB', 'newswarehouse')
PG_USER     = os.getenv('PG_USER', 'newsuser')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'newspass')


DDL = """
-- ── Dimensions ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_source (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) UNIQUE NOT NULL,
    country     CHAR(2),
    language    VARCHAR(10),
    url_base    TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_date (
    id          SERIAL PRIMARY KEY,
    full_date   DATE UNIQUE NOT NULL,
    year        SMALLINT,
    month       SMALLINT,
    day         SMALLINT,
    day_of_week SMALLINT,
    week        SMALLINT,
    quarter     SMALLINT,
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_category (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) UNIQUE NOT NULL,
    parent      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_language (
    code        CHAR(5) PRIMARY KEY,
    name        VARCHAR(50)
);

-- ── Fait central ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_articles (
    id              VARCHAR(64) PRIMARY KEY,
    title           TEXT NOT NULL,
    author          VARCHAR(200),
    content_length  INT,
    url             TEXT,
    source_id       INT REFERENCES dim_source(id),
    date_id         INT REFERENCES dim_date(id),
    category_id     INT REFERENCES dim_category(id),
    language_code   CHAR(5) REFERENCES dim_language(code),
    quality_score   FLOAT,
    is_valid        BOOLEAN DEFAULT TRUE,
    scraped_at      TIMESTAMP,
    processed_at    TIMESTAMP DEFAULT NOW()
);

-- ── Tables analytiques (Gold) ─────────────────────────────

CREATE TABLE IF NOT EXISTS agg_articles_per_source_day (
    source_id   INT REFERENCES dim_source(id),
    date_id     INT REFERENCES dim_date(id),
    article_count INT,
    avg_length  FLOAT,
    PRIMARY KEY (source_id, date_id)
);

CREATE TABLE IF NOT EXISTS agg_keywords (
    id          SERIAL PRIMARY KEY,
    keyword     VARCHAR(100) NOT NULL,
    source_id   INT REFERENCES dim_source(id),
    date_id     INT REFERENCES dim_date(id),
    frequency   INT,
    UNIQUE (keyword, source_id, date_id)
);

CREATE TABLE IF NOT EXISTS agg_trends (
    id          SERIAL PRIMARY KEY,
    topic       VARCHAR(200),
    category_id INT REFERENCES dim_category(id),
    date_id     INT REFERENCES dim_date(id),
    article_count INT,
    trend_score FLOAT
);

-- ── Gouvernance / Lineage ─────────────────────────────────

CREATE TABLE IF NOT EXISTS data_lineage (
    id              SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    source_layer    VARCHAR(20),   -- bronze | silver | gold
    target_layer    VARCHAR(20),
    record_count    INT,
    status          VARCHAR(20),   -- success | failed | partial
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    metadata        JSONB
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    id              SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    checked_at      TIMESTAMP DEFAULT NOW(),
    total_articles  INT,
    valid_articles  INT,
    validity_rate   FLOAT,
    top_errors      JSONB
);

-- ── Index de performance ─────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_articles(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_source ON fact_articles(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_category ON fact_articles(category_id);
CREATE INDEX IF NOT EXISTS idx_kw_keyword ON agg_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_trends_date ON agg_trends(date_id);
"""

SEED_DATA = """
-- Langues
INSERT INTO dim_language VALUES ('fr','Français') ON CONFLICT DO NOTHING;
INSERT INTO dim_language VALUES ('ar','Arabe')    ON CONFLICT DO NOTHING;
INSERT INTO dim_language VALUES ('en','Anglais')  ON CONFLICT DO NOTHING;
INSERT INTO dim_language VALUES ('es','Espagnol') ON CONFLICT DO NOTHING;

-- Catégories par défaut
INSERT INTO dim_category (name) VALUES
  ('Politique'),('Économie'),('Sport'),('Culture'),
  ('International'),('Société'),('Technologie'),('Santé'),('General')
ON CONFLICT DO NOTHING;

-- Sources
INSERT INTO dim_source (name, country, language, url_base) VALUES
  ('Hespress',   'MA', 'ar', 'https://www.hespress.com'),
  ('Akhbarona',  'MA', 'ar', 'https://www.akhbarona.com'),
  ('Barlamane',  'MA', 'ar', 'https://www.barlamane.com'),
  ('Al Jazeera', 'QA', 'ar', 'https://www.aljazeera.com'),
  ('BBC News',   'GB', 'en', 'https://www.bbc.com/news'),
  ('CNN',        'US', 'en', 'https://www.cnn.com'),
  ('Reuters',    'GB', 'en', 'https://www.reuters.com')
ON CONFLICT DO NOTHING;
"""


class DataWarehouse:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD
        )
        self.conn.autocommit = False

    def initialize(self):
        """Crée le schéma et insère les données de référence"""
        with self.conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(SEED_DATA)
            self._populate_dim_date(cur)
        self.conn.commit()
        logger.info("✅ Data Warehouse initialisé.")

    def _populate_dim_date(self, cur, years=(2024, 2025, 2026)):
        """Pré-popule la dimension date"""
        from datetime import date, timedelta
        cur.execute("SELECT COUNT(*) FROM dim_date")
        if cur.fetchone()[0] > 0:
            return
        d = date(years[0], 1, 1)
        end = date(years[-1], 12, 31)
        rows = []
        while d <= end:
            rows.append((d, d.year, d.month, d.day, d.weekday(), d.isocalendar()[1],
                         (d.month - 1) // 3 + 1, d.weekday() >= 5))
            d += timedelta(days=1)
        psycopg2.extras.execute_values(cur,
            "INSERT INTO dim_date (full_date,year,month,day,day_of_week,week,quarter,is_weekend) VALUES %s ON CONFLICT DO NOTHING",
            rows)

    def load_articles(self, articles: list[dict]):
        """Charge les articles dans fact_articles"""
        with self.conn.cursor() as cur:
            for art in articles:
                # Résoudre source_id
                cur.execute("SELECT id FROM dim_source WHERE name=%s", (art.get('source'),))
                row = cur.fetchone()
                source_id = row[0] if row else None

                # Résoudre date_id
                pub = art.get('published_at', '')[:10]
                cur.execute("SELECT id FROM dim_date WHERE full_date=%s", (pub,))
                row = cur.fetchone()
                date_id = row[0] if row else None

                # Résoudre category_id
                cur.execute("SELECT id FROM dim_category WHERE name=%s", (art.get('category', 'General'),))
                row = cur.fetchone()
                if not row:
                    cur.execute("INSERT INTO dim_category (name) VALUES (%s) RETURNING id", (art.get('category', 'General'),))
                    row = cur.fetchone()
                category_id = row[0] if row else None

                cur.execute("""
                    INSERT INTO fact_articles
                        (id, title, author, content_length, url, source_id, date_id, category_id, language_code, is_valid, scraped_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET
                        title=EXCLUDED.title, processed_at=NOW()
                """, (
                    art.get('id', ''),
                    art.get('title', '')[:500],
                    art.get('author', '')[:200],
                    len(art.get('content', '')),
                    art.get('url', ''),
                    source_id, date_id, category_id,
                    art.get('language', 'en')[:5],
                    art.get('is_valid', True),
                    art.get('scraped_at'),
                ))
        self.conn.commit()
        logger.info(f"✅ {len(articles)} articles chargés dans fact_articles")

    def log_quality(self, run_id: str, report: dict):
        with self.conn.cursor() as cur:
            import json as _json
            cur.execute("""
                INSERT INTO data_quality_log (pipeline_run_id, total_articles, valid_articles, validity_rate, top_errors)
                VALUES (%s,%s,%s,%s,%s)
            """, (run_id, report['total_articles'], report['valid_articles'],
                  report['validity_rate'], _json.dumps(report.get('top_errors', []))))
        self.conn.commit()

    def log_lineage(self, run_id: str, src: str, tgt: str, count: int, status: str, meta: dict = None):
        with self.conn.cursor() as cur:
            import json as _json
            cur.execute("""
                INSERT INTO data_lineage (pipeline_run_id, source_layer, target_layer, record_count, status, started_at, finished_at, metadata)
                VALUES (%s,%s,%s,%s,%s,NOW(),NOW(),%s)
            """, (run_id, src, tgt, count, status, _json.dumps(meta or {})))
        self.conn.commit()

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    dw = DataWarehouse()
    dw.initialize()
    dw.close()
    print("✅ Data Warehouse prêt.")