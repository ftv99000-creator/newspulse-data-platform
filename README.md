# News Big Data Platform

> Plateforme Big Data de collecte, traitement et analyse d'articles de presse  
> EMSI Casablanca — Filière IADATA — AU 2025/2026


## Architecture globale

┌─────────────────────────────────────────────────────────────────────────┐
│                        SOURCES D'ACTUALITÉ                              │
│  Hespress · Akhbarona · Barlamane · Al Jazeera · BBC · CNN · Reuters   │
└──────────────────────┬──────────────────────────────────────────────────┘
                       │ Web Scraping (Python + BeautifulSoup)
          ┌────────────▼────────────┐
          │      INGESTION          │
          │  Batch (1h) │ Streaming │
          │   Apache    │   Kafka   │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │     DATA LAKE (MinIO)   │
          │  Bronze ·  Silver  │
          │         Gold          │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │    DATA WAREHOUSE       │
          │      PostgreSQL         │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │     VISUALISATION       │
          │  Dashboard · Analytics  │
          └─────────────────────────┘
```

---

## Démarrage rapide

### Prérequis
- Docker ≥ 24.0
- Docker Compose ≥ 2.20
- 8 Go RAM minimum

### Lancement

```bash
# 1. Cloner le projet
git clone https://github.com/iadata/news-bigdata-platform
cd news-bigdata-platform

# 2. Démarrer tous les services
docker compose -f docker/docker-compose.yml up -d

# 3. Initialiser la base de données
docker exec -it postgres-dw psql -U newsuser -d newswarehouse -c "\dt"

# 4. Accéder aux interfaces
```

| Service | URL | Identifiants |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |
| Dashboard | http://localhost:3000 | — |
| PostgreSQL DW | localhost:5433 | newsuser / newspass |

---

##  Structure du projet

```
news-bigdata-platform/
├── scrapers/
│   └── scraper.py              # Scrapers multi-sources
├── ingestion/
│   └── ingestion.py            # Batch + Streaming (Kafka)
├── transformation/
│   └── medallion.py            # Pipeline Bronze→Silver→Gold
├── quality/
│   └── data_quality.py         # Contrôles qualité (15 tests)
├── warehouse/
│   └── warehouse.py            # Schéma étoile PostgreSQL
├── governance/
│   └── governance.py           # Catalogue + Lineage
├── orchestration/
│   └── dags.py                 # DAGs Apache Airflow
├── dashboard/
│   └── ...                     # Interface de visualisation
├── docker/
│   └── docker-compose.yml      # Infrastructure complète
├── Dockerfile
├── requirements.txt
└── README.md
```

##  Architecture Médaillon

###  Bronze — Données brutes
- Stockage JSON dans MinIO partitionné par `year/month/day`
- Contenu HTML original conservé
- Aucune transformation

###  Silver — Données nettoyées
- Suppression des balises HTML (`strip_html()`)
- Normalisation unicode et espaces (`normalize_text()`)
- Détection automatique de la langue (`langdetect`)
- Évaluation de la qualité (flags + score)

###  Gold — Tables analytiques
| Table | Description |
|---|---|
| `articles_by_source` | Nombre d'articles par source |
| `articles_by_day` | Évolution temporelle |
| `articles_by_category` | Répartition éditoriale |
| `articles_by_language` | Distribution linguistique |
| `top_keywords` | Top 50 mots-clés globaux |
| `trends_by_hour` | Tendances temps réel |

---

##  Ingestion

### Mode Batch (toutes les heures)
```bash
python ingestion/ingestion.py batch
```
- Scraping programmé via Airflow (`0 * * * *`)
- Sauvegarde JSON partitionnée dans MinIO Bronze

### Mode Streaming (Kafka)
```bash
python ingestion/ingestion.py producer

python ingestion/ingestion.py consumer
```

## Qualité des données

15 tests automatiques sur 3 dimensions :

**Complétude**
- Titre présent
- Contenu présent
- URL présente
- Date présente
- Source présente

**Validité**
- Longueur titre ≥ 10 caractères
- Longueur contenu ≥ 100 caractères
- URL format valide (`https?://...`)
- Langue reconnue (fr/ar/en/es...)

**Cohérence**
- Date parseable
- Titre sans HTML résiduel
- Contenu sans HTML excessif
- Titre distinct de l'URL

Seuil de validité : **score ≥ 70%** → article valide

---

## Data Warehouse — Schéma

```sql
dim_source
dim_date
dim_category  
dim_language  

fact_articles

agg_articles_per_source_day
agg_keywords
agg_trends

data_lineage 
data_quality_log
```

---

## Orchestration — DAGs Airflow

| DAG | Fréquence | Étapes |
|---|---|---|
| `news_batch_pipeline` | Toutes les heures | Scraping → Ingestion → Qualité → Silver → Gold → DW → Notif |
| `news_streaming_monitor` | Toutes les 5 min | Monitoring lag Kafka |
| `news_daily_quality_report` | 6h chaque matin | Rapport qualité quotidien |

---

## Gouvernance

- **Catalogue de données** : métadonnées de chaque dataset (Bronze/Silver/Gold)
- **Lineage** : traçabilité complète de chaque run du pipeline
- **Logs qualité** : historique des taux de validité dans PostgreSQL

---

## Technologies

| Composant | Technologie |
|---|---|
| Scraping | Python 3.11, BeautifulSoup4, Scrapy |
| Streaming | Apache Kafka 3.6, kafka-python |
| Data Lake | MinIO (S3-compatible) |
| Transformation | Python, Apache Spark 3.5 |
| Orchestration | Apache Airflow 2.9 |
| Data Warehouse | PostgreSQL 15 |
| Conteneurisation | Docker, Docker Compose |

---

## Équipe

Projet réalisé dans le cadre du module **Architecture de données**  
Réalisé par : Mehdi Moussaid / Badr Elhannak / Oussama Oulkaid
---
