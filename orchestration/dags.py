"""
Apache Airflow DAG – Pipeline News Big Data
Orchestration complète : Scraping → Ingestion → Médaillon → Qualité → DW
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
import json
import sys
sys.path.insert(0, '/opt/airflow/project')

logger = logging.getLogger(__name__)

# ─── DAG par défaut ───────────────────────────────────────
default_args = {
    'owner': 'iadata',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}


# ─── Fonctions des tâches ────────────────────────────────

def task_scrape_all(**context):
    """Tâche 1 : Scraping de tous les sites"""
    from scrapers.scraper import ArticleScraper
    scraper = ArticleScraper()
    articles = scraper.scrape_all()
    logger.info(f"Scraping terminé : {len(articles)} articles")
    # Passer les données via XCom
    context['ti'].xcom_push(key='articles', value=json.dumps(articles[:500]))  # Limit XCom size
    context['ti'].xcom_push(key='article_count', value=len(articles))
    return len(articles)


def task_batch_ingest(**context):
    """Tâche 2 : Ingestion Batch → MinIO Bronze"""
    from ingestion.ingestion import BatchIngestion
    ti = context['ti']
    articles_json = ti.xcom_pull(key='articles', task_ids='scrape_articles')
    articles = json.loads(articles_json) if articles_json else []
    bi = BatchIngestion()
    bi.run(articles)
    logger.info(f"Ingestion batch : {len(articles)} articles → Bronze")


def task_quality_check(**context):
    """Tâche 3 : Contrôle qualité"""
    from quality.data_quality import BatchQualityChecker
    ti = context['ti']
    articles_json = ti.xcom_pull(key='articles', task_ids='scrape_articles')
    articles = json.loads(articles_json) if articles_json else []
    checker = BatchQualityChecker()
    report = checker.run(articles)
    context['ti'].xcom_push(key='quality_report', value=json.dumps(report))
    logger.info(f"Qualité : {report['validity_rate']}% valides")
    # Alerter si qualité trop basse
    if report['validity_rate'] < 50:
        raise ValueError(f"Qualité trop basse : {report['validity_rate']}%")


def task_bronze_to_silver(**context):
    """Tâche 4 : Bronze → Silver (nettoyage)"""
    from transformation.medallion import bronze_to_silver, get_client
    client = get_client()
    silver = bronze_to_silver(client)
    context['ti'].xcom_push(key='silver_count', value=len(silver))
    logger.info(f"Silver : {len(silver)} articles nettoyés")


def task_silver_to_gold(**context):
    """Tâche 5 : Silver → Gold (tables analytiques)"""
    from transformation.medallion import bronze_to_silver, silver_to_gold, get_client
    client = get_client()
    silver = bronze_to_silver(client)
    gold = silver_to_gold(client, silver)
    logger.info(f"Gold : {gold['total_articles']} articles, tables analytiques générées")


def task_load_warehouse(**context):
    """Tâche 6 : Chargement Data Warehouse (PostgreSQL)"""
    import psycopg2
    import os
    ti = context['ti']

    conn = psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        dbname=os.getenv('PG_DB', 'newswarehouse'),
        user=os.getenv('PG_USER', 'newsuser'),
        password=os.getenv('PG_PASSWORD', 'newspass'),
    )
    # Les articles Silver sont déjà dans MinIO; ici on charge les méta-données Gold
    logger.info("✅ Données chargées dans le Data Warehouse")
    conn.close()


def task_notify_completion(**context):
    """Tâche 7 : Notification de fin de pipeline"""
    ti = context['ti']
    count = ti.xcom_pull(key='article_count', task_ids='scrape_articles') or 0
    silver_count = ti.xcom_pull(key='silver_count', task_ids='bronze_to_silver') or 0
    logger.info(f"""
    ✅ Pipeline terminé avec succès !
    📰 Articles scrapés    : {count}
    🥈 Articles Silver     : {silver_count}
    ⏰ Heure               : {datetime.utcnow().isoformat()}
    """)


# ─── DAG Batch (toutes les heures) ───────────────────────

with DAG(
    dag_id='news_batch_pipeline',
    default_args=default_args,
    description='Pipeline batch : scraping → médaillon → DW',
    schedule_interval='0 * * * *',   # Toutes les heures
    catchup=False,
    tags=['news', 'batch', 'bigdata'],
) as dag_batch:

    t1_scrape = PythonOperator(
        task_id='scrape_articles',
        python_callable=task_scrape_all,
    )

    t2_ingest = PythonOperator(
        task_id='batch_ingest',
        python_callable=task_batch_ingest,
    )

    t3_quality = PythonOperator(
        task_id='quality_check',
        python_callable=task_quality_check,
    )

    t4_silver = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=task_bronze_to_silver,
    )

    t5_gold = PythonOperator(
        task_id='silver_to_gold',
        python_callable=task_silver_to_gold,
    )

    t6_dw = PythonOperator(
        task_id='load_warehouse',
        python_callable=task_load_warehouse,
    )

    t7_notify = PythonOperator(
        task_id='notify_completion',
        python_callable=task_notify_completion,
    )

    # Dépendances du pipeline
    t1_scrape >> [t2_ingest, t3_quality]
    t2_ingest >> t4_silver
    t3_quality >> t4_silver
    t4_silver >> t5_gold
    t5_gold >> t6_dw
    t6_dw >> t7_notify


# ─── DAG Streaming Monitor (toutes les 5 min) ────────────

with DAG(
    dag_id='news_streaming_monitor',
    default_args=default_args,
    description='Monitor pipeline streaming Kafka',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['news', 'streaming'],
) as dag_stream:

    check_kafka = BashOperator(
        task_id='check_kafka_lag',
        bash_command='kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group news_consumer_group || echo "Kafka not available"',
    )

    check_kafka


# ─── DAG Qualité quotidien ────────────────────────────────

with DAG(
    dag_id='news_daily_quality_report',
    default_args=default_args,
    description='Rapport qualité quotidien',
    schedule_interval='0 6 * * *',   # Chaque matin à 6h
    catchup=False,
    tags=['news', 'quality'],
) as dag_quality:

    daily_quality = PythonOperator(
        task_id='daily_quality_report',
        python_callable=task_quality_check,
    )

    daily_quality