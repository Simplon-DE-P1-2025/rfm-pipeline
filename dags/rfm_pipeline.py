"""
DAG : rfm_pipeline
Pipeline RFM orchestrée par Apache Airflow.

Graphe des tâches :
  ingest → transform → load
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Les scripts ETL sont montés dans /opt/airflow/etl via docker-compose
sys.path.insert(0, "/opt/airflow/etl")

from ingest import ingest        # noqa: E402
from transform import transform  # noqa: E402
from load import load            # noqa: E402

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rfm_pipeline",
    default_args=default_args,
    description="Pipeline RFM : Ingestion → Transformation → Chargement",
    schedule_interval="@daily",
    catchup=False,
    tags=["rfm", "etl", "pipeline"],
) as dag:

    task_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        doc_md="""
        **Ingestion** : Lit le fichier Excel Online Retail II et charge
        les données brutes dans PostgreSQL (`raw_online_retail`).
        """,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        doc_md="""
        **Transformation** : Nettoie les données et calcule les scores
        RFM (Recency, Frequency, Monetary) par client (`rfm_scores`).
        """,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
        doc_md="""
        **Chargement** : Segmente les clients (Champions, Loyal, At Risk, Lost)
        et exporte le résultat final (`rfm_segments`).
        """,
    )

    # Dépendances séquentielles
    task_ingest >> task_transform >> task_load
