"""
Étape 1 — Ingestion
Charge les données brutes du dataset Online Retail II dans PostgreSQL (table raw_online_retail).
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@postgres:5432/rfm_db")
DATA_PATH = os.getenv("DATA_PATH", "/opt/airflow/data/online_retail_II.xlsx")


def ingest():
    logger.info("=== Démarrage de l'ingestion ===")
    logger.info(f"Lecture du fichier : {DATA_PATH}")

    sheets = ["Year 2009-2010", "Year 2010-2011"]
    frames = []

    for sheet in sheets:
        try:
            df_sheet = pd.read_excel(DATA_PATH, sheet_name=sheet, dtype={"Customer ID": str})
            frames.append(df_sheet)
            logger.info(f"  Sheet '{sheet}' : {len(df_sheet):,} lignes")
        except Exception as e:
            logger.warning(f"  Sheet '{sheet}' ignorée : {e}")

    if not frames:
        raise FileNotFoundError(
            f"Aucune feuille trouvée dans {DATA_PATH}. "
            "Vérifiez que le fichier est placé dans le dossier data/."
        )

    df = pd.concat(frames, ignore_index=True)

    # Normaliser les noms de colonnes : "Customer ID" → "customer_id", "InvoiceDate" → "invoicedate"
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    logger.info(f"Total données brutes : {len(df):,} lignes, {len(df.columns)} colonnes")
    logger.info(f"Colonnes : {list(df.columns)}")

    engine = create_engine(DB_CONN)
    df.to_sql("raw_online_retail", engine, if_exists="replace", index=False)

    logger.info(f"=== Ingestion terminée : {len(df):,} lignes → table 'raw_online_retail' ===")


if __name__ == "__main__":
    ingest()
