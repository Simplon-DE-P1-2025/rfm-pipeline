"""
Étape 3 — Chargement final
Segmente les clients selon leur score RFM total et charge le résultat
dans la table rfm_segments.

Segments :
  Champions       : rfm_total >= 12
  Loyal Customers : rfm_total >= 9
  At Risk         : rfm_total >= 6
  Lost            : rfm_total < 6
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@postgres:5432/rfm_db")


def _assign_segment(rfm_total: int) -> str:
    if rfm_total >= 12:
        return "Champions"
    elif rfm_total >= 9:
        return "Loyal Customers"
    elif rfm_total >= 6:
        return "At Risk"
    return "Lost"


def load():
    logger.info("=== Démarrage du chargement final ===")

    engine = create_engine(DB_CONN)

    rfm = pd.read_sql("SELECT * FROM rfm_scores", engine)
    logger.info(f"Données RFM chargées : {len(rfm):,} clients")

    rfm["segment"] = rfm["rfm_total"].apply(_assign_segment)

    # Distribution des segments
    dist = rfm["segment"].value_counts()
    logger.info(f"\nDistribution des segments :\n{dist.to_string()}")
    logger.info(
        f"\nTop 10 clients (Champions) :\n"
        f"{rfm[rfm['segment']=='Champions'].sort_values('rfm_total', ascending=False).head(10)[['customer_id','recency','frequency','monetary','rfm_score','segment']].to_string(index=False)}"
    )

    rfm.to_sql("rfm_segments", engine, if_exists="replace", index=False)
    logger.info("=== Chargement terminé : table 'rfm_segments' mise à jour ===")


if __name__ == "__main__":
    load()
