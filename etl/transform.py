"""
Étape 2 — Transformation RFM
Lit les données brutes, les nettoie et calcule les scores RFM par client.
Résultat stocké dans la table rfm_scores.

RFM :
  R — Recency   : nombre de jours depuis le dernier achat (score 5 = récent)
  F — Frequency : nombre de commandes distinctes         (score 5 = fréquent)
  M — Monetary  : montant total dépensé                  (score 5 = élevé)
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@postgres:5432/rfm_db")


def transform():
    logger.info("=== Démarrage de la transformation RFM ===")

    engine = create_engine(DB_CONN)

    # Chargement des données brutes
    df = pd.read_sql("SELECT * FROM raw_online_retail", engine)
    logger.info(f"Données brutes chargées : {len(df):,} lignes")

    # ── Nettoyage ──────────────────────────────────────────────────────────
    df = df.dropna(subset=["customer_id"])           # supprimer les clients inconnus
    df = df[~df["invoice"].astype(str).str.startswith("C")]  # exclure les retours
    df = df[df["quantity"] > 0]                       # garder les quantités positives
    df = df[df["price"] > 0]                          # garder les prix positifs

    df["invoicedate"] = pd.to_datetime(df["invoicedate"])

    # Conversion GBP → EUR (taux fixe historique 2009-2011)
    # Source : Online Retail II UCI — les prix sont en livres sterling (£)
    GBP_TO_EUR = 1.17
    df["total_amount"] = df["quantity"] * df["price"] * GBP_TO_EUR
    logger.info(f"Montants convertis GBP → EUR (taux fixe : {GBP_TO_EUR})")

    logger.info(f"Après nettoyage : {len(df):,} lignes valides")

    # ── Calcul RFM ─────────────────────────────────────────────────────────
    ref_date = df["invoicedate"].max() + pd.Timedelta(days=1)
    logger.info(f"Date de référence pour Recency : {ref_date.date()}")

    rfm = (
        df.groupby("customer_id")
        .agg(
            recency=("invoicedate", lambda x: (ref_date - x.max()).days),
            frequency=("invoice", "nunique"),
            monetary=("total_amount", "sum"),
        )
        .reset_index()
    )

    # ── Scoring 1-5 ────────────────────────────────────────────────────────
    # Recency : moins de jours = meilleur → labels inversés [5,4,3,2,1]
    rfm["r_score"] = pd.qcut(
        rfm["recency"], q=5, labels=[5, 4, 3, 2, 1], duplicates="drop"
    ).astype(int)

    # Frequency & Monetary : plus = meilleur → labels croissants [1,2,3,4,5]
    rfm["f_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]
    ).astype(int)

    rfm["m_score"] = pd.qcut(
        rfm["monetary"], q=5, labels=[1, 2, 3, 4, 5], duplicates="drop"
    ).astype(int)

    rfm["rfm_score"] = (
        rfm["r_score"].astype(str)
        + rfm["f_score"].astype(str)
        + rfm["m_score"].astype(str)
    )
    rfm["rfm_total"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]

    logger.info(f"Scores RFM calculés pour {len(rfm):,} clients")
    logger.info(f"\n{rfm[['recency','frequency','monetary','rfm_total']].describe().round(2)}")

    rfm.to_sql("rfm_scores", engine, if_exists="replace", index=False)
    logger.info("=== Transformation terminée : table 'rfm_scores' mise à jour ===")


if __name__ == "__main__":
    transform()
