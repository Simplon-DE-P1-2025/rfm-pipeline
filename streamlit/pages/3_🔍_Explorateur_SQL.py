import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect

st.set_page_config(page_title="Explorateur SQL", page_icon="🔍", layout="wide")

DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@postgres:5432/rfm_db")

# ── Requêtes prédéfinies ───────────────────────────────────────────────────
PREDEFINED_QUERIES = {
    "— Choisir une requête prédéfinie —": "",

    # ── Données brutes ──
    "[Brut] Aperçu — 50 premières lignes": """\
SELECT *
FROM raw_online_retail
LIMIT 50;""",

    "[Brut] Nombre total de lignes": """\
SELECT COUNT(*) AS total_lignes
FROM raw_online_retail;""",

    "[Brut] Ventes par pays (top 10)": """\
SELECT country,
       COUNT(DISTINCT invoice) AS nb_commandes,
       ROUND(SUM(quantity * price)::numeric, 2) AS ca_total
FROM raw_online_retail
WHERE quantity > 0 AND price > 0
GROUP BY country
ORDER BY ca_total DESC
LIMIT 10;""",

    "[Brut] Produits les plus vendus (top 10)": """\
SELECT stockcode,
       description,
       SUM(quantity) AS quantite_totale
FROM raw_online_retail
WHERE quantity > 0
GROUP BY stockcode, description
ORDER BY quantite_totale DESC
LIMIT 10;""",

    "[Brut] Évolution mensuelle du CA": """\
SELECT DATE_TRUNC('month', invoicedate) AS mois,
       COUNT(DISTINCT invoice)          AS nb_commandes,
       ROUND(SUM(quantity * price)::numeric, 2) AS ca
FROM raw_online_retail
WHERE quantity > 0 AND price > 0
GROUP BY mois
ORDER BY mois;""",

    # ── Scores RFM ──
    "[RFM] Scores — aperçu 50 lignes": """\
SELECT *
FROM rfm_scores
ORDER BY rfm_total DESC
LIMIT 50;""",

    "[RFM] Distribution des scores": """\
SELECT
    r_score, f_score, m_score,
    COUNT(*) AS nb_clients
FROM rfm_scores
GROUP BY r_score, f_score, m_score
ORDER BY r_score DESC, f_score DESC, m_score DESC;""",

    "[RFM] Statistiques globales R/F/M": """\
SELECT
    ROUND(AVG(recency)::numeric, 1)   AS recency_moy,
    ROUND(AVG(frequency)::numeric, 1) AS frequency_moy,
    ROUND(AVG(monetary)::numeric, 2)  AS monetary_moy,
    MAX(rfm_total)                     AS rfm_max,
    MIN(rfm_total)                     AS rfm_min
FROM rfm_scores;""",

    # ── Segments ──
    "[Segments] Résultat final — 50 lignes": """\
SELECT *
FROM rfm_segments
ORDER BY rfm_total DESC
LIMIT 50;""",

    "[Segments] Distribution par segment": """\
SELECT segment,
       COUNT(*)                           AS nb_clients,
       ROUND(AVG(recency)::numeric, 1)    AS recency_moy,
       ROUND(AVG(frequency)::numeric, 1)  AS frequency_moy,
       ROUND(AVG(monetary)::numeric, 2)   AS monetary_moy
FROM rfm_segments
GROUP BY segment
ORDER BY nb_clients DESC;""",

    "[Segments] Top 20 Champions": """\
SELECT customer_id, recency, frequency,
       ROUND(monetary::numeric, 2) AS monetary,
       rfm_score, rfm_total
FROM rfm_segments
WHERE segment = 'Champions'
ORDER BY rfm_total DESC
LIMIT 20;""",

    "[Segments] Clients perdus (Lost)": """\
SELECT customer_id, recency, frequency,
       ROUND(monetary::numeric, 2) AS monetary,
       rfm_score
FROM rfm_segments
WHERE segment = 'Lost'
ORDER BY recency DESC
LIMIT 30;""",
}

# ── Interface ──────────────────────────────────────────────────────────────
st.title("🔍 Explorateur SQL")
st.markdown("Interrogez directement les tables PostgreSQL de la pipeline.")

# Connexion
@st.cache_resource
def get_engine():
    return create_engine(DB_CONN)

engine = get_engine()

# Vérifier les tables disponibles
try:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
except Exception as e:
    st.error(f"Impossible de se connecter à la base : {e}")
    st.stop()

if not tables:
    st.warning("Aucune table trouvée. Lancez d'abord le DAG **rfm_pipeline** depuis Airflow.")
    st.stop()

# ── Sidebar : infos tables ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📋 Tables disponibles")
    for table in sorted(tables):
        try:
            count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0, 0]
            st.markdown(f"**`{table}`** — {count:,} lignes")
        except Exception:
            st.markdown(f"**`{table}`**")

    st.divider()
    st.markdown("### 💡 Conseils")
    st.markdown("""
- Utilisez `LIMIT` pour les grosses tables
- `raw_online_retail` contient ~1M lignes
- Les résultats sont exportables en CSV
    """)

# ── Sélecteur de requête prédéfinie ───────────────────────────────────────
selected = st.selectbox("Requêtes prédéfinies", options=list(PREDEFINED_QUERIES.keys()))

default_query = PREDEFINED_QUERIES[selected] if selected != "— Choisir une requête prédéfinie —" \
    else "SELECT * FROM rfm_segments LIMIT 10;"

# ── Éditeur SQL ───────────────────────────────────────────────────────────
col_editor, col_run = st.columns([6, 1])
with col_editor:
    query = st.text_area(
        "Requête SQL",
        value=default_query,
        height=200,
        label_visibility="collapsed",
        placeholder="Écrivez votre requête SQL ici...",
    )
with col_run:
    st.markdown("<br><br>", unsafe_allow_html=True)
    run = st.button("▶ Exécuter", type="primary", use_container_width=True)

# ── Exécution ─────────────────────────────────────────────────────────────
if run and query.strip():
    # Bloquer les instructions destructives
    forbidden = ("drop", "delete", "truncate", "alter", "insert", "update", "create")
    if any(query.strip().lower().startswith(kw) for kw in forbidden):
        st.error("Seules les requêtes SELECT sont autorisées dans cet explorateur.")
    else:
        try:
            with st.spinner("Exécution en cours..."):
                df = pd.read_sql(text(query), engine)

            st.success(f"{len(df):,} lignes retournées")

            # Métriques rapides
            m1, m2, m3 = st.columns(3)
            m1.metric("Lignes", f"{len(df):,}")
            m2.metric("Colonnes", len(df.columns))
            m3.metric("Taille mémoire", f"{df.memory_usage(deep=True).sum() / 1024:.1f} Ko")

            st.dataframe(df, use_container_width=True, height=450)

            # Export CSV
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Télécharger en CSV",
                data=csv,
                file_name="resultats.csv",
                mime="text/csv",
            )

        except Exception as e:
            st.error(f"Erreur SQL : {e}")
