import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ── Configuration ──────────────────────────────────────────────────────────
DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@postgres:5432/rfm_db")

st.set_page_config(
    page_title="RFM Dashboard",
    page_icon="📊",
    layout="wide",
)

SEGMENT_COLORS = {
    "Champions": "#2ecc71",
    "Loyal Customers": "#3498db",
    "At Risk": "#f39c12",
    "Lost": "#e74c3c",
}


# ── Chargement des données ─────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data():
    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        # Vérifier si la table existe
        result = conn.execute(
            text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='rfm_segments')")
        )
        exists = result.scalar()

    if not exists:
        return None

    df = pd.read_sql("SELECT * FROM rfm_segments", engine)
    return df


# ── Interface ──────────────────────────────────────────────────────────────
st.title("📊 Dashboard RFM — Online Retail")
st.markdown("Visualisation des scores RFM et de la segmentation client.")

df = load_data()

if df is None or df.empty:
    st.warning(
        "Aucune donnée disponible. "
        "Lancez d'abord le DAG **rfm_pipeline** depuis Airflow (http://localhost:8080)."
    )
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Clients total", f"{len(df):,}")
col2.metric("Champions", f"{(df['segment'] == 'Champions').sum():,}")
col3.metric("Recency médiane (j)", f"{int(df['recency'].median())}")
col4.metric("CA moyen / client", f"{df['monetary'].mean():,.0f} €")

st.divider()

# ── Ligne 1 : Distribution des segments + Scatter R vs M ──────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Distribution des segments")
    seg_counts = df["segment"].value_counts().reset_index()
    seg_counts.columns = ["segment", "count"]
    seg_counts["color"] = seg_counts["segment"].map(SEGMENT_COLORS)

    fig_pie = px.pie(
        seg_counts,
        names="segment",
        values="count",
        color="segment",
        color_discrete_map=SEGMENT_COLORS,
        hole=0.4,
    )
    fig_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    st.subheader("Recency vs Monetary (par segment)")
    fig_scatter = px.scatter(
        df,
        x="recency",
        y="monetary",
        color="segment",
        color_discrete_map=SEGMENT_COLORS,
        opacity=0.6,
        labels={"recency": "Recency (jours)", "monetary": "Monetary (€)"},
        hover_data=["customer_id", "frequency", "rfm_score"],
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ── Ligne 2 : RFM scores distribution + Heatmap ───────────────────────────
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Distribution des scores RFM")
    fig_box = go.Figure()
    for metric, label in [("r_score", "Recency"), ("f_score", "Frequency"), ("m_score", "Monetary")]:
        fig_box.add_trace(go.Box(y=df[metric], name=label, boxmean=True))
    fig_box.update_layout(yaxis_title="Score (1–5)", showlegend=False)
    st.plotly_chart(fig_box, use_container_width=True)

with col_right2:
    st.subheader("Heatmap R vs F (fréquence clients)")
    heat = df.groupby(["r_score", "f_score"]).size().reset_index(name="count")
    heat_pivot = heat.pivot(index="r_score", columns="f_score", values="count").fillna(0)
    fig_heat = px.imshow(
        heat_pivot,
        labels={"x": "F Score", "y": "R Score", "color": "Nb clients"},
        color_continuous_scale="Blues",
        aspect="auto",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ── Tableau détail ────────────────────────────────────────────────────────
st.divider()
st.subheader("Détail par segment")

segment_filter = st.selectbox(
    "Filtrer par segment",
    options=["Tous"] + list(SEGMENT_COLORS.keys()),
)

display_df = df if segment_filter == "Tous" else df[df["segment"] == segment_filter]
display_df = display_df.sort_values("rfm_total", ascending=False)

st.dataframe(
    display_df[["customer_id", "recency", "frequency", "monetary", "r_score", "f_score", "m_score", "rfm_score", "rfm_total", "segment"]].reset_index(drop=True),
    use_container_width=True,
    height=400,
)

st.caption(f"Données actualisées toutes les 60 secondes — {len(display_df):,} clients affichés")
