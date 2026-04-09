import pandas as pd
import streamlit as st

st.set_page_config(page_title="Présentation — Pipeline RFM", page_icon="🎓", layout="wide")

# ══════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
.slide-header {
    background: #1e2130;
    border-radius: 12px;
    padding: 2rem 2.5rem;
    margin-bottom: 1.5rem;
    border-left: 5px solid #4f8bf9;
}
.slide-title {
    font-size: 1.8rem;
    font-weight: 700;
    color: #4f8bf9;
    margin-bottom: 0.4rem;
}
.slide-subtitle {
    font-size: 1rem;
    color: #a0aec0;
}
.arrow { font-size: 1.4rem; color: #4f8bf9; text-align: center; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# SLIDES
# ══════════════════════════════════════════════════════════════════════════

def slide_titre():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🔁 Pipeline Data RFM — Docker &amp; Airflow</div>
    <div class="slide-subtitle">Simplon — Projet Data Engineering · Avril 2026</div>
</div>
""", unsafe_allow_html=True)
    st.markdown("Construction d'une **pipeline de données orchestrée et dockerisée** pour l'analyse **RFM** (Recency · Frequency · Monetary) d'un dataset e-commerce réel (~1 million de transactions, Online Retail II — UCI).")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Transactions", "~1 000 000")
    col2.metric("Services Docker", "5")
    col3.metric("Tables PostgreSQL", "3")
    col4.metric("Tâches Airflow", "3")


def slide_contexte():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🎯 Contexte &amp; Objectifs</div>
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Dataset")
        st.markdown("""
- **Online Retail II** — UCI ML Repository
- ~1 million de transactions e-commerce
- Période : 2009 → 2011
- Colonnes : Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country
        """)
    with col2:
        st.markdown("#### Objectifs pédagogiques")
        st.markdown("""
- ✅ Mettre en place une **pipeline de données** simple
- ✅ Comprendre l'**orchestration** avec Airflow
- ✅ Maîtriser la **dockerisation** d'un projet data
- ✅ Faire fonctionner plusieurs **services ensemble**
        """)


def slide_architecture():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🏗️ Architecture du projet</div>
</div>
""", unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns([2, 0.4, 2, 0.4, 2])
    with col1:
        st.markdown("""**📥 Ingestion**
```
online_retail_II.xlsx
        ↓
   ingest.py
        ↓
raw_online_retail
   (PostgreSQL)
```""")
    with col2:
        st.markdown("<div class='arrow'>→</div>", unsafe_allow_html=True)
    with col3:
        st.markdown("""**⚙️ Transformation**
```
raw_online_retail
        ↓
  transform.py
  - Nettoyage
  - Calcul R/F/M
  - Scoring 1-5
        ↓
   rfm_scores
  (PostgreSQL)
```""")
    with col4:
        st.markdown("<div class='arrow'>→</div>", unsafe_allow_html=True)
    with col5:
        st.markdown("""**📤 Chargement**
```
  rfm_scores
      ↓
   load.py
 Segmentation
  Champions
    Loyal
   At Risk
    Lost
      ↓
 rfm_segments
 (PostgreSQL)
```""")
    st.info("Les 3 étapes sont orchestrées séquentiellement par le DAG **rfm_pipeline** dans Apache Airflow.")


def slide_stack():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🛠️ Stack technique</div>
</div>
""", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("#### 🐳 Docker")
        st.markdown("- Docker Compose v2\n- 5 services orchestrés\n- Images : `postgres:15`, `apache/airflow:2.9.2`, `python:3.11-slim`\n- Volumes persistants")
    with col2:
        st.markdown("#### 🌀 Airflow")
        st.markdown("- Orchestration DAG\n- LocalExecutor\n- 3 PythonOperators\n- Dépendances séquentielles\n- Scheduler + Webserver")
    with col3:
        st.markdown("#### 🐘 PostgreSQL")
        st.markdown("- Version 15\n- Base `rfm_db`\n- 3 tables :\n  - `raw_online_retail`\n  - `rfm_scores`\n  - `rfm_segments`")
    with col4:
        st.markdown("#### 🐍 Python / ETL")
        st.markdown("- pandas 2.1\n- SQLAlchemy 2.0\n- openpyxl\n- **uv** (gestionnaire de packages)\n- Streamlit + Plotly")


def slide_rfm_definition():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">📐 C'est quoi RFM ?</div>
    <div class="slide-subtitle">Un modèle marketing pour segmenter les clients selon leur comportement d'achat</div>
</div>
""", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### 🕐 R — Recency")
        st.markdown("**Définition :** Nombre de jours écoulés depuis la **dernière commande** du client.\n\n**Calcul :**\n```\nR = date_référence − date_dernière_commande\n```\nLa date de référence = dernier jour du dataset + 1.\n\n**Interprétation :**\n- Moins de jours = client plus récent = **meilleur**\n- Score **inversé** : R bas → score élevé")
    with col2:
        st.markdown("### 🔄 F — Frequency")
        st.markdown("**Définition :** Nombre de **commandes distinctes** passées par le client.\n\n**Calcul :**\n```\nF = COUNT(DISTINCT invoice)\n  par customer_id\n```\nOn compte les factures uniques, pas les lignes produits.\n\n**Interprétation :**\n- Plus de commandes = plus fidèle = **meilleur**\n- Score **croissant** : F élevé → score élevé")
    with col3:
        st.markdown("### 💶 M — Monetary")
        st.markdown("**Définition :** Montant **total dépensé** par le client sur toute la période.\n\n**Calcul :**\n```\nM = SUM(quantity × price)\n  par customer_id\n```\nOn exclut les retours (facture \"C…\") et les prix/quantités négatifs.\n\n**Interprétation :**\n- Plus de CA généré = **meilleur**\n- Score **croissant** : M élevé → score élevé")


def slide_rfm_scoring():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🎯 Comment on attribue les scores 1 à 5 ?</div>
    <div class="slide-subtitle">Méthode des quintiles — pd.qcut</div>
</div>
""", unsafe_allow_html=True)
    st.markdown("On divise l'ensemble des clients en **5 groupes de taille égale** (quintiles). Chaque groupe reçoit un score de 1 à 5.")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""#### Exemple — 10 clients triés par Recency

| Client | Recency (j) | Quintile | Score R |
|--------|------------|----------|---------|
| C1 | 3 | Q1 (plus récents) | **5** |
| C2 | 7 | Q1 | **5** |
| C3 | 15 | Q2 | **4** |
| C4 | 22 | Q2 | **4** |
| C5 | 45 | Q3 | **3** |
| C6 | 67 | Q3 | **3** |
| C7 | 120 | Q4 | **2** |
| C8 | 180 | Q4 | **2** |
| C9 | 280 | Q5 (plus anciens) | **1** |
| C10 | 365 | Q5 | **1** |

> Recency est **inversé** : score 5 = le plus récent.
> Frequency et Monetary sont **croissants** : score 5 = le plus élevé.
        """)
    with col2:
        st.markdown("#### Code dans `transform.py`")
        st.code("""\
# Recency : labels inversés [5,4,3,2,1]
# → moins de jours = meilleur = score 5
rfm["r_score"] = pd.qcut(
    rfm["recency"],
    q=5,
    labels=[5, 4, 3, 2, 1],
    duplicates="drop"
).astype(int)

# Frequency : labels croissants [1,2,3,4,5]
# .rank() évite les ex-aequo
rfm["f_score"] = pd.qcut(
    rfm["frequency"].rank(method="first"),
    q=5,
    labels=[1, 2, 3, 4, 5]
).astype(int)

# Monetary : labels croissants [1,2,3,4,5]
rfm["m_score"] = pd.qcut(
    rfm["monetary"],
    q=5,
    labels=[1, 2, 3, 4, 5],
    duplicates="drop"
).astype(int)
""", language="python")


def slide_rfm_exemples():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🧮 Score RFM final — exemples concrets</div>
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Formules")
        st.markdown("Le **rfm_score** = concaténation des 3 scores (ex : `\"534\"`)\n\nLe **rfm_total** = somme des 3 scores (ex : `5+3+4 = 12`)")
        st.code("""\
rfm["rfm_score"] = (
    rfm["r_score"].astype(str)
    + rfm["f_score"].astype(str)
    + rfm["m_score"].astype(str)
)
rfm["rfm_total"] = (
    rfm["r_score"]
    + rfm["f_score"]
    + rfm["m_score"]
)
""", language="python")
        st.markdown("$$\\text{RFM Total} = R_{score} + F_{score} + M_{score} \\quad \\in [3,\\ 15]$$")
    with col2:
        st.markdown("#### 5 clients fictifs — du meilleur au moins bon")
        exemples = pd.DataFrame({
            "Client": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "Recency (j)": [5, 30, 90, 200, 350],
            "Frequency": [52, 18, 8, 3, 1],
            "Monetary (€)": [12500, 3200, 850, 180, 45],
            "R score": [5, 4, 3, 2, 1],
            "F score": [5, 4, 3, 2, 1],
            "M score": [5, 4, 3, 1, 1],
            "rfm_score": ["555", "444", "333", "221", "111"],
            "rfm_total": [15, 12, 9, 5, 3],
            "Segment": ["Champions", "Champions", "Loyal Customers", "Lost", "Lost"],
        })
        st.dataframe(exemples, use_container_width=True, hide_index=True)
        st.markdown("> **Alice** : achetée il y a 5 jours, 52 commandes, 12 500 € → **Champion** parfait (555)\n>\n> **Eve** : achetée il y a un an, 1 seule commande, 45 € → **Lost** (111)")


def slide_segmentation():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🏷️ Segmentation clients</div>
</div>
""", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.success("### 🏆 Champions\nScore ≥ 12\n\nAcheteurs récents, fréquents, gros dépensiers. À fidéliser en priorité.")
    with col2:
        st.info("### ⭐ Loyal Customers\nScore 9–11\n\nBons clients réguliers. À récompenser pour maintenir l'engagement.")
    with col3:
        st.warning("### ⚠️ At Risk\nScore 6–8\n\nClients qui s'essoufflent. Nécessitent une action de réactivation.")
    with col4:
        st.error("### 💤 Lost\nScore < 6\n\nClients inactifs. Coût de réactivation élevé.")
    st.divider()
    st.markdown("#### Code dans `load.py`")
    st.code("""\
def _assign_segment(rfm_total: int) -> str:
    if rfm_total >= 12:
        return "Champions"
    elif rfm_total >= 9:
        return "Loyal Customers"
    elif rfm_total >= 6:
        return "At Risk"
    return "Lost"

rfm["segment"] = rfm["rfm_total"].apply(_assign_segment)
""", language="python")


def slide_dag():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🌀 DAG Airflow — rfm_pipeline</div>
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("""
```
[ingest] ──────► [transform] ──────► [load]
   ↓                   ↓                  ↓
Lit Excel          Nettoie les        Segmente les
Charge dans        données            clients RFM
PostgreSQL         Calcule R/F/M      Champions /
(table brute)      Score 1-5          Loyal / At Risk /
                   (table scores)     Lost (table finale)
```
""")
        st.code("""\
task_ingest    = PythonOperator(task_id="ingest",    python_callable=ingest)
task_transform = PythonOperator(task_id="transform", python_callable=transform)
task_load      = PythonOperator(task_id="load",      python_callable=load)

task_ingest >> task_transform >> task_load
""", language="python")
    with col2:
        st.markdown("""**Paramètres :**
| Param | Valeur |
|-------|--------|
| Schedule | `@daily` |
| Catchup | `False` |
| Executor | `LocalExecutor` |
| Retries | 1 |
| Retry delay | 5 min |
        """)


def slide_docker():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🐳 Docker Compose — Vue d'ensemble</div>
    <div class="slide-subtitle">5 services qui collaborent, lancés en une seule commande</div>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
> **Analogie :** Docker Compose, c'est comme une fiche de recette qui dit à Docker
> "lance ce service, puis celui-là, attends que le premier soit prêt, et relie-les entre eux".
""")

    st.markdown("""
| Service | Image de base | Port | Rôle |
|---------|--------------|------|------|
| `postgres` | `postgres:15` (officielle) | 5432 | Stockage : données RFM **et** métadonnées Airflow |
| `airflow-init` | Image custom (`airflow/Dockerfile`) | — | Tâche unique : init de la BDD + création du user admin |
| `airflow-webserver` | Image custom (`airflow/Dockerfile`) | 8080 | Interface graphique Airflow |
| `airflow-scheduler` | Image custom (`airflow/Dockerfile`) | — | Surveille et exécute les DAGs en arrière-plan |
| `streamlit` | Image custom (`streamlit/Dockerfile`) | 8501 | Dashboard de visualisation |
""")

    st.divider()
    st.markdown("#### 🔗 Chaîne de dépendances — l'ordre de démarrage est contrôlé")
    st.code("""\
# PostgreSQL démarre en premier
postgres → healthcheck (pg_isready)
                ↓ service_healthy
         airflow-init  (db init + création admin)
                ↓ service_completed_successfully
  airflow-webserver  +  airflow-scheduler  +  streamlit
""")
    st.info("Sans le `healthcheck`, Airflow démarrerait avant que PostgreSQL soit prêt → erreur de connexion. Le `condition: service_healthy` garantit l'ordre.")

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📁 Volumes montés sur les services Airflow")
        st.markdown("""
```yaml
volumes:
  - ./dags:/opt/airflow/dags      # DAGs lus en temps réel
  - ./logs:/opt/airflow/logs      # Logs accessibles depuis l'hôte
  - ./etl:/opt/airflow/etl        # Scripts ETL importés dans les tâches
  - ./data:/opt/airflow/data      # Fichier Excel partagé
```
> **Avantage :** modifier un DAG ou un script ETL ne nécessite **pas de rebuild**.
> Le fichier est lu directement depuis le disque à chaque exécution.
        """)
    with col2:
        st.markdown("#### 🔐 Variables d'environnement")
        st.markdown("""
Les valeurs sensibles (mots de passe, clés) ne sont **jamais** écrites en dur dans le `docker-compose.yml`.
Elles sont dans le fichier `.env` (non commité sur Git) :

```bash
POSTGRES_PASSWORD=airflow
AIRFLOW__CORE__FERNET_KEY=ZmDfcT...
AIRFLOW__WEBSERVER__SECRET_KEY=rfm-...
DB_CONN=postgresql://airflow:airflow@postgres:5432/rfm_db
```

Le `docker-compose.yml` les injecte via `${VARIABLE}` :
```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```
        """)


def slide_docker_details():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🐳 Dockerfiles &amp; x-airflow-common</div>
    <div class="slide-subtitle">Pourquoi des images custom ? Comment éviter la répétition de config ?</div>
</div>
""", unsafe_allow_html=True)

    # ── Dockerfiles ────────────────────────────────────────────────────────
    st.markdown("### 📄 Les Dockerfiles — personnaliser une image existante")
    st.markdown("""
> **Analogie :** Docker Hub fournit une image "vanilla". Le `Dockerfile`, c'est la liste de modifications
> qu'on applique dessus avant de l'utiliser — comme installer des plugins sur un logiciel.
""")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### `airflow/Dockerfile`")
        st.markdown("**Problème :** L'image officielle Airflow n'inclut pas `pandas`, `openpyxl` ni `psycopg2` — nécessaires pour les scripts ETL.")
        st.code("""\
FROM apache/airflow:2.9.2
# L'image tourne en user 'airflow' (non-root)
# pip install --user → /home/airflow/.local/ ✅

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
""", language="dockerfile")
        st.markdown("`requirements.txt` contient : `pandas`, `openpyxl`, `psycopg2-binary`")
        st.warning("⚠️ On n'utilise PAS `uv` ici : l'image Airflow est en non-root et `uv --system` installerait dans le Python système inaccessible à l'utilisateur `airflow`.")

    with col2:
        st.markdown("#### `streamlit/Dockerfile`")
        st.markdown("**Contexte :** Image Python propre — on contrôle tout l'environnement.")
        st.code("""\
FROM python:3.11-slim

# uv : gestionnaire de packages ultra-rapide
# Copié depuis son image officielle (binaire statique)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml .
RUN uv sync --no-dev     # installe dans .venv

COPY app.py .
COPY pages/ pages/

CMD ["uv", "run", "streamlit", "run", "app.py",
     "--server.port=8501", "--server.address=0.0.0.0"]
""", language="dockerfile")
        st.markdown("`pyproject.toml` contient : `streamlit`, `pandas`, `plotly`, `psycopg2-binary`, `sqlalchemy`")
        st.info("✅ `uv sync` crée un `.venv` isolé et résout les dépendances ~10× plus vite que pip.")

    st.divider()

    # ── x-airflow-common ──────────────────────────────────────────────────
    st.markdown("### 🧩 L'ancre YAML `x-airflow-common` — principe DRY")
    st.markdown("""
> **Problème :** `airflow-init`, `airflow-webserver` et `airflow-scheduler` partagent
> exactement la même image, les mêmes variables d'env et les mêmes volumes.
> Les répéter 3 fois = source d'erreurs et de maintenance difficile.
>
> **Solution :** une **ancre YAML** — bloc de config défini une fois, réutilisé partout avec `<<: *nom`.
""")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Sans ancre (❌ répétitif)")
        st.code("""\
airflow-webserver:
  build:
    context: ./airflow
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    DB_CONN: ${DB_CONN}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./etl:/opt/airflow/etl
  # ... 10 lignes de config

airflow-scheduler:
  build:
    context: ./airflow       # ← copié/collé
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor  # ← idem
    DB_CONN: ${DB_CONN}      # ← idem
  volumes:
    - ./dags:/opt/airflow/dags  # ← idem
    - ./etl:/opt/airflow/etl    # ← idem
  # ... 10 lignes identiques
""", language="yaml")

    with col2:
        st.markdown("#### Avec ancre (✅ DRY)")
        st.code("""\
# Bloc commun défini UNE SEULE FOIS
x-airflow-common: &airflow-common
  build:
    context: ./airflow
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    DB_CONN: ${DB_CONN}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./etl:/opt/airflow/etl

# Chaque service hérite du bloc commun
# et ajoute seulement ce qui le différencie
airflow-webserver:
  <<: *airflow-common      # ← "copie tout le bloc"
  command: webserver
  ports:
    - "8080:8080"

airflow-scheduler:
  <<: *airflow-common      # ← même chose
  command: scheduler
""", language="yaml")

    st.success("**Résultat :** modifier une variable d'env ou un volume une seule fois → appliqué aux 3 services automatiquement.")


def slide_streamlit():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">📊 Streamlit — Visualisation &amp; Interface</div>
    <div class="slide-subtitle">Framework Python pour créer des apps data sans écrire de HTML/JS</div>
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Pourquoi Streamlit ?")
        st.markdown("- **Pur Python** : aucune connaissance frontend requise\n- Rechargement automatique à chaque modification\n- Composants intégrés : graphiques, tableaux, formulaires\n- Multi-pages natif via le dossier `pages/`\n- Déployable dans Docker en une ligne")
        st.markdown("#### Architecture multi-pages")
        st.code("""\
streamlit/
├── app.py              ← point d'entrée (accueil)
└── pages/
    ├── 1_🎓_Présentation.py
    ├── 2_📊_Dashboard_RFM.py
    └── 3_🔍_Explorateur_SQL.py
""")
    with col2:
        st.markdown("#### Fonctionnalités clés utilisées")
        with st.expander("**Connexion PostgreSQL + cache**"):
            st.code("""\
@st.cache_resource      # connexion créée une seule fois
def get_engine():
    return create_engine(os.getenv("DB_CONN"))

@st.cache_data(ttl=60)  # résultat mis en cache 60s
def load_data():
    return pd.read_sql("SELECT * FROM rfm_segments",
                       get_engine())
""", language="python")
        with st.expander("**Graphiques Plotly interactifs**"):
            st.code("""\
fig = px.pie(
    df, names="segment", values="count",
    color="segment",
    color_discrete_map={"Champions": "#2ecc71", ...},
    hole=0.4,
)
st.plotly_chart(fig, use_container_width=True)
""", language="python")
        with st.expander("**Navigation diaporama — session_state**"):
            st.code("""\
if "slide_idx" not in st.session_state:
    st.session_state.slide_idx = 0

if st.button("Suivant ▶"):
    st.session_state.slide_idx += 1
    st.rerun()

# Afficher la slide courante
SLIDES[st.session_state.slide_idx]()
""", language="python")
        with st.expander("**Explorateur SQL sécurisé**"):
            st.code("""\
forbidden = ("drop","delete","truncate","alter")
if any(query.lower().startswith(kw) for kw in forbidden):
    st.error("Seules les requêtes SELECT sont autorisées.")
else:
    df = pd.read_sql(text(query), engine)
    st.dataframe(df)
    st.download_button("⬇️ CSV", df.to_csv(), "data.csv")
""", language="python")


def slide_choix_techniques():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">💡 Justification des choix techniques</div>
</div>
""", unsafe_allow_html=True)
    choices = {
        "Airflow LocalExecutor": "Suffisant pour une pipeline mono-machine. CeleryExecutor serait surdimensionné ici.",
        "PostgreSQL comme stockage": "Base relationnelle robuste, compatible natif avec Airflow (métadonnées + données RFM) — un seul service pour tout.",
        "PythonOperator dans le DAG": "Permet d'importer directement les scripts ETL. Plus lisible et testable qu'un BashOperator.",
        "uv au lieu de pip (Streamlit)": "10× plus rapide que pip pour la résolution de dépendances. Standard émergent en data engineering Python.",
        "Streamlit pour la visualisation": "Prototypage rapide en pur Python, aucun frontend à écrire. Parfait pour un projet pédagogique.",
        "healthcheck + depends_on": "Garantit que PostgreSQL est prêt avant qu'Airflow démarre — évite les erreurs de connexion au boot.",
        "Ancre YAML x-airflow-common": "Principe DRY (Don't Repeat Yourself) : config partagée entre webserver, scheduler et init en un seul bloc.",
    }
    for title, justification in choices.items():
        with st.expander(f"**{title}**"):
            st.markdown(justification)


def slide_livrables():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">📦 Livrables</div>
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""**Fichiers produits :**
- `docker-compose.yml` — infrastructure complète
- `dags/rfm_pipeline.py` — DAG Airflow (3 tâches)
- `etl/ingest.py` — ingestion Excel → PostgreSQL
- `etl/transform.py` — nettoyage + calcul RFM
- `etl/load.py` — segmentation clients
- `streamlit/` — dashboard multi-pages 3 vues
- `README.md` — guide d'exécution
        """)
    with col2:
        st.markdown("""**Structure du projet :**
```
pipeline-docker/
├── docker-compose.yml
├── .env
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   └── rfm_pipeline.py
├── etl/
│   ├── ingest.py
│   ├── transform.py
│   └── load.py
├── streamlit/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── app.py
│   └── pages/
└── data/
```
        """)
    st.success("Pipeline opérationnelle ✅  |  Docker fonctionnel ✅  |  Airflow orchestré ✅")


def slide_demo():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🖥️ Démonstration en direct</div>
    <div class="slide-subtitle">Le projet tourne en une seule commande</div>
</div>
""", unsafe_allow_html=True)

    st.markdown("### Séquence de démo")
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("""**Étape 1 — Lancer l'infrastructure**""")
        st.code("docker compose up -d", language="bash")
        st.markdown("Lance les 5 services simultanément. PostgreSQL démarre en premier, puis Airflow, puis Streamlit.")
        st.divider()
        st.markdown("""**Étape 2 — Vérifier que tout est UP**""")
        st.code("docker compose ps", language="bash")
        st.markdown("Tous les services doivent être `running` ou `healthy`. `airflow-init` est `Exited (0)` — c'est normal, tâche unique terminée.")
        st.divider()
        st.markdown("""**Étape 3 — Ouvrir Airflow**""")
        st.markdown("http://localhost:8080 · login `admin` / `admin`")
        st.markdown("Activer le DAG `rfm_pipeline`, cliquer ▶ Trigger, suivre l'exécution en temps réel.")
    with col2:
        st.markdown("""**Étape 4 — Observer les tâches**""")
        st.markdown("""
| Tâche | Durée ~| Ce qui se passe |
|-------|-------|-----------------|
| `ingest` | 2-3 min | Lit le fichier Excel (~1M lignes), charge dans PostgreSQL |
| `transform` | 1-2 min | Nettoie, calcule R/F/M, score 1-5 par quintile |
| `load` | < 10 sec | Segmente et écrit la table finale |
        """)
        st.divider()
        st.markdown("""**Étape 5 — Dashboard Streamlit**""")
        st.markdown("http://localhost:8501")
        st.markdown("""
- Page **Dashboard** : KPIs, donut chart des segments, scatter R vs M
- Page **Explorateur SQL** : requêtes prédéfinies sur les 3 tables
- Page **Présentation** : cette page 👋
        """)
        st.divider()
        st.markdown("""**Étape 6 — Requête SQL live**""")
        st.code("""\
SELECT segment, COUNT(*) as nb_clients,
       ROUND(AVG(monetary)::numeric, 2) as ca_moyen
FROM rfm_segments
GROUP BY segment
ORDER BY nb_clients DESC;
""", language="sql")


def slide_retrospective():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🔍 Difficultés rencontrées &amp; retour d'expérience</div>
    <div class="slide-subtitle">Ce qui a posé problème, ce qu'on a appris, ce qu'on ferait différemment</div>
</div>
""", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### ⚠️ Difficultés rencontrées")
        st.markdown("""
**1. Permissions dans le Dockerfile Airflow**

Première tentative avec `uv --system` → `Permission denied` au build.
L'image Airflow tourne en user non-root, impossible d'écrire dans `/usr/local/lib/`.
→ Solution : rester sur `pip install --user` qui installe dans `/home/airflow/.local/`.

**2. `AIRFLOW__WEBSERVER__SECRET_KEY` manquante**

Sans cette clé fixe, chaque conteneur génère sa propre clé aléatoire.
Webserver et scheduler ne se reconnaissent plus → les logs n'apparaissent pas dans l'UI.
→ Solution : définir une valeur fixe dans `.env`, partagée par tous les services via l'ancre.

**3. Ordre de démarrage des services**

Airflow tentait de se connecter à PostgreSQL avant qu'il soit prêt.
→ Solution : `healthcheck` sur PostgreSQL + `condition: service_healthy` dans `depends_on`.

**4. `openpyxl` non trouvé à l'exécution du DAG**

Le package était dans `requirements.txt` mais n'était pas résolu au bon endroit.
→ Lié au problème de permissions ci-dessus, résolu en même temps.

**5. Les prix sont en livres sterling (£), pas en euros**

Le dataset UCI Online Retail II indique des prix en GBP — les montants `monetary`
étaient affichés en £ sur le dashboard sans conversion.
→ Quick fix : multiplication par un taux fixe `GBP_TO_EUR = 1.17`
(moyenne historique 2009-2011) dans `transform.py`, appliqué au `total_amount` avant
le calcul RFM.
        """)

    with col2:
        st.markdown("#### 💡 Ce qu'on ferait différemment")
        st.markdown("""
**Architecture**
- Séparer la base métadonnées Airflow de la base données RFM (deux instances PostgreSQL) pour une architecture plus propre en production.
- Utiliser le `KubernetesExecutor` ou `CeleryExecutor` pour scaler les tâches si le dataset grossit.

**ETL**
- Ajouter un système de détection des doublons à l'ingestion (contrôle idempotent).
- Externaliser les paramètres RFM (seuils de segmentation) dans des variables Airflow plutôt qu'en dur dans le code.

**Observabilité**
- Ajouter des métriques dans les logs (durée de chaque étape, nombre d'erreurs filtrées).
- Intégrer des alertes email en cas d'échec du DAG.

**Sécurité**
- Changer le mot de passe admin par défaut (`admin`/`admin`) en production.
- Utiliser Docker Secrets plutôt que `.env` pour les credentials sensibles.
        """)

        st.divider()
        st.markdown("#### ✅ Ce qu'on retient")
        st.success("""
        **La vraie difficulté de ce projet ce n'est pas le code ETL — c'est de faire fonctionner plusieurs services ensemble**
        de façon fiable et dans le bon ordre. C'est exactement ce que font les Data Engineers en production.
        """)


def slide_repartition():
    st.markdown("""
<div class="slide-header">
    <div class="slide-title">🗣️ Répartition de la parole — 15 min</div>
    <div class="slide-subtitle">Guide pour la présentation orale du groupe</div>
</div>
""", unsafe_allow_html=True)

    st.info("💡 Ce slide est un guide interne — il n'est pas destiné à être projeté au jury.")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""### 👤 Personne A
**Intro & Contexte métier**
⏱️ ~4 min

**Slides :**
1. 🔁 Titre — présentation du groupe
2. 🎯 Contexte & Objectifs
3. 🏗️ Architecture — vue d'ensemble
4. 📐 RFM — Définitions

**Script indicatif :**
> "Notre projet consiste à construire une pipeline complète
> pour analyser les clients d'un e-commerce avec le modèle RFM.
> RFM c'est...
> L'architecture se décompose en 3 étapes..."

**Points à maîtriser :**
- Expliquer RFM sans jargon
- Décrire le flux de données de bout en bout
- Présenter le dataset et son contexte réel
        """)

    with col2:
        st.markdown("""### 👤 Personne B
**Partie technique ETL & Orchestration**
⏱️ ~5 min

**Slides :**
5. 🎯 Scoring (quintiles)
6. 🧮 Score final + exemples
7. 🏷️ Segmentation
8. 🌀 DAG Airflow

**Script indicatif :**
> "Pour calculer les scores, on utilise les quintiles.
> Concrètement ça veut dire qu'on divise les clients
> en 5 groupes... Voici Alice et Eve comme exemple.
> Ces 3 étapes sont orchestrées par un DAG Airflow..."

**Points à maîtriser :**
- Expliquer pd.qcut et les quintiles simplement
- Montrer le DAG dans Airflow (démo)
- Question probable : pourquoi PythonOperator ?
        """)

    with col3:
        st.markdown("""### 👤 Personne C
**Infrastructure Docker & Démo**
⏱️ ~6 min

**Slides :**
9. 🐳 Docker Compose
10. 🐋 Dockerfiles & x-common
11. 📊 Streamlit
12. 🖥️ Démonstration live
13. 🔍 Difficultés & Retour d'exp.

**Script indicatif :**
> "Tout tourne dans Docker. On a 5 services...
> Je vous montre le docker-compose.yml...
> On lance tout avec une seule commande.
> [DEMO] → Airflow → Trigger le DAG → Streamlit"

**Points à maîtriser :**
- Expliquer x-airflow-common (DRY)
- Différence image officielle vs image custom
- Parler des difficultés rencontrées avec conviction
        """)

    st.divider()
    st.markdown("#### ⏱️ Planning détaillé")
    import pandas as pd
    planning = pd.DataFrame({
        "Qui": ["A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C", "C"],
        "Slide": [
            "Titre", "Contexte & Objectifs", "Architecture", "RFM Définitions",
            "Scoring quintiles", "Score final + exemples", "Segmentation", "DAG Airflow",
            "Docker Compose", "Dockerfiles & x-common", "Streamlit",
            "Démonstration live", "Difficultés & Retour",
        ],
        "Durée": [
            "30s", "1 min", "1 min", "1 min 30s",
            "1 min", "1 min", "30s", "1 min 30s",
            "1 min", "1 min", "30s",
            "3 min", "1 min 30s",
        ],
        "Cumulé": [
            "0:30", "1:30", "2:30", "4:00",
            "5:00", "6:00", "6:30", "8:00",
            "9:00", "10:00", "10:30",
            "13:30", "15:00",
        ],
    })
    st.dataframe(planning, use_container_width=True, hide_index=True)
    st.caption("La démo live est la partie la plus importante — elle doit être répétée et ne pas dépasser 3 min.")


# ══════════════════════════════════════════════════════════════════════════
# REGISTRE
# ══════════════════════════════════════════════════════════════════════════
SLIDES = [
    ("🔁 Titre",                  slide_titre),
    ("🎯 Contexte & Objectifs",   slide_contexte),
    ("🏗️ Architecture",           slide_architecture),
    ("🛠️ Stack technique",        slide_stack),
    ("📐 RFM — Définitions",      slide_rfm_definition),
    ("🎯 Scoring (quintiles)",    slide_rfm_scoring),
    ("🧮 Score final + exemples", slide_rfm_exemples),
    ("🏷️ Segmentation",           slide_segmentation),
    ("🌀 DAG Airflow",            slide_dag),
    ("🐳 Docker Compose",         slide_docker),
    ("� Dockerfiles & x-common", slide_docker_details),
    ("�📊 Streamlit",              slide_streamlit),
    ("💡 Choix techniques",       slide_choix_techniques),
    ("📦 Livrables",              slide_livrables),
    ("🖥️ Démonstration live",      slide_demo),
    ("🔍 Difficultés & Retour",    slide_retrospective),
    ("🗣️ Répartition parole",      slide_repartition),
]
N = len(SLIDES)

# ══════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════
if "slide_idx" not in st.session_state:
    st.session_state.slide_idx = 0

# ══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🎓 Présentation")
    mode = st.radio("Mode d'affichage", ["🎞️ Diaporama", "📄 Vue complète"], index=0)
    st.divider()
    if mode == "🎞️ Diaporama":
        st.markdown("**Aller à la slide :**")
        for i, (title, _) in enumerate(SLIDES):
            btn_type = "primary" if i == st.session_state.slide_idx else "secondary"
            if st.button(title, key=f"nav_{i}", use_container_width=True, type=btn_type):
                st.session_state.slide_idx = i
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════
# RENDU
# ══════════════════════════════════════════════════════════════════════════
if mode == "🎞️ Diaporama":
    idx = st.session_state.slide_idx

    # Barre de navigation haute
    col_prev, col_info, col_next = st.columns([1, 3, 1])
    with col_prev:
        if st.button("◀ Précédent", disabled=(idx == 0), use_container_width=True):
            st.session_state.slide_idx -= 1
            st.rerun()
    with col_info:
        st.markdown(
            f"<div style='text-align:center;padding:0.4rem;color:#a0aec0;font-weight:600;'>"
            f"Slide {idx + 1} / {N} — {SLIDES[idx][0]}</div>",
            unsafe_allow_html=True,
        )
        st.progress((idx + 1) / N)
    with col_next:
        if st.button("Suivant ▶", disabled=(idx == N - 1), use_container_width=True):
            st.session_state.slide_idx += 1
            st.rerun()

    st.divider()
    SLIDES[idx][1]()
    st.divider()

    # Barre de navigation basse
    col_prev2, _, col_next2 = st.columns([1, 3, 1])
    with col_prev2:
        if st.button("◀ Précédent ", disabled=(idx == 0), use_container_width=True):
            st.session_state.slide_idx -= 1
            st.rerun()
    with col_next2:
        if st.button(" Suivant ▶", disabled=(idx == N - 1), use_container_width=True):
            st.session_state.slide_idx += 1
            st.rerun()

else:
    for title, render_fn in SLIDES:
        render_fn()
        st.divider()
