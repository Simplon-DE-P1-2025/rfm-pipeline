# Pipeline Data RFM — Docker & Airflow

Pipeline de données pour l'analyse **RFM** (Recency · Frequency · Monetary) orchestrée avec Apache Airflow et dockerisée avec Docker Compose.

## Architecture

```
online_retail_II.xlsx
        │
        ▼
  [ingest.py] ──► raw_online_retail (PostgreSQL)
        │
        ▼
  [transform.py] ──► rfm_scores (PostgreSQL)
        │
        ▼
  [load.py] ──────► rfm_segments (PostgreSQL)
```

Les 3 tâches sont orchestrées séquentiellement par le DAG Airflow `rfm_pipeline`.

## Services Docker

| Service             | Image                     | Port | Rôle                               |
|---------------------|---------------------------|------|------------------------------------|
| `postgres`          | `postgres:15`             | 5432 | Stockage données + métadonnées Airflow |
| `airflow-init`      | image custom (airflow/)   | —    | Initialisation DB + création admin (tâche unique) |
| `airflow-webserver` | image custom (airflow/)   | 8080 | Interface web Airflow              |
| `airflow-scheduler` | image custom (airflow/)   | —    | Exécution des DAGs                 |
| `streamlit`         | image custom (streamlit/) | 8501 | Dashboard de visualisation RFM     |

## Prérequis

- Docker Engine ≥ 24
- Docker Compose v2+

## Installation

### 1. Télécharger le dataset

Télécharger `online_retail_II.xlsx` depuis [UCI ML Repository](https://archive.uci.edu/dataset/502/online+retail+ii) et le placer dans `data/` :

```
data/
└── online_retail_II.xlsx
```

### 2. Configurer l'UID (Linux uniquement)

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Builder les images et lancer les services

```bash
docker compose build
docker compose up -d
```

Le premier démarrage prend quelques minutes (build des images + init Airflow).

Vérifier que tout est up :
```bash
docker compose ps
```

> `airflow-init` avec le statut `Exited (0)` est **normal** — c'est une tâche unique qui se termine après l'initialisation.

### 4. Accéder à Airflow

- URL : **http://localhost:8080**
- Login : `admin` / `admin`

### 5. Lancer la pipeline

1. Ouvrir http://localhost:8080
2. Activer le DAG **`rfm_pipeline`** (toggle ON)
3. Cliquer sur **▶ Trigger DAG**
4. Suivre l'exécution dans la vue Graph ou Grid

### 6. Visualiser les résultats

- Dashboard Streamlit : **http://localhost:8501**

## Pipeline RFM — détail des tâches

```
ingest ──► transform ──► load
```

| Tâche       | Script             | Table PostgreSQL    | Description                            |
|-------------|--------------------|--------------------|----------------------------------------|
| `ingest`    | `etl/ingest.py`    | `raw_online_retail` | Charge les données brutes depuis Excel |
| `transform` | `etl/transform.py` | `rfm_scores`        | Nettoie et calcule les scores RFM 1-5  |
| `load`      | `etl/load.py`      | `rfm_segments`      | Segmente les clients en 4 catégories   |

### Scores RFM

Chaque client reçoit un score de 1 à 5 pour chaque dimension (méthode des quintiles) :

| Dimension | Calcul | Score 5 = meilleur |
|-----------|--------|--------------------|
| R — Recency | Jours depuis le dernier achat | Client très récent |
| F — Frequency | Nombre de commandes distinctes | Client très fidèle |
| M — Monetary | Montant total dépensé | Gros acheteur |

### Segments RFM

| Segment          | Score Total (R+F+M) |
|------------------|---------------------|
| Champions        | ≥ 12                |
| Loyal Customers  | 9 – 11              |
| At Risk          | 6 – 8               |
| Lost             | < 6                 |

## Dashboard Streamlit

Le dashboard est accessible sur **http://localhost:8501** et propose 3 pages :

| Page | Description |
|------|-------------|
| 🎓 Présentation | Diaporama du projet (14 slides, mode jury) |
| 📊 Dashboard RFM | KPIs, graphiques Plotly, segmentation clients |
| 🔍 Explorateur SQL | Éditeur SQL avec requêtes prédéfinies sur les 3 tables |

## Accéder aux données (PostgreSQL)

```bash
docker compose exec postgres psql -U airflow -d rfm_db
```

```sql
-- Résultat final
SELECT * FROM rfm_segments ORDER BY rfm_total DESC LIMIT 20;

-- Distribution des segments
SELECT segment, COUNT(*) FROM rfm_segments GROUP BY segment ORDER BY COUNT(*) DESC;
```

## Arrêter les services

```bash
# Arrêt simple (données conservées)
docker compose down

# Arrêt + suppression des volumes (réinitialisation complète)
docker compose down -v
```

## Structure du projet

```
pipeline-docker/
├── docker-compose.yml          # Orchestration des 5 services
├── .env                        # Variables d'environnement (non commité)
├── .gitignore
├── airflow/
│   ├── Dockerfile              # Image Airflow personnalisée (pip install --user)
│   └── requirements.txt        # pandas, openpyxl, psycopg2-binary
├── dags/
│   └── rfm_pipeline.py         # DAG Airflow : ingest → transform → load
├── etl/
│   ├── ingest.py               # Étape 1 : Ingestion Excel → PostgreSQL
│   ├── transform.py            # Étape 2 : Nettoyage + calcul RFM + scoring
│   └── load.py                 # Étape 3 : Segmentation clients
├── streamlit/
│   ├── Dockerfile              # Image Streamlit (uv + python:3.11-slim)
│   ├── pyproject.toml          # Dépendances Python (uv)
│   ├── app.py                  # Point d'entrée (accueil multi-pages)
│   └── pages/
│       ├── 1_🎓_Présentation.py    # Diaporama jury (17 slides)
│       ├── 2_📊_Dashboard_RFM.py   # Visualisations Plotly
│       └── 3_🔍_Explorateur_SQL.py # Éditeur SQL interactif
├── init-db/
│   └── init.sh                 # Création de la base rfm_db au démarrage PostgreSQL
├── data/                       # Placer online_retail_II.xlsx ici (non commité)
├── logs/                       # Logs Airflow (auto-généré, non commité)
└── plugins/                    # Plugins Airflow (vide)
```

## Stack technique

| Outil | Version | Rôle |
|-------|---------|------|
| Apache Airflow | 2.9.2 | Orchestration (LocalExecutor) |
| PostgreSQL | 15 | Stockage données + métadonnées |
| Streamlit | 1.33 | Dashboard de visualisation |
| pandas | 2.1 | Manipulation des données |
| SQLAlchemy | 2.0 | Connexion Python ↔ PostgreSQL |
| uv | latest | Gestionnaire de packages (Streamlit) |
| Docker Compose | v2 | Orchestration des conteneurs |

