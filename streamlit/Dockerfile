FROM python:3.11-slim

# Installer uv (binaire statique, aucune dépendance)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Copier uniquement le manifeste d'abord pour profiter du cache Docker
COPY pyproject.toml .
RUN uv sync --no-dev

COPY app.py .
COPY pages/ pages/

EXPOSE 8501

# uv run exécute dans le venv créé par uv sync
CMD ["uv", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
