FROM apache/airflow:2.9.2

# L'image Airflow utilise par défaut l'utilisateur 'airflow' (non-root).
# pip install --user installe dans /home/airflow/.local/ → chemin correct pour Airflow.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
