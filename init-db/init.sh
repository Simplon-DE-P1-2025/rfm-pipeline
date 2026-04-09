#!/bin/bash
set -e

# Créer la base de données rfm_db si elle n'existe pas
psql -U "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE rfm_db;
    GRANT ALL PRIVILEGES ON DATABASE rfm_db TO $POSTGRES_USER;
EOSQL
