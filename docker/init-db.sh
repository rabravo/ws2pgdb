#!/bin/bash
set -e

FUNCTIONS_URL="https://raw.githubusercontent.com/rabravo/ws2pgdb/main/sql/pg.spi.foo.sql"
DB_NAME="us_gis"

# Create the us_gis database
psql --username "${PGUSER:-postgres}" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS $DB_NAME;
EOSQL

# Create extensions in us_gis
psql --username "${PGUSER:-postgres}" --dbname "$DB_NAME" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS plr;
    CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
    CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
EOSQL

# Fetch and load custom SQL functions (requires PostGIS + PLR)
echo "Fetching SQL functions from GitHub..."
wget -q -O /tmp/pg.spi.foo.sql "$FUNCTIONS_URL"
psql --username "${PGUSER:-postgres}" --dbname "$DB_NAME" -f /tmp/pg.spi.foo.sql
echo "SQL functions loaded successfully."
