#!/bin/bash
set -e

FUNCTIONS_URL="https://raw.githubusercontent.com/rabravo/ws2pgdb/master/sql/functions.sql"

# Create extensions for the default database
psql --username "${PGUSER:-postgres}" --dbname "${PGDATABASE:-postgres}" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS plr;
    CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
    CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
EOSQL

# Fetch and load custom SQL functions (requires PostGIS + PLR)
echo "Fetching SQL functions from GitHub..."
wget -q -O /tmp/functions.sql "$FUNCTIONS_URL"
psql --username "${PGUSER:-postgres}" --dbname "${PGDATABASE:-postgres}" -f /tmp/functions.sql
echo "SQL functions loaded successfully."
