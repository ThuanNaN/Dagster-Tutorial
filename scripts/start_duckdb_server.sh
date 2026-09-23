#!/bin/bash
# scripts/start_duckdb_server.sh
# Starts DuckDB server for Metabase JDBC connection.
# Usage: ./scripts/start_duckdb_server.sh [data_dir] [port]

set -e

DATA_DIR="${1:-./data}"
PORT="${2:-8080}"
DB_FILE="${DATA_DIR}/mydb.duckdb"

echo "Starting DuckDB server..."
echo "  Data dir: $DATA_DIR"
echo "  Port: $PORT"
echo "  DB file: $DB_FILE"

duckdb --serve \
    --host 0.0.0.0 \
    --port "$PORT" \
    "$DB_FILE"
