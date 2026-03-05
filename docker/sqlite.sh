# SQLite does not need Docker — it is a file-based database.
# On macOS it is pre-installed at /usr/bin/sqlite3.
# If missing, install via:  brew install sqlite
#
# Usage (the setup script creates the DB file for you):
#   bash docker/sqlite.yml       # creates ./data/sqlite/smf.db with WAL mode
#   sqlite3 ./data/sqlite/smf.db # connect to the database
#
# To tear down:
#   rm -rf ./data/sqlite
#
# The file can also be used directly by Go tests:
#   DSN="./data/sqlite/smf.db"

#!/usr/bin/env bash
set -euo pipefail

DB_DIR="./data/sqlite"
DB_FILE="${DB_DIR}/smf.db"

if ! command -v sqlite3 &>/dev/null; then
  echo "sqlite3 not found. Install with:  brew install sqlite"
  exit 1
fi

mkdir -p "${DB_DIR}"

# Create the database and enable WAL mode for concurrent access
sqlite3 "${DB_FILE}" "PRAGMA journal_mode=WAL;"

echo "SQLite database ready at: ${DB_FILE}"
echo "Connect with:  sqlite3 ${DB_FILE}"
sqlite3 "${DB_FILE}" "SELECT sqlite_version();"
