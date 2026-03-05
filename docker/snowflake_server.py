#!/usr/bin/env python3
"""
Snowflake Local Emulator

Uses fakesnow (https://github.com/tekumara/fakesnow) which emulates
Snowflake locally using DuckDB.

PREREQUISITES:
  python3 -m pip install 'fakesnow[server]'

To start the server (foreground):
  python3 docker/snowflake_server.py

To start the server (background):
  python3 docker/snowflake_server.py &

To stop the background server:
  kill %1   (or kill the process)

Connection details (for Go / Python / any Snowflake connector):
  Host:      127.0.0.1
  Port:      8084
  Account:   fakesnow
  User:      fake
  Password:  snow
  Database:  smf
  Protocol:  http

Go DSN (gosnowflake):
  "fake:snow@fakesnow/smf?host=127.0.0.1&port=8084&protocol=http"

snowsql CLI:
  snowsql -a fakesnow -u fake -p snow -h 127.0.0.1 -P 8084 --protocol http
"""

import os
import signal
import sys

PORT = 8084
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "snowflake")

def main():
    try:
        import fakesnow
    except ImportError as e:
        print(f"fakesnow is not installed ({e}). Install it with:")
        print("  python3 -m pip install 'fakesnow[server]'")
        sys.exit(1)

    if not hasattr(fakesnow, "server"):
        print("fakesnow[server] extra is not installed. Install it with:")
        print("  python3 -m pip install 'fakesnow[server]'")
        sys.exit(1)

    os.makedirs(DB_PATH, exist_ok=True)

    print(f"Starting fakesnow server on port {PORT}...")
    print(f"Database persistence: {DB_PATH}")
    print()
    print("Connection details:")
    print(f"  Host:     127.0.0.1")
    print(f"  Port:     {PORT}")
    print(f"  Account:  fakesnow")
    print(f"  User:     fake")
    print(f"  Password: snow")
    print(f"  Protocol: http")
    print()
    print("Press Ctrl+C to stop.")
    print()

    with fakesnow.server(
        port=PORT,
        session_parameters={"FAKESNOW_DB_PATH": DB_PATH},
    ) as conn_kwargs:
        print(f"fakesnow server is ready! Listening on 127.0.0.1:{PORT}")

        # Block until interrupted
        signal.pause()


if __name__ == "__main__":
    main()
