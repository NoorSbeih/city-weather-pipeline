"""Start a throwaway Postgres without Docker (dev convenience; `pip install pgserver`).

Prints a DATABASE_URL and keeps the server running until Ctrl+C.
"""

import time
from pathlib import Path

import pgserver

DATA_DIR = Path(__file__).resolve().parent.parent / ".pgdata"

server = pgserver.get_server(DATA_DIR, cleanup_mode="stop")
server.psql("SELECT 1;")
print(f"DATABASE_URL={server.get_uri()}", flush=True)
try:
    while True:
        time.sleep(3600)
except KeyboardInterrupt:
    pass
