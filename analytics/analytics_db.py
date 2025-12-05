import os
import sqlite3
import logging
logger = logging.getLogger("MyPhotoApp.Analytics")

DB_PATH = os.path.join(os.path.dirname(__file__), "media_index.db")


def get_connection():
    """Abre uma ligação SQLite com foreign keys ativas."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Cria a base de dados e aplica schema.sql."""
    schema_path = os.path.join(os.path.dirname(__file__), "analytics_schema.sql")
    conn = get_connection()
    with open(schema_path, "r", encoding="utf8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()