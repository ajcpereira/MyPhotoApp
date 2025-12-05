
import logging
logger = logging.getLogger("MyPhotoApp.Analytics")
from .analytics_db import get_connection

def get_duplicates_sha256(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT sha256, COUNT(*)
        FROM hash_meta
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()
    return rows


def get_live_photos(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f1.full_path AS jpg, f2.full_path AS mov
        FROM files f1
        JOIN files f2 ON f1.basename = f2.basename
        WHERE f1.extension IN ('.jpg','.jpeg','.png','.heic')
          AND f2.extension = '.mov'
        """
    ).fetchall()
    return rows


def get_corrupted_files(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT full_path, read_error
        FROM files
        WHERE is_corrupted = 1
        """
    ).fetchall()
    return rows


def get_basic_stats(conn=None):
    conn = conn or get_connection()
    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    images = conn.execute("SELECT COUNT(*) FROM files WHERE is_image=1").fetchone()[0]
    videos = conn.execute("SELECT COUNT(*) FROM files WHERE is_video=1").fetchone()[0]
    corrupt = (
        conn.execute("SELECT COUNT(*) FROM files WHERE is_corrupted=1").fetchone()[0]
    )
    return {
        "total": total,
        "images": images,
        "videos": videos,
        "corrupted": corrupt,
    }