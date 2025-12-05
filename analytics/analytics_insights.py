import math
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from datetime import datetime
logger = logging.getLogger("MyPhotoApp.Analytics")

from .analytics_db import get_connection
import logging


# -------------------------------------------------------------------
#  SECÇÃO 1: ESTATÍSTICAS DO DATASET
# -------------------------------------------------------------------

def get_year_month_counts(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT year, month, COUNT(*)
        FROM files
        WHERE year IS NOT NULL AND month IS NOT NULL
        GROUP BY year, month
        ORDER BY year, month
        """
    ).fetchall()
    return rows


def get_resolution_stats(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT width, height, COUNT(*)
        FROM (
            SELECT file_id, width, height FROM image_meta
            UNION ALL
            SELECT file_id, width, height FROM video_meta
        )
        GROUP BY width, height
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()
    return rows


def get_camera_ranking(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT exif_camera_model, COUNT(*)
        FROM image_meta
        WHERE exif_camera_model IS NOT NULL
        GROUP BY exif_camera_model
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()
    return rows


def get_lens_ranking(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT exif_lens, COUNT(*)
        FROM image_meta
        WHERE exif_lens IS NOT NULL
        GROUP BY exif_lens
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()
    return rows


def get_dark_images(threshold=0.05, conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f.full_path, i.brightness_mean
        FROM files f
        JOIN image_meta i ON i.file_id = f.id
        WHERE i.brightness_mean < ?
        ORDER BY i.brightness_mean ASC
        """,
        (threshold,),
    ).fetchall()
    return rows


def get_bright_images(threshold=0.95, conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f.full_path, i.brightness_mean
        FROM files f
        JOIN image_meta i ON i.file_id = f.id
        WHERE i.brightness_mean > ?
        ORDER BY i.brightness_mean DESC
        """,
        (threshold,),
    ).fetchall()
    return rows


def get_truncated_videos(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f.full_path, v.nb_frames, v.bitrate
        FROM files f
        JOIN video_meta v ON v.file_id = f.id
        WHERE v.nb_frames < 5 OR v.bitrate IS NULL OR v.bitrate = 0
        """
    ).fetchall()
    return rows


# -------------------------------------------------------------------
#  SECÇÃO 2: DUPLICADOS (SHA + PERCEPTUAL)
# -------------------------------------------------------------------

def _hamming(a: str, b: str) -> int:
    if a is None or b is None:
        return 9999
    try:
        return bin(int(a, 16) ^ int(b, 16)).count("1")
    except Exception:
        return 9999


def find_sha_duplicates(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT sha256, COUNT(*)
        FROM hash_meta
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    return rows


def find_phash_similar(threshold=10, conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT file_id, phash FROM hash_meta
        WHERE phash IS NOT NULL
        """
    ).fetchall()

    results = []
    for i in range(len(rows)):
        id1, h1 = rows[i]
        for j in range(i + 1, len(rows)):
            id2, h2 = rows[j]
            dist = _hamming(h1, h2)
            if dist <= threshold:
                results.append((id1, id2, dist))

    return results


def find_visual_clusters(threshold=10, conn=None):
    similar = find_phash_similar(threshold, conn)
    clusters = defaultdict(set)

    def find_root(x, parent):
        while parent[x] != x:
            x = parent[x]
        return x

    parent = {}
    for a, b, _ in similar:
        parent.setdefault(a, a)
        parent.setdefault(b, b)

    for a, b, _ in similar:
        ra = find_root(a, parent)
        rb = find_root(b, parent)
        if ra != rb:
            parent[rb] = ra

    for k in parent:
        root = find_root(k, parent)
        clusters[root].add(k)

    return [list(v) for v in clusters.values() if len(v) > 1]


# -------------------------------------------------------------------
#  SECÇÃO 3: AGRUPAMENTO TEMPORAL, GPS & LIVE PHOTOS
# -------------------------------------------------------------------

def group_by_time(gap_seconds=120, conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT id, full_path, created_date
        FROM files
        WHERE created_date IS NOT NULL
        ORDER BY created_date
        """
    ).fetchall()

    groups = []
    current = []

    def parse(dt):
        return datetime.fromisoformat(dt) if dt else None

    for file_id, path, dt in rows:
        dtp = parse(dt)
        if dtp is None:
            continue

        if not current:
            current.append((file_id, path, dtp))
            continue

        last_dt = current[-1][2]
        if (dtp - last_dt).total_seconds() <= gap_seconds:
            current.append((file_id, path, dtp))
        else:
            groups.append(current)
            current = [(file_id, path, dtp)]

    if current:
        groups.append(current)

    return groups


def _gps_distance_m(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return 999999

    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def group_by_gps(distance_m=50, conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f.id, f.full_path, i.gps_lat, i.gps_lon
        FROM files f
        JOIN image_meta i ON i.file_id = f.id
        WHERE gps_lat IS NOT NULL AND gps_lon IS NOT NULL
        """
    ).fetchall()

    clusters = []
    used = set()

    for i in range(len(rows)):
        if rows[i][0] in used:
            continue

        group = [rows[i]]
        used.add(rows[i][0])

        for j in range(i + 1, len(rows)):
            if rows[j][0] in used:
                continue

            lat1 = rows[i][2]
            lon1 = rows[i][3]
            lat2 = rows[j][2]
            lon2 = rows[j][3]

            if _gps_distance_m(lat1, lon1, lat2, lon2) <= distance_m:
                group.append(rows[j])
                used.add(rows[j][0])

        clusters.append(group)

    return clusters


def detect_live_photo_pairs(conn=None):
    conn = conn or get_connection()
    rows = conn.execute(
        """
        SELECT f.id, f.full_path, f.basename, f.extension, f.created_date
        FROM files f
        ORDER BY f.basename
        """
    ).fetchall()

    images = defaultdict(list)
    videos = defaultdict(list)

    for file_id, path, base, ext, dt in rows:
        ext_l = (ext or "").lower()
        if ext_l in [".jpg", ".jpeg", ".png", ".heic"]:
            images[base].append((file_id, path, dt))
        elif ext_l in [".mov", ".mp4"]:
            videos[base].append((file_id, path, dt))

    pairs = []

    def parse(dt):
        try:
            return datetime.fromisoformat(dt)
        except Exception:
            return None

    for base in images:
        if base in videos:
            for img in images[base]:
                for vid in videos[base]:
                    t1 = parse(img[2])
                    t2 = parse(vid[2])
                    if t1 and t2 and abs((t1 - t2).total_seconds()) < 3:
                        pairs.append((img[1], vid[1]))

    return pairs