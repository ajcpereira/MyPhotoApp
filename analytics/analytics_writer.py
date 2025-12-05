import json
import logging
import sqlite3
from typing import Dict, Any
logger = logging.getLogger("MyPhotoApp.Analytics")

from .analytics_db import get_connection



def insert_entry(conn, entry: Dict[str, Any]) -> int:
    """Insere um FileEntry completo no SQLite e devolve o file_id."""
    cur = conn.cursor()

    # 1. Inserir ficheiro base
    try:
        cur.execute(
            """
            INSERT INTO files (
                full_path, filename, basename, extension, mime_type,
                size, created_date, modified_date, birth_date,
                year, month, inode,
                is_image, is_video, is_audio,
                is_corrupted, read_error, is_usable
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)

            """,
            (
                entry["full_path"],
                entry["filename"],
                entry["basename"],
                entry["extension"],
                entry["mime_type"],
                entry["size"],
                entry["created_date"],
                entry["modified_date"],
                entry["birth_date"],
                entry["year"],
                entry["month"],
                entry["inode"],
                int(bool(entry["is_image"])),
                int(bool(entry["is_video"])),
                int(bool(entry["is_audio"])),
                int(bool(entry["is_corrupted"])),
                entry["read_error"],
                int(bool(entry["is_usable"])),

            ),
        )
        file_id = cur.lastrowid
    except sqlite3.IntegrityError:
        row = cur.execute(
            "SELECT id FROM files WHERE full_path = ?", (entry["full_path"],)
        ).fetchone()
        file_id = row[0]

    # 2. Metadados de imagem
    if entry["is_image"]:
        cur.execute("DELETE FROM image_meta WHERE file_id = ?", (file_id,))
        cur.execute(
            """
            INSERT INTO image_meta (
                file_id, width, height, brightness_mean, hist_16bins,
                exif_datetime_original, exif_camera_model, exif_lens,
                exif_orientation, exif_iso, exif_fnumber,
                exif_exposure_time, exif_focal_length, gps_lat, gps_lon
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                file_id,
                entry["width"],
                entry["height"],
                entry["brightness_mean"],
                json.dumps(entry["hist_16bins"]),
                entry["exif_datetime_original"],
                entry["exif_camera_model"],
                entry["exif_lens"],
                entry["exif_orientation"],
                entry["exif_iso"],
                entry["exif_fnumber"],
                entry["exif_exposure_time"],
                entry["exif_focal_length"],
                entry["gps_lat"],
                entry["gps_lon"],
            ),
        )

    # 3. Metadados de vídeo
    if entry["is_video"]:
        cur.execute("DELETE FROM video_meta WHERE file_id = ?", (file_id,))
        cur.execute(
            """
            INSERT INTO video_meta (
                file_id, width, height, duration, fps,
                bitrate, nb_frames, rotation,
                video_codec, audio_codec
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                file_id,
                entry["width"],
                entry["height"],
                entry["duration"],
                entry["fps"],
                entry["bitrate"],
                entry["nb_frames"],
                entry["rotation"],
                entry["video_codec"],
                entry["audio_codec"],
            ),
        )

    # 4. Hashes
    cur.execute("DELETE FROM hash_meta WHERE file_id = ?", (file_id,))
    cur.execute(
        """
        INSERT INTO hash_meta (
            file_id, sha256, phash, ahash, dhash, whash
        ) VALUES (?,?,?,?,?,?)
        """,
        (
            file_id,
            entry["sha256"],
            entry["phash"],
            entry["ahash"],
            entry["dhash"],
            entry["whash"],
        ),
    )

    return file_id