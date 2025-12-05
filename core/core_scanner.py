import logging
logger = logging.getLogger("MyPhotoApp.Core")

import os
import io
import mimetypes
import hashlib
import platform
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

# python-magic (optional)
try:
    import magic
except ImportError:
    magic = None

# Pillow / EXIF / perceptual hashing
try:
    from PIL import Image, ExifTags
    import imagehash
except ImportError:
    Image = None
    ExifTags = None
    imagehash = None

# ffmpeg-python
try:
    import ffmpeg
except ImportError:
    ffmpeg = None


FileEntry = Dict[str, Any]

# -------------------------------------------------------------------
# Thread pool
# -------------------------------------------------------------------
MAX_WORKERS = max(2, (os.cpu_count() // 2))
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


# -------------------------------------------------------------------
# ffmpeg/ffprobe path helper
# -------------------------------------------------------------------
def _local_bin_path(name: str) -> str:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    bin_dir = os.path.join(root, "bin")
    exe = name + ".exe" if platform.system() == "Windows" else name
    return os.path.join(bin_dir, exe)


# -------------------------------------------------------------------
# SHA256
# -------------------------------------------------------------------
def compute_sha256(path: str) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return "ERROR"


# -------------------------------------------------------------------
# MIME detection
# -------------------------------------------------------------------
def detect_mime_type(path: str) -> str:
    if magic:
        try:
            m = magic.Magic(mime=True)
            return m.from_file(path) or "application/octet-stream"
        except Exception:
            pass

    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"


def classify_mime(mime_type: str) -> Dict[str, bool]:
    return {
        "is_image": mime_type.startswith("image/"),
        "is_video": mime_type.startswith("video/"),
        "is_audio": mime_type.startswith("audio/"),
    }
# -------------------------------------------------------------------
# Filesystem metadata
# -------------------------------------------------------------------
def get_fs_metadata(path: str) -> Dict[str, Any]:
    st = os.stat(path)

    modified = datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
    created = datetime.fromtimestamp(st.st_ctime).isoformat(timespec="seconds")

    birth = getattr(st, "st_birthtime", None)
    if birth:
        birth = datetime.fromtimestamp(birth).isoformat(timespec="seconds")
    else:
        birth = created

    return {
        "modified_date": modified,
        "fs_created_date": created,
        "birth_date": birth,
        "inode": getattr(st, "st_ino", None),
    }


# -------------------------------------------------------------------
# EXIF helpers
# -------------------------------------------------------------------
def _convert_to_degrees(value):
    try:
        d, m, s = value

        def _to_float(x):
            return float(x[0]) / float(x[1]) if isinstance(x, tuple) else float(x)

        d = _to_float(d)
        m = _to_float(m)
        s = _to_float(s)

        return d + m / 60 + s / 3600
    except Exception:
        return None


def _parse_gps_info(gps_info):
    lat = gps_info.get(2) or gps_info.get("GPSLatitude")
    lat_ref = gps_info.get(1) or gps_info.get("GPSLatitudeRef")
    lon = gps_info.get(4) or gps_info.get("GPSLongitude")
    lon_ref = gps_info.get(3) or gps_info.get("GPSLongitudeRef")

    if not (lat and lat_ref and lon and lon_ref):
        return None, None

    la = _convert_to_degrees(lat)
    lo = _convert_to_degrees(lon)

    if la is None or lo is None:
        return None, None

    if lat_ref.upper() == "S":
        la = -la
    if lon_ref.upper() == "W":
        lo = -lo

    return la, lo


def _safe_exif_value(v):
    if v is None:
        return None
    if hasattr(v, "numerator") and hasattr(v, "denominator"):
        try:
            return float(v.numerator) / float(v.denominator)
        except:
            return str(v)
    if isinstance(v, tuple) and len(v) == 2:
        try:
            return float(v[0]) / float(v[1])
        except:
            return str(v)
    if isinstance(v, (int, float, str)):
        return v
    return str(v)


# -------------------------------------------------------------------
# Image error classification (recommended rule set)
# -------------------------------------------------------------------
def _classify_image_error_message(msg: str) -> Dict[str, bool]:
    msg = msg.lower()

    soft = [
        "image file is truncated",
        "truncated",
        "broken data stream",
        "wrong number of bytes",
        "incomplete jpeg",
        "premature end of file",
        "unexpected end of data",
    ]

    hard = [
        "cannot identify image file",
        "zlib.error",
        "decoder error",
        "invalid",
        "missing soi",
        "missing eoi",
    ]

    if any(s in msg for s in soft):
        return {"is_corrupted": True, "is_usable": True}

    if any(h in msg for h in hard):
        return {"is_corrupted": True, "is_usable": False}

    return {"is_corrupted": True, "is_usable": False}


# -------------------------------------------------------------------
# IMAGE METADATA (WITH VERIFY + EXT/FORMAT MISMATCH)
# -------------------------------------------------------------------
def extract_image_metadata(path: str, logger=None) -> Dict[str, Any]:

    data = {
        "width": None,
        "height": None,
        "exif_datetime_original": None,
        "exif_camera_model": None,
        "exif_lens": None,
        "exif_orientation": None,
        "exif_iso": None,
        "exif_fnumber": None,
        "exif_exposure_time": None,
        "exif_focal_length": None,
        "gps_lat": None,
        "gps_lon": None,

        "phash": None,
        "ahash": None,
        "dhash": None,
        "whash": None,

        "brightness_mean": None,
        "hist_16bins": None,

        "is_corrupted": False,
        "is_usable": True,
        "read_error": None,
    }

    if not Image or not imagehash:
        return data

    ext = os.path.splitext(path)[1].lower()
    expected_format = {
        ".png": "PNG",
        ".jpg": "JPEG",
        ".jpeg": "JPEG",
        ".mpo": "MPO",
        ".tif": "TIFF",
        ".tiff": "TIFF",
        ".bmp": "BMP",
        ".gif": "GIF",
        ".webp": "WEBP",
    }

    # ---------------------------
    # 1) VERIFY STRUCTURE
    # ---------------------------
    soft_corruption = False
    real_format = None

    try:
        with Image.open(path) as img_verify:
            real_format = img_verify.format  # e.g. PNG, JPEG, MPO
            img_verify.verify()
    except Exception as e:
        msg = str(e)
        clas = _classify_image_error_message(msg)
        data["is_corrupted"] = clas["is_corrupted"]
        data["is_usable"] = clas["is_usable"]
        data["read_error"] = msg

        if data["is_usable"]:
            soft_corruption = True
            if logger:
                logger.warning(f"Soft corruption (verify): {path} ({msg})")
        else:
            if logger:
                logger.warning(f"Hard corruption (verify): {path} ({msg})")
            return data

    # ---------------------------
    # 2) EXTENSION ↔ FORMAT mismatch
    # ---------------------------
    if ext in expected_format and real_format and expected_format[ext] != real_format:
        data["is_corrupted"] = True
        data["is_usable"] = True
        data["read_error"] = f"Extension/format mismatch: ext={ext} format={real_format}"

        if logger:
            logger.warning(
                f"Mismatched extension: {path} (ext={ext}, format={real_format})"
            )

        soft_corruption = True

    # ---------------------------
    # 3) LOAD and EXTRACT METADATA
    # ---------------------------
    try:
        with Image.open(path) as img:
            try:
                img.load()
            except Exception as e:
                msg = str(e)
                clas = _classify_image_error_message(msg)
                data["is_corrupted"] = clas["is_corrupted"]
                data["is_usable"] = clas["is_usable"]

                if not data["read_error"]:
                    data["read_error"] = msg

                if not data["is_usable"]:
                    return data

                soft_corruption = True

            data["width"], data["height"] = img.size

            # HASHES
            try:
                data["phash"] = str(imagehash.phash(img))
                data["ahash"] = str(imagehash.average_hash(img))
                data["dhash"] = str(imagehash.dhash(img))
                data["whash"] = str(imagehash.whash(img))
            except Exception as e:
                soft_corruption = True
                if not data["read_error"]:
                    data["read_error"] = str(e)

            # BRIGHTNESS / HIST
            try:
                gray = img.convert("L")
                hist = gray.histogram()
                total = sum(hist) or 1
                data["brightness_mean"] = sum(i * c for i, c in enumerate(hist)) / (
                    255 * total
                )
                data["hist_16bins"] = [
                    sum(hist[i * 16 : (i + 1) * 16]) for i in range(16)
                ]
            except Exception as e:
                soft_corruption = True

            # EXIF
            raw = getattr(img, "_getexif", lambda: None)()
            if raw:
                exif = {ExifTags.TAGS.get(t, t): v for t, v in raw.items()}

                dt = exif.get("DateTimeOriginal") or exif.get("DateTime")
                if dt:
                    try:
                        data["exif_datetime_original"] = datetime.strptime(
                            dt, "%Y:%m:%d %H:%M:%S"
                        ).isoformat(timespec="seconds")
                    except:
                        soft_corruption = True

                data["exif_camera_model"] = exif.get("Model")
                data["exif_lens"] = exif.get("LensModel")
                data["exif_orientation"] = exif.get("Orientation")
                data["exif_iso"] = exif.get("ISOSpeedRatings")
                data["exif_fnumber"] = exif.get("FNumber")
                data["exif_exposure_time"] = exif.get("ExposureTime")
                data["exif_focal_length"] = exif.get("FocalLength")

                gps = exif.get("GPSInfo")
                if gps:
                    la, lo = _parse_gps_info(gps)
                    data["gps_lat"] = la
                    data["gps_lon"] = lo

    except Exception as e:
        data["is_corrupted"] = True
        data["is_usable"] = False
        data["read_error"] = str(e)
        return data

    if soft_corruption:
        data["is_corrupted"] = True

    return data
# -------------------------------------------------------------------
# VIDEO METADATA
# -------------------------------------------------------------------
def find_ffprobe(logger=None):
    p = shutil.which("ffprobe")
    if p:
        return p

    local = _local_bin_path("ffprobe")
    if os.path.exists(local):
        return local

    if logger:
        logger.warning("ffprobe não encontrado.")
    return None


def extract_video_metadata(path: str, logger=None) -> Dict[str, Any]:
    data = {
        "duration": None,
        "fps": None,
        "bitrate": None,
        "nb_frames": None,
        "rotation": None,
        "video_codec": None,
        "audio_codec": None,
        "width": None,
        "height": None,
        "is_corrupted": False,
        "is_usable": True,
        "read_error": None,
    }

    if not ffmpeg:
        return data

    cmd = find_ffprobe(logger)
    if not cmd:
        data["is_corrupted"] = True
        data["is_usable"] = False
        data["read_error"] = "ffprobe not found"
        return data

    try:
        info = ffmpeg.probe(path, cmd=cmd)

        fmt = info.get("format", {})
        data["duration"] = float(fmt.get("duration")) if fmt.get("duration") else None
        data["bitrate"] = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None

        usable_video_stream = False

        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                usable_video_stream = True

                data["video_codec"] = stream.get("codec_name")
                data["width"] = stream.get("width")
                data["height"] = stream.get("height")

                fr = stream.get("r_frame_rate") or stream.get("avg_frame_rate")
                if fr and "/" in fr:
                    n, d = fr.split("/")
                    data["fps"] = float(n) / float(d) if float(d) != 0 else None

                data["nb_frames"] = int(stream.get("nb_frames")) if stream.get("nb_frames") else None

                rot = stream.get("tags", {}).get("rotate")
                data["rotation"] = int(rot) if rot else None

            if stream.get("codec_type") == "audio":
                data["audio_codec"] = stream.get("codec_name")

        if not usable_video_stream:
            data["is_corrupted"] = True
            data["is_usable"] = False
            data["read_error"] = "No video stream"

        elif data["nb_frames"] is None or data["nb_frames"] == 0:
            data["is_corrupted"] = True
            data["is_usable"] = True
            data["read_error"] = "Incomplete video stream"

    except Exception as e:
        data["is_corrupted"] = True
        data["is_usable"] = False
        data["read_error"] = str(e)

    return data


# -------------------------------------------------------------------
# FRAME EXTRACTION
# -------------------------------------------------------------------
def find_ffmpeg(logger=None):
    p = shutil.which("ffmpeg")
    if p:
        return p

    local = _local_bin_path("ffmpeg")
    if os.path.exists(local):
        return local

    if logger:
        logger.warning("ffmpeg not found.")
    return None


def extract_video_frame_hashes(path: str, logger=None) -> Dict[str, Any]:
    data = {
        "phash": None,
        "ahash": None,
        "dhash": None,
        "whash": None,
        "brightness_mean": None,
        "hist_16bins": None,
        "is_corrupted": False,
        "is_usable": True,
        "read_error": None,
    }

    if not (ffmpeg and Image and imagehash):
        return data

    cmd = find_ffmpeg(logger)
    if not cmd:
        data["is_corrupted"] = True
        data["is_usable"] = False
        data["read_error"] = "ffmpeg not found"
        return data

    try:
        out, err = (
            ffmpeg.input(path, ss=0)
            .output("pipe:", vframes=1, format="image2", vcodec="mjpeg")
            .run(cmd=cmd, capture_stdout=True, capture_stderr=True)
        )

        if not out:
            data["is_corrupted"] = True
            data["is_usable"] = False
            data["read_error"] = "Could not extract frame"
            return data

        img = Image.open(io.BytesIO(out))
        img.load()

        try:
            data["phash"] = str(imagehash.phash(img))
            data["ahash"] = str(imagehash.average_hash(img))
            data["dhash"] = str(imagehash.dhash(img))
            data["whash"] = str(imagehash.whash(img))
        except Exception as e:
            data["is_corrupted"] = True
            data["is_usable"] = True
            data["read_error"] = str(e)

        try:
            gray = img.convert("L")
            hist = gray.histogram()
            total = sum(hist) or 1
            data["brightness_mean"] = sum(i * c for i, c in enumerate(hist)) / (255 * total)
            data["hist_16bins"] = [sum(hist[i * 16:(i + 1) * 16]) for i in range(16)]
        except:
            data["is_corrupted"] = True
            data["is_usable"] = True

    except Exception as e:
        data["is_corrupted"] = True
        data["is_usable"] = False
        data["read_error"] = str(e)

    return data


# -------------------------------------------------------------------
# SCAN DIRECTORY
# -------------------------------------------------------------------
def scan_directory(directory: str, callback=None, logger=None):
    results: List[FileEntry] = []

    if logger:
        logger.info(f"Início do scan: {directory}")

    for root, _, files in os.walk(directory):
        for filename in files:
            full_path = os.path.normpath(os.path.join(root, filename))
            basename, ext = os.path.splitext(filename)
            ext_lower = ext.lower()

            fut_sha = executor.submit(compute_sha256, full_path)
            fut_mime = executor.submit(detect_mime_type, full_path)

            fs_meta = get_fs_metadata(full_path)

            mime = fut_mime.result()
            kind = classify_mime(mime)

            fut_img = fut_vid = fut_vid_frame = None

            # Try processing images based on extension OR MIME
            image_exts = {".png", ".jpg", ".jpeg", ".mpo", ".bmp", ".gif", ".tiff", ".tif", ".webp", ".heic", ".heif"}

            if ext_lower in image_exts or kind["is_image"]:
                fut_img = executor.submit(extract_image_metadata, full_path, logger)

            if kind["is_video"]:
                fut_vid = executor.submit(extract_video_metadata, full_path, logger)
                fut_vid_frame = executor.submit(extract_video_frame_hashes, full_path, logger)

            sha256 = fut_sha.result()
            img = fut_img.result() if fut_img else {}
            vid = fut_vid.result() if fut_vid else {}
            vid_frame = fut_vid_frame.result() if fut_vid_frame else {}

            created = (
                img.get("exif_datetime_original")
                or fs_meta["birth_date"]
                or fs_meta["fs_created_date"]
            )

            modified = fs_meta["modified_date"]

            year = month = None
            if created:
                try:
                    dt = datetime.fromisoformat(created)
                    year, month = dt.year, dt.month
                except:
                    pass

            entry: FileEntry = {
                "full_path": full_path,
                "filename": filename,
                "basename": basename,
                "extension": ext_lower,
                "size": os.path.getsize(full_path),

                "mime_type": mime,
                "is_image": bool(fut_img),
                "is_video": kind["is_video"],
                "is_audio": kind["is_audio"],
                "is_aae": ext_lower == ".aae",
                "is_heic": ext_lower in (".heic", ".heif"),

                "sha256": sha256,

                "phash": img.get("phash") or vid_frame.get("phash"),
                "ahash": img.get("ahash") or vid_frame.get("ahash"),
                "dhash": img.get("dhash") or vid_frame.get("dhash"),
                "whash": img.get("whash") or vid_frame.get("whash"),

                "width": img.get("width") or vid.get("width"),
                "height": img.get("height") or vid.get("height"),

                "brightness_mean": img.get("brightness_mean") or vid_frame.get("brightness_mean"),
                "hist_16bins": img.get("hist_16bins") or vid_frame.get("hist_16bins"),

                "duration": vid.get("duration"),
                "video_codec": vid.get("video_codec"),
                "audio_codec": vid.get("audio_codec"),
                "fps": vid.get("fps"),
                "bitrate": vid.get("bitrate"),
                "nb_frames": vid.get("nb_frames"),
                "rotation": vid.get("rotation"),

                "exif_datetime_original": img.get("exif_datetime_original"),
                "exif_camera_model": img.get("exif_camera_model"),
                "exif_lens": img.get("exif_lens"),
                "exif_orientation": img.get("exif_orientation"),
                "exif_iso": _safe_exif_value(img.get("exif_iso")),
                "exif_fnumber": _safe_exif_value(img.get("exif_fnumber")),
                "exif_exposure_time": _safe_exif_value(img.get("exif_exposure_time")),
                "exif_focal_length": _safe_exif_value(img.get("exif_focal_length")),

                "gps_lat": img.get("gps_lat"),
                "gps_lon": img.get("gps_lon"),

                "created_date": created,
                "modified_date": modified,
                "year": year,
                "month": month,

                "fs_created_date": fs_meta["fs_created_date"],
                "birth_date": fs_meta["birth_date"],
                "inode": fs_meta["inode"],

                "is_corrupted": bool(img.get("is_corrupted") or vid.get("is_corrupted")),
                "is_usable": img.get("is_usable", True) if fut_img else vid.get("is_usable", True),
                "read_error": img.get("read_error") or vid.get("read_error"),

                "tags": [],
                "notes": "",
            }

            results.append(entry)

            if callback:
                callback(entry)

            if logger:
                logger.debug(f"Processado: {full_path}")

    if logger:
        logger.info(f"Scan concluído ({len(results)} ficheiros).")

    return results
