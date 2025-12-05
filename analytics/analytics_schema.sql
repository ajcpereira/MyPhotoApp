CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_path TEXT UNIQUE,
    filename TEXT,
    basename TEXT,
    extension TEXT,
    mime_type TEXT,
    size INTEGER,
    created_date TEXT,
    modified_date TEXT,
    birth_date TEXT,
    year INTEGER,
    month INTEGER,
    inode INTEGER,
    is_image INTEGER,
    is_video INTEGER,
    is_audio INTEGER,
    is_corrupted INTEGER,
    read_error TEXT,
    is_usable INTEGER
);

CREATE TABLE IF NOT EXISTS image_meta (
    file_id INTEGER,
    width INTEGER,
    height INTEGER,
    brightness_mean REAL,
    hist_16bins TEXT,
    exif_datetime_original TEXT,
    exif_camera_model TEXT,
    exif_lens TEXT,
    exif_orientation INTEGER,
    exif_iso INTEGER,
    exif_fnumber REAL,
    exif_exposure_time TEXT,
    exif_focal_length REAL,
    gps_lat REAL,
    gps_lon REAL,
    FOREIGN KEY(file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS video_meta (
    file_id INTEGER,
    width INTEGER,
    height INTEGER,
    duration REAL,
    fps REAL,
    bitrate INTEGER,
    nb_frames INTEGER,
    rotation INTEGER,
    video_codec TEXT,
    audio_codec TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS hash_meta (
    file_id INTEGER,
    sha256 TEXT,
    phash TEXT,
    ahash TEXT,
    dhash TEXT,
    whash TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id)
);

CREATE INDEX IF NOT EXISTS idx_sha256 ON hash_meta (sha256);
CREATE INDEX IF NOT EXISTS idx_basename ON files (basename);
CREATE INDEX IF NOT EXISTS idx_year_month ON files (year, month);
