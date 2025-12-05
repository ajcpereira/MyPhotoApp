import sqlite3
from pathlib import Path


DB_PATH = Path("analytics/media_index.db")   # Ajusta se estiver noutro local


def hamming_distance(hash1: str, hash2: str) -> int:
    """
    Compute Hamming Distance between two hex or binary hashes.
    Supports both 16-char hex (imagehash) and binary strings.
    """
    if hash1 is None or hash2 is None:
        return None

    # Convert hex strings to binary if needed
    try:
        b1 = bin(int(hash1, 16))[2:].zfill(64)
        b2 = bin(int(hash2, 16))[2:].zfill(64)
    except ValueError:
        # Already binary?
        b1 = hash1
        b2 = hash2

    return sum(c1 != c2 for c1, c2 in zip(b1, b2))


def analyze_heic(conn):
    cur = conn.cursor()
    total_heic = cur.execute(
        "SELECT COUNT(*) FROM files WHERE LOWER(extension) = '.heic'"
    ).fetchone()[0]

    print("\n=== HEIC CHECK ===")
    print(f"Total HEIC files: {total_heic}")
    print("===================\n")


def analyze_livepair(conn, base_name="IMG_9588"):
    cur = conn.cursor()

    # Retrieve JPG + MOV pair
    rows = cur.execute(
        """
        SELECT 
            f.full_path,
            f.filename,
            h.sha256,
            h.phash,
            h.ahash,
            h.dhash,
            h.whash
        FROM files f
        LEFT JOIN hash_meta h ON h.file_id = f.id
        WHERE f.filename IN (?, ?)
        """,
        (f"{base_name}.JPG", f"{base_name}.MOV")
    ).fetchall()

    if len(rows) == 0:
        print(f"No JPG/MOV pair found for base '{base_name}'")
        return

    print(f"\n=== HASHES FOR {base_name}.JPG & {base_name}.MOV ===")

    # Expecting two rows
    for row in rows:
        full_path, filename, sha256, phash, ahash, dhash, whash = row
        print("\n-----------------------------")
        print(f"File: {filename}")
        print(f"Path: {full_path}")
        print(f"SHA256: {sha256}")
        print(f"pHash:  {phash}")
        print(f"aHash:  {ahash}")
        print(f"dHash:  {dhash}")
        print(f"wHash:  {whash}")
        print("-----------------------------")

    # Unpack hashes into dict for comparison
    hashes = {row[1]: row[2:] for row in rows}  # filename -> tuple of hashes

    jpg_hashes = hashes.get(f"{base_name}.JPG")
    mov_hashes = hashes.get(f"{base_name}.MOV")

    if jpg_hashes is None or mov_hashes is None:
        print("\nMissing one of the files; cannot compare.")
        return

    _, jpg_phash, jpg_ahash, jpg_dhash, jpg_whash = jpg_hashes
    _, mov_phash, mov_ahash, mov_dhash, mov_whash = mov_hashes

    print("\n=== HAMMING DISTANCES (similarity check) ===")

    dp = hamming_distance(jpg_phash, mov_phash)
    da = hamming_distance(jpg_ahash, mov_ahash)
    dd = hamming_distance(jpg_dhash, mov_dhash)
    dw = hamming_distance(jpg_whash, mov_whash)

    print(f"pHash distance : {dp}")
    print(f"aHash distance : {da}")
    print(f"dHash distance : {dd}")
    print(f"wHash distance : {dw}")

    # Interpretation
    print("\n=== INTERPRETATION ===")
    print("Lower distances = higher similarity.")

    if dp is not None:
        if dp <= 10:
            print("pHash suggests: VERY similar (likely Live Photo pair).")
        elif dp <= 20:
            print("pHash suggests: Moderately similar.")
        else:
            print("pHash suggests: Not visually similar.")

    if da is not None:
        if da <= 10:
            print("aHash: similar.")
        else:
            print("aHash: different.")

    if dd is not None:
        if dd <= 10:
            print("dHash: similar.")
        else:
            print("dHash: different.")

    if dw is not None:
        if dw <= 10:
            print("wHash: similar.")
        else:
            print("wHash: different.")

    print("\n=============================================\n")


def main():
    if not DB_PATH.exists():
        print(f"Database not found at: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    # 1) Check HEIC
    analyze_heic(conn)

    # 2) Analyze JPG/MOV similarity (Live Photo test)
    analyze_livepair(conn, base_name="IMG_9588")

    conn.close()


if __name__ == "__main__":
    main()
