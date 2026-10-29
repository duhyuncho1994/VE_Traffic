
import csv
import hashlib
import os
import shutil
from pathlib import Path

import psycopg


# 경로 설정
BASE_DIR = Path(
    os.getenv("TRAFFIC_DATA_DIR", str(Path(__file__).resolve().parents[1] / "data"))
)

INCOMING_DIR = BASE_DIR / "incoming"
ARCHIVE_DIR = BASE_DIR / "archive"


# PostgreSQL 연결
DB_CONFIG = {
    "host": os.getenv("TRAFFIC_DB_HOST", "localhost"),
    "port": int(os.getenv("TRAFFIC_DB_PORT", "5433")),
    "dbname": os.getenv("TRAFFIC_DB_NAME", "traffic_db"),
    "user": os.getenv("TRAFFIC_DB_USER", "traffic_user"),
    "password": os.environ["TRAFFIC_DB_PASSWORD"],
}


INSERT_SQL = """
INSERT INTO traffic_raw (
    source_file,
    source_type,
    record_id,
    strom,
    kfz_klasse,
    mstimestamp,
    stimestamp,
    datum,
    geschwindigkeit_1,
    geschwindigkeit_2,
    q_wert
)
VALUES (
    %(source_file)s,
    %(source_type)s,
    %(record_id)s,
    %(strom)s,
    %(kfz_klasse)s,
    %(mstimestamp)s,
    %(stimestamp)s,
    %(datum)s,
    %(geschwindigkeit_1)s,
    %(geschwindigkeit_2)s,
    %(q_wert)s
)
"""


def calculate_hash(path):
    """파일 내용의 SHA-256 해시 계산"""
    hasher = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def optional_number(value):
    return value if value not in (None, "") else None


def get_source_type(filename):
    if "Kontrolle" in filename:
        return "KONTROLLE"

    if "Tracking" in filename:
        return "TRACKING"

    raise ValueError(f"Unknown source type: {filename}")


def load_file(conn, path):
    """CSV 데이터를 PostgreSQL에 적재"""

    source_type = get_source_type(path.name)

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    with conn.cursor() as cur:

        # 같은 파일명의 기존 데이터 교체
        cur.execute(
            "DELETE FROM traffic_raw WHERE source_file = %s",
            (path.name,),
        )

        for row in rows:
            data = {
                "source_file": path.name,
                "source_type": source_type,
                "record_id": int(row["ID"]),
                "strom": int(row["STROM"]),
                "kfz_klasse": row["KFZ_KLASSE"].upper(),
                "mstimestamp": optional_number(
                    row["MSTIMESTAMP"]
                ),
                "stimestamp": row["STIMESTAMP"],
                "datum": row["DATUM"],
                "geschwindigkeit_1": optional_number(
                    row.get("GESCHWINDIGKEIT_KM/H_1")
                ),
                "geschwindigkeit_2": optional_number(
                    row.get("GESCHWINDIGKEIT_KM/H_2")
                ),
                "q_wert": optional_number(
                    row.get("Q_WERT")
                ),
            }

            cur.execute(INSERT_SQL, data)

    return len(rows)


def process_file(conn, path):

    file_hash = calculate_hash(path)

    # 기존 성공 이력 확인
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM file_processing_log
            WHERE file_name = %s
              AND file_hash = %s
              AND status = 'SUCCESS'
            """,
            (path.name, file_hash),
        )

        already_processed = cur.fetchone() is not None

    if already_processed:
        print(f"SKIPPED: {path.name} (already processed)")
        return

    # 처리 중 상태 기록
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO file_processing_log (
                    file_name, file_hash, status
                )
                VALUES (%s, %s, 'PROCESSING')
                ON CONFLICT (file_name, file_hash)
                DO UPDATE SET
                    status = 'PROCESSING',
                    error_message = NULL,
                    processed_at = NOW()
                """,
                (path.name, file_hash),
            )

    try:
        # 데이터 적재 및 성공 이력을 하나의 트랜잭션으로 처리
        with conn.transaction():
            row_count = load_file(conn, path)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE file_processing_log
                    SET status = 'SUCCESS',
                        row_count = %s,
                        error_message = NULL,
                        processed_at = NOW()
                    WHERE file_name = %s
                      AND file_hash = %s
                    """,
                    (row_count, path.name, file_hash),
                )

        print(f"SUCCESS: {path.name} ({row_count} rows)")

        # 성공 후 파일 보관
        archive_path = ARCHIVE_DIR / path.name

        if archive_path.exists():
            archive_path = (
                ARCHIVE_DIR /
                f"{path.stem}_{file_hash[:12]}{path.suffix}"
            )

        shutil.move(str(path), str(archive_path))

    except Exception as error:
        # 적재 실패 시 데이터 변경은 롤백됨
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE file_processing_log
                    SET status = 'FAILED',
                        error_message = %s,
                        processed_at = NOW()
                    WHERE file_name = %s
                      AND file_hash = %s
                    """,
                    (str(error), path.name, file_hash),
                )

        print(f"FAILED: {path.name} - {error}")
        raise


def main():

    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(INCOMING_DIR.glob("*.csv"))

    if not files:
        print("No new CSV files found")
        return

    # autocommit=True로 트랜잭션 경계를 명확하게 관리
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        for path in files:
            process_file(conn, path)

    print("Incremental ETL completed")


if __name__ == "__main__":
    main()
