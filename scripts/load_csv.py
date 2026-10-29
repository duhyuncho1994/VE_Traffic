
import csv
import os
from pathlib import Path

import psycopg

RAW_DIR = Path(
    os.getenv("TRAFFIC_RAW_DIR", str(Path(__file__).resolve().parents[1] / "data" / "raw"))
)

DB_CONFIG = {
    "host": os.getenv("TRAFFIC_DB_HOST", "localhost"),
    "port": int(os.getenv("TRAFFIC_DB_PORT", "5433")),
    "dbname": os.getenv("TRAFFIC_DB_NAME", "traffic_db"),
    "user": os.getenv("TRAFFIC_DB_USER", "traffic_user"),
    "password": os.environ["TRAFFIC_DB_PASSWORD"],
}

INSERT_SQL = """
INSERT INTO traffic_raw (
    source_file, source_type, record_id, strom,
    kfz_klasse, mstimestamp, stimestamp, datum,
    geschwindigkeit_1, geschwindigkeit_2, q_wert
)
VALUES (
    %(source_file)s, %(source_type)s, %(record_id)s,
    %(strom)s, %(kfz_klasse)s, %(mstimestamp)s,
    %(stimestamp)s, %(datum)s, %(geschwindigkeit_1)s,
    %(geschwindigkeit_2)s, %(q_wert)s
)
"""


def optional_number(value):
    return value if value not in (None, "") else None


def load_file(conn, path):
    source_type = (
        "KONTROLLE" if "Kontrolle" in path.name else "TRACKING"
    )

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    with conn.transaction():
        with conn.cursor() as cur:
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
                    "mstimestamp": optional_number(row["MSTIMESTAMP"]),
                    "stimestamp": row["STIMESTAMP"],
                    "datum": row["DATUM"],
                    "geschwindigkeit_1": optional_number(
                        row.get("GESCHWINDIGKEIT_KM/H_1")
                    ),
                    "geschwindigkeit_2": optional_number(
                        row.get("GESCHWINDIGKEIT_KM/H_2")
                    ),
                    "q_wert": optional_number(row.get("Q_WERT")),
                }

                cur.execute(INSERT_SQL, data)

    print(f"Loaded: {path.name} ({len(rows)} rows)")


def main():
    files = sorted(RAW_DIR.glob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No CSV files found in {RAW_DIR}"
        )

    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        for path in files:
            load_file(conn, path)

    print(f"Completed: {len(files)} files")


if __name__ == "__main__":
    main()
