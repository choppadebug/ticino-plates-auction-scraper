#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "plates.db"
MERGED_CSV_PATH = DATA_DIR / "plates_merged.csv"


def main() -> int:
    csv_files = sorted(OUTPUTS_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {OUTPUTS_DIR}. Nothing to build.")
        return 1

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=4;")

    # Use union_by_name so older/newer CSV schema differences are handled safely.
    con.execute(
        """
        CREATE OR REPLACE TABLE snapshots AS
        WITH imported AS (
            SELECT
                plate_number,
                starting_price,
                min_increment,
                current_offer,
                time_to_go,
                offers_number,
                filename AS source_path
            FROM read_csv_auto(
                ?,
                all_varchar = true,
                union_by_name = true,
                filename = true
            )
        ),
        cleaned AS (
            SELECT
                NULLIF(TRIM(regexp_replace(COALESCE(plate_number, ''), '[^0-9A-Za-z ]', '', 'g')), '') AS plate_number,
                NULLIF(TRIM(starting_price), '') AS starting_price_raw,
                NULLIF(TRIM(min_increment), '') AS min_increment_raw,
                NULLIF(TRIM(current_offer), '') AS current_offer_raw,
                NULLIF(TRIM(time_to_go), '') AS time_to_go_raw,
                NULLIF(TRIM(offers_number), '') AS offers_number_raw,
                regexp_extract(source_path, '([^/\\\\]+)$', 1) AS source_file,
                strptime(regexp_extract(source_path, '(\\d{8}_\\d{6})', 1), '%Y%m%d_%H%M%S') AS scraped_at
            FROM imported
        )
        SELECT
            plate_number,
            CASE
                WHEN upper(COALESCE(starting_price_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL
                ELSE TRY_CAST(regexp_replace(starting_price_raw, '[^0-9]', '', 'g') AS INTEGER)
            END AS starting_price,
            CASE
                WHEN upper(COALESCE(min_increment_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL
                ELSE TRY_CAST(regexp_replace(min_increment_raw, '[^0-9]', '', 'g') AS INTEGER)
            END AS min_increment,
            CASE
                WHEN upper(COALESCE(current_offer_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL
                ELSE TRY_CAST(regexp_replace(current_offer_raw, '[^0-9]', '', 'g') AS INTEGER)
            END AS current_offer,
            CASE
                WHEN upper(COALESCE(offers_number_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL
                ELSE TRY_CAST(regexp_replace(offers_number_raw, '[^0-9]', '', 'g') AS INTEGER)
            END AS offers_number,
            time_to_go_raw AS time_to_go_label,
            CASE
                WHEN upper(COALESCE(time_to_go_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL
                ELSE
                    COALESCE(TRY_CAST(regexp_extract(time_to_go_raw, '(\\d+)\\s+giorni', 1) AS INTEGER), 0) * 86400
                    + COALESCE(TRY_CAST(regexp_extract(time_to_go_raw, '(\\d{1,2}):(\\d{2}):(\\d{2})', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(regexp_extract(time_to_go_raw, '(\\d{1,2}):(\\d{2}):(\\d{2})', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(regexp_extract(time_to_go_raw, '(\\d{1,2}):(\\d{2}):(\\d{2})', 3) AS INTEGER), 0)
            END AS time_to_go_seconds,
            CASE
                WHEN upper(COALESCE(current_offer_raw, '')) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN 'fixed_price'
                ELSE 'auction'
            END AS listing_type,
            source_file,
            scraped_at,
            CAST(scraped_at AS DATE) AS scraped_date
        FROM cleaned
        WHERE plate_number IS NOT NULL
          AND scraped_at IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY source_file, plate_number
            ORDER BY source_file
        ) = 1;
        """,
        [str(OUTPUTS_DIR / "*.csv")],
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW latest_snapshot AS
        WITH latest AS (
            SELECT MAX(scraped_at) AS max_scraped_at
            FROM snapshots
        )
        SELECT s.*
        FROM snapshots s
        JOIN latest l ON s.scraped_at = l.max_scraped_at
        ORDER BY s.current_offer DESC NULLS LAST, s.starting_price DESC NULLS LAST, s.plate_number;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW daily_summary AS
        SELECT
            scraped_date,
            COUNT(*) AS rows_count,
            COUNT(DISTINCT plate_number) AS unique_plates,
            SUM(CASE WHEN listing_type = 'auction' THEN 1 ELSE 0 END) AS auction_rows,
            SUM(CASE WHEN listing_type = 'fixed_price' THEN 1 ELSE 0 END) AS fixed_price_rows,
            AVG(current_offer) AS avg_current_offer,
            MAX(current_offer) AS max_current_offer
        FROM snapshots
        GROUP BY scraped_date
        ORDER BY scraped_date;
        """
    )

    con.execute(
        """
        COPY (
            SELECT *
            FROM snapshots
            ORDER BY scraped_at, plate_number
        )
        TO ?
        (HEADER, DELIMITER ',');
        """,
        [str(MERGED_CSV_PATH)],
    )

    summary = con.execute(
        """
        SELECT
            COUNT(*) AS rows_count,
            COUNT(DISTINCT plate_number) AS plates_count,
            COUNT(DISTINCT scraped_at) AS snapshots_count
        FROM snapshots;
        """
    ).fetchone()
    con.close()

    rows_count, plates_count, snapshots_count = summary
    print(f"Built {DB_PATH}")
    print(f"Exported {MERGED_CSV_PATH}")
    print(
        "Rows: {rows}, Unique plates: {plates}, Snapshots: {snapshots}".format(
            rows=rows_count,
            plates=plates_count,
            snapshots=snapshots_count,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
