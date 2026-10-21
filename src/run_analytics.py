
from pathlib import Path
import json
import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "plates.db"
SQL_PATH = ROOT / "sql" / "analytics_views.sql"
EXPORT_DIR = ROOT / "data" / "bi_exports"

EXPORT_DIR.mkdir(exist_ok=True)

con = duckdb.connect(str(DB_PATH))

# 1. Create/update analytics views
con.execute(SQL_PATH.read_text(encoding="utf-8"))

# 2. Define data-quality checks
checks = {
    "missing_plate_key": """
        SELECT COUNT(*)
        FROM analytics_snapshots
        WHERE plate_key IS NULL OR plate_key = ''
    """,

    "duplicate_observations": """
        SELECT COUNT(*)
        FROM (
            SELECT source_file, plate_key, listing_type
            FROM analytics_snapshots
            GROUP BY source_file, plate_key, listing_type
            HAVING COUNT(*) > 1
        )
    """,

    "negative_prices": """
        SELECT COUNT(*)
        FROM analytics_snapshots
        WHERE starting_price < 0
           OR current_offer < 0
    """,

    
    "invalid_deadlines": """
        SELECT COUNT(*)
        FROM analytics_snapshots
        WHERE listing_type = 'auction'
        AND deadline_format = 'unknown'
    """,

}

results = {}

for name, query in checks.items():
    count = con.execute(query).fetchone()[0]
    results[name] = count

    status = "PASS" if count == 0 else "FAIL"
    print(f"[{status}] {name}: {count}")


print("\nDEADLINE FORMAT DISTRIBUTION")

rows = con.execute("""
    SELECT
        deadline_format,
        COUNT(*) AS observations
    FROM analytics_snapshots
    WHERE listing_type = 'auction'
    GROUP BY deadline_format
    ORDER BY observations DESC
""").fetchall()

for deadline_format, count in rows:
    print(f"{deadline_format}: {count}")


# 3. Stop pipeline if critical checks fail
if any(count > 0 for count in results.values()):
    con.close()
    raise SystemExit("Data quality checks failed!")

# 4. Export analytics views
views = {
    "analytics_snapshots": "fact_plate_snapshots.csv",
    "analytics_latest_plate": "latest_plates.csv",
    "analytics_daily_market": "daily_market.csv",
}

for view, filename in views.items():
    output = EXPORT_DIR / filename

    con.execute(
        f"""
        COPY (SELECT * FROM {view})
        TO ? (HEADER, DELIMITER ',')
        """,
        [str(output)],
    )

    print(f"Exported: {output}")

# 5. Save reproducible quality report
with (EXPORT_DIR / "quality_report.json").open(
    "w", encoding="utf-8"
) as file:
    json.dump(results, file, indent=2)

con.close()

print("Analytics pipeline completed successfully.")
