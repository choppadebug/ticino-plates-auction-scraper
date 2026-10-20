
import duckdb

con = duckdb.connect("data/plates.db", read_only=True)

# Check identifier normalization
print(con.execute("""
    SELECT
        COUNT(DISTINCT source_plate_number) AS original_ids,
        COUNT(DISTINCT plate_key) AS normalized_ids
    FROM analytics_snapshots
""").df())

# Check deadline parsing
print(con.execute("""
    SELECT
        plate_label,
        time_to_go_label,
        auction_end_date,
        countdown_seconds
    FROM analytics_snapshots
    WHERE plate_key = '426'
    ORDER BY scraped_at DESC
    LIMIT 3
""").df())

# Check latest-observation view
print(con.execute("""
    SELECT
        plate_label,
        listing_type,
        current_offer,
        scraped_at
    FROM analytics_latest_plate
    WHERE plate_key = '566'
""").df())

con.close()
