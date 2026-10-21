
CREATE OR REPLACE VIEW analytics_snapshots AS

WITH normalized AS (
    SELECT
        *,
        regexp_replace(
            plate_number, '[^0-9]', '', 'g'
        ) AS plate_key,

        CAST(
            try_strptime(
                NULLIF(TRIM(time_to_go_label), 'N/A'),
                '%d.%m.%Y'
            ) AS DATE
        ) AS auction_end_date

    FROM snapshots
),

classified AS (
    SELECT
        *,
        CASE
            WHEN time_to_go_label IS NULL
              OR UPPER(TRIM(time_to_go_label))
                 IN ('', 'N/A', 'NA', 'NULL', 'NONE')
            THEN 'not_available'

            WHEN auction_end_date IS NOT NULL
            THEN 'absolute_date'

            WHEN regexp_full_match(
                LOWER(TRIM(time_to_go_label)),
                'heute +([01]?[0-9]|2[0-3]):[0-5][0-9]'
            )
            THEN 'relative_today'

            WHEN regexp_matches(
                time_to_go_label,
                '[0-9]{1,2}:[0-9]{2}:[0-9]{2}'
            )
            THEN 'countdown'

            ELSE 'unknown'
        END AS deadline_format

    FROM normalized
)

SELECT
    plate_key,
    'TI ' || plate_key AS plate_label,
    plate_number AS source_plate_number,
    LENGTH(plate_key) AS digit_count,

    listing_type,
    starting_price,
    min_increment,
    current_offer,
    offers_number,

    time_to_go_label,
    auction_end_date,
    deadline_format,

    CASE
        WHEN deadline_format = 'countdown'
        THEN time_to_go_seconds
        ELSE NULL
    END AS countdown_seconds,

    source_file,
    scraped_at,
    scraped_date

FROM classified;



CREATE OR REPLACE VIEW analytics_latest_plate AS

SELECT *
FROM analytics_snapshots

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY plate_key, listing_type
    ORDER BY scraped_at DESC, source_file DESC
) = 1;


CREATE OR REPLACE VIEW analytics_daily_market AS

WITH daily_latest AS (
    SELECT *
    FROM analytics_snapshots

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            scraped_date,
            plate_key,
            listing_type
        ORDER BY scraped_at DESC, source_file DESC
    ) = 1
)

SELECT
    scraped_date,
    listing_type,

    COUNT(*) AS observed_listings,

    ROUND(AVG(
        CASE
            WHEN listing_type = 'auction'
            THEN current_offer
        END
    ), 2) AS avg_current_bid_chf,

    ROUND(AVG(
        CASE
            WHEN listing_type = 'fixed_price'
            THEN starting_price
        END
    ), 2) AS avg_asking_price_chf

FROM daily_latest

GROUP BY
    scraped_date,
    listing_type

ORDER BY
    scraped_date,
    listing_type;
