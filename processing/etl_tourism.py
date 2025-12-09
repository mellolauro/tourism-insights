import psycopg2
import pandas as pd

conn = psycopg2.connect(
    dbname="tourism_dw",
    user="tourism",
    password="tourism123",
    host="postgres",
    port=5432
)
cur = conn.cursor()

# ------------------------------------------
# RAW → STAGING
# ------------------------------------------
cur.execute("""
INSERT INTO staging.visitation_events (
    event_date, country, region, city, visitors, source, raw_id
)
SELECT
    (payload->>'date')::DATE,
    UPPER(TRIM(payload->>'country')),
    UPPER(TRIM(payload->>'region')),
    INITCAP(payload->>'city'),
    (payload->>'visitors')::INT,
    source,
    id
FROM raw.embra_tur_api
WHERE id NOT IN (SELECT raw_id FROM staging.visitation_events);
""")

# ------------------------------------------
# STAGING → DIMENSIONS
# ------------------------------------------
cur.execute("""
INSERT INTO dw.dim_date (date_id, year, quarter, month, month_name, week, day)
SELECT DISTINCT
    event_date,
    EXTRACT(YEAR FROM event_date),
    EXTRACT(QUARTER FROM event_date),
    EXTRACT(MONTH FROM event_date),
    TO_CHAR(event_date, 'Month'),
    EXTRACT(WEEK FROM event_date),
    EXTRACT(DAY FROM event_date)
FROM staging.visitation_events
ON CONFLICT (date_id) DO NOTHING;
""")

cur.execute("""
INSERT INTO dw.dim_location (country, region, city)
SELECT DISTINCT country, region, city
FROM staging.visitation_events
ON CONFLICT DO NOTHING;
""")

cur.execute("""
INSERT INTO dw.dim_source (source_name)
SELECT DISTINCT source
FROM staging.visitation_events
ON CONFLICT DO NOTHING;
""")

# ------------------------------------------
# STAGING → FACT TABLE
# ------------------------------------------
cur.execute("""
INSERT INTO dw.fact_visitation (
    date_id, location_id, source_id, visitors
)
SELECT
    se.event_date,
    dl.location_id,
    ds.source_id,
    se.visitors
FROM staging.visitation_events se
JOIN dw.dim_location dl
    ON dl.country = se.country
    AND dl.region = se.region
    AND dl.city = se.city
JOIN dw.dim_source ds
    ON ds.source_name = se.source;
""")

conn.commit()
cur.close()
conn.close()
