-- ===============================
-- SCHEMA: RAW
-- ===============================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.tourism_data (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT NOW(),
    payload JSONB NOT NULL
);

-- ===============================
-- SCHEMA: STAGING
-- ===============================
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.cleaned_tourism (
    id SERIAL PRIMARY KEY,
    reference_date DATE NOT NULL,
    visitors INT,
    origin VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- ===============================
-- SCHEMA: DW (Data Warehouse)
-- ===============================
CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensão Tempo
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

-- Dimensão Origem
CREATE TABLE IF NOT EXISTS dw.dim_origin (
    origin_id SERIAL PRIMARY KEY,
    origin VARCHAR(50) UNIQUE
);

-- Fato
CREATE TABLE IF NOT EXISTS dw.fact_tourism (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dw.dim_date(date_id),
    origin_id INT REFERENCES dw.dim_origin(origin_id),
    visitors INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ===============================
-- SCHEMA: ANALYTICS (materialized views)
-- ===============================
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.monthly_visitors AS
SELECT
    dd.year,
    dd.month,
    SUM(ft.visitors) AS total_visitors
FROM dw.fact_tourism ft
JOIN dw.dim_date dd ON ft.date_id = dd.date_id
GROUP BY dd.year, dd.month
ORDER BY dd.year DESC, dd.month DESC;

