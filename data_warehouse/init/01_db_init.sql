CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS raw.tourism_data (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    extracted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_source
ON raw.tourism_data(source);



CREATE TABLE IF NOT EXISTS staging.cleaned_tourism (
    id SERIAL PRIMARY KEY,
    reference_date DATE NOT NULL,
    visitors INTEGER NOT NULL,
    origin TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staging_date
ON staging.cleaned_tourism(reference_date);





CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
);

-- Dimensão Origem
CREATE TABLE IF NOT EXISTS dw.dim_origin (
    origin_id SERIAL PRIMARY KEY,
    origin TEXT UNIQUE
);

-- Fato
CREATE TABLE IF NOT EXISTS dw.fact_tourism (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dw.dim_date(date_id),
    origin_id INT REFERENCES dw.dim_origin(origin_id),
    visitors INT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_date
ON dw.fact_tourism(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_origin
ON dw.fact_tourism(origin_id);


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
