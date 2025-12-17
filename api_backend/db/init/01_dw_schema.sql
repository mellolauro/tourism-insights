CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id INT PRIMARY KEY,
    date_value DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_origin (
    origin_id SERIAL PRIMARY KEY,
    origin VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.fact_tourism (
    fact_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL,
    origin_id INT NOT NULL,
    visitors INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id) REFERENCES dw.dim_date(date_id),
    FOREIGN KEY (origin_id) REFERENCES dw.dim_origin(origin_id)
);
