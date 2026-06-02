CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_activity_history (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    character_id VARCHAR(50) NOT NULL,
    page_number INT NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
