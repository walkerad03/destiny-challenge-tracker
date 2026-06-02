CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.raw_activity_history (
    account_id VARCHAR(50) NOT NULL,
    character_id VARCHAR(50) NOT NULL,
    activity_mode INT NOT NULL,
    page_number INT NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, character_id, activity_mode, page_number)
);



CREATE TABLE IF NOT EXISTS config.tracked_users (
    discord_id VARCHAR(50) PRIMARY KEY,
    bungie_membership_id VARCHAR(50) NOT NULL,
    bungie_membership_type INT NOT NULL,

    access_token TEXT,
    refresh_token TEXT,
    token_expires_at TIMESTAMP,

    is_active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
