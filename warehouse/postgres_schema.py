"""
warehouse/postgres_schema.py - Tạo Star Schema trên PostgreSQL
==============================================================
Chạy script này để khởi tạo toàn bộ bảng Star Schema trong PostgreSQL:
  - 1 Fact table : fact_transactions
  - 5 Dim tables : dim_users, dim_merchants, dim_transaction_type, dim_location, dim_date

Yêu cầu:
  - PostgreSQL đang chạy (docker-compose up -d postgres)
  - Biến POSTGRES_* trong .env
"""

import os
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy import text

load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


DDL_STATEMENTS = [
    # ── dim_transaction_type ──────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_transaction_type (
        type_id             VARCHAR(20) PRIMARY KEY,
        type_name           VARCHAR(50),
        is_reward_eligible  BOOLEAN,
        reward_multiplier   FLOAT
    );
    """,

    # ── dim_location ─────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id VARCHAR(20) PRIMARY KEY,
        city        VARCHAR(100),
        region      VARCHAR(50)
    );
    """,

    # ── dim_date ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key    BIGINT PRIMARY KEY,
        full_date   DATE,
        day_of_week INTEGER,
        is_weekend  BOOLEAN,
        month       INTEGER,
        quarter     INTEGER,
        year        INTEGER
    );
    """,

    # ── dim_time ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_key         BIGINT PRIMARY KEY,
        hour             INTEGER,
        minute           INTEGER,
        time_of_day      VARCHAR(20),
        is_business_hour BOOLEAN
    );
    """,

    # ── dim_channel ──────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_channel (
        channel_id   VARCHAR(20) PRIMARY KEY,
        channel_name VARCHAR(50),
        device_os    VARCHAR(50)
    );
    """,

    # ── dim_users ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id           VARCHAR(50) PRIMARY KEY,
        account_balance   NUMERIC(20, 2),
        user_segment      VARCHAR(20),
        registration_date DATE
    );
    """,

    # ── dim_account ──────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_account (
        account_id     VARCHAR(50) PRIMARY KEY,
        user_id        VARCHAR(50),
        account_type   VARCHAR(50),
        account_status VARCHAR(20),
        created_date   DATE,
        CONSTRAINT fk_account_user FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
    );
    """,

    # ── dim_merchants ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_merchants (
        merchant_id       VARCHAR(50) PRIMARY KEY,
        merchant_name     VARCHAR(100),
        merchant_category VARCHAR(50)
    );
    """,

    # ── fact_transactions ────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_id   VARCHAR(50) PRIMARY KEY,
        account_id       VARCHAR(50),
        merchant_id      VARCHAR(50),
        type_id          VARCHAR(20),
        location_id      VARCHAR(20),
        channel_id       VARCHAR(20),
        date_key         BIGINT,
        time_key         BIGINT,
        transaction_time TIMESTAMP,
        amount           NUMERIC(20, 2),
        reward_points    BIGINT,
        type             VARCHAR(20),
        step             INTEGER,
        "oldbalanceOrg"  NUMERIC(20, 2),
        "newbalanceOrig" NUMERIC(20, 2),
        "oldbalanceDest" NUMERIC(20, 2),
        "newbalanceDest" NUMERIC(20, 2),
        "isFraud"        INTEGER,
        "isFlaggedFraud" INTEGER,
        ip_address       VARCHAR(50),
        CONSTRAINT fk_account FOREIGN KEY (account_id) REFERENCES dim_account(account_id),
        CONSTRAINT fk_merchant FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id),
        CONSTRAINT fk_type FOREIGN KEY (type_id) REFERENCES dim_transaction_type(type_id),
        CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        CONSTRAINT fk_channel FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
        CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        CONSTRAINT fk_time FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
    );
    """,

    # ── Indexes ──────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_fact_account ON fact_transactions(account_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_merchant ON fact_transactions(merchant_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_transactions(transaction_time);",
    "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(date_key);",
]


def main():
    print("=" * 60)
    print("  PostgreSQL Star Schema - Khởi tạo Data Warehouse")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}")
    print(f"  DB   : {PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)

    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))

    print("[DONE] HOÀN TẤT! Star Schema đã tạo trong PostgreSQL:")
    print("   fact_transactions, dim_users, dim_account, dim_merchants,")
    print("   dim_transaction_type, dim_location, dim_date, dim_time, dim_channel")
    print("=" * 60)


if __name__ == "__main__":
    main()
