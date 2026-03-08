"""
warehouse/postgres_schema.py – Tạo Star Schema trên PostgreSQL
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

    # ── dim_users ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id           VARCHAR(50) PRIMARY KEY,
        account_balance   NUMERIC(20, 2),
        user_segment      VARCHAR(20),
        registration_date DATE
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
        user_id          VARCHAR(50),
        merchant_id      VARCHAR(50),
        type_id          VARCHAR(20),
        location_id      VARCHAR(20),
        date_key         BIGINT,
        transaction_time TIMESTAMP,
        amount           NUMERIC(20, 2),
        reward_points    BIGINT
    );
    """,

    # ── Indexes ──────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_fact_user    ON fact_transactions(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_merchant ON fact_transactions(merchant_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_time    ON fact_transactions(transaction_time);",
]


def main():
    print("=" * 60)
    print("  PostgreSQL Star Schema – Khởi tạo Data Warehouse")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}")
    print(f"  DB   : {PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)

    with engine.connect() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))
        conn.commit()

    print("✅ HOÀN TẤT! Star Schema đã tạo trong PostgreSQL:")
    print("   fact_transactions, dim_users, dim_merchants,")
    print("   dim_transaction_type, dim_location, dim_date")
    print("=" * 60)


if __name__ == "__main__":
    main()
