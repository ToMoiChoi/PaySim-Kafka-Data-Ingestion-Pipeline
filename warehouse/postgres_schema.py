"""
warehouse/postgres_schema.py - Khởi tạo Native Crypto Pipeline Schema & Star Schema
==============================================================
Chạy script này để khởi tạo bảng Data Warehouse mới:
  - 1 Fact table: fact_binance_trades
  - 5 Dim tables: dim_date, dim_time, dim_volume_category, dim_crypto_pair, dim_exchange_rate

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


# Lệnh dọn dẹp các Schema cũ (do đập đi xây lại)
DROP_LEGACY_STATEMENTS = [
    "DROP TABLE IF EXISTS fact_binance_trades CASCADE;",
    "DROP TABLE IF EXISTS fact_transactions CASCADE;",
    "DROP TABLE IF EXISTS dim_users CASCADE;",
    "DROP TABLE IF EXISTS dim_account CASCADE;",
    "DROP TABLE IF EXISTS dim_merchants CASCADE;",
    "DROP TABLE IF EXISTS dim_transaction_type CASCADE;",
    "DROP TABLE IF EXISTS dim_location CASCADE;",
    "DROP TABLE IF EXISTS dim_channel CASCADE;",
    "DROP TABLE IF EXISTS dim_time CASCADE;",
    "DROP TABLE IF EXISTS dim_date CASCADE;",
    "DROP TABLE IF EXISTS dim_volume_category CASCADE;",
    "DROP TABLE IF EXISTS dim_crypto_pair CASCADE;",
    "DROP TABLE IF EXISTS dim_exchange_rate CASCADE;"
]

DDL_STATEMENTS = [
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

    # ── dim_volume_category ──────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_volume_category (
        volume_category VARCHAR(50) PRIMARY KEY,
        description     VARCHAR(255),
        min_usd         NUMERIC(20, 2),
        max_usd         NUMERIC(20, 2)
    );
    """,

    # ── dim_crypto_pair ──────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_crypto_pair (
        crypto_symbol VARCHAR(20) PRIMARY KEY,
        base_asset    VARCHAR(20),
        quote_asset   VARCHAR(20),
        pair_name     VARCHAR(100)
    );
    """,

    # ── dim_exchange_rate ────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_exchange_rate (
        date_key        BIGINT PRIMARY KEY,
        currency_code   VARCHAR(10),
        vnd_rate        NUMERIC(15, 2),
        CONSTRAINT fk_fx_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    );
    """,

    # ── fact_binance_trades ────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_binance_trades (
        transaction_id   VARCHAR(50) PRIMARY KEY,
        trade_id         BIGINT,
        crypto_symbol    VARCHAR(20),
        date_key         BIGINT,
        time_key         BIGINT,
        trade_time       TIMESTAMP,
        price            NUMERIC(38, 9),
        quantity         NUMERIC(38, 9),
        amount_usd       NUMERIC(38, 9),
        is_buyer_maker   BOOLEAN,
        volume_category  VARCHAR(50),
        is_anomaly       BOOLEAN,
        buyer_order_id   BIGINT,
        seller_order_id  BIGINT,
        CONSTRAINT fk_trade_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        CONSTRAINT fk_trade_time FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
        CONSTRAINT fk_trade_category FOREIGN KEY (volume_category) REFERENCES dim_volume_category(volume_category),
        CONSTRAINT fk_trade_symbol FOREIGN KEY (crypto_symbol) REFERENCES dim_crypto_pair(crypto_symbol)
    );
    """,

    # ── Indexes ──────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_binance_symbol ON fact_binance_trades(crypto_symbol);",
    "CREATE INDEX IF NOT EXISTS idx_binance_time ON fact_binance_trades(trade_time);",
    "CREATE INDEX IF NOT EXISTS idx_binance_amount ON fact_binance_trades(amount_usd);",
]

def main():
    print("=" * 60)
    print("  PostgreSQL Schema - Native Crypto Pipeline with Star Schema")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}")
    print(f"  DB   : {PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)

    with engine.begin() as conn:
        print("Xoá các bảng cũ (Legacy Mode)...")
        for stmt in DROP_LEGACY_STATEMENTS:
            conn.execute(text(stmt))
            
        print("Đang tạo bảng DB mới (1 Fact, 5 Dims)...")
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))

    print("[DONE] HOÀN TẤT! Data Warehouse đã sẵn sàng thiết lập Power BI.")
    print("=" * 60)


if __name__ == "__main__":
    main()
