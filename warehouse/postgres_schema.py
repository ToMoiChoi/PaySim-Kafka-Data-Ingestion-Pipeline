"""
warehouse/postgres_schema.py - Create Native Crypto Pipeline Schema & Star Schema
==============================================================
Run this script to set up the Data Warehouse tables:
  - 1 Fact table: fact_binance_trades
  - 5 Dim tables: dim_date, dim_time, dim_volume_category, dim_crypto_pair, dim_exchange_rate

Kimball Methodology:
  - All Dimension tables use INTEGER Surrogate Keys (SK) as Primary Keys.
  - Fact table references Dimensions via INTEGER Foreign Keys only.
  - Natural keys (e.g., "BTCUSDT", "RETAIL") are stored as attributes
    in Dimension tables, NOT as PKs or FKs.

Requirements:
  - PostgreSQL must be running (docker-compose up -d postgres)
  - POSTGRES_* variables set in .env
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


# --- Drop old tables in correct order (respect FK constraints) ---
DROP_LEGACY_STATEMENTS = [
    "DROP TABLE IF EXISTS fact_binance_trades CASCADE;",
    "DROP TABLE IF EXISTS fact_transactions CASCADE;",
    "DROP TABLE IF EXISTS dim_exchange_rate CASCADE;",
    "DROP TABLE IF EXISTS dim_volume_category CASCADE;",
    "DROP TABLE IF EXISTS dim_crypto_pair CASCADE;",
    "DROP TABLE IF EXISTS dim_time CASCADE;",
    "DROP TABLE IF EXISTS dim_date CASCADE;",
]


DDL_STATEMENTS = [
    # -- dim_date ---------------------------------------------------------
    # Smart Key: date_key = yyyyMMdd (e.g., 20260414)
    # Kimball: Acceptable as integer surrogate key with built-in meaning.
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

    # -- dim_time ---------------------------------------------------------
    # Smart Key: time_key = HHmm (e.g., 1430 = 14:30)
    """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_key         BIGINT PRIMARY KEY,
        hour             INTEGER,
        minute           INTEGER,
        time_of_day      VARCHAR(20),
        is_business_hour BOOLEAN
    );
    """,

    # -- dim_volume_category ----------------------------------------------
    # Kimball: Surrogate Key (volume_category_key) as PK.
    # Natural key (volume_category) is kept as a descriptive attribute.
    """
    CREATE TABLE IF NOT EXISTS dim_volume_category (
        volume_category_key  INTEGER PRIMARY KEY,
        volume_category      VARCHAR(50) UNIQUE NOT NULL,
        description          VARCHAR(255),
        min_usd              NUMERIC(20, 2),
        max_usd              NUMERIC(20, 2)
    );
    """,

    # -- dim_crypto_pair --------------------------------------------------
    # Kimball: Surrogate Key (crypto_pair_key) as PK.
    # Natural key (crypto_symbol) is kept as a descriptive attribute.
    """
    CREATE TABLE IF NOT EXISTS dim_crypto_pair (
        crypto_pair_key  INTEGER PRIMARY KEY,
        crypto_symbol    VARCHAR(20) UNIQUE NOT NULL,
        base_asset       VARCHAR(20),
        quote_asset      VARCHAR(20),
        pair_name        VARCHAR(100)
    );
    """,

    # -- dim_exchange_rate ------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS dim_exchange_rate (
        date_key        BIGINT PRIMARY KEY,
        currency_code   VARCHAR(10),
        vnd_rate        NUMERIC(15, 2),
        CONSTRAINT fk_fx_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    );
    """,

    # -- fact_binance_trades ----------------------------------------------
    # Kimball: All FK references use INTEGER Surrogate Keys from Dimensions.
    # transaction_id is a Degenerate Dimension (kept in fact, no dim table).
    """
    CREATE TABLE IF NOT EXISTS fact_binance_trades (
        transaction_id        VARCHAR(64) PRIMARY KEY,
        trade_id              BIGINT,
        date_key              BIGINT,
        time_key              BIGINT,
        crypto_pair_key       INTEGER,
        volume_category_key   INTEGER,
        trade_time            TIMESTAMP,
        price                 NUMERIC(38, 9),
        quantity              NUMERIC(38, 9),
        amount_usd            NUMERIC(38, 9),
        is_buyer_maker        BOOLEAN,
        is_anomaly            BOOLEAN,
        buyer_order_id        BIGINT,
        seller_order_id       BIGINT,
        CONSTRAINT fk_trade_date     FOREIGN KEY (date_key)             REFERENCES dim_date(date_key),
        CONSTRAINT fk_trade_time     FOREIGN KEY (time_key)             REFERENCES dim_time(time_key),
        CONSTRAINT fk_trade_pair     FOREIGN KEY (crypto_pair_key)      REFERENCES dim_crypto_pair(crypto_pair_key),
        CONSTRAINT fk_trade_category FOREIGN KEY (volume_category_key)  REFERENCES dim_volume_category(volume_category_key)
    );
    """,

    # -- Indexes ----------------------------------------------------------
    "CREATE INDEX IF NOT EXISTS idx_binance_pair   ON fact_binance_trades(crypto_pair_key);",
    "CREATE INDEX IF NOT EXISTS idx_binance_time   ON fact_binance_trades(trade_time);",
    "CREATE INDEX IF NOT EXISTS idx_binance_amount ON fact_binance_trades(amount_usd);",
    "CREATE INDEX IF NOT EXISTS idx_binance_volcat ON fact_binance_trades(volume_category_key);",
]

def main():
    print("=" * 60)
    print("  PostgreSQL Schema - Kimball Star Schema (Surrogate Keys)")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}")
    print(f"  DB   : {PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)

    with engine.begin() as conn:
        print("Dropping old tables...")
        for stmt in DROP_LEGACY_STATEMENTS:
            conn.execute(text(stmt))
            
        print("Creating new tables (1 Fact, 5 Dims — Kimball Surrogate Keys)...")
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))

    print("[DONE] Data Warehouse is ready (Kimball-compliant).")
    print("=" * 60)


if __name__ == "__main__":
    main()
