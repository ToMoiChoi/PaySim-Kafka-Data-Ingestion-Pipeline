"""
warehouse/seed_dimensions_pg.py - Seed dimension data for the Star Schema
====================================================================================
Run AFTER `postgres_schema.py` has been executed to create the tables.

This script populates:
 - dim_date & dim_time: Generated entirely by Python logic.
 - dim_volume_category: Initialises volume tiers (Kimball Surrogate Keys).
 - dim_crypto_pair: Defines tracked trading pairs (Kimball Surrogate Keys).
 - dim_exchange_rate: Generates daily USD/VND exchange rates (Random Walk simulation).

Kimball Methodology:
 - dim_volume_category: Surrogate Key volume_category_key (1-4)
 - dim_crypto_pair: Surrogate Key crypto_pair_key (1-5)
"""

import os
import random
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa
import psycopg2.extras

# --- 1. Load config ---
load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


# --- 2. Static Data (Kimball Surrogate Keys) ---

# Surrogate Key mapping for dim_volume_category
VOLUME_CATEGORIES = [
    {"volume_category_key": 1, "volume_category": "RETAIL",        "description": "Small trades under 10,000 USD",                    "min_usd": 0,         "max_usd": 9999.99},
    {"volume_category_key": 2, "volume_category": "PROFESSIONAL",  "description": "Professional individual trades 10k-100k",          "min_usd": 10000,     "max_usd": 99999.99},
    {"volume_category_key": 3, "volume_category": "INSTITUTIONAL", "description": "Large block / institutional trades 100k-1M",       "min_usd": 100000,    "max_usd": 999999.99},
    {"volume_category_key": 4, "volume_category": "WHALE",         "description": "Whale trades - anomalous orders above 1 Million",  "min_usd": 1000000,   "max_usd": 99999999999.99},
]

# Surrogate Key mapping for dim_crypto_pair
CRYPTO_PAIRS = [
    {"crypto_pair_key": 1, "crypto_symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT", "pair_name": "Bitcoin / TetherUS"},
    {"crypto_pair_key": 2, "crypto_symbol": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT", "pair_name": "Ethereum / TetherUS"},
    {"crypto_pair_key": 3, "crypto_symbol": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT", "pair_name": "Binance Coin / TetherUS"},
    {"crypto_pair_key": 4, "crypto_symbol": "SOLUSDT", "base_asset": "SOL", "quote_asset": "USDT", "pair_name": "Solana / TetherUS"},
    {"crypto_pair_key": 5, "crypto_symbol": "XRPUSDT", "base_asset": "XRP", "quote_asset": "USDT", "pair_name": "Ripple / TetherUS"},
]


# --- 3. Insert Helper ---
def seed_table(engine, table_name: str, df: pd.DataFrame) -> int:
    """Insert a DataFrame into a PostgreSQL table using psycopg2 (TRUNCATE first)."""
    print(f"    [SEND] Loading {len(df):,} rows -> {table_name}...")
    
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        
        # 1. Truncate table
        try:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        except Exception:
            raw_conn.rollback()
            cur = raw_conn.cursor()
            try:
                 cur.execute(f"DELETE FROM {table_name};")
            except Exception as e2:
                 print(f"      [WARN] Could not clean table {table_name}: {e2}")
                 raw_conn.rollback()
                 cur = raw_conn.cursor()

        # 2. Insert data using execute_values
        if not df.empty:
            columns = ",".join(df.columns)
            values = [tuple(x) for x in df.to_numpy()]
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_query, values, page_size=2000)
            
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        raise e
    finally:
        raw_conn.close()
        
    return len(df)


# --- 4. Main Controller ---
def main():
    print("=" * 65)
    print("  Seed Dimension Tables - Kimball Star Schema (Surrogate Keys)")
    print("=" * 65)
    print(f"  Host : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)
    results = {}
    random.seed(42)  # Fixed random seed for reproducibility

    # Start and end dates
    start_dt = date(2024, 1, 1)
    end_dt   = date(2030, 12, 31)

    # ------------------------------------------------------------------
    # 1. dim_date & 2. dim_exchange_rate
    # Generate data together in the same date loop to produce daily FX rates
    # ------------------------------------------------------------------
    print("[STEP] [1/5] & [2/5] Seeding dim_date & dim_exchange_rate...")
    date_rows = []
    fx_rows = []
    
    current_date = start_dt
    
    # Simulate exchange rate fluctuation (starting at 24,500 VND/USD)
    current_rate = 24500.0

    while current_date <= end_dt:
        date_key = int(current_date.strftime("%Y%m%d"))
        dow = current_date.isoweekday()
        
        # 1. Date row
        date_rows.append({
            "date_key": date_key,
            "full_date": current_date.isoformat(),
            "day_of_week": dow,
            "is_weekend": dow >= 6,
            "month": current_date.month,
            "quarter": (current_date.month - 1) // 3 + 1,
            "year": current_date.year,
        })
        
        # 2. FX Rate row (USD/VND fluctuates +/- 15 VND per day)
        fluctuation = random.uniform(-15.0, 15.0)
        current_rate += fluctuation
        
        # Clamp exchange rate within a realistic range
        current_rate = max(23000.0, min(current_rate, 26500.0))
        
        fx_rows.append({
            "date_key": date_key,
            "currency_code": "VND",
            "vnd_rate": round(current_rate, 2)
        })
        
        current_date += timedelta(days=1)

    results["dim_date"] = seed_table(engine, "dim_date", pd.DataFrame(date_rows))
    results["dim_exchange_rate"] = seed_table(engine, "dim_exchange_rate", pd.DataFrame(fx_rows))


    # ------------------------------------------------------------------
    # 3. dim_time
    # ------------------------------------------------------------------
    print("[STEP] [3/5] Seeding dim_time...")
    time_rows = []
    for h in range(24):
        for m in range(60):
            time_key = h * 100 + m
            if 5 <= h < 11:    tod = "Morning"
            elif 11 <= h < 14: tod = "Noon"
            elif 14 <= h < 18: tod = "Afternoon"
            elif 18 <= h < 22: tod = "Evening"
            else:              tod = "Night"
            
            is_biz = (8 <= h < 17)
            time_rows.append({
                "time_key": time_key,
                "hour": h,
                "minute": m,
                "time_of_day": tod,
                "is_business_hour": is_biz
            })
    results["dim_time"] = seed_table(engine, "dim_time", pd.DataFrame(time_rows))


    # ------------------------------------------------------------------
    # 4. dim_volume_category (Kimball Surrogate Keys: 1-4)
    # ------------------------------------------------------------------
    print("[STEP] [4/5] Seeding dim_volume_category (SK: 1-4)...")
    df_vol = pd.DataFrame(VOLUME_CATEGORIES)
    results["dim_volume_category"] = seed_table(engine, "dim_volume_category", df_vol)


    # ------------------------------------------------------------------
    # 5. dim_crypto_pair (Kimball Surrogate Keys: 1-5)
    # ------------------------------------------------------------------
    print("[STEP] [5/5] Seeding dim_crypto_pair (SK: 1-5)...")
    df_pair = pd.DataFrame(CRYPTO_PAIRS)
    results["dim_crypto_pair"] = seed_table(engine, "dim_crypto_pair", df_pair)


    # Summary
    print(f"\n{'='*65}")
    print("[DONE] SEED RESULTS (PostgreSQL - Kimball Surrogate Keys):")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print("=" * 65)


if __name__ == "__main__":
    main()
