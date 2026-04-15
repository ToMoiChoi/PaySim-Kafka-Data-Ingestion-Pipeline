"""
warehouse/seed_dimensions_bq.py - Seed dimension data DIRECTLY to BigQuery
====================================================================================
Run AFTER `bigquery_schema.py` has been executed to create the tables.

This script populates BigQuery tables WITHOUT needing PostgreSQL:
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
from google.cloud import bigquery

# --- 1. Load config ---
load_dotenv()

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")


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


# --- 3. BigQuery Upload Helper ---
def seed_table_bq(client: bigquery.Client, table_name: str, df: pd.DataFrame) -> int:
    """Upload a DataFrame to BigQuery using WRITE_TRUNCATE (replace all existing data)."""
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    print(f"    [SEND] Loading {len(df):,} rows -> {table_name} (BigQuery)...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Truncate + reload
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion

    print(f"    [OK]   {table_name} -> {len(df):,} rows uploaded.")
    return len(df)


# --- 4. Main Controller ---
def main():
    if not BQ_PROJECT_ID:
        print("[ERROR] BQ_PROJECT_ID is not configured in .env")
        return

    print("=" * 65)
    print("  Seed Dimension Tables - BigQuery (Kimball Surrogate Keys)")
    print("=" * 65)
    print(f"  Project  : {BQ_PROJECT_ID}")
    print(f"  Dataset  : {BQ_DATASET}")
    print()

    client = bigquery.Client(project=BQ_PROJECT_ID)
    results = {}
    random.seed(42)  # Fixed random seed for reproducibility

    # Start and end dates
    start_dt = date(2024, 1, 1)
    end_dt   = date(2030, 12, 31)

    # ------------------------------------------------------------------
    # 1. dim_date & 2. dim_exchange_rate
    # ------------------------------------------------------------------
    print("[STEP] [1/5] & [2/5] Seeding dim_date & dim_exchange_rate...")
    date_rows = []
    fx_rows = []

    current_date = start_dt
    current_rate = 24500.0

    while current_date <= end_dt:
        date_key = int(current_date.strftime("%Y%m%d"))
        dow = current_date.isoweekday()

        date_rows.append({
            "date_key": date_key,
            "full_date": current_date.isoformat(),
            "day_of_week": dow,
            "is_weekend": dow >= 6,
            "month": current_date.month,
            "quarter": (current_date.month - 1) // 3 + 1,
            "year": current_date.year,
        })

        fluctuation = random.uniform(-15.0, 15.0)
        current_rate += fluctuation
        current_rate = max(23000.0, min(current_rate, 26500.0))

        fx_rows.append({
            "date_key": date_key,
            "currency_code": "VND",
            "vnd_rate": round(current_rate, 2)
        })

        current_date += timedelta(days=1)

    df_date = pd.DataFrame(date_rows)
    df_date["full_date"] = pd.to_datetime(df_date["full_date"])

    results["dim_date"] = seed_table_bq(client, "dim_date", df_date)
    results["dim_exchange_rate"] = seed_table_bq(client, "dim_exchange_rate", pd.DataFrame(fx_rows))


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
    results["dim_time"] = seed_table_bq(client, "dim_time", pd.DataFrame(time_rows))


    # ------------------------------------------------------------------
    # 4. dim_volume_category (Kimball Surrogate Keys: 1-4)
    # ------------------------------------------------------------------
    print("[STEP] [4/5] Seeding dim_volume_category (SK: 1-4)...")
    df_vol = pd.DataFrame(VOLUME_CATEGORIES)
    results["dim_volume_category"] = seed_table_bq(client, "dim_volume_category", df_vol)


    # ------------------------------------------------------------------
    # 5. dim_crypto_pair (Kimball Surrogate Keys: 1-5)
    # ------------------------------------------------------------------
    print("[STEP] [5/5] Seeding dim_crypto_pair (SK: 1-5)...")
    df_pair = pd.DataFrame(CRYPTO_PAIRS)
    results["dim_crypto_pair"] = seed_table_bq(client, "dim_crypto_pair", df_pair)


    # Summary
    print(f"\n{'='*65}")
    print("[DONE] SEED RESULTS (BigQuery - Kimball Surrogate Keys):")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print("=" * 65)


if __name__ == "__main__":
    main()
