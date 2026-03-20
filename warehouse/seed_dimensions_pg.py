"""
warehouse/seed_dimensions_pg.py - Seed dữ liệu mẫu vào PostgreSQL Dimension tables
====================================================================================
Chạy SAU khi đã chạy postgres_schema.py để tạo bảng.

Logic giống seed_dimensions.py (BigQuery) nhưng ghi vào PostgreSQL
dùng psycopg2.extras.execute_values (tối ưu tốc độ insert hàng loạt).
"""

import os
import random
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa

load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")
CSV_PATH    = os.getenv("CSV_PATH", "data/PS_20174392719_1491204439457_log.csv")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

import json

def load_seed_data() -> dict:
    with open("data/dimension_seed.json", "r", encoding="utf-8") as f:
        return json.load(f)

SEED_DATA = load_seed_data()

MERCHANT_CATEGORIES = SEED_DATA["MERCHANT_CATEGORIES"]
USER_SEGMENTS = SEED_DATA["USER_SEGMENTS"]
SEGMENT_WEIGHTS = SEED_DATA["SEGMENT_WEIGHTS"]
LOCATIONS = SEED_DATA["LOCATIONS"]
TRANSACTION_TYPES = SEED_DATA["TRANSACTION_TYPES"]
CHANNELS = SEED_DATA["CHANNELS"]
ACCOUNT_TYPES = SEED_DATA["ACCOUNT_TYPES"]
ACCOUNT_STATUSES = SEED_DATA["ACCOUNT_STATUSES"]
ACCOUNT_STAT_W = SEED_DATA["ACCOUNT_STAT_W"]


import psycopg2.extras

def seed_table(engine, table_name: str, df: pd.DataFrame) -> int:
    """Insert DataFrame vào PostgreSQL table bằng psycopg2 (TRUNCATE trước nếu cần)."""
    print(f"    [SEND] Nap {len(df):,} rows -> {table_name}...")
    
    # Lấy raw connection (psycopg2) từ engine để tránh lỗi tương thích
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        
        # 1. Truncate table
        try:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        except Exception as e:
            print(f"      [WARN] Could not truncate: {e}")
            raw_conn.rollback()
            
        # 2. Insert data using execute_values
        if not df.empty:
            columns = ",".join(df.columns)
            values = [tuple(x) for x in df.to_numpy()]
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_query, values, page_size=1000)
            
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        raise e
    finally:
        raw_conn.close()
        
    return len(df)


def main():
    print("=" * 60)
    print("  Seed Dimension Tables - PostgreSQL")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)
    results = {}
    random.seed(42)

    # 1. dim_transaction_type
    print("[STEP] [1/8] Seeding dim_transaction_type...")
    df_tt = pd.DataFrame(TRANSACTION_TYPES)
    results["dim_transaction_type"] = seed_table(engine, "dim_transaction_type", df_tt)

    # 2. dim_location
    print("[STEP] [2/8] Seeding dim_location...")
    df_loc = pd.DataFrame(LOCATIONS)
    results["dim_location"] = seed_table(engine, "dim_location", df_loc)

    # 3. dim_date
    print("[STEP] [3/8] Seeding dim_date (năm 2025-2030)...")
    current = date(2025, 1, 1)
    end     = date(2030, 12, 31)
    date_rows = []
    while current <= end:
        dow = current.isoweekday()
        date_rows.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current.isoformat(),
            "day_of_week": dow,
            "is_weekend": dow >= 6,
            "month": current.month,
            "quarter": (current.month - 1) // 3 + 1,
            "year": current.year,
        })
        current += timedelta(days=1)
    results["dim_date"] = seed_table(engine, "dim_date", pd.DataFrame(date_rows))

    # 4. dim_time
    print("[STEP] [4/8] Seeding dim_time...")
    time_rows = []
    for h in range(24):
        for m in range(60):
            time_key = h * 100 + m
            if 5 <= h < 11:
                tod = "Morning"
            elif 11 <= h < 14:
                tod = "Noon"
            elif 14 <= h < 18:
                tod = "Afternoon"
            elif 18 <= h < 22:
                tod = "Evening"
            else:
                tod = "Night"
            is_biz = (8 <= h < 17)
            time_rows.append({
                "time_key": time_key,
                "hour": h,
                "minute": m,
                "time_of_day": tod,
                "is_business_hour": is_biz
            })
    results["dim_time"] = seed_table(engine, "dim_time", pd.DataFrame(time_rows))

    # 5. dim_channel
    print("[STEP] [5/8] Seeding dim_channel...")
    df_channel = pd.DataFrame(CHANNELS)
    results["dim_channel"] = seed_table(engine, "dim_channel", df_channel)

    # 6. dim_users (từ CSV)
    print(f"[STEP] [6/8] Seeding dim_users (từ {CSV_PATH})...")
    df = pd.read_csv(CSV_PATH, usecols=["type", "nameOrig", "oldbalanceOrg"])
    df = df[df["type"] == "PAYMENT"].dropna(subset=["nameOrig"])
    users = df.groupby("nameOrig")["oldbalanceOrg"].max().reset_index()
    users.columns = ["user_id", "account_balance"]
    base_date = date(2024, 1, 1)
    users["user_segment"]      = random.choices(USER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=len(users))
    users["registration_date"] = [(base_date + timedelta(days=random.randint(0, 730))).isoformat()
                                   for _ in range(len(users))]
    results["dim_users"] = seed_table(engine, "dim_users", users)

    # 7. dim_account
    print("[STEP] [7/8] Seeding dim_account...")
    account_rows = []
    for uid in users["user_id"]:
        # Generate 1 to 2 accounts per user
        num_accounts = random.choices([1, 2], weights=[0.8, 0.2])[0]
        for i in range(1, num_accounts + 1):
            account_rows.append({
                "account_id": f"{uid}_ACC{i}",
                "user_id": uid,
                "account_type": random.choice(ACCOUNT_TYPES),
                "account_status": random.choices(ACCOUNT_STATUSES, weights=ACCOUNT_STAT_W)[0],
                "created_date": (base_date + timedelta(days=random.randint(0, 730))).isoformat()
            })
    results["dim_account"] = seed_table(engine, "dim_account", pd.DataFrame(account_rows))

    # 8. dim_merchants (từ CSV)
    print(f"[STEP] [8/8] Seeding dim_merchants (từ {CSV_PATH})...")
    df2 = pd.read_csv(CSV_PATH, usecols=["type", "nameDest"])
    df2 = df2[df2["type"] == "PAYMENT"].dropna(subset=["nameDest"])
    merchants_ids = [m for m in df2["nameDest"].unique() if str(m).startswith("M")]
    df_mer = pd.DataFrame({
        "merchant_id":       merchants_ids,
        "merchant_name":     [f"Store_{m[1:8]}" for m in merchants_ids],
        "merchant_category": [random.choice(MERCHANT_CATEGORIES) for _ in merchants_ids],
    })
    results["dim_merchants"] = seed_table(engine, "dim_merchants", df_mer)

    # Tổng kết
    print(f"\n{'='*60}")
    print("[DONE] KẾT QUẢ SEED (PostgreSQL):")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
