"""
warehouse/seed_dimensions_pg.py - Tự động bơm dữ liệu cho Star Schema Mới
====================================================================================
Chạy SAU khi đã chạy `postgres_schema.py` để tạo bảng.

Mục tiêu thiết lập:
 - dim_date & dim_time: Tự sinh 100% bằng logic Python.
 - dim_volume_category: Khởi tạo các Hạng Khối lượng (Định mức Chainalysis).
 - dim_crypto_pair: Định nghĩa các cặp giao dịch hiện hành.
 - dim_exchange_rate: Sinh tỷ giá USD/VND theo từng ngày (Random Walk).
"""

import os
import random
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa
import psycopg2.extras

# ─── 1. Load config ───
load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


# ─── 2. Dữ Liệu Tĩnh ───
VOLUME_CATEGORIES = [
    {"volume_category": "RETAIL",        "description": "Giao dịch nhỏ lẻ dưới 10,000 USD",          "min_usd": 0,         "max_usd": 9999.99},
    {"volume_category": "PROFESSIONAL",  "description": "Giao dịch cá nhân chuyên nghiệp 10k-100k",  "min_usd": 10000,     "max_usd": 99999.99},
    {"volume_category": "INSTITUTIONAL", "description": "Giao dịch khối lớn/Tổ chức 100k-1Tr",       "min_usd": 100000,    "max_usd": 999999.99},
    {"volume_category": "WHALE",         "description": "Cá Mập/Cá Voi - Bất thường trên 1 Triệu",   "min_usd": 1000000,   "max_usd": 99999999999.99},
]

CRYPTO_PAIRS = [
    {"crypto_symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT", "pair_name": "Bitcoin / TetherUS"},
    {"crypto_symbol": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT", "pair_name": "Ethereum / TetherUS"},
    {"crypto_symbol": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT", "pair_name": "Binance Coin / TetherUS"},
    {"crypto_symbol": "SOLUSDT", "base_asset": "SOL", "quote_asset": "USDT", "pair_name": "Solana / TetherUS"},
    {"crypto_symbol": "XRPUSDT", "base_asset": "XRP", "quote_asset": "USDT", "pair_name": "Ripple / TetherUS"},
]


# ─── 3. Hàm Bơm Dữ Liệu ───
def seed_table(engine, table_name: str, df: pd.DataFrame) -> int:
    """Insert DataFrame vào PostgreSQL table bằng psycopg2 (TRUNCATE trước nếu cần)."""
    print(f"    [SEND] Nạp {len(df):,} rows -> {table_name}...")
    
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        
        # 1. Truncate table
        try:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        except Exception:
            raw_conn.rollback()
            cur = raw_conn.cursor() # reset cursor after failed truncate
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


# ─── 4. Main Controller ───
def main():
    print("=" * 65)
    print("  Seed Dimension Tables - Star Schema Native Crypto")
    print("=" * 65)
    print(f"  Host : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)
    results = {}
    random.seed(42)  # Cố định random

    # Khởi tạo ngày bắt đầu/kết thúc
    start_dt = date(2024, 1, 1)
    end_dt   = date(2030, 12, 31)

    # ────────────────────────────────────────────────────────
    # 1. Bảng dim_date & 2. Bảng dim_exchange_rate
    # Sinh dữ liệu chung trong vòng lặp ngày để lấy tỷ giá hàng ngày
    # ────────────────────────────────────────────────────────
    print("[STEP] [1/5] & [2/5] Seeding dim_date & dim_exchange_rate...")
    date_rows = []
    fx_rows = []
    
    current_date = start_dt
    
    # Giả lập biến động tỷ giá (Bắt đầu với mức hối đoái 24,500)
    current_rate = 24500.0

    while current_date <= end_dt:
        date_key = int(current_date.strftime("%Y%m%d"))
        dow = current_date.isoweekday()
        
        # 1. Dòng Date
        date_rows.append({
            "date_key": date_key,
            "full_date": current_date.isoformat(),
            "day_of_week": dow,
            "is_weekend": dow >= 6,
            "month": current_date.month,
            "quarter": (current_date.month - 1) // 3 + 1,
            "year": current_date.year,
        })
        
        # 2. Dòng FX Rate (Tỷ giá USD/VND biến động +- 15 VND mỗi ngày)
        fluctuation = random.uniform(-15.0, 15.0)
        current_rate += fluctuation
        
        # Chặn biên độ tỷ giá không rớt quá thấp hoặc cao quá vô lý
        current_rate = max(23000.0, min(current_rate, 26500.0))
        
        fx_rows.append({
            "date_key": date_key,
            "currency_code": "VND",
            "vnd_rate": round(current_rate, 2)
        })
        
        current_date += timedelta(days=1)

    results["dim_date"] = seed_table(engine, "dim_date", pd.DataFrame(date_rows))
    results["dim_exchange_rate"] = seed_table(engine, "dim_exchange_rate", pd.DataFrame(fx_rows))


    # ────────────────────────────────────────────────────────
    # 3. Bảng dim_time
    # ────────────────────────────────────────────────────────
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


    # ────────────────────────────────────────────────────────
    # 4. Bảng dim_volume_category
    # ────────────────────────────────────────────────────────
    print("[STEP] [4/5] Seeding dim_volume_category...")
    df_vol = pd.DataFrame(VOLUME_CATEGORIES)
    results["dim_volume_category"] = seed_table(engine, "dim_volume_category", df_vol)


    # ────────────────────────────────────────────────────────
    # 5. Bảng dim_crypto_pair
    # ────────────────────────────────────────────────────────
    print("[STEP] [5/5] Seeding dim_crypto_pair...")
    df_pair = pd.DataFrame(CRYPTO_PAIRS)
    results["dim_crypto_pair"] = seed_table(engine, "dim_crypto_pair", df_pair)


    # Tổng kết
    print(f"\n{'='*65}")
    print("[DONE] KẾT QUẢ SEED (PostgreSQL):")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print("=" * 65)


if __name__ == "__main__":
    main()
