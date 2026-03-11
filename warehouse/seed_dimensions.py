"""
Seed Dimension Tables - Nạp dữ liệu mẫu cho 5 bảng Dimension
================================================================
Chạy SAU khi đã chạy bigquery_schema.py để tạo bảng.

Logic sinh dữ liệu:
  - dim_users       : Trích nameOrig unique từ PaySim CSV, gán segment & balance.
  - dim_merchants   : Trích nameDest unique (bắt đầu bằng M), gán category.
  - dim_transaction_type : 5 loại giao dịch PaySim cố định.
  - dim_location    : 10 thành phố Việt Nam + vùng miền.
  - dim_date        : Toàn bộ ngày trong năm 2026.
"""

import os
import random
from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# ─── Load Configuration ──────────────────────────────────────────
load_dotenv()
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET", "paysim_dw")
CSV_PATH = os.getenv("CSV_PATH", "data/PS_20174392719_1491204439457_log.csv")

if not PROJECT_ID:
    raise ValueError("[ERRO] Thiếu BQ_PROJECT_ID trong .env")

FULL_DATASET = f"{PROJECT_ID}.{DATASET_ID}"

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


def seed_dim_transaction_type(client: bigquery.Client) -> int:
    """Nạp 5 loại giao dịch cố định."""
    table_id = f"{FULL_DATASET}.dim_transaction_type"
    rows = TRANSACTION_TYPES
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_transaction_type: {job.errors}")
        return 0
    return len(rows)


def seed_dim_location(client: bigquery.Client) -> int:
    """Nạp 10 thành phố Việt Nam."""
    table_id = f"{FULL_DATASET}.dim_location"
    rows = LOCATIONS
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_location: {job.errors}")
        return 0
    return len(rows)


import calendar

def seed_dim_date(client: bigquery.Client, start_year: int = 2025, end_year: int = 2030) -> int:
    """Sinh toàn bộ ngày từ start_year đến end_year với 13 cột đầy đủ."""
    table_id = f"{FULL_DATASET}.dim_date"
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)

    WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    MONTH_NAMES   = ["January", "February", "March", "April", "May", "June",
                     "July", "August", "September", "October", "November", "December"]

    rows = []
    current = start
    while current <= end:
        dow = current.isoweekday()  # 1=Mon ... 7=Sun
        rows.append({
            "date_key":      int(current.strftime("%Y%m%d")),
            "date":          current,
            "yyyymmdd":      current.strftime("%Y%m%d"),
            "yyyy_mm_dd":    current.strftime("%Y-%m-%d"),
            "year":          current.year,
            "quarter":       (current.month - 1) // 3 + 1,
            "month_number":  current.month,
            "month_name":    MONTH_NAMES[current.month - 1],
            "year_month":    current.strftime("%Y-%m"),
            "day":           current.day,
            "weekday_number": dow,
            "weekday_name":  WEEKDAY_NAMES[dow - 1],
            "is_weekend":    dow >= 6,
            "is_leap_year":  calendar.isleap(current.year),
        })
        current += timedelta(days=1)

    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_date: {job.errors}")
        return 0
    return len(rows)


def seed_dim_time(client: bigquery.Client) -> int:
    """Nạp 1440 phút trong ngày."""
    table_id = f"{FULL_DATASET}.dim_time"
    rows = []
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
            rows.append({
                "time_key": time_key,
                "hour": h,
                "minute": m,
                "time_of_day": tod,
                "is_business_hour": is_biz
            })
    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_time: {job.errors}")
        return 0
    return len(rows)


def seed_dim_channel(client: bigquery.Client) -> int:
    """Nạp 5 kênh giao dịch."""
    table_id = f"{FULL_DATASET}.dim_channel"
    rows = CHANNELS
    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_channel: {job.errors}")
        return 0
    return len(rows)


def seed_dim_users(client: bigquery.Client):
    """Trích unique nameOrig từ PaySim CSV, gán segment & balance."""
    table_id = f"{FULL_DATASET}.dim_users"

    print(f"  [FILE] Đọc CSV: {CSV_PATH} (chỉ cột nameOrig, oldbalanceOrg)...")
    df = pd.read_csv(CSV_PATH, usecols=["type", "nameOrig", "oldbalanceOrg"])
    df = df[df["type"] == "PAYMENT"]

    # Lấy unique users, giữ balance lớn nhất
    users = df.groupby("nameOrig")["oldbalanceOrg"].max().reset_index()
    users.columns = ["user_id", "account_balance"]

    random.seed(42)
    base_date = date(2024, 1, 1)

    rows = []
    for _, row in users.iterrows():
        rows.append({
            "user_id": row["user_id"],
            "account_balance": Decimal(str(row["account_balance"])),
            "user_segment": random.choices(USER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0],
            "registration_date": (base_date + timedelta(days=random.randint(0, 730))),
        })

    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_users: {job.errors}")
        return 0, pd.DataFrame()
    return len(rows), users


def seed_dim_account(client: bigquery.Client, users_df: pd.DataFrame) -> int:
    """Nạp account cho mock users."""
    table_id = f"{FULL_DATASET}.dim_account"
    rows = []
    base_date = date(2024, 1, 1)
    for uid in users_df["user_id"]:
        num_accounts = random.choices([1, 2], weights=[0.8, 0.2])[0]
        for i in range(1, num_accounts + 1):
            rows.append({
                "account_id": f"{uid}_ACC{i}",
                "user_id": uid,
                "account_type": random.choice(ACCOUNT_TYPES),
                "account_status": random.choices(ACCOUNT_STATUSES, weights=ACCOUNT_STAT_W)[0],
                "created_date": (base_date + timedelta(days=random.randint(0, 730))),
            })
    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_account: {job.errors}")
        return 0
    return len(rows)


def seed_dim_merchants(client: bigquery.Client) -> int:
    """Trích unique nameDest (bắt đầu M) từ PaySim CSV, gán category."""
    table_id = f"{FULL_DATASET}.dim_merchants"

    print(f"  [FILE] Đọc CSV: {CSV_PATH} (chỉ cột nameDest)...")
    df = pd.read_csv(CSV_PATH, usecols=["type", "nameDest"])
    df = df[df["type"] == "PAYMENT"]

    # Merchants trong PaySim bắt đầu bằng "M"
    merchants = df["nameDest"].unique()
    merchants = [m for m in merchants if str(m).startswith("M")]

    random.seed(42)
    rows = []
    for m_id in merchants:
        rows.append({
            "merchant_id": m_id,
            "merchant_name": f"Store_{m_id[1:8]}",  # Tạo tên giả từ ID
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
        })

    print(f"    [SEND] Nạp {len(rows):,} rows vào {table_id}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(pd.DataFrame(rows), table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"  [ERRO] Lỗi insert dim_merchants: {job.errors}")
        return 0
    return len(rows)


def main():
    print("=" * 60)
    print("  Seed Dimension Tables - Nạp dữ liệu mẫu")
    print("=" * 60)
    print(f"  Project : {PROJECT_ID}")
    print(f"  Dataset : {DATASET_ID}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    results = {}

    # 1. dim_transaction_type (nhỏ nhất, chạy trước)
    print("[STEP] [1/5] Seeding dim_transaction_type...")
    results["dim_transaction_type"] = seed_dim_transaction_type(client)

    # 2. dim_location
    print("[STEP] [2/5] Seeding dim_location...")
    results["dim_location"] = seed_dim_location(client)

    # 3. dim_date
    print("[STEP] [3/8] Seeding dim_date (2025 -> 2030)...")
    results["dim_date"] = seed_dim_date(client, start_year=2025, end_year=2030)

    # 4. dim_time
    print("[STEP] [4/8] Seeding dim_time...")
    results["dim_time"] = seed_dim_time(client)

    # 5. dim_channel
    print("[STEP] [5/8] Seeding dim_channel...")
    results["dim_channel"] = seed_dim_channel(client)

    # 6. dim_users (cần đọc CSV)
    print("[STEP] [6/8] Seeding dim_users...")
    count_users, users_df = seed_dim_users(client)
    results["dim_users"] = count_users

    # 7. dim_account
    print("[STEP] [7/8] Seeding dim_account...")
    results["dim_account"] = seed_dim_account(client, users_df)

    # 8. dim_merchants (cần đọc CSV)
    print("[STEP] [8/8] Seeding dim_merchants...")
    results["dim_merchants"] = seed_dim_merchants(client)

    # Tổng kết
    print(f"\n{'='*60}")
    print("[DONE] KẾT QUẢ SEED:")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
