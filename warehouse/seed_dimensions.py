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

# ─── Seed Constants ──────────────────────────────────────────────
MERCHANT_CATEGORIES = ["F&B", "E-commerce", "Bill Payment", "Travel", "Entertainment"]
USER_SEGMENTS = ["Standard", "Gold", "Diamond"]
SEGMENT_WEIGHTS = [0.60, 0.30, 0.10]

LOCATIONS = [
    # ─ Bắc ─────────────────────────────────────────────────────
    ("LOC_VN_HNI", "Hà Nội",             "Bắc"),
    ("LOC_VN_HPG", "Hải Phòng",           "Bắc"),
    ("LOC_VN_HGG", "Hà Giang",             "Bắc"),
    ("LOC_VN_CBG", "Cao Bằng",             "Bắc"),
    ("LOC_VN_LCI", "Lào Cai",              "Bắc"),
    ("LOC_VN_LSN", "Lạng Sơn",             "Bắc"),
    ("LOC_VN_TQG", "Tuyên Quang",           "Bắc"),
    ("LOC_VN_TNG", "Thái Nguyên",           "Bắc"),
    ("LOC_VN_BGG", "Bắc Giang",             "Bắc"),
    ("LOC_VN_PTH", "Phú Thọ",              "Bắc"),
    ("LOC_VN_BNH", "Bắc Ninh",             "Bắc"),
    ("LOC_VN_HYN", "Hưng Yên",             "Bắc"),
    ("LOC_VN_HDG", "Hải Dương",            "Bắc"),
    ("LOC_VN_NDH", "Nam Định",             "Bắc"),
    # ─ Trung ───────────────────────────────────────────────
    ("LOC_VN_THA", "Thanh Hóa",             "Trung"),
    ("LOC_VN_NAN", "Nghệ An",              "Trung"),
    ("LOC_VN_HTH", "Hà Tĩnh",              "Trung"),
    ("LOC_VN_DNG", "Đà Nẵng",              "Trung"),
    ("LOC_VN_HUE", "Thừa Thiên-Huế",       "Trung"),
    ("LOC_VN_QNM", "Quảng Nam",             "Trung"),
    ("LOC_VN_QNG", "Quảng Ngãi",            "Trung"),
    ("LOC_VN_BDH", "Bình Định",             "Trung"),
    ("LOC_VN_PYN", "Phú Yên",              "Trung"),
    ("LOC_VN_KHA", "Khánh Hòa",             "Trung"),
    ("LOC_VN_DLK", "Đắk Lậk",              "Trung"),
    ("LOC_VN_LDG", "Lâm Đồng",              "Trung"),
    # ─ Nam ─────────────────────────────────────────────────────
    ("LOC_VN_HCM", "TP. Hồ Chí Minh",      "Nam"),
    ("LOC_VN_CTH", "Cần Thơ",              "Nam"),
    ("LOC_VN_BPC", "Bình Phước",            "Nam"),
    ("LOC_VN_TNH", "Tây Ninh",              "Nam"),
    ("LOC_VN_BDG", "Bình Dương",            "Nam"),
    ("LOC_VN_DNI", "Đồng Nai",              "Nam"),
    ("LOC_VN_BVT", "Bà Rịa-Vũng Tàu",      "Nam"),
]

TRANSACTION_TYPES = [
    ("TYP_PAYMENT",  "Payment",  True,  1.0),
    ("TYP_TRANSFER", "Transfer", False, 0.0),
    ("TYP_CASH_OUT", "Cash Out", False, 0.0),
    ("TYP_DEBIT",    "Debit",    True,  0.5),
    ("TYP_CASH_IN",  "Cash In",  False, 0.0),
]


def seed_dim_transaction_type(client: bigquery.Client) -> int:
    """Nạp 5 loại giao dịch cố định."""
    table_id = f"{FULL_DATASET}.dim_transaction_type"
    rows = [
        {
            "type_id": tid,
            "type_name": name,
            "is_reward_eligible": eligible,
            "reward_multiplier": multiplier,
        }
        for tid, name, eligible, multiplier in TRANSACTION_TYPES
    ]
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
    rows = [
        {"location_id": loc_id, "city": city, "region": region}
        for loc_id, city, region in LOCATIONS
    ]
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



def seed_dim_users(client: bigquery.Client) -> int:
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
    print("[STEP] [3/5] Seeding dim_date (2025 -> 2030)...")
    results["dim_date"] = seed_dim_date(client, start_year=2025, end_year=2030)

    # 4. dim_users (cần đọc CSV)
    print("[STEP] [4/5] Seeding dim_users...")
    results["dim_users"] = seed_dim_users(client)

    # 5. dim_merchants (cần đọc CSV)
    print("[STEP] [5/5] Seeding dim_merchants...")
    results["dim_merchants"] = seed_dim_merchants(client)

    # Tổng kết
    print(f"\n{'='*60}")
    print("[DONE] KẾT QUẢ SEED:")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
