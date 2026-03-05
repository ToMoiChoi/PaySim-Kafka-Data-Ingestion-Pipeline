"""
Seed Dimension Tables – Nạp dữ liệu mẫu cho 5 bảng Dimension
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

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# ─── Load Configuration ──────────────────────────────────────────
load_dotenv()
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET", "paysim_dw")
CSV_PATH = os.getenv("CSV_PATH", "data/PS_20174392719_1491204439457_log.csv")

if not PROJECT_ID:
    raise ValueError("❌ Thiếu BQ_PROJECT_ID trong .env")

FULL_DATASET = f"{PROJECT_ID}.{DATASET_ID}"

# ─── Seed Constants ──────────────────────────────────────────────
MERCHANT_CATEGORIES = ["F&B", "E-commerce", "Bill Payment", "Travel", "Entertainment"]
USER_SEGMENTS = ["Standard", "Gold", "Diamond"]
SEGMENT_WEIGHTS = [0.60, 0.30, 0.10]

LOCATIONS = [
    ("LOC_HCM", "TP. Hồ Chí Minh", "Nam"),
    ("LOC_HN",  "Hà Nội",           "Bắc"),
    ("LOC_DN",  "Đà Nẵng",          "Trung"),
    ("LOC_HP",  "Hải Phòng",        "Bắc"),
    ("LOC_CT",  "Cần Thơ",          "Nam"),
    ("LOC_HUE", "Huế",              "Trung"),
    ("LOC_NT",  "Nha Trang",        "Trung"),
    ("LOC_DL",  "Đà Lạt",           "Trung"),
    ("LOC_VT",  "Vũng Tàu",         "Nam"),
    ("LOC_BD",  "Bình Dương",        "Nam"),
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
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print(f"  ❌ Lỗi insert dim_transaction_type: {errors}")
        return 0
    return len(rows)


def seed_dim_location(client: bigquery.Client) -> int:
    """Nạp 10 thành phố Việt Nam."""
    table_id = f"{FULL_DATASET}.dim_location"
    rows = [
        {"location_id": loc_id, "city": city, "region": region}
        for loc_id, city, region in LOCATIONS
    ]
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print(f"  ❌ Lỗi insert dim_location: {errors}")
        return 0
    return len(rows)


def seed_dim_date(client: bigquery.Client, year: int = 2026) -> int:
    """Sinh toàn bộ ngày trong năm."""
    table_id = f"{FULL_DATASET}.dim_date"
    start = date(year, 1, 1)
    end = date(year, 12, 31)

    rows = []
    current = start
    while current <= end:
        dow = current.isoweekday()  # 1=Mon ... 7=Sun
        rows.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current.isoformat(),
            "day_of_week": dow,
            "is_weekend": dow >= 6,
            "month": current.month,
            "quarter": (current.month - 1) // 3 + 1,
            "year": current.year,
        })
        current += timedelta(days=1)

    # Insert theo batch 500 dòng (tránh payload quá lớn)
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            print(f"  ❌ Lỗi insert dim_date batch {i}: {errors}")
            return 0
    return len(rows)


def seed_dim_users(client: bigquery.Client) -> int:
    """Trích unique nameOrig từ PaySim CSV, gán segment & balance."""
    table_id = f"{FULL_DATASET}.dim_users"

    print(f"  📂 Đọc CSV: {CSV_PATH} (chỉ cột nameOrig, oldbalanceOrg)...")
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
            "account_balance": float(row["account_balance"]),
            "user_segment": random.choices(USER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0],
            "registration_date": (base_date + timedelta(days=random.randint(0, 730))).isoformat(),
        })

    # Insert theo batch
    batch_size = 500
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            print(f"  ❌ Lỗi insert dim_users batch {i}: {errors[:3]}...")
            return total_inserted
        total_inserted += len(batch)
        if total_inserted % 5000 == 0:
            print(f"    📤 dim_users: {total_inserted:,} / {len(rows):,}")

    return total_inserted


def seed_dim_merchants(client: bigquery.Client) -> int:
    """Trích unique nameDest (bắt đầu M) từ PaySim CSV, gán category."""
    table_id = f"{FULL_DATASET}.dim_merchants"

    print(f"  📂 Đọc CSV: {CSV_PATH} (chỉ cột nameDest)...")
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

    # Insert theo batch
    batch_size = 500
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            print(f"  ❌ Lỗi insert dim_merchants batch {i}: {errors[:3]}...")
            return total_inserted
        total_inserted += len(batch)
        if total_inserted % 5000 == 0:
            print(f"    📤 dim_merchants: {total_inserted:,} / {len(rows):,}")

    return total_inserted


def main():
    print("=" * 60)
    print("  Seed Dimension Tables – Nạp dữ liệu mẫu")
    print("=" * 60)
    print(f"  Project : {PROJECT_ID}")
    print(f"  Dataset : {DATASET_ID}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    results = {}

    # 1. dim_transaction_type (nhỏ nhất, chạy trước)
    print("📋 [1/5] Seeding dim_transaction_type...")
    results["dim_transaction_type"] = seed_dim_transaction_type(client)

    # 2. dim_location
    print("📋 [2/5] Seeding dim_location...")
    results["dim_location"] = seed_dim_location(client)

    # 3. dim_date
    print("📋 [3/5] Seeding dim_date (năm 2026)...")
    results["dim_date"] = seed_dim_date(client, year=2026)

    # 4. dim_users (cần đọc CSV)
    print("📋 [4/5] Seeding dim_users...")
    results["dim_users"] = seed_dim_users(client)

    # 5. dim_merchants (cần đọc CSV)
    print("📋 [5/5] Seeding dim_merchants...")
    results["dim_merchants"] = seed_dim_merchants(client)

    # Tổng kết
    print(f"\n{'='*60}")
    print("✅ KẾT QUẢ SEED:")
    for table, count in results.items():
        print(f"   {table:30s} → {count:>8,} rows")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
