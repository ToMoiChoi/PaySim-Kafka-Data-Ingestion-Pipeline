"""
warehouse/seed_dimensions_pg.py – Seed dữ liệu mẫu vào PostgreSQL Dimension tables
====================================================================================
Chạy SAU khi đã chạy postgres_schema.py để tạo bảng.

Logic giống seed_dimensions.py (BigQuery) nhưng ghi vào PostgreSQL
dùng sqlalchemy + pandas.to_sql.
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

MERCHANT_CATEGORIES = ["F&B", "E-commerce", "Bill Payment", "Travel", "Entertainment"]
USER_SEGMENTS       = ["Standard", "Gold", "Diamond"]
SEGMENT_WEIGHTS     = [0.60, 0.30, 0.10]

LOCATIONS = [
    ("LOC_HCM", "TP. Hồ Chí Minh", "Nam"),
    ("LOC_HN",  "Hà Nội",          "Bắc"),
    ("LOC_DN",  "Đà Nẵng",         "Trung"),
    ("LOC_HP",  "Hải Phòng",       "Bắc"),
    ("LOC_CT",  "Cần Thơ",         "Nam"),
    ("LOC_HUE", "Huế",             "Trung"),
    ("LOC_NT",  "Nha Trang",       "Trung"),
    ("LOC_DL",  "Đà Lạt",          "Trung"),
    ("LOC_VT",  "Vũng Tàu",        "Nam"),
    ("LOC_BD",  "Bình Dương",       "Nam"),
]

TRANSACTION_TYPES = [
    ("TYP_PAYMENT",  "Payment",  True,  1.0),
    ("TYP_TRANSFER", "Transfer", False, 0.0),
    ("TYP_CASH_OUT", "Cash Out", False, 0.0),
    ("TYP_DEBIT",    "Debit",    True,  0.5),
    ("TYP_CASH_IN",  "Cash In",  False, 0.0),
]


def seed_table(engine, table_name: str, df: pd.DataFrame) -> int:
    """Insert DataFrame vào PostgreSQL table (TRUNCATE trước nếu cần)."""
    print(f"    📤 Nạp {len(df):,} rows → {table_name}...")
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(df)


def main():
    print("=" * 60)
    print("  Seed Dimension Tables – PostgreSQL")
    print("=" * 60)
    print(f"  Host : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    engine = sa.create_engine(DATABASE_URL)
    results = {}
    random.seed(42)

    # 1. dim_transaction_type
    print("📋 [1/5] Seeding dim_transaction_type...")
    df_tt = pd.DataFrame([
        {"type_id": t, "type_name": n, "is_reward_eligible": e, "reward_multiplier": m}
        for t, n, e, m in TRANSACTION_TYPES
    ])
    results["dim_transaction_type"] = seed_table(engine, "dim_transaction_type", df_tt)

    # 2. dim_location
    print("📋 [2/5] Seeding dim_location...")
    df_loc = pd.DataFrame([
        {"location_id": lid, "city": city, "region": region}
        for lid, city, region in LOCATIONS
    ])
    results["dim_location"] = seed_table(engine, "dim_location", df_loc)

    # 3. dim_date
    print("📋 [3/5] Seeding dim_date (năm 2026)...")
    current = date(2026, 1, 1)
    end     = date(2026, 12, 31)
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

    # 4. dim_users (từ CSV)
    print(f"📋 [4/5] Seeding dim_users (từ {CSV_PATH})...")
    df = pd.read_csv(CSV_PATH, usecols=["type", "nameOrig", "oldbalanceOrg"])
    df = df[df["type"] == "PAYMENT"]
    users = df.groupby("nameOrig")["oldbalanceOrg"].max().reset_index()
    users.columns = ["user_id", "account_balance"]
    base_date = date(2024, 1, 1)
    users["user_segment"]      = random.choices(USER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=len(users))
    users["registration_date"] = [(base_date + timedelta(days=random.randint(0, 730))).isoformat()
                                   for _ in range(len(users))]
    results["dim_users"] = seed_table(engine, "dim_users", users)

    # 5. dim_merchants (từ CSV)
    print(f"📋 [5/5] Seeding dim_merchants (từ {CSV_PATH})...")
    df2 = pd.read_csv(CSV_PATH, usecols=["type", "nameDest"])
    df2 = df2[df2["type"] == "PAYMENT"]
    merchants_ids = [m for m in df2["nameDest"].unique() if str(m).startswith("M")]
    df_mer = pd.DataFrame({
        "merchant_id":       merchants_ids,
        "merchant_name":     [f"Store_{m[1:8]}" for m in merchants_ids],
        "merchant_category": [random.choice(MERCHANT_CATEGORIES) for _ in merchants_ids],
    })
    results["dim_merchants"] = seed_table(engine, "dim_merchants", df_mer)

    # Tổng kết
    print(f"\n{'='*60}")
    print("✅ KẾT QUẢ SEED (PostgreSQL):")
    for table, count in results.items():
        print(f"   {table:30s} → {count:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
