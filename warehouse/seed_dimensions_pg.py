"""
warehouse/seed_dimensions_pg.py - Seed dữ liệu mẫu vào PostgreSQL Dimension tables
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
ACCOUNT_TYPES       = ["Debit Card", "Credit Card", "E-Wallet", "Bank Account"]
ACCOUNT_STATUSES    = ["Active", "Locked", "Pending"]
ACCOUNT_STAT_W      = [0.90, 0.05, 0.05]

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

CHANNELS = [
    ("CHN_APP_IOS", "Mobile App - iOS", "iOS"),
    ("CHN_APP_AND", "Mobile App - Android", "Android"),
    ("CHN_WEB", "Web Portal", "Windows/macOS"),
    ("CHN_ATM", "ATM Machine", "Embedded"),
    ("CHN_POS", "POS Terminal", "Embedded"),
]


def seed_table(engine, table_name: str, df: pd.DataFrame) -> int:
    """Insert DataFrame vào PostgreSQL table (TRUNCATE trước nếu cần)."""
    print(f"    [SEND] Nap {len(df):,} rows -> {table_name}...")
    with engine.connect() as conn:
        try:
            conn.execute(sa.text(f"TRUNCATE TABLE {table_name} CASCADE;"))
            conn.commit()
        except Exception as e:
            print(f"      [WARN] Could not truncate: {e}")
            conn.rollback()
            
        df.to_sql(table_name, conn, if_exists="append", index=False, method="multi", chunksize=1000)
        conn.commit()
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
    df_tt = pd.DataFrame([
        {"type_id": t, "type_name": n, "is_reward_eligible": e, "reward_multiplier": m}
        for t, n, e, m in TRANSACTION_TYPES
    ])
    results["dim_transaction_type"] = seed_table(engine, "dim_transaction_type", df_tt)

    # 2. dim_location
    print("[STEP] [2/8] Seeding dim_location...")
    df_loc = pd.DataFrame([
        {"location_id": lid, "city": city, "region": region}
        for lid, city, region in LOCATIONS
    ])
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
    df_channel = pd.DataFrame([
        {"channel_id": cid, "channel_name": cname, "device_os": os_ver}
        for cid, cname, os_ver in CHANNELS
    ])
    results["dim_channel"] = seed_table(engine, "dim_channel", df_channel)

    # 6. dim_users (từ CSV)
    print(f"[STEP] [6/8] Seeding dim_users (từ {CSV_PATH})...")
    df = pd.read_csv(CSV_PATH, usecols=["type", "nameOrig", "oldbalanceOrg"])
    df = df[df["type"] == "PAYMENT"]
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
    print("[DONE] KẾT QUẢ SEED (PostgreSQL):")
    for table, count in results.items():
        print(f"   {table:30s} -> {count:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
