"""
BigQuery Star Schema – Tạo Dataset & 6 Bảng
=============================================
Chạy script này một lần để khởi tạo Data Warehouse trên BigQuery:
  - 1 Fact table : fact_transactions
  - 5 Dim tables : dim_users, dim_merchants, dim_transaction_type, dim_location, dim_date

Yêu cầu:
  - Biến môi trường GOOGLE_APPLICATION_CREDENTIALS trỏ tới file Service Account JSON.
  - Biến BQ_PROJECT_ID và BQ_DATASET trong file .env.
"""

import os
from dotenv import load_dotenv
from google.cloud import bigquery

# ─── Load Configuration ──────────────────────────────────────────
load_dotenv()
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET", "paysim_dw")

if not PROJECT_ID:
    raise ValueError("❌ Thiếu BQ_PROJECT_ID trong .env")

FULL_DATASET = f"{PROJECT_ID}.{DATASET_ID}"


# ─── Table Schemas ───────────────────────────────────────────────

TABLES = {
    # ─── FACT TABLE ──────────────────────────────────────────────
    "fact_transactions": [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED",
                             description="Mã giao dịch duy nhất (UUID)"),
        bigquery.SchemaField("user_id", "STRING", mode="NULLABLE",
                             description="FK → dim_users"),
        bigquery.SchemaField("merchant_id", "STRING", mode="NULLABLE",
                             description="FK → dim_merchants"),
        bigquery.SchemaField("type_id", "STRING", mode="NULLABLE",
                             description="FK → dim_transaction_type"),
        bigquery.SchemaField("location_id", "STRING", mode="NULLABLE",
                             description="FK → dim_location"),
        bigquery.SchemaField("date_key", "INT64", mode="NULLABLE",
                             description="FK → dim_date (YYYYMMDD)"),
        bigquery.SchemaField("transaction_time", "TIMESTAMP", mode="NULLABLE",
                             description="Thời gian xử lý giao dịch"),
        bigquery.SchemaField("amount", "NUMERIC", mode="NULLABLE",
                             description="Giá trị giao dịch"),
        bigquery.SchemaField("reward_points", "INT64", mode="NULLABLE",
                             description="Điểm thưởng (amount × 0.01)"),
    ],

    # ─── DIMENSION: USERS ────────────────────────────────────────
    "dim_users": [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED",
                             description="Mã khách hàng (nameOrig)"),
        bigquery.SchemaField("account_balance", "NUMERIC", mode="NULLABLE",
                             description="Số dư tài khoản hiện tại"),
        bigquery.SchemaField("user_segment", "STRING", mode="NULLABLE",
                             description="Phân khúc: Standard / Gold / Diamond"),
        bigquery.SchemaField("registration_date", "DATE", mode="NULLABLE",
                             description="Ngày đăng ký tài khoản"),
    ],

    # ─── DIMENSION: MERCHANTS ────────────────────────────────────
    "dim_merchants": [
        bigquery.SchemaField("merchant_id", "STRING", mode="REQUIRED",
                             description="Mã cửa hàng (nameDest)"),
        bigquery.SchemaField("merchant_name", "STRING", mode="NULLABLE",
                             description="Tên cửa hàng / thương hiệu"),
        bigquery.SchemaField("merchant_category", "STRING", mode="NULLABLE",
                             description="Ngành hàng: F&B, E-commerce, Bill Payment..."),
    ],

    # ─── DIMENSION: TRANSACTION TYPE ─────────────────────────────
    "dim_transaction_type": [
        bigquery.SchemaField("type_id", "STRING", mode="REQUIRED",
                             description="Mã loại giao dịch"),
        bigquery.SchemaField("type_name", "STRING", mode="NULLABLE",
                             description="Tên hiển thị loại giao dịch"),
        bigquery.SchemaField("is_reward_eligible", "BOOL", mode="NULLABLE",
                             description="TRUE nếu được phép cộng điểm"),
        bigquery.SchemaField("reward_multiplier", "FLOAT64", mode="NULLABLE",
                             description="Hệ số nhân điểm thưởng"),
    ],

    # ─── DIMENSION: LOCATION ─────────────────────────────────────
    "dim_location": [
        bigquery.SchemaField("location_id", "STRING", mode="REQUIRED",
                             description="Mã khu vực"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE",
                             description="Thành phố / Tỉnh thành"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE",
                             description="Vùng miền: Bắc / Trung / Nam"),
    ],

    # ─── DIMENSION: DATE ─────────────────────────────────────────
    "dim_date": [
        bigquery.SchemaField("date_key", "INT64", mode="REQUIRED",
                             description="Khóa định dạng YYYYMMDD"),
        bigquery.SchemaField("full_date", "DATE", mode="NULLABLE",
                             description="Ngày tháng năm chuẩn"),
        bigquery.SchemaField("day_of_week", "INT64", mode="NULLABLE",
                             description="Thứ trong tuần (1=Mon … 7=Sun)"),
        bigquery.SchemaField("is_weekend", "BOOL", mode="NULLABLE",
                             description="TRUE nếu Thứ 7 hoặc CN"),
        bigquery.SchemaField("month", "INT64", mode="NULLABLE",
                             description="Tháng (1-12)"),
        bigquery.SchemaField("quarter", "INT64", mode="NULLABLE",
                             description="Quý (1-4)"),
        bigquery.SchemaField("year", "INT64", mode="NULLABLE",
                             description="Năm"),
    ],
}


def create_dataset(client: bigquery.Client) -> None:
    """Tạo dataset nếu chưa tồn tại."""
    dataset_ref = bigquery.Dataset(FULL_DATASET)
    dataset_ref.location = "US"
    dataset_ref.description = "PaySim Data Warehouse – Star Schema (KLTN)"

    dataset = client.create_dataset(dataset_ref, exists_ok=True)
    print(f"✅ Dataset '{dataset.dataset_id}' sẵn sàng (location: {dataset.location})")


def create_tables(client: bigquery.Client) -> None:
    """Tạo tất cả bảng theo schema đã định nghĩa."""
    for table_name, schema in TABLES.items():
        table_id = f"{FULL_DATASET}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)

        # Fact table dùng partitioning theo ngày để tối ưu query
        if table_name == "fact_transactions":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="transaction_time",
            )
            table.clustering_fields = ["user_id", "merchant_id"]
            table.description = "Bảng Fact – Lịch sử giao dịch & điểm thưởng"
        elif table_name == "dim_users":
            table.description = "Dimension – Thông tin khách hàng"
        elif table_name == "dim_merchants":
            table.description = "Dimension – Thông tin cửa hàng"
        elif table_name == "dim_transaction_type":
            table.description = "Dimension – Cấu hình loại giao dịch & luật thưởng"
        elif table_name == "dim_location":
            table.description = "Dimension – Thông tin địa lý"
        elif table_name == "dim_date":
            table.description = "Dimension – Trục thời gian chuẩn"

        table = client.create_table(table, exists_ok=True)
        print(f"  📋 Bảng '{table_name}' đã tạo ({len(schema)} cột)")


def main():
    print("=" * 60)
    print("  BigQuery Star Schema – Khởi tạo Data Warehouse")
    print("=" * 60)
    print(f"  Project : {PROJECT_ID}")
    print(f"  Dataset : {DATASET_ID}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    # 1. Tạo Dataset
    create_dataset(client)

    # 2. Tạo 6 bảng
    print(f"\n📦 Tạo {len(TABLES)} bảng Star Schema:")
    create_tables(client)

    print(f"\n{'='*60}")
    print(f"✅ HOÀN TẤT! Đã tạo {len(TABLES)} bảng trong dataset '{DATASET_ID}'")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
