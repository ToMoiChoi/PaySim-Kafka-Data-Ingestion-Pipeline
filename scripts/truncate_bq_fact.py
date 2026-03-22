"""
scripts/truncate_bq_fact.py - Xóa & tạo lại bảng fact_transactions trên BigQuery
==================================================================================
Dùng cho BigQuery Sandbox (miễn phí) vì Sandbox không cho phép DML (DELETE/TRUNCATE).
Thay vào đó: DROP TABLE rồi chạy lại bigquery_schema.py để tạo bảng mới sạch.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")

if not BQ_PROJECT_ID:
    print("BQ_PROJECT_ID chua duoc cau hinh trong .env")
    sys.exit(1)

try:
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.fact_transactions"

    # Xóa bảng cũ (Sandbox cho phép DROP TABLE)
    print(f"Dang xoa bang cu: {table_id} ...")
    client.delete_table(table_id, not_found_ok=True)
    print("Da xoa thanh cong!")

    # Tạo lại bảng với schema mới (có ip_address, không có user_id)
    print("\nDang tao lai bang voi schema moi ...")
    os.system('python warehouse/bigquery_schema.py')
    print("\nHoan tat! Bang fact_transactions da duoc tao lai sach.")

except Exception as e:
    print(f"Loi: {e}")
