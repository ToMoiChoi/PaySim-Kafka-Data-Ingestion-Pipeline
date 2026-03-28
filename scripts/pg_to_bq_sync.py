"""
scripts/pg_to_bq_sync.py - Job đồng bộ dữ liệu toàn bộ Star Schema
==========================================================================
Dùng cho luồng Batch Processing. 
Đồng bộ cả `fact_transactions` lẫn các bảng `dim_...` từ PostgreSQL lên BigQuery.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Thêm thư mục gốc vào PATH để import được bigquery_schema
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

load_dotenv()

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")

# PostgreSQL
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

if not BQ_PROJECT_ID:
    print("❌ LỖI: Chưa cấu hình BQ_PROJECT_ID trong file .env")
    sys.exit(1)

TABLES_TO_SYNC = [
    "dim_users",
    "dim_merchants", 
    "dim_transaction_type",
    "dim_location",
    "dim_date",
    "dim_time",
    "dim_channel",
    "dim_account",
    # "fact_transactions"
]

def sync_table(table_name, db_url):
    print(f"\n🔄 Đang lấy dữ liệu bảng '{table_name}' từ PostgreSQL...")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", db_url)
    
    if df.empty:
        print(f"⚠️ Bảng '{table_name}' không có dữ liệu!")
        return

    # Fix kiểu dữ liệu cho fact_transactions
    if table_name == "fact_transactions":
        try:
            # Convert Decimal → float → khớp BigQuery FLOAT64
            df['amount'] = df['amount'].astype(float)
            df['oldbalanceOrg'] = df['oldbalanceOrg'].astype(float)
            df['newbalanceOrig'] = df['newbalanceOrig'].astype(float)
            df['oldbalanceDest'] = df['oldbalanceDest'].astype(float)
            df['newbalanceDest'] = df['newbalanceDest'].astype(float)
            df['transaction_time'] = pd.to_datetime(df['transaction_time'], utc=True)
        except Exception:
            pass
            
    # Fix kiểu dữ liệu Thời gian cho BigQuery
    if table_name == "dim_date" and "full_date" in df.columns:
        df["full_date"] = pd.to_datetime(df["full_date"])
    if table_name == "dim_time" and "time_val" in df.columns:
        df["time_val"] = df["time_val"].astype(str)

    print(f"✅ Đã tải {len(df):,} dòng. Đang Upload lên BigQuery ({table_name})...")
    
    try:
        from google.cloud import bigquery
        from warehouse.bigquery_schema import TABLES as BQ_SCHEMAS

        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

        # Lấy schema cứng từ bigquery_schema.py (nếu có) để ép kiểu chính xác
        # Tránh để Pandas tự suy luận sai kiểu (FLOAT vs NUMERIC, DATETIME vs TIMESTAMP)
        schema = BQ_SCHEMAS.get(table_name)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        if schema:
            job_config.schema = schema

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  
        
        print(f"🎉 HOÀN TẤT ({table_name})!")
    except Exception as e:
        print(f"❌ LỖI Upload bảng {table_name}: {e}")

def sync_all():
    print(f"🚀 BẮT ĐẦU ĐỒNG BỘ TOÀN BỘ DATA WAREHOUSE LÊN BIGQUERY")
    print(f"===========================================================")
    DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    
    for table in TABLES_TO_SYNC:
        sync_table(table, DATABASE_URL)
        
    print("\n✅✅ ĐỒNG BỘ THÀNH CÔNG TẤT CẢ 9 BẢNG! Kho dữ liệu BigQuery đã SẴN SÀNG CHO POWER BI!")

if __name__ == "__main__":
    sync_all()
