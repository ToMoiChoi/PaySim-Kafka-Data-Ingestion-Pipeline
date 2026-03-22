"""
scripts/pg_to_bq_sync.py - Job đồng bộ dữ liệu từ PostgreSQL lên BigQuery
==========================================================================
Dùng cho luồng Batch Processing (Chạy hàng giờ hoặc hàng ngày).
Đọc data từ PostgreSQL và Load thẳng lên BigQuery.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

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

def sync_data():
    print(f"🔄 Đang tải toàn bộ dữ liệu từ PostgreSQL ({PG_DB})...")
    DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    
    # 1. Tải từ PostgreSQL
    df = pd.read_sql_query("SELECT * FROM fact_transactions", DATABASE_URL)
    
    if df.empty:
        print("⚠️ Không có dữ liệu trong PostgreSQL để đồng bộ!")
        return

    # Định dạng lại dữ liệu cho BigQuery (Pandas xử lý kiểu dữ liệu Decimal thành float và Datetime)
    for col in df.select_dtypes(include=['object']):
        # Ignore UUID and string columns for decimal conversion
        pass
        
    df['amount'] = df['amount'].astype(float)
    df['oldbalanceOrg'] = df['oldbalanceOrg'].astype(float)
    df['newbalanceOrig'] = df['newbalanceOrig'].astype(float)
    df['oldbalanceDest'] = df['oldbalanceDest'].astype(float)
    df['newbalanceDest'] = df['newbalanceDest'].astype(float)
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])

    print(f"✅ Đã tải {len(df):,} dòng từ PostgreSQL.")
    print(f"☁️ Đang Upload lên BigQuery ({BQ_PROJECT_ID}.{BQ_DATASET}.fact_transactions)...")
    
    # 2. Upload lên BigQuery (Sử dụng google-cloud-bigquery/pandas-gbq)
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.fact_transactions"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Chế độ ghi đè toàn bộ (Batch)
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Đợi hoàn tất
        
        print(f"🎉 HOÀN TẤT! Đã đồng bộ {len(df):,} dòng lên BigQuery Sandbox thành công!")
    except Exception as e:
        print(f"❌ LỖI Upload lên BigQuery: {e}")

if __name__ == "__main__":
    sync_data()
