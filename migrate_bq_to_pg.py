import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras
from google.cloud import bigquery
from dotenv import load_dotenv

# Sửa lỗi in tiếng Việt trên console Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# 1. Tải cấu hình
load_dotenv(".env")

# Cấu hình BQ
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "ecommerce-db2025")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(GOOGLE_APPLICATION_CREDENTIALS)

# Cấu hình PG
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

def main():
    print("="*60)
    print(" BQ TO PG MIGRATION (CÓ LỌC TRÙNG LẶP)")
    print("="*60)

    # 1. Kết nối BigQuery
    try:
        bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    except Exception as e:
        print(f"[LỖI] Không thể kết nối BigQuery: {e}")
        return

    source_table = f"{BQ_PROJECT_ID}.paysim_dw.fact_binance_trades_v2"
    print(f"1. Đang tải và lọc trùng lặp từ bảng: {source_table}...")

    # Câu lệnh SQL để tải và tự động khử trùng lặp (Dùng ROW_NUMBER)
    # Lấy bản ghi mới nhất (theo trade_time) nếu có nhiều transaction_id trùng nhau
    query = f"""
        SELECT * EXCEPT(rn) 
        FROM (
            SELECT *, 
                   ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY trade_time DESC) as rn
            FROM `{source_table}`
        )
        WHERE rn = 1
    """
    
    try:
        # Tải thẳng về Pandas DataFrame
        df = bq_client.query(query).to_dataframe()
        print(f"-> Đã tải thành công {len(df)} dòng dữ liệu (đã lọc trùng) từ BigQuery.")
    except Exception as e:
        print(f"[LỖI] Lỗi khi kéo dữ liệu BQ: {e}")
        return

    if df.empty:
        print("Bảng không có dữ liệu để copy.")
        return

    # Xử lý cột null nếu có (đảm bảo phù hợp với schema Postgres)
    if 'is_anomaly' not in df.columns:
        df['is_anomaly'] = False
    
    # 2. Kết nối PostgreSQL và Insert
    print(f"2. Đang kết nối tới PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})...")
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD
        )
        pg_conn.autocommit = False
    except Exception as e:
        print(f"[LỖI] Không thể kết nối PostgreSQL: {e}")
        return

    # Chuẩn bị dữ liệu để UPSERT
    cols = [
        "transaction_id", "trade_id", "date_key", "time_key",
        "crypto_pair_key", "volume_category_key", "trade_time",
        "price", "quantity", "amount_usd", "is_buyer_maker", "is_anomaly",
        "z_score", "price_dev_pct", "wash_cluster_size",
        "buyer_order_id", "seller_order_id"
    ]
    
    # Lọc lại các cột có trong df, nếu thiếu thì cho None
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # Lấy đúng thứ tự cột
    df_insert = df[cols]
    
    df_insert = df_insert.astype(object)
    df_insert[df_insert.isna()] = None
    values = [tuple(x) for x in df_insert.to_numpy()]

    print(f"3. Đang đẩy dữ liệu vào PostgreSQL (bảng fact_binance_trades)...")
    try:
        with pg_conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO fact_binance_trades (
                    {", ".join(cols)}
                ) VALUES %s
                ON CONFLICT (transaction_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    amount_usd = EXCLUDED.amount_usd,
                    is_anomaly = EXCLUDED.is_anomaly,
                    z_score = EXCLUDED.z_score,
                    price_dev_pct = EXCLUDED.price_dev_pct,
                    wash_cluster_size = EXCLUDED.wash_cluster_size
            """
            psycopg2.extras.execute_values(cur, insert_sql, values, page_size=2000)
        pg_conn.commit()
        print("✅ ĐÃ HOÀN TẤT ĐẨY DỮ LIỆU VỀ POSTGRESQL!")
    except Exception as e:
        pg_conn.rollback()
        print(f"[LỖI] Gặp lỗi trong quá trình Insert vào PostgreSQL: {e}")
    finally:
        pg_conn.close()

if __name__ == "__main__":
    main()
