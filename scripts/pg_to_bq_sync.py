"""
scripts/pg_to_bq_sync.py - Full Star Schema data synchronisation job
==========================================================================
Used for the Batch Processing workflow.
Syncs all tables (fact_binance_trades and dim_... tables) from PostgreSQL to BigQuery.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Add root directory to PATH so we can import bigquery_schema
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
    print("[ERROR] BQ_PROJECT_ID is not configured in .env")
    sys.exit(1)

TABLES_TO_SYNC = [
    "dim_volume_category",
    "dim_crypto_pair",
    "dim_exchange_rate",
    "dim_date",
    "dim_time",
    "fact_binance_trades",
    "fact_pipeline_latency"
]                                                                                     

def sync_table(table_name, db_url):
    print(f"\n[SYNC] Fetching table '{table_name}' from PostgreSQL...")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", db_url)
    
    if df.empty:
        print(f"[WARN] Table '{table_name}' has no data.")
        return

    # Fix data types for fact_binance_trades
    if table_name == "fact_binance_trades":
        try:
            # Convert Decimal -> float to match BigQuery FLOAT64
            df['price'] = df['price'].astype(float)
            df['quantity'] = df['quantity'].astype(float)
            df['amount_usd'] = df['amount_usd'].astype(float)
            if 'z_score' in df.columns: df['z_score'] = df['z_score'].astype(float)
            if 'price_dev_pct' in df.columns: df['price_dev_pct'] = df['price_dev_pct'].astype(float)
            
            # Enforce INT64 for BigQuery (use pandas Nullable Integer)
            int_cols = ["trade_id", "date_key", "time_key", "crypto_pair_key", 
                        "volume_category_key", "wash_cluster_size", 
                        "buyer_order_id", "seller_order_id"]
            for c in int_cols:
                if c in df.columns:
                    df[c] = df[c].astype("Int64")
                    
            df['trade_time'] = pd.to_datetime(df['trade_time'], utc=True)
        except Exception as e:
            print(f"[WARN] Failed to convert types in {table_name}: {e}")
    # Fix date/time types for BigQuery compatibility
    if table_name == "dim_date" and "full_date" in df.columns:
        df["full_date"] = pd.to_datetime(df["full_date"])
    if table_name == "dim_time" and "time_val" in df.columns:
        df["time_val"] = df["time_val"].astype(str)
    if table_name == "fact_pipeline_latency" and "recorded_at" in df.columns:
        df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)

    print(f"[OK] Loaded {len(df):,} rows. Uploading to BigQuery ({table_name})...")
    
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

        print(f"[SYNC] Deleting existing table {table_id} if it exists...")
        client.delete_table(table_id, not_found_ok=True)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  
        
        print(f"[DONE] Upload complete ({table_name}).")
    except Exception as e:
        print(f"[ERROR] Failed to upload table {table_name}: {e}")

def sync_all(target_table=None):
    if target_table:
        print(f"[START] SYNCHRONISING {target_table.upper()} TO BIGQUERY")
    else:
        print(f"[START] SYNCHRONISING FULL DATA WAREHOUSE TO BIGQUERY")
    print(f"===========================================================")
    DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    
    tables = [target_table] if target_table else TABLES_TO_SYNC
    
    for table in tables:
        sync_table(table, DATABASE_URL)
        
    print("\n[DONE] All tables synchronised successfully. BigQuery warehouse is READY FOR POWER BI.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync PostgreSQL to BigQuery")
    parser.add_argument("--table", type=str, help="Specific table to sync (e.g. fact_binance_trades)")
    args = parser.parse_args()
    
    sync_all(args.table)
