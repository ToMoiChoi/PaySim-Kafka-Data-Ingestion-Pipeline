"""
scripts/truncate_bq_fact.py - Drop and recreate fact table on BigQuery
==================================================================================
For BigQuery Sandbox (free tier) which does not allow DML (DELETE/TRUNCATE).
Instead: DROP TABLE then re-run bigquery_schema.py to create a clean table.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")

if not BQ_PROJECT_ID:
    print("[ERROR] BQ_PROJECT_ID is not configured in .env")
    sys.exit(1)

try:
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.fact_binance_trades"

    # Drop old table (Sandbox allows DROP TABLE)
    print(f"Dropping old table: {table_id} ...")
    client.delete_table(table_id, not_found_ok=True)
    print("Table dropped successfully.")

    # Recreate table with updated schema
    print("\nRecreating table with current schema ...")
    os.system('python warehouse/bigquery_schema.py')
    print("\nDone. fact_binance_trades has been recreated.")

except Exception as e:
    print(f"Error: {e}")
