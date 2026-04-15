"""
warehouse/bigquery_schema.py - Create Star Schema on Google BigQuery
==============================================================
Run this script to set up the new schema (Native Crypto) on Google BigQuery.
- Creates 1 Fact table (fact_binance_trades)
- Creates 5 Dimension tables (dim_date, dim_time, dim_volume_category, dim_crypto_pair, dim_exchange_rate)

Kimball Methodology:
  - All Dimension tables use INTEGER Surrogate Keys (SK) as Primary Keys.
  - Fact table references Dimensions via INTEGER keys only.
  - Natural keys are stored as descriptive attributes, NOT as PKs.
"""

import os
from dotenv import load_dotenv
from google.cloud import bigquery

# --- 1. Load config ---
load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET", "paysim_dw")

def main():
    if not PROJECT_ID:
        print("[ERROR] BQ_PROJECT_ID is not configured in .env")
        return

    print("=" * 65)
    print("  BigQuery Star Schema Init - Kimball Surrogate Keys")
    print("=" * 65)
    print(f"  Project ID : {PROJECT_ID}")
    print(f"  Dataset    : {DATASET_ID}")
    print("-" * 65)

    try:
        # Load API
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        
        try:
            client.delete_table(f"{dataset_ref}.fact_transactions", not_found_ok=True)
            client.delete_table(f"{dataset_ref}.fact_binance_trades", not_found_ok=True)
            print("  Removed old tables to clean up schema.")
        except Exception as e:
            pass
            
        try:
            dataset = client.create_dataset(dataset, exists_ok=True)
            print(f"  [OK] Dataset ready: {dataset_ref}")
        except Exception as e:
            print(f"  [WARN] Error creating dataset: {e}")

        # --- Schema Definitions (Kimball-compliant Surrogate Keys) ---
        
        TABLES = {
            "dim_date": [
                bigquery.SchemaField("date_key", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("full_date", "DATE"),
                bigquery.SchemaField("day_of_week", "INT64"),
                bigquery.SchemaField("is_weekend", "BOOLEAN"),
                bigquery.SchemaField("month", "INT64"),
                bigquery.SchemaField("quarter", "INT64"),
                bigquery.SchemaField("year", "INT64"),
            ],
            "dim_time": [
                bigquery.SchemaField("time_key", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("hour", "INT64"),
                bigquery.SchemaField("minute", "INT64"),
                bigquery.SchemaField("time_of_day", "STRING"),
                bigquery.SchemaField("is_business_hour", "BOOLEAN"),
            ],
            # Kimball: Surrogate Key (volume_category_key) as PK
            "dim_volume_category": [
                bigquery.SchemaField("volume_category_key", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("volume_category", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("min_usd", "FLOAT64"),
                bigquery.SchemaField("max_usd", "FLOAT64"),
            ],
            # Kimball: Surrogate Key (crypto_pair_key) as PK
            "dim_crypto_pair": [
                bigquery.SchemaField("crypto_pair_key", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("crypto_symbol", "STRING"),
                bigquery.SchemaField("base_asset", "STRING"),
                bigquery.SchemaField("quote_asset", "STRING"),
                bigquery.SchemaField("pair_name", "STRING"),
            ],
            "dim_exchange_rate": [
                bigquery.SchemaField("date_key", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("currency_code", "STRING"),
                bigquery.SchemaField("vnd_rate", "FLOAT64"),
            ],
            # Fact table: All FKs are INTEGER surrogate keys
            "fact_binance_trades": [
                bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("trade_id", "INT64"),
                bigquery.SchemaField("date_key", "INT64"),
                bigquery.SchemaField("time_key", "INT64"),
                bigquery.SchemaField("crypto_pair_key", "INT64"),
                bigquery.SchemaField("volume_category_key", "INT64"),
                bigquery.SchemaField("trade_time", "TIMESTAMP"),
                bigquery.SchemaField("price", "FLOAT64"),
                bigquery.SchemaField("quantity", "FLOAT64"),
                bigquery.SchemaField("amount_usd", "FLOAT64"),
                bigquery.SchemaField("is_buyer_maker", "BOOLEAN"),
                bigquery.SchemaField("is_anomaly", "BOOLEAN"),
                bigquery.SchemaField("buyer_order_id", "INT64"),
                bigquery.SchemaField("seller_order_id", "INT64"),
            ]
        }

        for table_name, schema in TABLES.items():
            table_id = f"{dataset_ref}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            # Special configuration for the Fact table
            if table_name == "fact_binance_trades":
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="trade_time"
                )
                table.clustering_fields = ["crypto_pair_key", "volume_category_key"]

            print(f"  Creating/updating table {table_name}...")
            client.create_table(table, exists_ok=True)

        print("\n[DONE] All BigQuery Star Schema tables are READY (Kimball-compliant).")

    except Exception as e:
        print(f"\n[ERROR] Failed during BQ schema setup: {e}")


if __name__ == "__main__":
    main()
