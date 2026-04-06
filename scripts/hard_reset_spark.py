"""
scripts/hard_reset_spark.py - Clean up Spark environment and PostgreSQL
===========================================================================
Run this script to remove stale checkpoints and truncate fact tables.
"""

import os
import shutil
import psycopg2

print("1. Removing Spark Checkpoint...")
for base in [r"C:\tmp", r"\tmp"]:
    chk = os.path.join(base, "spark_checkpoint_dual_sink_v2")
    if os.path.exists(chk):
        try:
            shutil.rmtree(chk)
            print(f"  -> Removed: {chk}")
        except Exception as e:
            print(f"  -> Could not remove (Spark may still be running): {e}")

print("2. Cleaning up PostgreSQL staging and fact tables...")
try:
    conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_binance_trades;")
    print("  -> TRUNCATED fact_binance_trades")
    
    for i in range(5):
        cur.execute(f"DROP TABLE IF EXISTS fact_transactions_staging_{i};")
    conn.commit()
    print("  -> Dropped legacy staging tables")
except Exception as e:
    print(f"  -> PostgreSQL error: {e}")

print("Done.")
