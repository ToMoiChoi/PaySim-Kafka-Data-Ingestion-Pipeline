import os
from pathlib import Path

# Check Spark Checkpoints
paths_to_check = [
    r"C:\tmp",
    r"C:\Users\Admin\OneDrive\Desktop\Self_Project\KLTN_2\tmp",
    r"\tmp"
]

print("--- Checkpoint Paths ---")
for p in paths_to_check:
    target = os.path.join(p, "spark_checkpoint_dual_sink_v2")
    if os.path.exists(target):
        print(f"FOUND: {target}")
        offset_file = os.path.join(target, "offsets")
        if os.path.exists(offset_file):
            print(f"  Offsets files: {len(os.listdir(offset_file))}")
        else:
            print("  No offsets found.")

import psycopg2
try:
    conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
    tables = [t[0] for t in cur.fetchall()]
    
    print("\n--- PostgreSQL Tables ---")
    stg = [t for t in tables if "fact_transactions_staging" in t]
    print(f"Staging tables found: {stg}")
    for s in stg:
        cur.execute(f"SELECT COUNT(*) FROM {s}")
        print(f"{s} count: {cur.fetchone()[0]}")
    
    cur.execute("SELECT COUNT(*) FROM fact_transactions")
    print(f"fact_transactions count: {cur.fetchone()[0]}")
    
except Exception as e:
    print(f"Postgres Error: {e}")
