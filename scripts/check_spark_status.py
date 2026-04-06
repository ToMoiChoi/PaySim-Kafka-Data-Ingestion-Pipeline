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
    target = os.path.join(p, "spark_checkpoint_binance_v4")
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
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"{t} count: {cur.fetchone()[0]}")
    
except Exception as e:
    print(f"Postgres Error: {e}")
