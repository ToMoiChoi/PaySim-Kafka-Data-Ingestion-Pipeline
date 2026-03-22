"""
scripts/hard_reset_spark.py - Script làm sạch môi trường Spark & PostgreSQL
===========================================================================
Chạy kịch bản này để dọn dẹp các truy vấn đang bị treo do Spark đẩy 2.3 triệu dòng trực tiếp.
"""

import os
import shutil
import psycopg2

print("1. Xoa Spark Checkpoint...")
for base in [r"C:\tmp", r"\tmp"]:
    chk = os.path.join(base, "spark_checkpoint_dual_sink_v2")
    if os.path.exists(chk):
        try:
            shutil.rmtree(chk)
            print(f"  -> Da xoa: {chk}")
        except Exception as e:
            print(f"  -> Khong the xoa (Co the Spark van dang chay): {e}")

print("2. Don dep PostgreSQL Staging & Fact Tables tren Local...")
try:
    conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_transactions;")
    print("  -> Da TRUNCATE fact_transactions")
    
    for i in range(5):
        cur.execute(f"DROP TABLE IF EXISTS fact_transactions_staging_{i};")
    conn.commit()
    print("  -> Da DROP cac staging tables cu")
except Exception as e:
    print(f"  -> Loi PostgreSQL: {e}")

print("Hoan tat!")
