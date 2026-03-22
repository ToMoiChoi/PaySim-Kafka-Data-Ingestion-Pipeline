import psycopg2

conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
cur = conn.cursor()

try:
    cur.execute("SELECT COUNT(transaction_id) - COUNT(DISTINCT transaction_id) FROM fact_transactions_staging_1")
    dupes = cur.fetchone()[0]
    print(f"Duplicates in batch: {dupes}")
    
    cur.execute("SELECT COUNT(*) FROM fact_transactions_staging_1 WHERE transaction_time IS NULL")
    null_time = cur.fetchone()[0]
    print(f"NULL transaction_time: {null_time}")
    
    cur.execute("SELECT transaction_time, amount FROM fact_transactions_staging_1 LIMIT 5")
    print(f"Sample data: {cur.fetchall()}")
except Exception as e:
    print(e)
