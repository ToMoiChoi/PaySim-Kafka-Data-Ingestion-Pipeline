import psycopg2

conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
cur = conn.cursor()
cur.execute("UPDATE fact_transactions SET user_id = REPLACE(account_id, '_ACC1', '') WHERE user_id IS NULL")
print(f"Fixed {cur.rowcount} rows in PostgreSQL!")
conn.commit()
