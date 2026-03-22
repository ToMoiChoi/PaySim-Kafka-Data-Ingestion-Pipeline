import psycopg2

conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
cur = conn.cursor()

def get_columns(table_name):
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position")
    return [row[0] for row in cur.fetchall()]

fact_cols = get_columns("fact_transactions")
staging_cols = get_columns("fact_transactions_staging_0")

print(f"Fact columns ({len(fact_cols)}):")
print(fact_cols)
print(f"Staging columns ({len(staging_cols)}):")
print(staging_cols)

if fact_cols != staging_cols:
    print("MISMATCH DETECTED!")
else:
    print("MATCH!")
