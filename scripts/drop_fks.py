import psycopg2

try:
    conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
    cur = conn.cursor()
    
    cur.execute("""
        SELECT constraint_name 
        FROM information_schema.table_constraints 
        WHERE table_name = 'fact_transactions' AND constraint_type = 'FOREIGN KEY'
    """)
    constraints = [row[0] for row in cur.fetchall()]
    
    for c in constraints:
        print(f"Dropping constraint: {c}")
        cur.execute(f"ALTER TABLE fact_transactions DROP CONSTRAINT IF EXISTS {c}")
        
    conn.commit()
    print("All foreign key constraints dropped successfully. Spark can now ingest streaming data freely!")
except Exception as e:
    print(f"Error: {e}")
