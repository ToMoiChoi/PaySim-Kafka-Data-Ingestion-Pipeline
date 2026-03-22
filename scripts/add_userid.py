import psycopg2

try:
    conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
    cur = conn.cursor()
    
    # 1. Thêm user_id vào fact_transactions nếu chưa có
    cur.execute("""
        ALTER TABLE fact_transactions 
        ADD COLUMN IF NOT EXISTS user_id VARCHAR(50);
    """)
    print("Added user_id to fact_transactions")
    
    # 2. Xóa các staging tables cũ vì schema đã thay đổi
    for i in range(5):
        cur.execute(f"DROP TABLE IF EXISTS fact_transactions_staging_{i};")
    print("Dropped old staging tables")
    
    conn.commit()
    print("Hoàn tất cập nhật schema PostgreSQL!")
except Exception as e:
    print(f"Lỗi: {e}")
