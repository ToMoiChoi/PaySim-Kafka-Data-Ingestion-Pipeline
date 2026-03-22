import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()
# CONN = "postgresql://paysim:paysim123@localhost:5432/paysim_dw"
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

import sqlalchemy as sa

def fetch_data(query: str) -> pd.DataFrame:
    """Hàm phụ trợ để chạy query và trả về Pandas DataFrame giúp in ra bảng đẹp hơn."""
    try:
        DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        # Dùng trực tiếp chuỗi URL để Pandas tự quản lý Engine & Connection (chuẩn nhất cho pandas >= 2.0)
        df = pd.read_sql_query(query, DATABASE_URL)
        return df
    except Exception as e:
        print(f"❌ Lỗi kết nối PostgreSQL: {e}")
        return pd.DataFrame()

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
        
    print("=" * 70)
    print("🔍 KIỂM TRA DỮ LIỆU TRONG POSTGRESQL DATA WAREHOUSE")
    print("=" * 70)

    # 1. Đếm tổng số giao dịch
    print("\n1️⃣ TỔNG SỐ GIAO DỊCH TRONG FACT TABLE:")
    df_count = fetch_data("SELECT COUNT(*) AS total_transactions FROM fact_transactions;")
    if not df_count.empty:
        print(f"   ➤ Đang có {df_count['total_transactions'].iloc[0]:,} giao dịch trong hệ thống.")

    # 2. Xem 5 giao dịch mới nhất (Kèm theo thông tin User và Điểm thưởng)
    print("\n2️⃣ 5 GIAO DỊCH GẦN NHẤT (Có JOIN để xem hạng User và Điểm thưởng):")
    query_recent = """
        SELECT 
            f.transaction_id,
            f.transaction_time,
            f.type,
            f.amount,
            u.user_segment,
            f.reward_points,
            f.user_id,
            f."isFraud" as is_fraud
        FROM 
            fact_transactions f
        LEFT JOIN 
            dim_account a ON f.account_id = a.account_id
        LEFT JOIN 
            dim_users u ON f.user_id = u.user_id
        ORDER BY 
            f.transaction_time DESC 
        LIMIT 5;
    """
    df_recent = fetch_data(query_recent)
    if not df_recent.empty:
        # Căn chỉnh pandas để in ra màn hình đẹp không bị cắt ngang
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df_recent.to_string(index=False))

    # 3. Thống kê theo Hạng User
    print("\n3️⃣ THỐNG KÊ TỔNG GIAO DỊCH VÀ ĐIỂM THƯỞNG THEO HẠNG USER:")
    query_stats = """
        SELECT 
            u.user_segment,
            COUNT(f.transaction_id) as tx_count,
            SUM(f.amount) as total_volume,
            SUM(f.reward_points) as total_rewards
        FROM 
            fact_transactions f
        LEFT JOIN 
            dim_account a ON f.account_id = a.account_id
        LEFT JOIN 
            dim_users u ON f.user_id = u.user_id
        GROUP BY 
            u.user_segment
        ORDER BY 
            total_volume DESC;
    """
    df_stats = fetch_data(query_stats)
    if not df_stats.empty:
        # Format số tiền cho dễ đọc
        if 'total_volume' in df_stats.columns:
            df_stats['total_volume'] = df_stats['total_volume'].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0")
        if 'total_rewards' in df_stats.columns:
            df_stats['total_rewards'] = df_stats['total_rewards'].apply(lambda x: f"{x:,.0f} pts" if pd.notnull(x) else "0 pts")
        print(df_stats.to_string(index=False))

    print("\n" + "=" * 70)
    print("Mẹo: Nếu hệ thống chưa có dữ liệu, hãy chạy Docker, sau đó chạy Spark và Producer!")

if __name__ == "__main__":
    main()
