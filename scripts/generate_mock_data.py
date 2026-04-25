"""
scripts/generate_mock_data.py - Sinh dữ liệu ảo cho tháng 2 và tháng 3 năm 2026.
Tuân thủ Kimball Star Schema (Surrogate Keys).
"""

import os
import sys
import random
import time
import hashlib
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

NUM_ROWS_PER_DAY = 5000
BATCH_SIZE = 5000

# Khoảng thời gian: 01/02/2026 - 31/03/2026
START_DATE = datetime(2026, 2, 1)
END_DATE = datetime(2026, 3, 31)

BASE_PRICES = {
    1: 60000.0, # BTC
    2: 3000.0,  # ETH
    3: 600.0,   # BNB
    4: 150.0,   # SOL
    5: 0.6      # XRP
}

def generate_and_insert():
    total_days = (END_DATE - START_DATE).days + 1
    print(f"[MOCK DATA] Bắt đầu sinh {NUM_ROWS_PER_DAY} dòng/ngày cho T2 & T3/2026 ({total_days} ngày)...")
    
    trade_id_start = int(time.time() * 1000)
    current_date = START_DATE
    
    conn = None
    try:
        print("[MOCK DATA] Đang kết nối tới PostgreSQL...")
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD
        )
        cur = conn.cursor()
        
        insert_sql = """
            INSERT INTO fact_binance_trades (
                transaction_id, trade_id, date_key, time_key,
                crypto_pair_key, volume_category_key, trade_time,
                price, quantity, amount_usd, is_buyer_maker,
                is_anomaly, z_score, price_dev_pct, wash_cluster_size,
                buyer_order_id, seller_order_id
            ) VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
        """
        
        trade_count = 0
        while current_date <= END_DATE:
            rows = []
            print(f"  -> Đang sinh và đẩy dữ liệu ngày {current_date.strftime('%Y-%m-%d')}...")
            
            for _ in range(NUM_ROWS_PER_DAY):
                random_seconds = random.randint(0, 86399)
                trade_time = current_date + timedelta(seconds=random_seconds)
                
                date_key = int(trade_time.strftime("%Y%m%d"))
                time_key = int(trade_time.strftime("%H%M"))
                
                crypto_pair_key = random.randint(1, 5)
                
                base_price = BASE_PRICES[crypto_pair_key]
                price_variance = base_price * 0.05
                price = round(base_price + random.uniform(-price_variance, price_variance), 4)
                
                if crypto_pair_key == 1:
                    quantity = round(random.uniform(0.001, 5.0), 6)
                elif crypto_pair_key == 2:
                    quantity = round(random.uniform(0.01, 50.0), 6)
                else:
                    quantity = round(random.uniform(1.0, 5000.0), 4)
                    
                amount_usd = round(price * quantity, 2)
                
                if amount_usd >= 1_000_000:
                    volume_category_key = 4
                elif amount_usd >= 100_000:
                    volume_category_key = 3
                elif amount_usd >= 10_000:
                    volume_category_key = 2
                else:
                    volume_category_key = 1
                    
                trade_id = trade_id_start + trade_count
                trade_count += 1
                raw_str = f"binance.trade.{trade_id}"
                transaction_id = hashlib.sha256(raw_str.encode()).hexdigest()
                
                is_buyer_maker = random.choice([True, False])
                is_anomaly = (random.random() < 0.02) 
                z_score = round(random.uniform(3.1, 5.0) if is_anomaly else random.uniform(0.1, 2.5), 4)
                price_dev_pct = round(random.uniform(0.011, 0.05) if is_anomaly else random.uniform(0.001, 0.009), 4)
                wash_cluster_size = random.randint(4, 10) if is_anomaly else random.randint(1, 3)
                
                buyer_order_id = random.randint(1000000, 9999999)
                seller_order_id = random.randint(1000000, 9999999)
                
                rows.append((
                    transaction_id, trade_id, date_key, time_key,
                    crypto_pair_key, volume_category_key, trade_time,
                    price, quantity, amount_usd, is_buyer_maker,
                    is_anomaly, z_score, price_dev_pct, wash_cluster_size,
                    buyer_order_id, seller_order_id
                ))
            
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=BATCH_SIZE)
            conn.commit()
            
            current_date += timedelta(days=1)
            
        print("[MOCK DATA] Hoàn tất toàn bộ!")
        
    except Exception as e:
        print(f"[LỖI] {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    generate_and_insert()
