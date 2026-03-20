"""
Live Producer - Mô phỏng giao dịch Real-Time
================================================
Kịch bản: Bắn dữ liệu liên tục (1-2 giao dịch / giây) vào Kafka
để quan sát luồng Spark Streaming & PostgreSQL xử lý theo thời gian thực.
"""

import json
import os
import time
import uuid
import random
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
import psycopg2

# ─── Load .env ───────────────────────────────────────────────────
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events")

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def generate_mock_transaction(valid_users, valid_merchants, valid_accounts):
    """Hàm ảo tạo 1 giao dịch hợp lệ với schema fact_transactions hiện tại"""
    tx_type = random.choice(["PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT", "DEBIT"])
    amount = round(random.uniform(10.0, 50000.0), 2)

    # Lấy User, Merchant, Account hợp lệ từ Database để tránh lỗi Foreign Key
    nameOrig = random.choice(valid_users) if valid_users else f"U{random.randint(10000, 99999)}"
    nameDest = random.choice(valid_merchants) if valid_merchants else f"M{random.randint(10000, 99999)}"
    account_id = random.choice(valid_accounts) if valid_accounts else f"{nameOrig}_ACC1"

    # Logic tính toán balance ảo
    oldbalanceOrg = round(random.uniform(amount, amount * 10), 2)
    newbalanceOrig = round(oldbalanceOrg - amount, 2)

    oldbalanceDest = round(random.uniform(0.0, 10000.0), 2)
    newbalanceDest = round(oldbalanceDest + amount, 2)

    # Channel và IP
    channels = ["CHN_APP_IOS", "CHN_APP_AND", "CHN_WEB", "CHN_ATM", "CHN_POS"]
    channel_id = random.choice(channels)
    ip_address = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"

    # Random gian lận (~1%)
    isFraud = 1 if random.random() < 0.01 else 0
    isFlaggedFraud = 1 if amount > 40000 and isFraud == 1 else 0

    return {
        "step": 1,
        "type": tx_type,
        "amount": amount,
        "nameOrig": nameOrig,
        "oldbalanceOrg": oldbalanceOrg,
        "newbalanceOrig": newbalanceOrig,
        "nameDest": nameDest,
        "oldbalanceDest": oldbalanceDest,
        "newbalanceDest": newbalanceDest,
        "isFraud": isFraud,
        "isFlaggedFraud": isFlaggedFraud,
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": account_id,
        "channel_id": channel_id,
        "ip_address": ip_address
    }


def get_valid_ids():
    """Lấy danh sách ID hợp lệ từ PostgreSQL (user, merchant, account)."""
    users = []
    merchants = []
    accounts = []
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD
        )
        with conn.cursor() as cur:
            # Lấy 500 users ngẫu nhiên (đủ lớn để match khi Spark join dim_users)
            cur.execute("SELECT user_id FROM dim_users ORDER BY RANDOM() LIMIT 500;")
            users = [r[0] for r in cur.fetchall()]

            cur.execute("SELECT merchant_id FROM dim_merchants ORDER BY RANDOM() LIMIT 500;")
            merchants = [r[0] for r in cur.fetchall()]

            # Lấy account_id thật từ DB thay vì tự ghép chuỗi
            cur.execute("SELECT account_id FROM dim_account ORDER BY RANDOM() LIMIT 500;")
            accounts = [r[0] for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        print(f"⚠️ Không thể lấy IDs từ DB: {e}")
    return users, merchants, accounts


def main():
    print("⏳ Đang tải danh sách IDs hợp lệ từ PostgreSQL...")
    valid_users, valid_merchants, valid_accounts = get_valid_ids()
    print(f"   Đã tải {len(valid_users)} Users, {len(valid_merchants)} Merchants, {len(valid_accounts)} Accounts.")

    print(f"🔄 Đang kết nối Kafka: {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = create_producer()
    except Exception as e:
        print(f"❌ Không thể kết nối Kafka: {e}")
        return

    print(f"✅ Đã kết nối. Chuẩn bị bắn DATA REAL-TIME vào topic '{KAFKA_TOPIC}'.")
    print("👉 Nhấn Ctrl + C để dừng.")
    print("-" * 60)

    count = 0
    try:
        while True:
            payload = generate_mock_transaction(valid_users, valid_merchants, valid_accounts)

            producer.send(KAFKA_TOPIC, value=payload)
            count += 1

            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"⚡ Sent TX: {payload['transaction_id'][:8]}... | "
                  f"Type: {payload['type']:<8} | "
                  f"Amount: ${payload['amount']:,.2f} | "
                  f"Fraud: {payload['isFraud']}")

            time.sleep(random.uniform(0.2, 1.5))

    except KeyboardInterrupt:
        print("\n🛑 Đã nhận lệnh dừng (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        print(f"✅ Hoàn tất. Đã đẩy tổng cộng {count} giao dịch ảo.")

if __name__ == "__main__":
    main()
