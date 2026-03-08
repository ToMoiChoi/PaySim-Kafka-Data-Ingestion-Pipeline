"""
Live Producer - Mô phỏng giao dịch Real-Time
================================================
Kịch bản: Bắn dữ liệu liên tục (1-2 giao dịch / giây) vào Kafka
để quan sát luồng Spark Streaming & BigQuery/PostgreSQL xử lý theo thời gian thực.
"""

import json
import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "payment_events"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_mock_transaction():
    """Hàm ảo tạo 1 giao dịch hợp lệ với schema fact_transactions hiện tại"""
    tx_type = random.choice(["PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT", "DEBIT"])
    amount = round(random.uniform(10.0, 50000.0), 2)
    
    # User và Merchant random
    nameOrig = f"U{random.randint(10000, 99999)}"
    nameDest = f"M{random.randint(10000, 99999)}"
    
    # Logic tính toán balance ảo (đơn giản hoá để đúng định dạng schema)
    oldbalanceOrg = round(random.uniform(amount, amount * 10), 2)
    newbalanceOrig = round(oldbalanceOrg - amount, 2)
    
    oldbalanceDest = round(random.uniform(0.0, 10000.0), 2)
    newbalanceDest = round(oldbalanceDest + amount, 2)
    
    # Random gian lận (~1%)
    isFraud = 1 if random.random() < 0.01 else 0
    isFlaggedFraud = 1 if amount > 40000 and isFraud == 1 else 0

    return {
        "step": 1, # Bước mô phỏng (trong paper là số giờ)
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
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

def main():
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
            # Sinh data ảo
            payload = generate_mock_transaction()
            
            # Đẩy vào Kafka
            producer.send(KAFKA_TOPIC, value=payload)
            count += 1
            
            # In log ra terminal để user dễ nhìn tiến độ
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"⚡ Sent TX: {payload['transaction_id'][:8]}... | "
                  f"Type: {payload['type']:<8} | "
                  f"Amount: ${payload['amount']:,.2f} | "
                  f"Fraud: {payload['isFraud']}")
            
            # Tốc độ bắn: 0.2s - 1.5s / 1 giao dịch (giả lập lưu lượng thực tế, đứt quãng)
            time.sleep(random.uniform(0.2, 1.5))
            
    except KeyboardInterrupt:
        print("\n🛑 Đã nhận lệnh dừng (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        print(f"✅ Hoàn tất. Đã đẩy tổng cộng {count} giao dịch ảo.")

if __name__ == "__main__":
    main()
