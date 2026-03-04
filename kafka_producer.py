"""
PaySim CSV → Kafka Producer Pipeline : Data Ingestion Pipeline
=====================================
1. Đọc file PaySim CSV bằng pandas - Dataset: Kaggle PaySim Dataset
2. Lọc giao dịch type == "PAYMENT"
3. Thêm cột transaction_id (UUID4) và event_timestamp (current time)
4. Fault Injection: nhân bản ngẫu nhiên 10% records (cùng transaction_id) để tạo dữ liệu trùng
5. Gửi từng dòng JSON vào Kafka topic "payment_events"
"""

import json
import os
import uuid
import time
import random
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── Load .env ───────────────────────────────────────────────────
load_dotenv()

# ─── Configuration (all from .env) ──────────────────────────────
CSV_PATH            = os.getenv("CSV_PATH", "data/PS_20174392719_1491204439457_log.csv")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC         = os.getenv("KAFKA_TOPIC", "payment_events")
DUPLICATE_RATIO     = float(os.getenv("DUPLICATE_RATIO", "0.10"))
BATCH_SIZE          = int(os.getenv("BATCH_SIZE", "65536"))
LINGER_MS           = int(os.getenv("LINGER_MS", "50"))
BUFFER_MEMORY       = int(os.getenv("BUFFER_MEMORY", "67108864"))
MAX_RETRIES         = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL_SEC  = int(os.getenv("RETRY_INTERVAL_SEC", "5"))


def load_and_transform(csv_path: str) -> pd.DataFrame:
    """Đọc CSV, lọc PAYMENT, thêm transaction_id & event_timestamp."""
    print(f"[1/4] Đọc file CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"      Tổng số dòng trong file: {total_rows:,}")

    # Lọc chỉ giao dịch PAYMENT
    df = df[df["type"] == "PAYMENT"].copy()
    payment_rows = len(df)
    print(f"[2/4] Lọc PAYMENT: {payment_rows:,} / {total_rows:,} dòng")

    # Sinh transaction_id (UUID4) cho mỗi dòng
    df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Gán event_timestamp = thời gian hiện tại
    df["event_timestamp"] = datetime.now(timezone.utc).isoformat()

    print(f"      Đã thêm cột transaction_id & event_timestamp")
    return df


def inject_duplicates(df: pd.DataFrame, ratio: float = DUPLICATE_RATIO) -> pd.DataFrame:
    """
    Fault Injection: chọn ngẫu nhiên `ratio` phần trăm records,
    nhân bản chúng (giữ nguyên transaction_id) và trộn lại vào dataframe.
    """
    n_duplicates = int(len(df) * ratio)
    print(f"[3/4] Fault Injection: nhân bản {n_duplicates:,} records "
          f"({ratio*100:.0f}% of {len(df):,})")

    # Chọn ngẫu nhiên các dòng cần duplicate
    duplicate_indices = random.sample(range(len(df)), n_duplicates)
    duplicates = df.iloc[duplicate_indices].copy()

    # Ghép duplicates vào cuối, sau đó shuffle toàn bộ
    df_with_dupes = pd.concat([df, duplicates], ignore_index=True)
    df_with_dupes = df_with_dupes.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"      Tổng records sau injection: {len(df_with_dupes):,} "
          f"(gốc: {len(df):,} + trùng: {n_duplicates:,})")
    return df_with_dupes


def create_kafka_producer() -> KafkaProducer:
    """Kết nối tới Kafka broker với retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
                batch_size=BATCH_SIZE,
                linger_ms=LINGER_MS,
                buffer_memory=BUFFER_MEMORY,
                compression_type="gzip",
            )
            print(f"      Kết nối Kafka thành công (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            print(f"      Kafka chưa sẵn sàng, thử lại sau {RETRY_INTERVAL_SEC}s "
                  f"(attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(RETRY_INTERVAL_SEC)

    raise ConnectionError(f"Không thể kết nối Kafka sau {MAX_RETRIES} lần thử")


def produce_messages(producer: KafkaProducer, df: pd.DataFrame, topic: str):
    """Gửi từng dòng dataframe dưới dạng JSON vào Kafka topic."""
    total = len(df)
    print(f"[4/4] Bắt đầu gửi {total:,} messages vào topic '{topic}'...")

    success_count = 0
    error_count = 0

    start_time = time.time()

    for idx, row in df.iterrows():
        message = row.to_dict()

        try:
            producer.send(topic, value=message)
            success_count += 1
        except Exception as e:
            error_count += 1
            print(f"      ❌ Lỗi gửi record {idx}: {e}")

        # Log tiến trình mỗi 50,000 messages
        sent = success_count + error_count
        if sent % 50000 == 0:
            elapsed = time.time() - start_time
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"      📤 {sent:,} / {total:,} messages "
                  f"({sent/total*100:.1f}%) | {rate:,.0f} msg/s")

    # Flush đảm bảo tất cả messages đã được gửi
    producer.flush()
    producer.close()

    print(f"\n{'='*60}")
    print(f"✅ HOÀN TẤT!")
    print(f"   Topic:           {topic}")
    print(f"   Tổng gửi:        {success_count:,} messages")
    print(f"   Lỗi:             {error_count:,} messages")
    print(f"   Trong đó trùng:  ~{int(total * DUPLICATE_RATIO / (1 + DUPLICATE_RATIO)):,} "
          f"(duplicates)")
    print(f"{'='*60}")


def main():
    print("=" * 60)
    print("  PaySim CSV → Kafka Producer Pipeline")
    print("=" * 60)
    print()

    # Step 1-2: Đọc & biến đổi
    df = load_and_transform(CSV_PATH)

    # Step 3: Bơm lỗi (Fault Injection)
    df = inject_duplicates(df)

    # Step 4: Gửi vào Kafka
    print(f"\n[4/4] Kết nối Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    producer = create_kafka_producer()
    produce_messages(producer, df, KAFKA_TOPIC)


if __name__ == "__main__":
    main()
