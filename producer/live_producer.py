"""
Live Producer - Real-Time Binance WebSocket to Kafka Pipeline
================================================================
INGESTION LAYER ONLY — Single Responsibility Principle:
  - Connect to Binance WebSocket (public, no API key required)
  - Receive raw trade messages
  - Forward raw data to Kafka topic WITHOUT any business logic

All data processing (cleansing, deduplication, classification,
anomaly detection) is performed downstream by Spark Structured
Streaming (processor/spark_processor.py).

Data source: wss://stream.binance.com:9443 (free, no API key required)
"""

import json
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import websocket

# --- Load .env ---------------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events_v3")

MAX_RETRIES         = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL_SEC  = int(os.getenv("RETRY_INTERVAL_SEC", "5"))

# --- Binance WebSocket Config -----------------------------------------
# Crypto trading pairs monitored (real-time, public data)
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# Binance Multi-stream URL
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


# --- Kafka Producer ---------------------------------------------------

def create_kafka_producer() -> KafkaProducer:
    """Connect to Kafka broker with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
                compression_type="lz4",
            )
            print(f"   [OK] Connected to Kafka (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            print(f"   [WAIT] Kafka not ready, retrying in {RETRY_INTERVAL_SEC}s "
                  f"(attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(RETRY_INTERVAL_SEC)

    raise ConnectionError(f"[ERROR] Could not connect to Kafka after {MAX_RETRIES} attempts")


# --- Main: Binance WebSocket -> Kafka (INGESTION LAYER - RAW DATA ONLY) ---

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

    print("=" * 70)
    print("  LIVE PRODUCER -- Binance Real-Time to Kafka Pipeline")
    print("  Role: INGESTION ONLY (No business logic)")
    print("=" * 70)
    print(f"  Source   : Binance WebSocket (PUBLIC, NO API KEY)")
    print(f"  Symbols  : {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS} -> topic '{KAFKA_TOPIC}'")
    print()

    print("[INFO] Ingestion-Only Mode:")
    print("   - Raw Binance trade data is forwarded to Kafka as-is.")

    # 1. Connect to Kafka
    print(f"\n[CONN] Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = create_kafka_producer()
    except ConnectionError as e:
        print(f"[ERROR] {e}")
        return

    # 2. Counters
    count = 0
    error_count = 0
    start_time = time.time()

    print(f"\n[OK] Ready. Connecting to Binance WebSocket...")
    print(f"   URL: {BINANCE_WS_URL[:80]}...")
    print("Press Ctrl + C to stop.")
    print("-" * 70)

    # 3. WebSocket callbacks
    def on_message(ws, message):
        """
        Callback xử lý tin nhắn thời gian thực từ Binance.
        - Tiếp nhận chuỗi JSON văn bản (Text Frame).
        - Thực hiện nguyên lý Single Responsibility Principle (SRP): 
          Chỉ trích xuất các trường cơ bản và chuyển tiếp ngay sang Kafka,
          hoàn toàn không thực hiện tính toán nặng hay ép kiểu tại đây để giữ băng thông lớn nhất.
        """
        nonlocal count, error_count
        try:
            # Chuyển đổi dữ liệu sang dạng Bytes để mô phỏng quá trình đọc luồng Bytes thô
            if isinstance(message, str):
                message_bytes = message.encode('utf-8')
            else:
                message_bytes = message

            data = json.loads(message_bytes.decode('utf-8'))

            # Định dạng Multi-stream của Binance: {"stream": "btcusdt@trade", "data": {...}}
            if "data" in data:
                trade = data["data"]
            else:
                trade = data

            # Chỉ lọc và xử lý các sự kiện khớp lệnh thành công (trade events)
            if trade.get("e") != "trade":
                return

            # ===================================================================
            # RAW DATA MAPPING — Giữ nguyên định dạng chuỗi (String) của Binance
            # Không ép kiểu float/int tại đây để tránh gây bottleneck hiệu năng của Producer.
            # Tác vụ ép kiểu và sinh khóa định danh sẽ được thực hiện ở tầng xử lý hạ nguồn (Spark).
            # ===================================================================
            raw_payload = {
                "trade_id":        trade.get("t"),       # ID giao dịch gốc của Binance (Natural Key)
                "crypto_symbol":   trade.get("s"),       # Ký hiệu cặp tiền (ví dụ: BTCUSDT)
                "price":           trade.get("p"),       # Giá khớp lệnh dạng String từ Binance
                "quantity":        trade.get("q"),       # Khối lượng khớp lệnh dạng String từ Binance
                "trade_time_ms":   trade.get("T"),       # Thời gian khớp lệnh dạng epoch milliseconds
                "is_buyer_maker":  trade.get("m"),       # Phe bán chủ động (True) hay phe mua chủ động (False)
                "buyer_order_id":  trade.get("b"),       # ID lệnh mua
                "seller_order_id": trade.get("a"),       # ID lệnh bán
            }

            # Đẩy dữ liệu vào Kafka Topic. Thư viện sử dụng nén LZ4 để tối ưu hóa đường truyền mạng.
            producer.send(KAFKA_TOPIC, value=raw_payload)
            count += 1

            # Log each trade (minimal formatting for monitoring)
            symbol = raw_payload["crypto_symbol"] or "?"
            price  = raw_payload["price"] or "0"
            qty    = raw_payload["quantity"] or "0"

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"{symbol:<9} | "
                f"Pri: {price:>12} | "
                f"Qty: {qty:>12} | "
                f"TradeID: {raw_payload['trade_id']}"
            )

            # Summary every 100 trades
            if count % 100 == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                print(f"\n   [{count:,} trades | {rate:.1f} trades/s | "
                      f"errors: {error_count}]\n")

        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only log the first 5 errors
                print(f"   [WARN] Error processing message: {e}")

    def on_error(ws, error):
        print(f"\n[WARN] WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"\n[CLOSE] WebSocket connection closed. Code: {close_status_code}, Msg: {close_msg}")
        print(f"   Total trades sent: {count:,}")

    def on_open(ws):
        print("\n[OPEN] Binance WebSocket CONNECTED. Receiving live RAW data...")
        print("-" * 70)

    # 4. Run WebSocket (blocking, with auto-reconnect)
    try:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        ws.run_forever(reconnect=5)  # Auto-reconnect tests natural duplicate replay from Binance

    except KeyboardInterrupt:
        print(f"\n\n[STOP] Shutdown signal received (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        rate = count / elapsed if elapsed > 0 else 0
        print(f"\n{'=' * 70}")
        print(f"  LIVE STREAMING SESSION SUMMARY")
        print(f"  {'-' * 40}")
        print(f"  Trades sent       : {count:,}")
        print(f"  Errors/Exceptions : {error_count:,}")
        print(f"  Duration          : {elapsed:.0f}s")
        print(f"  Throughput        : {rate:.1f} trades/s")
        print(f"  Mode              : INGESTION")
        print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
