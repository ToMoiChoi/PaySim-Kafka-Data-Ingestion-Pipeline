"""
Live Producer - Real-Time Binance WebSocket → Kafka Pipeline
================================================================
Thiết kế pipeline xử lý dữ liệu streaming từ Binance WebSocket:
 - Kiểm soát trùng lặp (Idempotent UUID generation)
 - Phát hiện bất thường (Anomaly Detection cho Whale trades)
 - Phân hạng khối lượng giao dịch (Retail, Professional, Institutional, Whale) trên Kafka

Nguồn dữ liệu: wss://stream.binance.com:9443 (miễn phí, không cần API key)
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import websocket

# ─── Load .env ───────────────────────────────────────────────────
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events_v3")

MAX_RETRIES         = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL_SEC  = int(os.getenv("RETRY_INTERVAL_SEC", "5"))

# ─── Binance WebSocket Config ───────────────────────────────────
# Các cặp giao dịch crypto theo dõi (real-time, public data)
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# Binance Multi-stream URL
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


def classify_transaction_volume(amount_usd: float) -> tuple[str, bool]:
    """
    Phân loại khối lượng giao dịch dựa báo cáo Chainalysis và Whale Alert.
    Trả về: (volume_category, is_anomaly)
    
    Tham chiếu:
      - < $10k: Retail Trade (Giao dịch cá nhân/nhỏ lẻ)
      - $10k - $100k: Professional Trade (Giao dịch cá nhân chuyên nghiệp)
      - $100k - $1M: Institutional/Block Trade (Giao dịch khối lớn/tổ chức)
      - > $1M: Whale (Cá voi) - Kích hoạt cờ bất thường `is_anomaly = True`
    """
    if amount_usd >= 1_000_000:
        return "WHALE", True
    elif amount_usd >= 100_000:
        return "INSTITUTIONAL", False
    elif amount_usd >= 10_000:
        return "PROFESSIONAL", False
    else:
        return "RETAIL", False


# ─── MAPPING: Binance Trade -> Kafka Payload ─────────────────────

def map_binance_trade_to_event(trade: dict) -> dict:
    """
    Map 1 Binance trade message thật → Native Crypto Schema.
    Không có fake, không inject data giả.
    
    Binance trade fields:
      - t: trade ID (unique, integer)
      - p: price (string, ví dụ "87234.50")
      - q: quantity (string, ví dụ "0.00150")  
      - T: trade time (milliseconds timestamp)
      - s: symbol (ví dụ "BTCUSDT")
      - m: is buyer the market maker? (boolean) -> Mặc định True = Sell, False = Buy
      - b: buyer order ID
      - a: seller order ID
    """
    trade_id    = trade.get("t", int(time.time() * 1000))
    price       = float(trade.get("p", 0.0))
    quantity    = float(trade.get("q", 0.0))
    trade_time  = trade.get("T", int(time.time() * 1000))
    symbol      = trade.get("s", "UNKNOWN")
    is_maker    = trade.get("m", False)
    buyer_id    = trade.get("b", trade_id)
    seller_id   = trade.get("a", trade_id)

    # 1. Kiểm soát Trùng lặp tự nhiên (Natural Idempotence)
    # Cơ chế: Khi WebSocket rớt mạng và reconnect, Binance sẽ "replay" các trade gần nhất.
    # Spark Streaming `.dropDuplicates(["transaction_id"])` yêu cầu transaction_id phải tĩnh.
    # Ta sinh transaction_id từ `trade_id` nguyên bản thông qua thuật toán sinh UUID5.
    # Như vậy, trade bị Binance gửi lại SẼ LUÔN LUÔN tạo ra đúng UUID cũ -> Spark tự triệt tiêu.
    # TUYỆT ĐỐI KHÔNG CÓ VIỆC TIÊM HOẶC LÀM GIẢ DỮ LIỆU LỖI.
    transaction_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"binance.trade.{trade_id}"))

    # 2. Định dạng thời gian
    event_timestamp = datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc).isoformat()

    # 3. Tổng giá trị lệnh (Amount)
    amount_usd = round(price * quantity, 2)

    # 4. Phân hạng khối lượng theo Báo cáo Chainalysis
    volume_category, is_anomaly = classify_transaction_volume(amount_usd)

    return {
        "transaction_id": transaction_id,      # Deduplication Key
        "trade_id": trade_id,                  # ID Vết GD Gốc
        "crypto_symbol": symbol,               # Currency Pair (e.g., BTCUSDT)
        "event_timestamp": event_timestamp,    # Timestamp gốc từ Binance
        "price": price,
        "quantity": quantity,
        "amount_usd": amount_usd,              # Size Giao Dịch
        "is_buyer_maker": is_maker,            # Đánh dấu hướng Buy / Sell
        "volume_category": volume_category,    # Phân hạng (Retail, Pro, Institutional, Whale)
        "is_anomaly": is_anomaly,              # Phát hiện Whale Anomaly (True/False)
        "buyer_order_id": buyer_id,            # ID Ẩn danh Lệnh Mua (Giữ nguyên, không map User)
        "seller_order_id": seller_id           # ID Ẩn danh Lệnh Bán (Giữ nguyên, không map User)
    }


# ─── Kafka Producer ──────────────────────────────────────────────

def create_kafka_producer() -> KafkaProducer:
    """Kết nối tới Kafka broker với retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
                compression_type="gzip",
            )
            print(f"   ✅ Kết nối Kafka thành công (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            print(f"   ⏳ Kafka chưa sẵn sàng, thử lại sau {RETRY_INTERVAL_SEC}s "
                  f"(attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(RETRY_INTERVAL_SEC)

    raise ConnectionError(f"❌ Không thể kết nối Kafka sau {MAX_RETRIES} lần thử")


# ─── Main: Binance WebSocket → Kafka ─────────────────────────────

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

    print("=" * 70)
    print("  🔴 LIVE PRODUCER — Binance Real-Time → Kafka Pipeline")
    print("  Chuyên đề: Kiểm soát trùng lặp, Phân hạng GD & Phát hiện Bất thường")
    print("=" * 70)
    print(f"  📡 Source  : Binance WebSocket (PUBLIC, NO API KEY)")
    print(f"  🪙 Symbols : {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"  📨 Kafka   : {KAFKA_BOOTSTRAP_SERVERS} → topic '{KAFKA_TOPIC}'")
    print()

    # Nhắn gửi thông điệp Pipeline Native (Không Fake Data)
    print("⏳ Native Pipeline Mode Enabled.")
    print("   [!] Không sử dụng PaySim Legacy Users.")
    print("   [!] Kích hoạt Cơ chế Idempotent (Trade UUID) chống Duplicate tự nhiên.")
    print("   [!] Phân hạng chuẩn: RETAIL, PROFESSIONAL, INSTITUTIONAL, WHALE.")

    # 1. Kết nối Kafka
    print(f"\n🔄 Đang kết nối Kafka: {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = create_kafka_producer()
    except ConnectionError as e:
        print(f"❌ {e}")
        return

    # 2. Biến đếm
    count = 0
    error_count = 0
    start_time = time.time()

    print(f"\n✅ SẴN SÀNG! Đang kết nối Binance WebSocket...")
    print(f"   URL: {BINANCE_WS_URL[:80]}...")
    print("👉 Nhấn Ctrl + C để dừng.")
    print("-" * 70)

    # 3. WebSocket callbacks
    def on_message(ws, message):
        nonlocal count, error_count
        try:
            data = json.loads(message)

            # Multi-stream format: {"stream": "btcusdt@trade", "data": {...}}
            if "data" in data:
                trade = data["data"]
            else:
                trade = data

            # Chỉ xử lý trade events
            if trade.get("e") != "trade":
                return

            # Map Binance trade → Pure Native Crypto Event (No Faked Users)
            payload = map_binance_trade_to_event(trade)

            # Gửi vào Kafka
            producer.send(KAFKA_TOPIC, value=payload)
            count += 1

            # Log mỗi giao dịch
            symbol = payload["crypto_symbol"]
            price = payload["price"]
            qty = payload["quantity"]
            total_usd = payload["amount_usd"]
            vol_cat = payload["volume_category"]
            anom_flag = "⚠️ ANOMALY" if payload["is_anomaly"] else ""

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"⚡ {symbol:<9} | "
                f"Pri: ${price:>9,.1f} | "
                f"Qty: {qty:>8.4f} | "
                f"Val: ${total_usd:>10,.2f} | "
                f"Cls: {vol_cat:<13} | "
                f"TX: {payload['transaction_id'][:8]} {anom_flag}"
            )

            # Thống kê mỗi 100 trades
            if count % 100 == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                print(f"\n   📈 [{count:,} trades | {rate:.1f} trades/s | "
                      f"errors: {error_count}]\n")

        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Chỉ log 5 lỗi đầu
                print(f"   ⚠️ Lỗi xử lý message: {e}")

    def on_error(ws, error):
        print(f"\n⚠️ WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"\n🔌 WebSocket đóng kết nối. Code: {close_status_code}, Msg: {close_msg}")
        print(f"   → Tổng trades đã gửi: {count:,}")

    def on_open(ws):
        print("\n🟢 Binance WebSocket CONNECTED! Đang nhận dữ liệu thật...")
        print("-" * 70)

    # 4. Chạy WebSocket (blocking, tự reconnect)
    try:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        ws.run_forever(reconnect=5)  # Tự reconnect qua đó test duplicate replay từ Binance

    except KeyboardInterrupt:
        print(f"\n\n🛑 Đã nhận lệnh dừng (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        rate = count / elapsed if elapsed > 0 else 0
        print(f"\n{'=' * 70}")
        print(f"  📊 KẾT QUẢ PHIÊN LIVE STREAMING")
        print(f"  {'─' * 40}")
        print(f"  Tổng trades gửi  : {count:,}")
        print(f"  Lỗi/Ngoại lệ     : {error_count:,}")
        print(f"  Thời gian chạy   : {elapsed:.0f}s")
        print(f"  Throughput       : {rate:.1f} trades/s")
        print(f"  Nguồn dữ liệu    : WSS Binance Public (Idempotent Enabled)")
        print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
