"""
Live Producer - Real-Time Binance WebSocket → Kafka Pipeline
================================================================
Kết nối Binance Public WebSocket API để nhận giao dịch crypto THẬT
(BTC, ETH, BNB, SOL, XRP) và map vào schema payment_events cho pipeline.

Nguồn dữ liệu: wss://stream.binance.com:9443 (miễn phí, không cần API key)
"""

import json
import os
import sys
import time
import uuid
import hashlib
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import websocket
import psycopg2

# ─── Load .env ───────────────────────────────────────────────────
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events_v3")

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

MAX_RETRIES         = int(os.getenv("MAX_RETRIES", "10"))
RETRY_INTERVAL_SEC  = int(os.getenv("RETRY_INTERVAL_SEC", "5"))

# ─── Binance WebSocket Config ───────────────────────────────────
# Các cặp giao dịch crypto theo dõi (đều là real-time, public data)
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# Binance Multi-stream URL (nhận data từ nhiều cặp cùng lúc)
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


def classify_transaction(amount_usd: float, is_buyer_maker: bool) -> str:
    """
    Phân loại giao dịch dựa trên ĐẶC ĐIỂM THẬT của Binance trade.
    
    2 chiều phân tích:
      1. Hướng giao dịch: is_buyer_maker (True = Sell, False = Buy)
      2. Quy mô giao dịch: theo ngưỡng USD
    
    Returns: SPOT_BUY | SPOT_SELL | LARGE_TRADE | MICRO_TRADE | WHALE_ALERT
    """
    # Whale Alert: Giao dịch cực lớn (> $100,000)
    if amount_usd > 100_000:
        return "WHALE_ALERT"
    
    # Large Trade: Giao dịch lớn (> $10,000)
    if amount_usd > 10_000:
        return "LARGE_TRADE"
    
    # Micro Trade: Giao dịch rất nhỏ (< $100)
    if amount_usd < 100:
        return "MICRO_TRADE"
    
    # Standard trades: Phân loại theo hướng Mua/Bán
    if is_buyer_maker:
        return "SPOT_SELL"   # Buyer đặt limit → bên bán chủ động
    else:
        return "SPOT_BUY"    # Buyer là taker → bên mua chủ động


# ─── Helper Functions ────────────────────────────────────────────

def deterministic_id(prefix: str, seed_value) -> str:
    """Tạo ID xác định (deterministic) từ seed, tránh random."""
    hash_hex = hashlib.md5(f"{prefix}_{seed_value}".encode()).hexdigest()[:10]
    return f"{prefix}{hash_hex.upper()}"


def map_binance_trade_to_event(trade: dict, valid_users: list, 
                                valid_merchants: list, valid_accounts: list) -> dict:
    """
    Map 1 Binance trade message thật → schema payment_events của pipeline.
    
    Binance trade fields:
      - t: trade ID (unique, integer)
      - p: price (string, ví dụ "87234.50")
      - q: quantity (string, ví dụ "0.00150")  
      - T: trade time (milliseconds timestamp)
      - s: symbol (ví dụ "BTCUSDT")
      - m: is buyer the market maker? (boolean)
      - b: buyer order ID
      - a: seller order ID (ask)
    """
    trade_id    = trade["t"]          # Trade ID thật từ Binance
    price       = float(trade["p"])   # Giá thật (USD)
    quantity    = float(trade["q"])   # Khối lượng thật
    trade_time  = trade["T"]          # Timestamp thật (ms)
    symbol      = trade["s"]          # Ví dụ: "BTCUSDT"
    is_maker    = trade["m"]          # Buyer is maker?
    buyer_id    = trade["b"]          # Buyer order ID thật
    seller_id   = trade["a"]          # Seller order ID thật (ask)

    # 1. Transaction ID: UUID dựa trên trade_id thật (deterministic)
    transaction_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"binance.trade.{trade_id}"))

    # 2. Event timestamp: convert ms → ISO format
    event_timestamp = datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc).isoformat()

    # 3. Amount: Giá giao dịch thật (price × quantity = tổng giá trị USD)
    amount = round(price * quantity, 2)

    # 4. Transaction type: Phân loại từ ĐẶC ĐIỂM THẬT (hướng + quy mô)
    tx_type = classify_transaction(amount, is_maker)

    # 5. User & Merchant IDs: Dùng buyer/seller order ID thật để chọn từ DB
    #    (deterministic mapping: cùng buyer_id luôn map cùng user)
    if valid_users:
        user_idx = buyer_id % len(valid_users)
        nameOrig = valid_users[user_idx]
    else:
        nameOrig = deterministic_id("C", buyer_id)

    if valid_merchants:
        merchant_idx = seller_id % len(valid_merchants)
        nameDest = valid_merchants[merchant_idx]
    else:
        nameDest = deterministic_id("M", seller_id)

    # 6. Account ID: lấy từ DB dựa trên buyer_id
    if valid_accounts:
        acc_idx = buyer_id % len(valid_accounts)
        account_id = valid_accounts[acc_idx]
    else:
        account_id = f"{nameOrig}_ACC1"

    # 7. Balances: Dùng price và quantity thật (không random)
    oldbalanceOrg  = round(price, 2)          # Giá giao dịch thật
    newbalanceOrig = round(price - amount, 2)  # Sau giao dịch
    oldbalanceDest = round(quantity * 10000, 2) # Khối lượng × hệ số
    newbalanceDest = round(oldbalanceDest + amount, 2)

    # 8. Fraud detection: Dựa trên giao dịch lớn bất thường (thật)
    #    Nếu tổng giá trị > 50,000 USD → flag nghi ngờ
    isFraud = 1 if amount > 50000 else 0
    isFlaggedFraud = 1 if amount > 100000 else 0

    # 9. Channel & IP: Map deterministic từ trade_id (không random)
    channels = ["CHN_APP_IOS", "CHN_APP_AND", "CHN_WEB", "CHN_ATM", "CHN_POS"]
    channel_id = channels[trade_id % len(channels)]

    # IP từ trade_id (deterministic, không random)
    ip_parts = [
        (trade_id >> 24) & 0xFF or 1,
        (trade_id >> 16) & 0xFF,
        (trade_id >> 8) & 0xFF,
        trade_id & 0xFF or 1,
    ]
    ip_address = f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}.{ip_parts[3]}"

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
        "transaction_id": transaction_id,
        "event_timestamp": event_timestamp,
        "account_id": account_id,
        "channel_id": channel_id,
        "ip_address": ip_address,
    }


# ─── Database Lookup ─────────────────────────────────────────────

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
            cur.execute("SELECT user_id FROM dim_users ORDER BY RANDOM() LIMIT 500;")
            users = [r[0] for r in cur.fetchall()]

            cur.execute("SELECT merchant_id FROM dim_merchants ORDER BY RANDOM() LIMIT 500;")
            merchants = [r[0] for r in cur.fetchall()]

            cur.execute("SELECT account_id FROM dim_account ORDER BY RANDOM() LIMIT 500;")
            accounts = [r[0] for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        print(f"⚠️ Không thể lấy IDs từ DB: {e}")
        print("   → Sẽ dùng IDs deterministic từ Binance trade data.")
    return users, merchants, accounts


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

    print("=" * 65)
    print("  🔴 LIVE PRODUCER — Binance Real-Time → Kafka Pipeline")
    print("=" * 65)
    print(f"  📡 Source  : Binance WebSocket (PUBLIC, NO API KEY)")
    print(f"  🪙 Symbols : {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"  📨 Kafka   : {KAFKA_BOOTSTRAP_SERVERS} → topic '{KAFKA_TOPIC}'")
    print()

    # 1. Load valid IDs từ PostgreSQL
    print("⏳ Đang tải danh sách IDs hợp lệ từ PostgreSQL...")
    valid_users, valid_merchants, valid_accounts = get_valid_ids()
    print(f"   📊 Đã tải {len(valid_users)} Users, {len(valid_merchants)} Merchants, "
          f"{len(valid_accounts)} Accounts.")

    # 2. Kết nối Kafka
    print(f"\n🔄 Đang kết nối Kafka: {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = create_kafka_producer()
    except ConnectionError as e:
        print(f"❌ {e}")
        return

    # 3. Biến đếm
    count = 0
    error_count = 0
    start_time = time.time()

    print(f"\n✅ SẴN SÀNG! Đang kết nối Binance WebSocket...")
    print(f"   URL: {BINANCE_WS_URL[:80]}...")
    print("👉 Nhấn Ctrl + C để dừng.")
    print("-" * 65)

    # 4. WebSocket callbacks
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

            # Map Binance trade → payment event
            payload = map_binance_trade_to_event(
                trade, valid_users, valid_merchants, valid_accounts
            )

            # Gửi vào Kafka
            producer.send(KAFKA_TOPIC, value=payload)
            count += 1

            # Log mỗi giao dịch
            symbol = trade["s"]
            price = float(trade["p"])
            qty = float(trade["q"])
            total_usd = round(price * qty, 2)

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"⚡ {symbol:<10} | "
                f"Price: ${price:>10,.2f} | "
                f"Qty: {qty:>12.6f} | "
                f"Total: ${total_usd:>10,.2f} | "
                f"→ {payload['type']:<10} | "
                f"TX: {payload['transaction_id'][:8]}..."
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
        print("-" * 65)

    # 5. Chạy WebSocket (blocking, tự reconnect)
    try:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        ws.run_forever(reconnect=5)  # Tự reconnect sau 5 giây nếu mất kết nối

    except KeyboardInterrupt:
        print(f"\n\n🛑 Đã nhận lệnh dừng (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        rate = count / elapsed if elapsed > 0 else 0
        print(f"\n{'=' * 65}")
        print(f"  📊 KẾT QUẢ PHIÊN LIVE STREAMING")
        print(f"  {'─' * 40}")
        print(f"  Tổng trades gửi  : {count:,}")
        print(f"  Lỗi              : {error_count:,}")
        print(f"  Thời gian chạy   : {elapsed:.0f}s")
        print(f"  Throughput       : {rate:.1f} trades/s")
        print(f"  Nguồn dữ liệu   : Binance Public WebSocket (REAL DATA)")
        print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
