"""
Live Producer - Real-Time Binance WebSocket to Kafka Pipeline
================================================================
Streaming data pipeline designed for processing Binance WebSocket data:
 - Duplicate control (Idempotent UUID generation)
 - Anomaly detection for Whale-sized trades
 - Volume classification (Retail, Professional, Institutional, Whale)

Data source: wss://stream.binance.com:9443 (free, no API key required)
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


def classify_transaction_volume(amount_usd: float) -> tuple[str, bool]:
    """
    Classify trade volume based on Chainalysis and Whale Alert thresholds.
    Returns: (volume_category, is_anomaly)

    Reference:
      - < $10k: Retail Trade (small individual trades)
      - $10k - $100k: Professional Trade (experienced individual traders)
      - $100k - $1M: Institutional / Block Trade (large orders from institutions)
      - > $1M: Whale -- triggers anomaly flag `is_anomaly = True`
    """
    if amount_usd >= 1_000_000:
        return "WHALE", True
    elif amount_usd >= 100_000:
        return "INSTITUTIONAL", False
    elif amount_usd >= 10_000:
        return "PROFESSIONAL", False
    else:
        return "RETAIL", False


# --- MAPPING: Binance Trade -> Kafka Payload ---------------------------

def map_binance_trade_to_event(trade: dict) -> dict:
    """
    Map a single real Binance trade message to the Native Crypto Schema.
    No fake data is injected at any point.

    Binance trade fields:
      - t: trade ID (unique integer)
      - p: price (string, e.g. "87234.50")
      - q: quantity (string, e.g. "0.00150")
      - T: trade time (milliseconds timestamp)
      - s: symbol (e.g. "BTCUSDT")
      - m: is buyer the market maker? (boolean) -- True = Sell side, False = Buy side
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

    # 1. Natural Idempotent Deduplication
    # When the WebSocket disconnects and reconnects, Binance may replay recent trades.
    # Spark Streaming `.dropDuplicates(["transaction_id"])` requires a stable ID.
    # We generate transaction_id from the original `trade_id` using UUID5.
    # This means a replayed trade will always produce the same UUID, so Spark drops it.
    transaction_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"binance.trade.{trade_id}"))

    # 2. Timestamp formatting
    event_timestamp = datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc).isoformat()

    # 3. Total order value (Amount)
    amount_usd = round(price * quantity, 2)

    # 4. Volume classification based on Chainalysis thresholds
    volume_category, is_anomaly = classify_transaction_volume(amount_usd)

    return {
        "transaction_id": transaction_id,      # Deduplication Key
        "trade_id": trade_id,                  # Original Binance Trade ID
        "crypto_symbol": symbol,               # Currency Pair (e.g., BTCUSDT)
        "event_timestamp": event_timestamp,    # Original timestamp from Binance
        "price": price,
        "quantity": quantity,
        "amount_usd": amount_usd,              # Trade size in USD
        "is_buyer_maker": is_maker,            # Buy / Sell direction flag
        "volume_category": volume_category,    # Classification (Retail, Pro, Institutional, Whale)
        "is_anomaly": is_anomaly,              # Whale Anomaly flag (True/False)
        "buyer_order_id": buyer_id,            # Anonymous Buyer Order ID (kept as-is)
        "seller_order_id": seller_id           # Anonymous Seller Order ID (kept as-is)
    }


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
                compression_type="gzip",
            )
            print(f"   [OK] Connected to Kafka (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            print(f"   [WAIT] Kafka not ready, retrying in {RETRY_INTERVAL_SEC}s "
                  f"(attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(RETRY_INTERVAL_SEC)

    raise ConnectionError(f"[ERROR] Could not connect to Kafka after {MAX_RETRIES} attempts")


# --- Main: Binance WebSocket -> Kafka ----------------------------------

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

    print("=" * 70)
    print("  LIVE PRODUCER -- Binance Real-Time to Kafka Pipeline")
    print("  Features: Deduplication, Volume Classification, Anomaly Detection")
    print("=" * 70)
    print(f"  Source   : Binance WebSocket (PUBLIC, NO API KEY)")
    print(f"  Symbols  : {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS} -> topic '{KAFKA_TOPIC}'")
    print()

    # Native Pipeline mode info
    print("[INFO] Native Pipeline Mode enabled.")
    print("   - No legacy PaySim users.")
    print("   - Idempotent deduplication via Trade UUID is active.")
    print("   - Volume thresholds: RETAIL, PROFESSIONAL, INSTITUTIONAL, WHALE.")

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
        nonlocal count, error_count
        try:
            data = json.loads(message)

            # Multi-stream format: {"stream": "btcusdt@trade", "data": {...}}
            if "data" in data:
                trade = data["data"]
            else:
                trade = data

            # Only process trade events
            if trade.get("e") != "trade":
                return

            # Map Binance trade -> Pure Native Crypto Event (no faked users)
            payload = map_binance_trade_to_event(trade)

            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=payload)
            count += 1

            # Log each trade
            symbol = payload["crypto_symbol"]
            price = payload["price"]
            qty = payload["quantity"]
            total_usd = payload["amount_usd"]
            vol_cat = payload["volume_category"]
            anom_flag = " ANOMALY" if payload["is_anomaly"] else ""

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"{symbol:<9} | "
                f"Pri: ${price:>9,.1f} | "
                f"Qty: {qty:>8.4f} | "
                f"Val: ${total_usd:>10,.2f} | "
                f"Cls: {vol_cat:<13} | "
                f"TX: {payload['transaction_id'][:8]}{anom_flag}"
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
        print("\n[OPEN] Binance WebSocket CONNECTED. Receiving live data...")
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
        print(f"  Data source       : WSS Binance Public (Idempotent Enabled)")
        print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
