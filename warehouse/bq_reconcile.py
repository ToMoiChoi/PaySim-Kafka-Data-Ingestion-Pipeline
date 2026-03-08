"""
bq_reconcile.py – Đối soát dữ liệu BigQuery vs Kafka
======================================================
Mục đích: Kiểm tra xem số row trong fact_transactions có khớp
           với số message Kafka đã produce hay không.

Cách chạy:
    python bq_reconcile.py

Kết quả:
    - Bảng so sánh Kafka messages vs BigQuery rows
    - Match percentage
    - Exit code 0 nếu match ≥ 90%, 1 nếu thấp hơn

Yêu cầu:
    - BQ_PROJECT_ID, BQ_DATASET đặt trong .env
    - GOOGLE_APPLICATION_CREDENTIALS trỏ tới service account JSON
    - Kafka đang chạy tại KAFKA_BOOTSTRAP_SERVERS
"""

import os
import sys
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

# ─── Logging Setup ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BQReconcile")

# ─── Configuration ────────────────────────────────────────────────
load_dotenv()
PROJECT_ID              = os.getenv("BQ_PROJECT_ID", "")
DATASET_ID              = os.getenv("BQ_DATASET", "paysim_dw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payment_events")
MATCH_THRESHOLD         = float(os.getenv("RECONCILE_THRESHOLD", "0.90"))  # 90%

if not PROJECT_ID:
    logger.error("❌ Thiếu BQ_PROJECT_ID trong .env")
    sys.exit(1)


# ─── Helper: đếm rows BigQuery ───────────────────────────────────
def count_bq_rows(table_name: str = "fact_transactions") -> int:
    """Trả về số row trong bảng BigQuery."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        query  = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        result = client.query(query).result()
        for row in result:
            return int(row.cnt)
    except Exception as exc:
        logger.warning(f"⚠️  Không thể query BigQuery: {exc}")
        return -1
    return 0


# ─── Helper: đếm messages Kafka ──────────────────────────────────
def count_kafka_messages() -> int:
    """
    Tính tổng số message đã produce vào topic bằng cách cộng
    end_offset - beginning_offset của tất cả partition.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka import KafkaAdminClient

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="reconcile_group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
        )

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            logger.warning(f"⚠️  Topic '{KAFKA_TOPIC}' không tồn tại hoặc chưa có data.")
            consumer.close()
            return -1

        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        consumer.assign(tps)

        # end offsets
        end_offsets   = consumer.end_offsets(tps)
        begin_offsets = consumer.beginning_offsets(tps)

        total = sum(
            end_offsets[tp] - begin_offsets[tp]
            for tp in tps
        )
        consumer.close()
        return total

    except Exception as exc:
        logger.warning(f"⚠️  Không thể kết nối Kafka: {exc}")
        return -1


# ─── Helper: thống kê chi tiết BigQuery ──────────────────────────
def bq_detail_stats() -> dict:
    """
    Trả về thống kê giao dịch: tổng amount, avg reward_points, 
    và số row theo ngày gần nhất.
    """
    stats = {}
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)

        # Tổng amount và avg reward
        q1 = f"""
            SELECT
                COUNT(*)           AS total_rows,
                ROUND(SUM(amount), 2)      AS total_amount,
                ROUND(AVG(reward_points), 2) AS avg_reward
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_transactions`
        """
        for row in client.query(q1).result():
            stats["total_rows"]    = int(row.total_rows)
            stats["total_amount"]  = float(row.total_amount or 0)
            stats["avg_reward"]    = float(row.avg_reward or 0)

        # Phân bố theo type_id
        q2 = f"""
            SELECT type_id, COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_transactions`
            GROUP BY type_id
            ORDER BY cnt DESC
        """
        stats["by_type"] = {}
        for row in client.query(q2).result():
            stats["by_type"][row.type_id] = int(row.cnt)

        # Top 5 ngày gần nhất
        q3 = f"""
            SELECT DATE(transaction_time) AS txn_date, COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_transactions`
            GROUP BY txn_date
            ORDER BY txn_date DESC
            LIMIT 5
        """
        stats["recent_dates"] = []
        for row in client.query(q3).result():
            stats["recent_dates"].append({
                "date": str(row.txn_date),
                "count": int(row.cnt),
            })

    except Exception as exc:
        logger.warning(f"⚠️  Không lấy được detail stats: {exc}")

    return stats


# ─── Main ─────────────────────────────────────────────────────────
def main():
    print()
    print("=" * 65)
    print("  📊  BigQuery Reconciliation Report")
    print(f"  ⏱️   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  🏗️   Project : {PROJECT_ID}  |  Dataset : {DATASET_ID}")
    print(f"  📡  Kafka   : {KAFKA_BOOTSTRAP_SERVERS}  |  Topic : {KAFKA_TOPIC}")
    print("=" * 65)

    # ── 1. Lấy số liệu ──────────────────────────────────────────
    logger.info("🔍 Đang đếm rows trong BigQuery fact_transactions...")
    bq_rows = count_bq_rows("fact_transactions")

    logger.info(f"🔍 Đang đọc Kafka offsets (topic: {KAFKA_TOPIC})...")
    kafka_msgs = count_kafka_messages()

    # ── 2. Tính Match % ──────────────────────────────────────────
    print()
    print("┌─────────────────────────────────────────────────────────┐")
    print("│            RECONCILIATION SUMMARY                       │")
    print("├──────────────────────────────────┬──────────────────────┤")

    if kafka_msgs > 0:
        match_pct = (bq_rows / kafka_msgs) * 100
    elif kafka_msgs == 0:
        match_pct = 100.0 if bq_rows == 0 else 0.0
    else:
        match_pct = None

    bq_str    = f"{bq_rows:>12,}" if bq_rows >= 0 else "       N/A (err)"
    kafka_str = f"{kafka_msgs:>12,}" if kafka_msgs >= 0 else "       N/A (err)"

    print(f"│  {'Kafka Messages Produced':<30}  │  {kafka_str}  │")
    print(f"│  {'BigQuery fact_transactions rows':<30}  │  {bq_str}  │")

    if match_pct is not None:
        status_icon = "✅" if match_pct >= MATCH_THRESHOLD * 100 else "❌"
        print(f"│  {'Match Percentage':<30}  │  {match_pct:>10.2f} %  │")
        print(f"│  {'Status':<30}  │  {status_icon + ' PASS' if match_pct >= MATCH_THRESHOLD * 100 else status_icon + ' FAIL':<18}  │")
    else:
        print(f"│  {'Match Percentage':<30}  │  {'N/A (connection error)':<20}  │")

    print("└──────────────────────────────────┴──────────────────────┘")

    # ── 3. Detail stats ──────────────────────────────────────────
    if bq_rows > 0:
        logger.info("📈 Đang lấy thống kê chi tiết từ BigQuery...")
        stats = bq_detail_stats()
        if stats:
            print()
            print("┌─────────────────────────────────────────────────────────┐")
            print("│              DETAIL STATS (BigQuery)                    │")
            print("├─────────────────────────────────────────────────────────┤")
            print(f"│  Total Amount   : {stats.get('total_amount', 0):>30,.2f} VND  │")
            print(f"│  Avg Reward Pts : {stats.get('avg_reward', 0):>30.2f} pts  │")

            if stats.get("by_type"):
                print("├─────────────────────────────────────────────────────────┤")
                print("│  Transaction Types:                                     │")
                for ttype, cnt in stats["by_type"].items():
                    print(f"│    {ttype:<16} : {cnt:>8,} rows                         │")

            if stats.get("recent_dates"):
                print("├─────────────────────────────────────────────────────────┤")
                print("│  Recent Activity (top 5 dates):                         │")
                for entry in stats["recent_dates"]:
                    print(f"│    {entry['date']} : {entry['count']:>8,} rows                     │")

            print("└─────────────────────────────────────────────────────────┘")

    print()

    # ── 4. Exit code ─────────────────────────────────────────────
    if match_pct is not None and match_pct < MATCH_THRESHOLD * 100:
        logger.error(
            f"❌ Match {match_pct:.1f}% < threshold {MATCH_THRESHOLD*100:.0f}% → EXIT 1"
        )
        sys.exit(1)

    logger.info("✅ Reconciliation hoàn tất.")


if __name__ == "__main__":
    main()
