"""
bq_reconcile.py - BigQuery vs Kafka Data Reconciliation
======================================================
Purpose: Check whether the row count in fact_binance_trades matches
         the number of messages produced to Kafka.

Usage:
    python bq_reconcile.py

Output:
    - Comparison table: Kafka messages vs BigQuery rows
    - Match percentage
    - Exit code 0 if match >= 90%, 1 if lower

Requirements:
    - BQ_PROJECT_ID, BQ_DATASET set in .env
    - GOOGLE_APPLICATION_CREDENTIALS pointing to a service account JSON
    - Kafka running at KAFKA_BOOTSTRAP_SERVERS
"""

import os
import sys
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

# --- Logging Setup ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BQReconcile")

# --- Configuration ----------------------------------------------------
load_dotenv()
PROJECT_ID              = os.getenv("BQ_PROJECT_ID", "")
DATASET_ID              = os.getenv("BQ_DATASET", "paysim_dw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payment_events_v3")
MATCH_THRESHOLD         = float(os.getenv("RECONCILE_THRESHOLD", "0.90"))  # 90%

if not PROJECT_ID:
    logger.error("[ERROR] BQ_PROJECT_ID is missing from .env")
    sys.exit(1)


# --- Helper: count BigQuery rows -------------------------------------
def count_bq_rows(table_name: str = "fact_binance_trades") -> int:
    """Return the number of rows in a BigQuery table."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        query  = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        result = client.query(query).result()
        for row in result:
            return int(row.cnt)
    except Exception as exc:
        logger.warning(f"[WARN] Could not query BigQuery: {exc}")
        return -1
    return 0


# --- Helper: count Kafka messages ------------------------------------
def count_kafka_messages() -> int:
    """
    Calculate total messages produced to the topic by summing
    end_offset - beginning_offset across all partitions.
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
            logger.warning(f"[WARN] Topic '{KAFKA_TOPIC}' does not exist or has no data.")
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
        logger.warning(f"[WARN] Could not connect to Kafka: {exc}")
        return -1


# --- Helper: detailed BigQuery stats ---------------------------------
def bq_detail_stats() -> dict:
    """
    Return trade statistics: total amount_usd, and row counts
    by recent dates.
    """
    stats = {}
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)

        # Total amount
        q1 = f"""
            SELECT
                COUNT(*)           AS total_rows,
                ROUND(SUM(amount_usd), 2) AS total_amount_usd
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_binance_trades`
        """
        for row in client.query(q1).result():
            stats["total_rows"]       = int(row.total_rows)
            stats["total_amount_usd"] = float(row.total_amount_usd or 0)

        # Distribution by volume_category
        q2 = f"""
            SELECT volume_category, COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_binance_trades`
            GROUP BY volume_category
            ORDER BY cnt DESC
        """
        stats["by_category"] = {}
        for row in client.query(q2).result():
            stats["by_category"][row.volume_category] = int(row.cnt)

        # Top 5 most recent dates
        q3 = f"""
            SELECT DATE(trade_time) AS txn_date, COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_binance_trades`
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
        logger.warning(f"[WARN] Could not retrieve detail stats: {exc}")

    return stats


# --- Main -------------------------------------------------------------
def main():
    print()
    print("=" * 65)
    print("  BigQuery Reconciliation Report")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Project : {PROJECT_ID}  |  Dataset : {DATASET_ID}")
    print(f"  Kafka   : {KAFKA_BOOTSTRAP_SERVERS}  |  Topic : {KAFKA_TOPIC}")
    print("=" * 65)

    # -- 1. Collect counts ------------------------------------------------
    logger.info("Counting rows in BigQuery fact_binance_trades...")
    bq_rows = count_bq_rows("fact_binance_trades")

    logger.info(f"Reading Kafka offsets (topic: {KAFKA_TOPIC})...")
    kafka_msgs = count_kafka_messages()

    # -- 2. Calculate Match % ---------------------------------------------
    print()
    print("+----------------------------------------------------------+")
    print("|            RECONCILIATION SUMMARY                         |")
    print("+-----------------------------------+-----------------------+")

    if kafka_msgs > 0:
        match_pct = (bq_rows / kafka_msgs) * 100
    elif kafka_msgs == 0:
        match_pct = 100.0 if bq_rows == 0 else 0.0
    else:
        match_pct = None

    bq_str    = f"{bq_rows:>12,}" if bq_rows >= 0 else "       N/A (err)"
    kafka_str = f"{kafka_msgs:>12,}" if kafka_msgs >= 0 else "       N/A (err)"

    print(f"|  {'Kafka Messages Produced':<30}  |  {kafka_str}  |")
    print(f"|  {'BigQuery fact rows':<30}  |  {bq_str}  |")

    if match_pct is not None:
        status_icon = "[PASS]" if match_pct >= MATCH_THRESHOLD * 100 else "[FAIL]"
        print(f"|  {'Match Percentage':<30}  |  {match_pct:>10.2f} %  |")
        print(f"|  {'Status':<30}  |  {status_icon:<18}  |")
    else:
        print(f"|  {'Match Percentage':<30}  |  {'N/A (connection error)':<20}  |")

    print("+-----------------------------------+-----------------------+")

    # -- 3. Detail stats ---------------------------------------------------
    if bq_rows > 0:
        logger.info("Retrieving detailed statistics from BigQuery...")
        stats = bq_detail_stats()
        if stats:
            print()
            print("+----------------------------------------------------------+")
            print("|              DETAIL STATS (BigQuery)                      |")
            print("+----------------------------------------------------------+")
            print(f"|  Total Amount USD : {stats.get('total_amount_usd', 0):>30,.2f}  |")

            if stats.get("by_category"):
                print("+----------------------------------------------------------+")
                print("|  Volume Categories:                                      |")
                for cat, cnt in stats["by_category"].items():
                    print(f"|    {cat:<16} : {cnt:>8,} rows                         |")

            if stats.get("recent_dates"):
                print("+----------------------------------------------------------+")
                print("|  Recent Activity (top 5 dates):                          |")
                for entry in stats["recent_dates"]:
                    print(f"|    {entry['date']} : {entry['count']:>8,} rows                     |")

            print("+----------------------------------------------------------+")

    print()

    # -- 4. Exit code ------------------------------------------------------
    if match_pct is not None and match_pct < MATCH_THRESHOLD * 100:
        logger.error(
            f"[FAIL] Match {match_pct:.1f}% < threshold {MATCH_THRESHOLD*100:.0f}% -> EXIT 1"
        )
        sys.exit(1)

    logger.info("[DONE] Reconciliation complete.")


if __name__ == "__main__":
    main()
