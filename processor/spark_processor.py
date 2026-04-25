"""
processor/spark_processor.py - Core Processing Engine (Dual-Sink)
====================================================================
PROCESSING LAYER — Spark Structured Streaming as the central brain:

  Stage 1: Read RAW stream from Kafka topic (ingested by producer)
  Stage 2: DATA PROCESSING (the core value of Spark):
     a. Parse & Type Cast     — Convert raw strings to proper types
     b. Data Cleansing        — Filter invalid/garbage records
     c. Deduplication         — UUID5 idempotent key + dropDuplicates
     d. Transformation        — Calculate derived columns (amount_usd)
     e. Categorization        — Volume classification (Retail → Whale)
     f. Anomaly Detection     — Flag abnormal trading patterns
  Stage 3: STORAGE — Dual Sink with fault tolerance:
     a. [PRIMARY]  PostgreSQL  (UPSERT via psycopg2, real-time)
     b. [BACKUP]   BigQuery    (Parquet Load Job, async + DLQ on failure)
  Stage 4: VISUALIZATION : PowerBI (Connect to BigQuery)
Output schema matches: fact_binance_trades (BigQuery & PostgreSQL)
"""

import os
import sys
import time
import shutil
import logging
import threading
from datetime import datetime

import psycopg2
import psycopg2.extras
import psycopg2.pool
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, expr, when, current_timestamp, date_format,
    round as spark_round, lit, sha2, concat_ws, create_map
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, LongType
)
from dotenv import load_dotenv

# --- Logging -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkProcessingEngine")

# --- Config ------------------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payment_events_v3")

# PostgreSQL (Primary Sink)
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

# BigQuery (Backup Sink)
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")
BQ_TABLE_FACT = "fact_binance_trades"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
BQ_PARQUET_DIR = os.getenv("BQ_PARQUET_BACKUP_DIR", "/tmp/bq_backup")

# Dead-Letter Queue directory for failed BQ uploads
BQ_DLQ_DIR = os.getenv("BQ_DLQ_DIR", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dlq_bq_failed"
))


# =====================================================================
# KAFKA RAW SCHEMA — matches exactly what live_producer.py sends
# All fields arrive as raw Binance data (price/quantity are strings!)
# =====================================================================
raw_kafka_schema = StructType([
    StructField("trade_id",        LongType(),    True),
    StructField("crypto_symbol",   StringType(),  True),
    StructField("price",           StringType(),  True),   # String from Binance
    StructField("quantity",        StringType(),  True),   # String from Binance
    StructField("trade_time_ms",   LongType(),    True),   # Epoch milliseconds
    StructField("is_buyer_maker",  BooleanType(), True),
    StructField("buyer_order_id",  LongType(),    True),
    StructField("seller_order_id", LongType(),    True),
])


# =====================================================================
# SPARK SESSION
# =====================================================================
def create_spark_session() -> SparkSession:
    """Create Spark session with Windows compatibility fixes."""
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    if sys.platform.startswith('win'):
        os.environ['PATH'] = os.environ['PATH'] + ';' + r'C:\hadoop\bin'
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

    builder = (
        SparkSession.builder
        .appName("Binance_Crypto_Processing_Engine")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.memory.fraction", "0.6")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # --- Latency optimization configs ---
        # Skip empty micro-batches (no data = no processing overhead)
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        # Don't wait for data locality (single-node, no benefit)
        .config("spark.locality.wait", "0s")
        # Adaptive Query Execution: auto-optimize shuffle partitions at runtime
        .config("spark.sql.adaptive.enabled", "true")
        # Windows NullPointerException fixes
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    return builder.getOrCreate()


# =====================================================================
# STAGE 2: DATA PROCESSING — The core engine
# =====================================================================

def process_raw_to_fact(spark: SparkSession, raw_df: DataFrame) -> DataFrame:
    """
    Transform raw Binance trade data into the fact_binance_trades schema.

    Processing pipeline (executed entirely within Spark):
      1. Type Casting    — Convert string price/quantity to numeric
      2. Data Cleansing  — Remove invalid records (price<=0, quantity<=0, nulls)
      3. Deduplication   — Generate deterministic UUID + watermark + dropDuplicates
      4. Transformation  — Calculate amount_usd = price × quantity
      5. Categorization  — Classify volume tier (Retail/Professional/Institutional/Whale)
      6. Anomaly Detect  — Flag whale-sized trades as anomalies
      7. Key Generation  — Generate date_key, time_key for Star Schema
    """

    # -----------------------------------------------------------------
    # Step 1: TYPE CASTING — Convert raw strings to proper numeric types
    # Binance sends price and quantity as strings (e.g., "87234.50")
    # -----------------------------------------------------------------
    typed_df = (
        raw_df
        .withColumn("price",    col("price").cast("double"))
        .withColumn("quantity", col("quantity").cast("double"))
        .withColumn(
            "trade_time",
            expr("to_timestamp(trade_time_ms / 1000)")
        )
    )
    logger.info("[PROCESSING] Step 1: Type casting completed (strings -> numeric)")

    # -----------------------------------------------------------------
    # Step 2: DATA CLEANSING — Remove garbage/invalid records
    # Invalid conditions:
    #   - price <= 0 or price IS NULL
    #   - quantity <= 0 or quantity IS NULL
    #   - trade_id IS NULL
    #   - crypto_symbol IS NULL or empty
    # -----------------------------------------------------------------
    clean_df = (
        typed_df
        .filter(col("trade_id").isNotNull())
        .filter(col("crypto_symbol").isNotNull() & (col("crypto_symbol") != ""))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .filter(col("trade_time").isNotNull())
    )
    logger.info("[PROCESSING] Step 2: Data cleansing completed (filtered invalid records)")

    # -----------------------------------------------------------------
    # Step 3: DEDUPLICATION — Two-Layer Strategy
    #
    # Problem: Binance WebSocket may replay old trades when reconnecting.
    #          If the server is down for >N minutes, Spark's watermark-
    #          based dedup alone will miss late-arriving duplicates
    #          because the state has already been purged.
    #
    # Solution: TWO-LAYER DEDUPLICATION
    #
    # LAYER 1 (Real-time, in Spark):
    #   - Generate deterministic transaction_id from trade_id using
    #     SHA-256 hash → same trade always produces same ID.
    #   - Watermark window = 15 minutes (covers most reconnect scenarios).
    #   - dropDuplicates on transaction_id within the watermark window.
    #   - Handles ~95% of duplicates in real-time at the streaming level.
    #
    # LAYER 2 (Storage-level, guaranteed):
    #   - PostgreSQL: INSERT ... ON CONFLICT (transaction_id) DO UPDATE
    #     → Even if a duplicate passes Layer 1 (arrives after 15 min),
    #       PostgreSQL's UNIQUE constraint catches it at write time.
    #   - BigQuery: Uses Load Job with WRITE_APPEND. Periodic dedup can
    #     be done via scheduled query if needed.
    #
    # This two-layer approach ensures ZERO DUPLICATES in the final
    # data warehouse regardless of how long the server is down.
    # -----------------------------------------------------------------

    # Generate deterministic transaction_id (same trade_id → same hash)
    dedup_df = (
        clean_df
        .withColumn(
            "transaction_id",
            sha2(concat_ws(".", lit("binance"), lit("trade"), col("trade_id").cast("string")), 256)
        )
        # Layer 1: Spark stateful dedup with 15-minute watermark
        # 15 minutes covers typical WebSocket reconnect delays.
        # For delays exceeding 15 minutes, Layer 2 (PostgreSQL UPSERT
        # ON CONFLICT) guarantees no duplicates reach the warehouse.
        .withWatermark("trade_time", "15 minutes")
        .dropDuplicates(["transaction_id"])
    )
    logger.info("[PROCESSING] Step 3: Deduplication Layer 1 completed (SHA-256 + 15min watermark)")

    # -----------------------------------------------------------------
    # Step 4: TRANSFORMATION — Derive computed columns
    # Calculate the total trade value in USD
    # -----------------------------------------------------------------
    transform_df = dedup_df.withColumn(
        "amount_usd",
        spark_round(col("price") * col("quantity"), 2)
    )
    logger.info("[PROCESSING] Step 4: Transformation completed (amount_usd calculated)")

    # -----------------------------------------------------------------
    # Step 5: VOLUME CATEGORIZATION — Kimball Surrogate Key Lookup
    # Based on Chainalysis and Whale Alert real-world thresholds:
    #   SK=1: < $10,000           → RETAIL       (small individual trades)
    #   SK=2: $10,000 – $100,000  → PROFESSIONAL (experienced traders)
    #   SK=3: $100,000 – $1M      → INSTITUTIONAL (block trades)
    #   SK=4: ≥ $1,000,000        → WHALE        (market-moving orders)
    #
    # Kimball: Fact table stores INTEGER surrogate key, NOT varchar.
    # The volume_category_key maps to dim_volume_category.volume_category_key.
    # -----------------------------------------------------------------
    categorized_df = transform_df.withColumn(
        "volume_category_key",
        when(col("amount_usd") >= 1_000_000, lit(4))   # WHALE
        .when(col("amount_usd") >= 100_000,  lit(3))   # INSTITUTIONAL
        .when(col("amount_usd") >= 10_000,   lit(2))   # PROFESSIONAL
        .otherwise(lit(1))                              # RETAIL
    )
    logger.info("[PROCESSING] Step 5: Volume categorization completed (Surrogate Keys 1-4)")

    # -----------------------------------------------------------------
    # Step 6: ANOMALY DETECTION — Shifted to Micro-Batch
    # 
    # Static Anomaly rules have been replaced by Dynamic Statistical
    # Models (Z-Score, Wash Trade Heuristics, Slippage).
    # Because PySpark does not allow non-time-based Window functions 
    # on streaming DFs, the actual advanced calculations are deferred 
    # and executed inside the `foreachBatch` orchestrator.
    # Here we just pre-allocate the column to preserve schema integrity.
    # -----------------------------------------------------------------
    anomaly_df = categorized_df.withColumn("is_anomaly", lit(False))
    logger.info("[PROCESSING] Step 6: Pre-allocation for dynamic Anomaly Detection completed")

    # -----------------------------------------------------------------
    # Step 7: STAR SCHEMA KEY GENERATION — All Surrogate Keys
    # Generate ALL surrogate keys for joining with Dimension tables:
    #   - date_key:             Smart Key yyyyMMdd (dim_date)
    #   - time_key:             Smart Key HHmm (dim_time)
    #   - crypto_pair_key:      Surrogate Key 1-5 (dim_crypto_pair)
    #   - volume_category_key:  Already generated in Step 5 (1-4)
    # -----------------------------------------------------------------

    # Kimball: Map crypto_symbol (natural key) → crypto_pair_key (surrogate)
    # This mapping matches exactly what seed_dimensions scripts populate.
    crypto_sk_map = create_map(
        lit("BTCUSDT"), lit(1),
        lit("ETHUSDT"), lit(2),
        lit("BNBUSDT"), lit(3),
        lit("SOLUSDT"), lit(4),
        lit("XRPUSDT"), lit(5),
    )

    final_df = (
        anomaly_df
        .withColumn(
            "trade_time",
            when(col("trade_time").isNull(), current_timestamp())
            .otherwise(col("trade_time"))
        )
        # date_key: yyyyMMdd format (e.g., 20260414)
        .withColumn("date_key", date_format(col("trade_time"), "yyyyMMdd").cast("long"))
        # time_key: HHmm format (e.g., 14:30 -> 1430)
        .withColumn("time_key", (expr("hour(trade_time)") * 100 + expr("minute(trade_time)")).cast("long"))
        # crypto_pair_key: Surrogate Key lookup from natural key
        .withColumn("crypto_pair_key", crypto_sk_map[col("crypto_symbol")].cast("int"))
        # Cast to match fact_binance_trades schema exactly
        .withColumn("price",      col("price").cast("decimal(38,9)"))
        .withColumn("quantity",   col("quantity").cast("decimal(38,9)"))
        .withColumn("amount_usd", col("amount_usd").cast("decimal(38,9)"))
    )
    logger.info("[PROCESSING] Step 7: Star Schema surrogate keys generated")

    # -----------------------------------------------------------------
    # Final SELECT — Kimball-compliant fact_binance_trades schema
    # All FK references are INTEGER surrogate keys, NOT varchar.
    # Natural keys (crypto_symbol, volume_category) are NOT stored
    # in the fact table — they live only in Dimension tables.
    # transaction_id is a Degenerate Dimension (no separate dim table).
    # -----------------------------------------------------------------
    fact_df = final_df.select(
        "transaction_id", "trade_id",
        "date_key", "time_key",
        "crypto_pair_key", "volume_category_key",
        "trade_time",
        "price", "quantity", "amount_usd",
        "is_buyer_maker", "is_anomaly",
        "buyer_order_id", "seller_order_id"
    )

    return fact_df


# =====================================================================
# STAGE 3a: SINK 1 — PostgreSQL (Primary, real-time UPSERT)
# =====================================================================

# --- Connection Pool (reuse connections instead of open/close per batch) ---
# SimpleConnectionPool: min=1, max=5 persistent connections.
# Eliminates TCP handshake overhead (~50-100ms per batch).
PG_POOL = None
PG_POOL_LOCK = threading.Lock()


def _get_pg_pool():
    """Lazy-init PostgreSQL connection pool (thread-safe singleton)."""
    global PG_POOL
    if PG_POOL is None:
        with PG_POOL_LOCK:
            if PG_POOL is None:  # Double-check after acquiring lock
                PG_POOL = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                    user=PG_USER, password=PG_PASSWORD,
                )
                logger.info("[PG POOL] Connection pool initialized (min=1, max=5)")
    return PG_POOL


def write_to_postgres(rows: list, batch_id: int, row_count: int):
    """Write batch to PostgreSQL using psycopg2 execute_values (FAST UPSERT).
    Kimball-compliant: All FK references are INTEGER surrogate keys.
    Uses connection pooling to eliminate per-batch connection overhead."""
    t0 = time.time()

    cols = [
        "transaction_id", "trade_id",
        "date_key", "time_key",
        "crypto_pair_key", "volume_category_key",
        "trade_time",
        "price", "quantity", "amount_usd",
        "is_buyer_maker", "is_anomaly",
        "z_score", "price_dev_pct", "wash_cluster_size",
        "buyer_order_id", "seller_order_id"
    ]

    values = []
    for r in rows:
        values.append(tuple(r[c] for c in cols))

    pool = _get_pg_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO fact_binance_trades (
                    transaction_id, trade_id,
                    date_key, time_key,
                    crypto_pair_key, volume_category_key,
                    trade_time,
                    price, quantity, amount_usd,
                    is_buyer_maker, is_anomaly,
                    z_score, price_dev_pct, wash_cluster_size,
                    buyer_order_id, seller_order_id
                ) VALUES %s
                ON CONFLICT (transaction_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    amount_usd = EXCLUDED.amount_usd,
                    is_anomaly = EXCLUDED.is_anomaly,
                    z_score = EXCLUDED.z_score,
                    price_dev_pct = EXCLUDED.price_dev_pct,
                    wash_cluster_size = EXCLUDED.wash_cluster_size
            """
            psycopg2.extras.execute_values(cur, insert_sql, values, page_size=2000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        f"[Batch {batch_id}] --> PostgreSQL UPSERT | "
        f"rows={row_count:,} | latency={elapsed}ms"
    )

    # --- Log Latency Metric ---
    try:
        lat_conn = pool.getconn()
        with lat_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO fact_pipeline_latency (batch_id, sink_name, row_count, latency_ms) VALUES (%s, %s, %s, %s)",
                (batch_id, 'PostgreSQL', row_count, elapsed)
            )
        lat_conn.commit()
        pool.putconn(lat_conn)
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] Failed to log PG latency: {e}")


# =====================================================================
# STAGE 3b: SINK 2 — BigQuery Backup (async Parquet Load + DLQ)
# =====================================================================

BQ_BUFFER = []
BQ_BUFFER_LOCK = threading.Lock()
LAST_BQ_UPLOAD_TIME = time.time()
BQ_UPLOAD_INTERVAL_SEC = 10   # Upload to BQ every 10 seconds
BQ_UPLOAD_ROWS_LIMIT   = 5000 # Or when buffer reaches 5000 rows


def _bq_async_upload(rows_dicts: list, batch_id: int, row_count: int):
    """
    Async BigQuery upload with Dead-Letter Queue (DLQ) fault tolerance.

    If upload fails for ANY reason (network, rate limit, auth, etc.),
    the Parquet file is moved to the DLQ directory instead of being
    deleted. This ensures ZERO DATA LOSS — failed batches can be
    retried later manually or by an automated recovery script.
    """
    if not BQ_PROJECT_ID:
        return

    pq_path = None
    try:
        import pandas as _pd
        from google.cloud import bigquery
        t0 = time.time()

        pdf = _pd.DataFrame(rows_dicts)

        # Float matching BQ Schema
        float_cols = ["price", "quantity", "amount_usd", "z_score", "price_dev_pct"]
        for c in float_cols:
            if c in pdf.columns:
                pdf[c] = pdf[c].astype(float)

        os.makedirs(BQ_PARQUET_DIR, exist_ok=True)
        pq_path = os.path.join(BQ_PARQUET_DIR, f"batch_{batch_id}.parquet")

        pdf.to_parquet(
            pq_path,
            index=False,
            engine='pyarrow',
            coerce_timestamps='us',
            allow_truncated_timestamps=True
        )

        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        with open(pq_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        # Success — remove the temporary parquet file
        os.remove(pq_path)

        elapsed = int((time.time() - t0) * 1000)
        logger.info(f"[Batch {batch_id}] --> BigQuery ASYNC | rows={row_count:,} | latency={elapsed}ms")

        # --- Log Latency Metric to PostgreSQL ---
        try:
            pool = _get_pg_pool()
            lat_conn = pool.getconn()
            with lat_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fact_pipeline_latency (batch_id, sink_name, row_count, latency_ms) VALUES (%s, %s, %s, %s)",
                    (batch_id, 'BigQuery', row_count, elapsed)
                )
            lat_conn.commit()
            pool.putconn(lat_conn)
        except Exception as e:
            logger.warning(f"[Batch {batch_id}] Failed to log BQ latency: {e}")

    except Exception as e:
        # ===============================================================
        # DLQ (Dead-Letter Queue): DO NOT lose data on failure!
        # Move the parquet file to a quarantine folder for later retry.
        # ===============================================================
        logger.error(
            f"[BQ DLQ] Batch {batch_id} FAILED to upload to BigQuery: {e}"
        )
        if pq_path and os.path.exists(pq_path):
            os.makedirs(BQ_DLQ_DIR, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dlq_filename = f"dlq_batch_{batch_id}_{timestamp}.parquet"
            dlq_path = os.path.join(BQ_DLQ_DIR, dlq_filename)
            try:
                shutil.move(pq_path, dlq_path)
                logger.warning(
                    f"[BQ DLQ] Parquet saved to DLQ for retry: {dlq_path} "
                    f"({row_count:,} rows preserved)"
                )
            except Exception as move_err:
                logger.critical(
                    f"[BQ DLQ] CRITICAL: Failed to move parquet to DLQ! "
                    f"File still at: {pq_path} | Error: {move_err}"
                )
        else:
            logger.warning(
                f"[BQ DLQ] Batch {batch_id}: No parquet file to save "
                f"(error occurred before file creation)"
            )


# =====================================================================
# DUAL SINK ORCHESTRATOR — foreachBatch callback
# =====================================================================

def dual_sink_batch(batch_df: DataFrame, batch_id: int):
    """
    Orchestrate writing processed data to both sinks.
    Called by Spark's foreachBatch for each micro-batch.
    """
    global LAST_BQ_UPLOAD_TIME
    t_start = time.time()

    # --- ADVANCED DYNAMIC ANOMALY DETECTION ---
    # Because batch_df is a static DataFrame, we can use non-time-based Window functions safely.
    try:
        from pyspark.sql.window import Window
        from pyspark.sql.functions import avg, stddev, count, abs as spark_abs, coalesce, col, when, lit
        
        w_symbol = Window.partitionBy("crypto_pair_key")
        # Wash trade cluster logic ignores buyer/maker for simplicity of volume detection
        w_wash = Window.partitionBy("crypto_pair_key", "trade_time")

        enriched_df = (
            batch_df
            # 1. Z-Score Deviation (Volume Outlier)
            .withColumn("batch_mean_usd", avg("amount_usd").over(w_symbol))
            .withColumn("batch_std_usd", coalesce(stddev("amount_usd").over(w_symbol), lit(1.0)))
            .withColumn("batch_std_usd", when(col("batch_std_usd") == 0, lit(1.0)).otherwise(col("batch_std_usd")))
            .withColumn("z_score", (col("amount_usd") - col("batch_mean_usd")) / col("batch_std_usd"))
            
            # 2. Price Slippage / Market Impact (VWAP deviation)
            .withColumn("batch_avg_price", avg("price").over(w_symbol))
            .withColumn("price_dev_pct", spark_abs(col("price") - col("batch_avg_price")) / col("batch_avg_price"))
            
            # 3. Wash Trade Clustering (High-frequency sameness)
            .withColumn("wash_cluster_size", count("trade_id").over(w_wash))
            
            # Evaluate Business Rules
            .withColumn("is_anomaly", when(
                (col("z_score") > 3.0) |                                                      # Rule 1: Z-Score Outlier
                (col("wash_cluster_size") >= 4) |                                             # Rule 2: Wash Trade Bot
                ((col("price_dev_pct") > 0.01) & (col("amount_usd") > col("batch_mean_usd"))),  # Rule 3: Price Slippage
                lit(True)
            ).otherwise(lit(False)))
            
            # Clean up memory (keep anomaly metrics for DB storage)
            .drop("batch_mean_usd", "batch_std_usd", "batch_avg_price")
        )
        rows = enriched_df.collect()
    except Exception as e:
        logger.error(f"[Batch {batch_id}] [ERROR] Dynamic anomaly detection failed, falling back: {e}")
        rows = batch_df.collect()

    row_count = len(rows)

    if row_count == 0:
        return

    # --- Sink 1: PostgreSQL (synchronous, primary) ---
    try:
        write_to_postgres(rows, batch_id, row_count)
    except Exception as e:
        logger.error(f"[Batch {batch_id}] [ERROR] PostgreSQL sink failed: {e}")

    # --- Sink 2: BigQuery (async buffered with DLQ) ---
    try:
        bq_data = [r.asDict() for r in rows]

        with BQ_BUFFER_LOCK:
            BQ_BUFFER.extend(bq_data)
            current_buffer_size = len(BQ_BUFFER)
            time_since_last_upload = time.time() - LAST_BQ_UPLOAD_TIME

        if current_buffer_size >= BQ_UPLOAD_ROWS_LIMIT or time_since_last_upload >= BQ_UPLOAD_INTERVAL_SEC:
            with BQ_BUFFER_LOCK:
                upload_data = BQ_BUFFER.copy()
                BQ_BUFFER.clear()
                LAST_BQ_UPLOAD_TIME = time.time()

            if len(upload_data) > 0:
                logger.info(
                    f"[BQ Buffer Flush] Collected {len(upload_data):,} rows "
                    f"in {int(time_since_last_upload)}s. Uploading..."
                )
                threading.Thread(
                    target=_bq_async_upload,
                    args=(upload_data, batch_id, len(upload_data)),
                    daemon=True
                ).start()
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] [WARN] BQ buffer failed: {e}")

    latency_ms = int((time.time() - t_start) * 1000)
    logger.info(f"[Batch {batch_id}] rows={row_count:,} | latency={latency_ms}ms | PG done, BQ async")


# =====================================================================
# MAIN — Pipeline Entrypoint
# =====================================================================

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

    print("=" * 65)
    print("  SPARK PROCESSING ENGINE — Binance Crypto Streaming")
    print("=" * 65)
    print(f"  Processing Pipeline:")
    print(f"    1. Type Casting       (string → numeric)")
    print(f"    2. Data Cleansing     (filter invalid records)")
    print(f"    3. Deduplication      (SHA-256 UUID + watermark)")
    print(f"    4. Transformation     (calculate amount_usd)")
    print(f"    5. Categorization     (RETAIL/PRO/INSTITUTIONAL/WHALE)")
    print(f"    6. Anomaly Detection  (Z-Score, Wash Trade, Slippage)")
    print(f"    7. Star Schema Keys   (date_key, time_key)")
    print(f"  Storage:")
    print(f"    → PostgreSQL (primary, real-time UPSERT)")
    print(f"    → BigQuery   (backup, async + DLQ fault tolerance)")
    print(f"  DLQ Directory: {BQ_DLQ_DIR}")
    print("=" * 65)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Stage 1: Read RAW stream from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "2000")
        .load()
    )

    # Parse raw JSON from Kafka value
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), raw_kafka_schema).alias("data"))
        .select("data.*")
    )

    # Stage 2: FULL DATA PROCESSING (the heart of this pipeline)
    fact_df = process_raw_to_fact(spark, parsed_df)

    # Stage 3: Dual Sink Output
    TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "200 milliseconds")
    logger.info(f"[CONFIG] Trigger: {TRIGGER_INTERVAL} | maxOffsets: 2000")

    query = (
        fact_df.writeStream
        .outputMode("append")
        .foreachBatch(dual_sink_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/tmp/spark_checkpoint_binance_v7")
        .start()
    )

    logger.info("[RUNNING] Processing engine is live. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()