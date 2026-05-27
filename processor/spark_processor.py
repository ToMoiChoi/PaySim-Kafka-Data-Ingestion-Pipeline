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
Output schema matches: fact_binance_trades_v2 (BigQuery & PostgreSQL)
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
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(ENV_PATH)

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
BQ_TABLE_FACT = "fact_binance_trades_v2"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

if GOOGLE_APPLICATION_CREDENTIALS and not os.path.isabs(GOOGLE_APPLICATION_CREDENTIALS):
    GOOGLE_APPLICATION_CREDENTIALS = os.path.join(PROJECT_ROOT, GOOGLE_APPLICATION_CREDENTIALS)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

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
    os.environ["HADOOP_HOME"] = r"C:\Users\Admin\.hadoop"
    if "JAVA_HOME" not in os.environ or "jdk-17" not in os.environ["JAVA_HOME"]:
        os.environ["JAVA_HOME"] = r"C:\Users\Admin\.java\jdk-17.0.19+10"
        
    if sys.platform.startswith('win'):
        os.environ['PATH'] = os.environ.get('PATH', '') + ';' + r'C:\Users\Admin\.hadoop\bin' + ';' + r'C:\Users\Admin\.java\jdk-17.0.19+10\bin'
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
    # Step 1: SCHEMA ENFORCEMENT & TYPE CASTING (Ràng buộc lược đồ dữ liệu)
    # Chuyển đổi định dạng Price/Quantity từ chuỗi String của Binance sang Double
    # Quy đổi epoch time (milliseconds) sang định dạng Timestamp chuẩn SQL.
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
    logger.info("[PROCESSING] Step 1: Schema enforcement & type casting completed")

    # -----------------------------------------------------------------
    # Step 2: DATA QUALITY GUARD (Làm sạch và kiểm soát chất lượng dữ liệu)
    # Loại bỏ các bản ghi nhiễu, lỗi mạng, giá trị trống (Null/NaN/Negative)
    # Đảm bảo tính toàn vẹn dữ liệu đầu vào trước khi thực hiện phân tích sâu.
    # -----------------------------------------------------------------
    clean_df = (
        typed_df
        .filter(col("trade_id").isNotNull())
        .filter(col("crypto_symbol").isNotNull() & (col("crypto_symbol") != ""))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .filter(col("trade_time").isNotNull())
    )
    logger.info("[PROCESSING] Step 2: Data cleansing (Data Quality Guard) completed")

    # -----------------------------------------------------------------
    # Step 3: DEDUPLICATION LAYER 1 (Khử trùng lặp đa tầng - Spark Stateful)
    # Sử dụng khóa ghép định danh tự nhiên transaction_id (Symbol + TradeID)
    # Áp dụng cơ chế Watermark 30 giây để kiểm soát dữ liệu trễ trong RAM (State Store)
    # Kết hợp dropDuplicates nhằm loại bỏ các thông điệp bị gửi lặp lại.
    # -----------------------------------------------------------------
    dedup_df = (
        clean_df
        .withColumn(
            "transaction_id",
            concat_ws("_", col("crypto_symbol"), col("trade_id").cast("string"))
        )
        .withWatermark("trade_time", "30 seconds")
        .dropDuplicates(["transaction_id"])
    )
    logger.info("[PROCESSING] Step 3: Deduplication Layer 1 (Stateful Spark) completed")

    # -----------------------------------------------------------------
    # Step 4: DATA ENRICHMENT & TRANSFORMATION (Làm giàu và biến đổi dữ liệu)
    # Tính toán chỉ số nghiệp vụ phát sinh: Tổng giá trị giao dịch bằng USD (amount_usd)
    # -----------------------------------------------------------------
    transform_df = dedup_df.withColumn(
        "amount_usd",
        spark_round(col("price") * col("quantity"), 2)
    )
    logger.info("[PROCESSING] Step 4: Transformation (amount_usd computation) completed")

    # -----------------------------------------------------------------
    # Step 5: VOLUME CATEGORIZATION (Kimball Surrogate Key Mapping - Phân loại volume)
    # Phân hạng quy mô giao dịch theo các ngưỡng chuẩn ngành (Retail -> Whale)
    # Ánh xạ kết quả sang Khóa thay thế (Surrogate Key) từ 1 đến 4 thay vì lưu chuỗi text
    # Nhằm tối ưu hóa hiệu năng lưu trữ và đẩy nhanh tốc độ truy vấn JOIN.
    # -----------------------------------------------------------------
    categorized_df = transform_df.withColumn(
        "volume_category_key",
        when(col("amount_usd") >= 1_000_000, lit(4))   # Khóa 4: WHALE (Cá voi - Giao dịch lớn)
        .when(col("amount_usd") >= 100_000,  lit(3))   # Khóa 3: INSTITUTIONAL (Tổ chức)
        .when(col("amount_usd") >= 10_000,   lit(2))   # Khóa 2: PROFESSIONAL (Chuyên nghiệp)
        .otherwise(lit(1))                              # Khóa 1: RETAIL (Nhỏ lẻ)
    )
    logger.info("[PROCESSING] Step 5: Volume categorization & Surrogate Key mapping completed")

    # -----------------------------------------------------------------
    # Step 6: ANOMALY DETECTION — Shifted to Micro-Batch
    # Áp dụng bộ khung lý thuyết từ Chandola và các cộng sự (2009):
    # Phát hiện 3 loại bất thường (Point, Contextual, Collective Anomalies)
    # bằng phương pháp thống kê động (Statistical-based).
    # Do Spark Structured Streaming không cho phép tính toán hàm Window phi thời gian
    # trực tiếp trên DataFrame luồng, logic này được dời vào hàm foreachBatch
    # (xử lý trên DataFrame lô tĩnh). Tại đây chỉ khai báo cột để giữ cấu trúc.
    # -----------------------------------------------------------------
    anomaly_df = categorized_df.withColumn("is_anomaly", lit(False))
    logger.info("[PROCESSING] Step 6: Pre-allocation for dynamic Anomaly Detection (Chandola et al., 2009) completed")

    # -----------------------------------------------------------------
    # Step 7: DIMENSIONAL MODELING SURROGATE KEYS (Tạo các khóa chiều Star Schema)
    # Sinh các khóa thời gian (date_key, time_key) dưới dạng số nguyên thông minh (Smart Keys)
    # Ánh xạ crypto_symbol (Natural Key) sang crypto_pair_key (Surrogate Key 1-5)
    # -----------------------------------------------------------------
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
        .withColumn("date_key", date_format(col("trade_time"), "yyyyMMdd").cast("long"))
        .withColumn("time_key", (expr("hour(trade_time)") * 100 + expr("minute(trade_time)")).cast("long"))
        .withColumn("crypto_pair_key", crypto_sk_map[col("crypto_symbol")].cast("int"))
        .withColumn("price",      col("price").cast("decimal(38,9)"))
        .withColumn("quantity",   col("quantity").cast("decimal(38,9)"))
        .withColumn("amount_usd", col("amount_usd").cast("decimal(38,9)"))
    )
    logger.info("[PROCESSING] Step 7: Star Schema surrogate keys alignment completed")

    # -----------------------------------------------------------------
    # Final SELECT — Cấu trúc dữ liệu chuẩn Fact Table (Kimball Fact Schema)
    # Chỉ lưu trữ Khóa thay thế (Surrogate Keys) và các Chỉ số đo lường (Metrics).
    # Không lưu trữ thông tin văn bản thô (Natural Keys) ngoại trừ Degenerate Dimension (transaction_id).
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
    """
    Khởi tạo PostgreSQL Connection Pool theo mẫu thiết kế Singleton (Thread-Safe).
    - Tạo sẵn từ 1 đến 5 kết nối tồn tại lâu dài (Persistent Connections).
    - Triệt tiêu hoàn toàn chi phí bắt tay TCP Handshake (~50-100ms) ở mỗi micro-batch.
    """
    global PG_POOL
    if PG_POOL is None:
        with PG_POOL_LOCK:
            if PG_POOL is None:  # Double-check sau khi acquire lock
                PG_POOL = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                    user=PG_USER, password=PG_PASSWORD,
                )
                logger.info("[PG POOL] Connection pool initialized (min=1, max=5)")
    return PG_POOL


def write_to_postgres(rows: list, batch_id: int, row_count: int):
    """
    Ghi dữ liệu micro-batch vào PostgreSQL sử dụng cơ chế Fast UPSERT (psycopg2 execute_values).
    - Tuân thủ chuẩn thiết kế Kimball: Các khóa ngoại tham chiếu đều là số nguyên (Surrogate Keys).
    - Sử dụng Connection Pool để lấy và trả kết nối nhanh chóng.
    - Cơ chế UPSERT (ON CONFLICT DO UPDATE):
      Đóng vai trò là chốt chặn khử trùng lặp lớp thứ 2 ở mức lưu trữ (Storage Deduplication Layer).
      Ngăn ngừa tuyệt đối trùng lặp dữ liệu ngay cả khi sự kiện trễ trôi ra ngoài cửa sổ Watermark của Spark.
    """
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

    # --- Log chỉ số độ trễ ghi (Latency Metrics) phục vụ giám sát hiệu năng ---
    try:
        lat_conn = pool.getconn()
        try:
            with lat_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fact_pipeline_latency (batch_id, sink_name, row_count, latency_ms) VALUES (%s, %s, %s, %s)",
                    (batch_id, 'PostgreSQL', row_count, elapsed)
                )
            lat_conn.commit()
        except Exception as inner_e:
            lat_conn.rollback()
            logger.warning(f"[Batch {batch_id}] Failed to log PG latency: {inner_e}")
        finally:
            pool.putconn(lat_conn)
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] Failed to get PG connection for latency log: {e}")


# =====================================================================
# STAGE 3b: SINK 2 — BigQuery Backup (async Parquet Load + DLQ)
# =====================================================================

BQ_BUFFER = []
BQ_BUFFER_LOCK = threading.Lock()
LAST_BQ_UPLOAD_TIME = time.time()
BQ_UPLOAD_INTERVAL_SEC = 10   # Khoảng thời gian flush đệm lên BigQuery (10 giây)
BQ_UPLOAD_ROWS_LIMIT   = 5000 # Kích thước đệm tối đa trước khi tự động đẩy (5000 bản ghi)


def _bq_async_upload(rows_dicts: list, batch_id: int, row_count: int):
    """
    Ghi dữ liệu bất đồng bộ lên BigQuery sử dụng định dạng nén Parquet và cơ chế đệm.
    - Lưu tạm thời dữ liệu của micro-batch thành tệp Parquet cục bộ (nén dạng cột PyArrow).
    - Gọi BigQuery Load Job đẩy tệp Parquet trực tiếp (tối ưu hóa tần suất API và giảm chi phí Cloud GCP).
    - Tích hợp Dead-Letter Queue (DLQ):
      Nếu quá trình tải lên thất bại do mạng ngoại vi hoặc lỗi xác thực xác quyền,
      file Parquet sẽ được di chuyển sang thư mục cách ly lỗi `dlq_bq_failed`
      để phục vụ khôi phục thủ công bằng script `retry_dlq_to_bq.py` sau này,
      đảm bảo tính chống thất thoát dữ liệu tuyệt đối (Fault Tolerance).
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
            try:
                with lat_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO fact_pipeline_latency (batch_id, sink_name, row_count, latency_ms) VALUES (%s, %s, %s, %s)",
                        (batch_id, 'BigQuery', row_count, elapsed)
                    )
                lat_conn.commit()
            except Exception as inner_e:
                lat_conn.rollback()
                logger.warning(f"[Batch {batch_id}] Failed to log BQ latency: {inner_e}")
            finally:
                pool.putconn(lat_conn)
        except Exception as e:
            logger.warning(f"[Batch {batch_id}] Failed to get PG connection for BQ latency log: {e}")

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

    # --- ADVANCED DYNAMIC ANOMALY DETECTION (Chandola et al., 2009 Taxonomy) ---
    # Sử dụng phương pháp Statistical-based (Dựa trên thống kê) trên micro-batch tĩnh
    try:
        from pyspark.sql.window import Window
        from pyspark.sql.functions import avg, stddev, count, abs as spark_abs, coalesce, col, when, lit
        
        w_symbol = Window.partitionBy("crypto_pair_key")
        w_wash = Window.partitionBy("crypto_pair_key", "trade_time")

        enriched_df = (
            batch_df
            .withColumn("batch_count", count("trade_id").over(w_symbol))
            
            # 1. Z-Score Deviation (Volume Outlier - Point Anomaly)
            .withColumn("batch_mean_usd", avg("amount_usd").over(w_symbol))
            .withColumn("batch_std_usd", coalesce(stddev("amount_usd").over(w_symbol), lit(1.0)))
            .withColumn("batch_std_usd", when(col("batch_std_usd") == 0, lit(1.0)).otherwise(col("batch_std_usd")))
            .withColumn("z_score", when(col("batch_count") > 10, 
                                         (col("amount_usd") - col("batch_mean_usd")) / col("batch_std_usd"))
                                   .otherwise(lit(0.0)))
            # 2. Price Slippage / Market Impact (Contextual Anomaly)
            .withColumn("batch_avg_price", avg("price").over(w_symbol))
            .withColumn("price_dev_pct", spark_abs(col("price") - col("batch_avg_price")) / col("batch_avg_price"))
            # 3. Wash Trade Clustering (Collective Anomaly)
            .withColumn("wash_cluster_size", count("trade_id").over(w_wash))
            # Gắn cờ Anomaly theo các giả định phân phối dữ liệu từ Chandola (2009)
            .withColumn("is_anomaly", when(
                # --- NHÓM 1: POINT ANOMALIES (Bất thường điểm đơn lẻ) ---
                (col("amount_usd") >= 1000000) |                                                   # Whale Alert (> 1M USD)
                (col("amount_usd") < 0.01) |                                                       # Dust Trade (< 0.01 USD)
                ((col("batch_count") > 30) & (col("z_score") > 3.0)) |                             # Z-Score Outlier (3-Sigma)
                
                # --- NHÓM 2: COLLECTIVE ANOMALIES (Bất thường nhóm/tập hợp) ---
                (col("wash_cluster_size") >= 4) |                                                  # Bot Wash Trade (Hành vi tần suất cao)
                
                # --- NHÓM 3: CONTEXTUAL ANOMALIES (Bất thường ngữ cảnh trượt giá) ---
                ((col("batch_count") > 30) & (col("price_dev_pct") > 0.01) & (col("amount_usd") > col("batch_mean_usd"))), # Trượt giá lớn đi kèm khối lượng cao trong micro-batch
                lit(True)
            ).otherwise(lit(False)))
            
            # Clean up memory (keep anomaly metrics for DB storage, drop temporary fields including batch_count)
            .drop("batch_mean_usd", "batch_std_usd", "batch_avg_price", "batch_count")
        )
        logger.info(f"[Batch {batch_id}] Dynamic anomaly detection applied (Z-Score, Wash Trade, Slippage)")
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
    print(f"    3. Deduplication      (Symbol_TradeID + watermark)")
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