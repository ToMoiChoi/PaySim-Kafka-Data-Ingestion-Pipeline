"""
processor/spark_processor.py - Dual-Sink Spark Structured Streaming
====================================================================
Luồng xử lý:
  1. Đọc stream từ Kafka topic `payment_events`.
  2. Parse JSON schema.
  3. Ép kiểu event_timestamp -> TimestampType.
  4. Watermark (5 phút) + dropDuplicates(transaction_id) - Kiểm soát trùng lặp.
  5. Tính reward_points = int(amount × 0.01).
  6. Transform -> Star Schema (fact_transactions).
  7. foreachBatch - Dual Sink:
       a. [PRIMARY]  Ghi vào PostgreSQL  (UPSERT qua JDBC, real-time).
       b. [BACKUP]   Ghi Parquet -> Load Job lên BigQuery (batch, miễn phí Sandbox).
"""

import os
import sys
import time
import random
import logging

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, expr, udf, when, current_timestamp, date_format,
    array, lit, rand, floor
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from dotenv import load_dotenv

# ─── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkDualSink")

# ─── Config ──────────────────────────────────────────────────────
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payment_events")

# PostgreSQL (Primary)
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# BigQuery (Backup)
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "")
BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")
BQ_TABLE_FACT = "fact_transactions"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
BQ_PARQUET_DIR = os.getenv("BQ_PARQUET_BACKUP_DIR", "/tmp/bq_backup")

# Location pool - 34 tỉnh/thành Việt Nam
LOCATION_IDS = [
    # Bắc
    "LOC_VN_HNI", "LOC_VN_HPG", "LOC_VN_HGG", "LOC_VN_CBG",
    "LOC_VN_LCI", "LOC_VN_LSN", "LOC_VN_TQG", "LOC_VN_TNG",
    "LOC_VN_BGG", "LOC_VN_PTH", "LOC_VN_BNH", "LOC_VN_HYN",
    "LOC_VN_HDG", "LOC_VN_NDH",
    # Trung
    "LOC_VN_THA", "LOC_VN_NAN", "LOC_VN_HTH", "LOC_VN_DNG",
    "LOC_VN_HUE", "LOC_VN_QNM", "LOC_VN_QNG", "LOC_VN_BDH",
    "LOC_VN_PYN", "LOC_VN_KHA", "LOC_VN_DLK", "LOC_VN_LDG",
    # Nam
    "LOC_VN_HCM", "LOC_VN_CTH", "LOC_VN_BPC", "LOC_VN_TNH",
    "LOC_VN_BDG", "LOC_VN_DNI", "LOC_VN_BVT",
]


# ─── Kafka Schema ────────────────────────────────────────────────    # 3. Phân tích cú pháp JSON
payment_schema = StructType([
    StructField("step",            IntegerType(), True),
    StructField("type",            StringType(),  True),
    StructField("amount",          DoubleType(),  True),
    StructField("nameOrig",        StringType(),  True),
    StructField("oldbalanceOrg",   DoubleType(),  True),
    StructField("newbalanceOrig",  DoubleType(),  True),
    StructField("nameDest",        StringType(),  True),
    StructField("oldbalanceDest",  DoubleType(),  True),
    StructField("newbalanceDest",  DoubleType(),  True),
    StructField("isFraud",         IntegerType(), True),
    StructField("isFlaggedFraud",  IntegerType(), True),
    StructField("transaction_id",  StringType(),  True),
    StructField("event_timestamp", StringType(),  True),
    StructField("account_id",      StringType(),  True),
    StructField("channel_id",      StringType(),  True),
    StructField("ip_address",      StringType(),  True)
])


# ─── Spark Session ───────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    # Set Hadoop Home for Windows environment explicitly
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    # To fix potential Winutils missing dll error:
    import sys
    if sys.platform.startswith('win'):
        os.environ['PATH'] = os.environ['PATH'] + ';' + r'C:\hadoop\bin'
        # Fix BlockManagerId NullPointerException on Windows
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

    builder = (
        SparkSession.builder
        .appName("PaySim_DualSink_Streaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "3g")  # <-- Tăng lên 3g để chứa broadcast dim_users
        .config("spark.executor.memory", "3g") # <-- Tăng lên 3g
        .config("spark.memory.fraction", "0.6") # Giữ heap memory gọn nhẹ
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Fix BlockManagerId NullPointerException on Windows
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    return builder.getOrCreate()


def build_fact_df(spark: SparkSession, parsed_df: DataFrame) -> DataFrame:

    # 1. Cast event_timestamp string to native TimestampType
    typed_df = parsed_df.withColumn(
        "event_timestamp", expr("to_timestamp(event_timestamp)")
    )

    # 2. Watermark & Drop Duplicates (Stateful Streaming)
    # Khử trùng lặp các giao dịch gửi đúp (cùng transaction_id) trong window 5 phút
    clean_df = (
        typed_df
        .withWatermark("event_timestamp", "5 minutes")
        .dropDuplicates(["transaction_id"])
    )

    # 3. Data Formatting & Type Casting
    base_df = (
        clean_df
        .withColumnRenamed("nameOrig",  "user_id")
        .withColumnRenamed("nameDest",  "merchant_id")
        .withColumn("transaction_time",
            when(col("event_timestamp").isNull(), current_timestamp())
            .otherwise(col("event_timestamp"))
        )
        # Sinh date_key (yyyyMMdd)
        .withColumn("date_key", date_format(col("transaction_time"), "yyyyMMdd").cast("long"))
        # Sinh time_key (Thí dụ 14:30 -> 1430)
        .withColumn("time_key", (expr("hour(transaction_time)") * 100 + expr("minute(transaction_time)")).cast("long"))
        .withColumn("amount",          col("amount").cast("decimal(38,9)"))
        .withColumn("oldbalanceOrg",   col("oldbalanceOrg").cast("decimal(38,9)"))
        .withColumn("newbalanceOrig",  col("newbalanceOrig").cast("decimal(38,9)"))
        .withColumn("oldbalanceDest",  col("oldbalanceDest").cast("decimal(38,9)"))
        .withColumn("newbalanceDest",  col("newbalanceDest").cast("decimal(38,9)"))
        # Type ID mapping: Hỗ trợ cả batch (CSV) và live (Binance) types
        .withColumn("type_id",
            # --- Legacy types (từ CSV PaySim batch) ---
            when(col("type") == "PAYMENT",  "TYP_PAYMENT")
            .when(col("type") == "TRANSFER","TYP_TRANSFER")
            .when(col("type") == "CASH_OUT","TYP_CASH_OUT")
            .when(col("type") == "DEBIT",   "TYP_DEBIT")
            .when(col("type") == "CASH_IN", "TYP_CASH_IN")
            # --- Crypto types (từ Binance live stream) ---
            .when(col("type") == "SPOT_BUY",    "TYP_SPOT_BUY")
            .when(col("type") == "SPOT_SELL",   "TYP_SPOT_SELL")
            .when(col("type") == "LARGE_TRADE", "TYP_LARGE_TRADE")
            .when(col("type") == "MICRO_TRADE", "TYP_MICRO_TRADE")
            .when(col("type") == "WHALE_ALERT", "TYP_WHALE_ALERT")
            .otherwise("TYP_PAYMENT")
        )
    )

    # 4. Stream-Static Joins (Tích hợp Dữ liệu Tham chiếu)
    from pyspark.sql.functions import broadcast

    # Lấy thông tin Hạng người dùng (User Segment) từ PostgreSQL
    # Cache để không đọc lại JDBC mỗi micro-batch
    dim_users = (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL).option("dbtable", "dim_users")
        .option("user", PG_USER).option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver").load()
        .select("user_id", "user_segment")
        .cache()
    )
    dim_users.count()  # Force materialization vào memory
    logger.info(f"[CACHE] dim_users loaded: {dim_users.count():,} rows")

    # Lấy Hệ số Thưởng - Broadcast vì chỉ 10 rows
    dim_txn_type = broadcast(
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL).option("dbtable", "dim_transaction_type")
        .option("user", PG_USER).option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver").load()
        .select("type_id", "reward_multiplier")
    )

    # Nối (Join) luồng stream với batch data
    joined_df = base_df.join(dim_users, on="user_id", how="left")
    joined_df = joined_df.join(dim_txn_type, on="type_id", how="left")

    # 5. Logic Nghiệp vụ Core: Tính Điểm Thưởng (Reward Points Calculation)
    # Điểm thưởng = Số tiền * Hệ số loại GD * Hệ số hạng khách hàng
    # Standard x1, Gold x1.5, Diamond x2
    fact_df = joined_df.withColumn(
        "user_multiplier",
        when(col("user_segment") == "Diamond", 2.0)
        .when(col("user_segment") == "Gold", 1.5)
        .otherwise(1.0)
    ).withColumn(
        "reward_points",
        (col("amount") * col("reward_multiplier") * col("user_multiplier")).cast("long")
    )

    # 6. Gán Location ngẫu nhiên (chỉ để phục vụ Map Dashboard nếu chưa có trong source)
    
    # Create an array column of location IDs
    loc_array = array(*[lit(loc) for loc in LOCATION_IDS])
    loc_count = len(LOCATION_IDS)
    
    final_fact_df = fact_df.withColumn(
        "location_id", 
        loc_array.getItem(floor(rand() * loc_count).cast("int"))
    ).select(
        "transaction_id", "account_id", "merchant_id",
        "type_id", "location_id", "channel_id", "date_key", "time_key",
        "transaction_time", "amount", "reward_points",
        "type", "step", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud", "ip_address", "user_id"
    )

    return final_fact_df


# ─── Sink 1: PostgreSQL (Primary, real-time) ─────────────────────
def write_to_postgres(batch_df: DataFrame, batch_id: int, row_count: int):
    """Ghi batch vào PostgreSQL bằng psycopg2 execute_values (FAST UPSERT)."""
    t0 = time.time()

    # Collect rows từ Spark về Python driver (OK cho batch nhỏ ≤ 500 rows)
    rows = batch_df.collect()

    # Cột theo thứ tự schema fact_transactions
    cols = [
        "transaction_id", "account_id", "merchant_id", "type_id",
        "location_id", "channel_id", "date_key", "time_key",
        "transaction_time", "amount", "reward_points", "type", "step",
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud", "ip_address", "user_id"
    ]

    # Chuyển Spark Row → tuple
    values = []
    for r in rows:
        values.append(tuple(r[c] for c in cols))

    # Direct UPSERT bằng psycopg2.extras.execute_values
    import psycopg2.extras
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    with conn.cursor() as cur:
        insert_sql = """
            INSERT INTO fact_transactions (
                transaction_id, account_id, merchant_id, type_id,
                location_id, channel_id, date_key, time_key,
                transaction_time, amount, reward_points, type, step,
                "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
                "isFraud", "isFlaggedFraud", ip_address, user_id
            ) VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                amount       = EXCLUDED.amount,
                reward_points = EXCLUDED.reward_points,
                transaction_time = EXCLUDED.transaction_time
        """
        psycopg2.extras.execute_values(cur, insert_sql, values, page_size=500)
    conn.commit()
    conn.close()

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        f"[Batch {batch_id}] --> PostgreSQL UPSERT | "
        f"rows={row_count:,} | latency={elapsed}ms"
    )


# ─── Sink 2: BigQuery Backup (batch Parquet -> Load Job) ──────────
def write_to_bq_backup(batch_df: DataFrame, batch_id: int, row_count: int):
    """Ghi Parquet file -> BigQuery Load Job (miễn phí Sandbox)."""
    if not BQ_PROJECT_ID:
        logger.warning("[BQ Backup] BQ_PROJECT_ID trống -> skip BigQuery backup.")
        return

    t0 = time.time()
    parquet_path = f"{BQ_PARQUET_DIR}/batch_{batch_id}"

    # Cast decimal -> double để khớp BigQuery FLOAT64 schema
    from pyspark.sql.types import DecimalType, DoubleType
    for field in batch_df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            batch_df = batch_df.withColumn(field.name, col(field.name).cast(DoubleType()))

    # Ghi Parquet ra disk (nhiều part-file)
    batch_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)

    # Dùng google-cloud-bigquery để load TẤT CẢ file lên BigQuery
    try:
        from google.cloud import bigquery
        import glob

        client   = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"

        # Lấy danh sách TẤT CẢ file Parquet vừa ghi
        parquet_files = glob.glob(f"{parquet_path}/*.parquet")
        if not parquet_files:
            logger.warning(f"[BQ Backup] Batch {batch_id}: Không có file Parquet để load.")
            return

        logger.info(f"[BQ Backup] Batch {batch_id}: Đang load {len(parquet_files)} parquet file(s) lên BigQuery...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        # Load từng file một (vì Sandbox không hỗ trợ multi-file URI)
        total_loaded = 0
        for pf in parquet_files:
            with open(pf, "rb") as f:
                job = client.load_table_from_file(f, table_id, job_config=job_config)
                job.result()  # chờ load xong
                total_loaded += 1

        elapsed = int((time.time() - t0) * 1000)
        logger.info(
            f"[Batch {batch_id}] -->  BigQuery backup | "
            f"files={total_loaded} | rows={row_count:,} | latency={elapsed}ms"
        )
    except Exception as e:
        logger.warning(f"[BQ Backup] Batch {batch_id}: Lỗi backup -> {e}")


# ─── foreachBatch callback (Dual Sink) ───────────────────────────
# BigQuery backup chạy ASYNC trong background thread
import threading

def dual_sink_batch(batch_df: DataFrame, batch_id: int):
    row_count = batch_df.count()
    if row_count == 0:
        return

    t_start = time.time()
    batch_df.cache()

    # Sink 1: PostgreSQL (primary, real-time)
    try:
        write_to_postgres(batch_df, batch_id, row_count)
    except Exception as e:
        logger.error(f"[Batch {batch_id}] [ERRO] PostgreSQL sink lỗi: {e}")

    # Sink 2: BigQuery — collect rồi chạy async thread
    try:
        bq_data = [r.asDict() for r in batch_df.collect()]
        threading.Thread(
            target=_bq_async_upload,
            args=(bq_data, batch_id, row_count),
            daemon=True
        ).start()
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] [WARN] BQ collect lỗi: {e}")

    batch_df.unpersist()
    latency_ms = int((time.time() - t_start) * 1000)
    logger.info(f"[Batch {batch_id}] rows={row_count:,} | latency={latency_ms}ms | PG done, BQ async")


def _bq_async_upload(rows_dicts, batch_id, row_count):
    """Background: pandas → Parquet → BigQuery Load Job (Sandbox compatible)."""
    if not BQ_PROJECT_ID:
        return
    try:
        import pandas as _pd
        from google.cloud import bigquery
        t0 = time.time()

        pdf = _pd.DataFrame(rows_dicts)
        os.makedirs(BQ_PARQUET_DIR, exist_ok=True)
        pq_path = os.path.join(BQ_PARQUET_DIR, f"batch_{batch_id}.parquet")
        pdf.to_parquet(pq_path, index=False)

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
        os.remove(pq_path)

        elapsed = int((time.time() - t0) * 1000)
        logger.info(f"[Batch {batch_id}] --> BigQuery ASYNC | rows={row_count:,} | latency={elapsed}ms")
    except Exception as e:
        logger.warning(f"[BQ Async] Batch {batch_id}: {e}")


# ─── Main ────────────────────────────────────────────────────────

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
        
    print("=" * 65)
    print("--> PaySim Dual-Sink Streaming (PostgreSQL + BigQuery backup)")
    print(f"--> Kafka  : {KAFKA_BOOTSTRAP_SERVERS}  |  Topic: {KAFKA_TOPIC}")
    print(f"--> PG     : {PG_HOST}:{PG_PORT}/{PG_DB}")
    if BQ_PROJECT_ID:
        print(f"--> BQ     : {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}  (backup)")
    else:
        print("--> BigQuery chưa cấu hình -> chỉ ghi PostgreSQL")
    print("=" * 65)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc Kafka stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "500")   # Giới hạn nhỏ → batch nhỏ → latency thấp
        .load()
    )

    # 2. Parse JSON
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), payment_schema).alias("data"))
        .select("data.*")
    )

    # 3. Transform & Stream-Static Join
    fact_df = build_fact_df(spark, parsed_df)

    # 4. Dual Sink via foreachBatch — Trigger mỗi 200ms cho near real-time
    TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
    logger.info(f"[CONFIG] Trigger: {TRIGGER_INTERVAL} | maxOffsets: 500 | BQ backup mỗi {BQ_BACKUP_INTERVAL} batches")

    query = (
        fact_df.writeStream
        .outputMode("append")
        .foreachBatch(dual_sink_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/tmp/spark_checkpoint_dual_sink_v3")
        .start()
    )

    logger.info("[DONE] Stream đang chạy. Nhấn Ctrl+C để dừng.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
