"""
processor/spark_processor.py - Dual-Sink Spark Structured Streaming
====================================================================
Streaming data pipeline designed for processing Binance WebSocket data:
  1. Read stream from Kafka topic `payment_events_v3`.
  2. Parse the native JSON schema (no fake data).
  3. Watermark (5 minutes) + dropDuplicates(transaction_id) -- natural deduplication.
  4. Transform -> Schema fact_binance_trades.
  5. foreachBatch - Dual Sink:
       a. [PRIMARY]  Write to PostgreSQL  (UPSERT via JDBC, real-time).
       b. [BACKUP]   Write Parquet -> Load Job to BigQuery (batch, free Sandbox).
"""

import os
import sys
import time
import logging

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, expr, when, current_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, LongType
)
from dotenv import load_dotenv

# --- Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkDualSink")

# --- Config 
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payment_events_v3")

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
BQ_TABLE_FACT = "fact_binance_trades"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
BQ_PARQUET_DIR = os.getenv("BQ_PARQUET_BACKUP_DIR", "/tmp/bq_backup")

# --- Kafka Schema
binance_schema = StructType([
    StructField("transaction_id",  StringType(),  True),
    StructField("trade_id",        LongType(),    True),
    StructField("crypto_symbol",   StringType(),  True),
    StructField("event_timestamp", StringType(),  True),
    StructField("price",           DoubleType(),  True),
    StructField("quantity",        DoubleType(),  True),
    StructField("amount_usd",      DoubleType(),  True),
    StructField("is_buyer_maker",  BooleanType(), True),
    StructField("volume_category", StringType(),  True),
    StructField("is_anomaly",      BooleanType(), True),
    StructField("buyer_order_id",  LongType(),    True),
    StructField("seller_order_id", LongType(),    True)
])


# --- Spark Session 
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
        .appName("Binance_Crypto_Streaming")
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
        # Fix BlockManagerId NullPointerException on Windows
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    return builder.getOrCreate()


def build_fact_df(spark: SparkSession, parsed_df: DataFrame) -> DataFrame:
    """
    Transform raw data into the storage schema.
    Includes automatic deduplication control based on Idempotent UUID.
    """
    # 1. Cast event_timestamp string to native TimestampType
    typed_df = parsed_df.withColumn(
        "event_timestamp", expr("to_timestamp(event_timestamp)")
    )

    # 2. Watermark & Drop Duplicates (Stateful Streaming)
    # Natural deduplication when Binance replays old trades on reconnect.
    # Since transaction_id is generated deterministically from trade_id, the UUID is always identical.
    clean_df = (
        typed_df
        .withWatermark("event_timestamp", "5 minutes")
        .dropDuplicates(["transaction_id"])
    )

    # 3. Data Formatting & Type Casting
    base_df = (
        clean_df
        .withColumn("trade_time",
            when(col("event_timestamp").isNull(), current_timestamp())
            .otherwise(col("event_timestamp"))
        )
        # Generate date_key (yyyyMMdd)
        .withColumn("date_key", date_format(col("trade_time"), "yyyyMMdd").cast("long"))
        # Generate time_key (e.g. 14:30 -> 1430)
        .withColumn("time_key", (expr("hour(trade_time)") * 100 + expr("minute(trade_time)")).cast("long"))
        .withColumn("price",      col("price").cast("decimal(38,9)"))
        .withColumn("quantity",   col("quantity").cast("decimal(38,9)"))
        .withColumn("amount_usd", col("amount_usd").cast("decimal(38,9)"))
    )

    # 4. Select final columns for fact_binance_trades
    final_fact_df = base_df.select(
        "transaction_id", "trade_id", "crypto_symbol",
        "date_key", "time_key", "trade_time", 
        "price", "quantity", "amount_usd", 
        "is_buyer_maker", "volume_category", "is_anomaly",
        "buyer_order_id", "seller_order_id"
    )

    return final_fact_df


# --- Sink 1: PostgreSQL (Primary, real-time) --------------------------
def write_to_postgres(rows: list, batch_id: int, row_count: int):
    """Write batch to PostgreSQL using psycopg2 execute_values (FAST UPSERT)."""
    t0 = time.time()

    # Columns in fact_binance_trades order
    cols = [
        "transaction_id", "trade_id", "crypto_symbol",
        "date_key", "time_key", "trade_time",
        "price", "quantity", "amount_usd",
        "is_buyer_maker", "volume_category", "is_anomaly",
        "buyer_order_id", "seller_order_id"
    ]

    values = []
    for r in rows:
        values.append(tuple(r[c] for c in cols))

    import psycopg2.extras
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    with conn.cursor() as cur:
        insert_sql = """
            INSERT INTO fact_binance_trades (
                transaction_id, trade_id, crypto_symbol,
                date_key, time_key, trade_time,
                price, quantity, amount_usd,
                is_buyer_maker, volume_category, is_anomaly,
                buyer_order_id, seller_order_id
            ) VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                price = EXCLUDED.price,
                quantity = EXCLUDED.quantity,
                amount_usd = EXCLUDED.amount_usd,
                is_anomaly = EXCLUDED.is_anomaly
        """
        psycopg2.extras.execute_values(cur, insert_sql, values, page_size=500)
    conn.commit()
    conn.close()

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        f"[Batch {batch_id}] --> PostgreSQL UPSERT | "
        f"rows={row_count:,} | latency={elapsed}ms"
    )


# --- Sink 2: BigQuery Backup (batch Parquet -> Load Job) --------------
import threading

BQ_BUFFER = []
BQ_BUFFER_LOCK = threading.Lock()
LAST_BQ_UPLOAD_TIME = time.time()
BQ_UPLOAD_INTERVAL_SEC = 10  # Upload to BQ every 10 seconds
BQ_UPLOAD_ROWS_LIMIT = 5000  # Or when the buffer reaches 5000 rows

def dual_sink_batch(batch_df: DataFrame, batch_id: int):
    global LAST_BQ_UPLOAD_TIME
    t_start = time.time()
    
    rows = batch_df.collect()
    row_count = len(rows)

    if row_count == 0:
        return

    # Sink 1: PostgreSQL 
    try:
        write_to_postgres(rows, batch_id, row_count)
    except Exception as e:
        logger.error(f"[Batch {batch_id}] [ERROR] PostgreSQL sink failed: {e}")

    # Sink 2: BigQuery Async 
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
                logger.info(f"[BQ Buffer Flush] Collected {len(upload_data):,} rows in {int(time_since_last_upload)} sec. Uploading...")
                threading.Thread(
                    target=_bq_async_upload,
                    args=(upload_data, batch_id, len(upload_data)),
                    daemon=True
                ).start()
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] [WARN] BQ mapping failed: {e}")

    latency_ms = int((time.time() - t_start) * 1000)
    logger.info(f"[Batch {batch_id}] rows={row_count:,} | latency={latency_ms}ms | PG done, BQ async")


def _bq_async_upload(rows_dicts, batch_id, row_count):
    if not BQ_PROJECT_ID:
        return
    try:
        import pandas as _pd
        from google.cloud import bigquery
        t0 = time.time()

        pdf = _pd.DataFrame(rows_dicts)
        
        # Float matching BQ Schema
        float_cols = ["price", "quantity", "amount_usd"]
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
        os.remove(pq_path)

        elapsed = int((time.time() - t0) * 1000)
        logger.info(f"[Batch {batch_id}] --> BigQuery ASYNC | rows={row_count:,} | latency={elapsed}ms")
    except Exception as e:
        logger.warning(f"[BQ Async] Batch {batch_id}: {e}")


# --- Main -------------------------------------------------------------

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
        
    print("=" * 65)
    print("--> Binance Crypto Streaming (PG + BQ Backup)")
    print(f"--> Natural deduplication via Idempotent UUID")
    print(f"--> Volume classification and native Anomaly detection")
    print("=" * 65)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Kafka stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "500")   # Small limit -> small batches -> low latency
        .load()
    )

    # 2. Parse JSON
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), binance_schema).alias("data"))
        .select("data.*")
    )

    # 3. Transform & Duplicate Control
    fact_df = build_fact_df(spark, parsed_df)

    # 4. Sink Output
    TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "200 milliseconds")
    logger.info(f"[CONFIG] Trigger: {TRIGGER_INTERVAL} | maxOffsets: 500")

    query = (
        fact_df.writeStream
        .outputMode("append")
        .foreachBatch(dual_sink_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/tmp/spark_checkpoint_binance_v4")
        .start()
    )

    logger.info("[DONE] Stream is running. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
