"""
processor/spark_processor.py – Dual-Sink Spark Structured Streaming
====================================================================
Luồng xử lý:
  1. Đọc stream từ Kafka topic `payment_events`.
  2. Parse JSON schema.
  3. Ép kiểu event_timestamp → TimestampType.
  4. Watermark (5 phút) + dropDuplicates(transaction_id) – Kiểm soát trùng lặp.
  5. Tính reward_points = int(amount × 0.01).
  6. Transform → Star Schema (fact_transactions).
  7. foreachBatch – Dual Sink:
       a. [PRIMARY]  Ghi vào PostgreSQL  (UPSERT qua JDBC, real-time).
       b. [BACKUP]   Ghi Parquet → Load Job lên BigQuery (batch, miễn phí Sandbox).
"""

import os
import time
import random
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, expr, udf, when, current_timestamp, date_format
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

# Location pool
LOCATION_IDS = [
    "LOC_HCM", "LOC_HN", "LOC_DN", "LOC_HP", "LOC_CT",
    "LOC_HUE", "LOC_NT", "LOC_DL", "LOC_VT", "LOC_BD",
]

# ─── Kafka Schema ────────────────────────────────────────────────
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
])


# ─── Spark Session ───────────────────────────────────────────────
def create_spark_session() -> SparkSession:
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
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )

    return builder.getOrCreate()


# ─── Transform: Kafka DF → fact schema ──────────────────────────
def build_fact_df(parsed_df: DataFrame) -> DataFrame:

    @udf(returnType=StringType())
    def random_location_udf():
        return random.choice(LOCATION_IDS)

    typed_df = parsed_df.withColumn(
        "event_timestamp", expr("to_timestamp(event_timestamp)")
    )

    clean_df = (
        typed_df
        .withWatermark("event_timestamp", "5 minutes")
        .dropDuplicates(["transaction_id"])
    )

    fact_df = (
        clean_df
        .withColumnRenamed("nameOrig",  "user_id")
        .withColumnRenamed("nameDest",  "merchant_id")
        .withColumn("type_id",
            when(col("type") == "PAYMENT",  "TYP_PAYMENT")
            .when(col("type") == "TRANSFER","TYP_TRANSFER")
            .when(col("type") == "CASH_OUT","TYP_CASH_OUT")
            .when(col("type") == "DEBIT",   "TYP_DEBIT")
            .when(col("type") == "CASH_IN", "TYP_CASH_IN")
            .otherwise("TYP_PAYMENT")
        )
        .withColumn("location_id",      random_location_udf())
        .withColumn("transaction_time",
            when(col("event_timestamp").isNull(), current_timestamp())
            .otherwise(col("event_timestamp"))
        )
        .withColumn("date_key",
            date_format(col("transaction_time"), "yyyyMMdd").cast("long")
        )
        .withColumn("amount",        col("amount").cast("decimal(38,9)"))
        .withColumn("reward_points", (col("amount") * 0.01).cast("long"))
        .select(
            "transaction_id", "user_id", "merchant_id",
            "type_id", "location_id", "date_key",
            "transaction_time", "amount", "reward_points",
        )
    )
    return fact_df


# ─── Sink 1: PostgreSQL (Primary, real-time) ─────────────────────
def write_to_postgres(batch_df: DataFrame, batch_id: int, row_count: int):
    """Ghi batch vào PostgreSQL bằng JDBC – UPSERT qua temp table."""
    t0 = time.time()

    # Ghi vào staging table tạm, rồi UPSERT vào fact_transactions
    staging = f"fact_transactions_staging_{batch_id % 5}"  # rotate 5 staging tables

    (
        batch_df.write
        .format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", staging)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # UPSERT từ staging → fact_transactions (dùng psycopg2)
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    with conn.cursor() as cur:
        # Tạo staging table nếu chưa có (giống cấu trúc fact)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {staging} (LIKE fact_transactions INCLUDING ALL)
        """)
        # UPSERT: INSERT ... ON CONFLICT DO UPDATE
        cur.execute(f"""
            INSERT INTO fact_transactions
                SELECT * FROM {staging}
            ON CONFLICT (transaction_id) DO UPDATE SET
                user_id          = EXCLUDED.user_id,
                merchant_id      = EXCLUDED.merchant_id,
                type_id          = EXCLUDED.type_id,
                location_id      = EXCLUDED.location_id,
                date_key         = EXCLUDED.date_key,
                transaction_time = EXCLUDED.transaction_time,
                amount           = EXCLUDED.amount,
                reward_points    = EXCLUDED.reward_points
        """)
        cur.execute(f"TRUNCATE TABLE {staging}")
    conn.commit()
    conn.close()

    elapsed = int((time.time() - t0) * 1000)
    logger.info(
        f"[Batch {batch_id}] 🐘 PostgreSQL UPSERT | "
        f"rows={row_count:,} | latency={elapsed}ms"
    )


# ─── Sink 2: BigQuery Backup (batch Parquet → Load Job) ──────────
def write_to_bq_backup(batch_df: DataFrame, batch_id: int, row_count: int):
    """Ghi Parquet file → BigQuery Load Job (miễn phí Sandbox)."""
    if not BQ_PROJECT_ID:
        logger.warning("[BQ Backup] BQ_PROJECT_ID trống → skip BigQuery backup.")
        return

    t0 = time.time()
    parquet_path = f"{BQ_PARQUET_DIR}/batch_{batch_id}"

    # Ghi Parquet ra disk
    batch_df.write.mode("overwrite").parquet(parquet_path)

    # Dùng google-cloud-bigquery để load file lên BigQuery
    try:
        from google.cloud import bigquery
        client   = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"

        # Lấy danh sách file Parquet vừa ghi
        import glob
        parquet_files = glob.glob(f"{parquet_path}/*.parquet")
        if not parquet_files:
            logger.warning(f"[BQ Backup] Batch {batch_id}: Không có file Parquet để load.")
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        with open(parquet_files[0], "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        elapsed = int((time.time() - t0) * 1000)
        logger.info(
            f"[Batch {batch_id}] ☁️  BigQuery backup | "
            f"rows={row_count:,} | latency={elapsed}ms"
        )
    except Exception as e:
        logger.warning(f"[BQ Backup] Batch {batch_id}: Lỗi backup → {e}")


# ─── foreachBatch callback (Dual Sink) ───────────────────────────
def dual_sink_batch(batch_df: DataFrame, batch_id: int):
    row_count = batch_df.count()

    if row_count == 0:
        logger.info(f"[Batch {batch_id}] ⚪ 0 rows – skip.")
        return

    logger.info(f"[Batch {batch_id}] 📦 Processing {row_count:,} rows...")

    # Cache để tránh tính lại 2 lần
    batch_df.cache()

    # Sink 1: PostgreSQL (primary)
    try:
        write_to_postgres(batch_df, batch_id, row_count)
    except Exception as e:
        logger.error(f"[Batch {batch_id}] ❌ PostgreSQL sink lỗi: {e}")

    # Sink 2: BigQuery backup
    try:
        write_to_bq_backup(batch_df, batch_id, row_count)
    except Exception as e:
        logger.warning(f"[Batch {batch_id}] ⚠️ BigQuery backup lỗi: {e}")

    batch_df.unpersist()


# ─── Main ────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("🚀  PaySim Dual-Sink Streaming  (PostgreSQL + BigQuery backup)")
    print(f"📡  Kafka  : {KAFKA_BOOTSTRAP_SERVERS}  |  Topic: {KAFKA_TOPIC}")
    print(f"🐘  PG     : {PG_HOST}:{PG_PORT}/{PG_DB}")
    if BQ_PROJECT_ID:
        print(f"☁️   BQ     : {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}  (backup)")
    else:
        print("⚠️   BigQuery chưa cấu hình → chỉ ghi PostgreSQL")
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
        .load()
    )

    # 2. Parse JSON
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), payment_schema).alias("data"))
        .select("data.*")
    )

    # 3. Transform
    fact_df = build_fact_df(parsed_df)

    # 4. Dual Sink via foreachBatch
    query = (
        fact_df.writeStream
        .outputMode("append")
        .foreachBatch(dual_sink_batch)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", "/tmp/spark_checkpoint_dual_sink")
        .start()
    )

    logger.info("✅ Stream đang chạy. Nhấn Ctrl+C để dừng.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
