"""
Tuần 2: Spark Structured Streaming Processor → BigQuery
========================================================
1. Kết nối Kafka topic `payment_events`.
2. Parse JSON schema (transaction_id, amount, event_timestamp,...).
3. Ép kiểu event_timestamp sang TimestampType.
4. Cài đặt Watermark trễ tối đa 5 phút (`withWatermark`).
5. Loại bỏ dữ liệu trùng lặp do Fault Injection (`dropDuplicates("transaction_id")`).
6. Tính toán điểm thưởng `reward_points = int(amount * 0.01)`.
7. Transform dữ liệu sang format Star Schema (fact_transactions).
8. Ghi dữ liệu vào BigQuery bằng foreachBatch + google-cloud-bigquery.
"""

import os
import random
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, expr, udf, lit, when, current_timestamp, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)
from dotenv import load_dotenv

# ─── Load Configuration ──────────────────────────────────────────
load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events")

# BigQuery Configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "")
BQ_DATASET = os.getenv("BQ_DATASET", "paysim_dw")
BQ_TABLE_FACT = "fact_transactions"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# Location IDs pool (phải khớp với dim_location đã seed)
LOCATION_IDS = [
    "LOC_HCM", "LOC_HN", "LOC_DN", "LOC_HP", "LOC_CT",
    "LOC_HUE", "LOC_NT", "LOC_DL", "LOC_VT", "LOC_BD",
]

# ─── Định nghĩa Schema tương thích với dữ liệu PaySim sinh ra ─────
payment_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Tạo SparkSession local và tự động load package kafka và bigquery."""
    return SparkSession.builder \
        .appName("PaySimStreamingProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()





def main():
    print("=" * 60)
    print("🚀 Khởi chạy Spark Structured Streaming Processor...")
    print(f"📡 Lắng nghe Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    if BQ_PROJECT_ID:
        print(f"☁️  BigQuery: {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}")
    else:
        print("⚠️  BigQuery chưa cấu hình (BQ_PROJECT_ID trống) → chỉ xuất Console")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc stream trực tiếp từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Extract Value từ message Kafka và đúc vào schema JSON
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), payment_schema).alias("data")) \
        .select("data.*")

    # 3. Ép kiểu event_timestamp từ dạng chuỗi ISO sang Timestamp Type
    typed_df = parsed_df.withColumn(
        "event_timestamp", expr("to_timestamp(event_timestamp)")
    )

    # 4. KHAI BÁO WATERMARK & DEDUPLICATION (Lọc Lỗi)
    #    Watermark trễ tối đa 5 phút + dropDuplicates theo transaction_id
    clean_df = typed_df \
        .withWatermark("event_timestamp", "5 minutes") \
        .dropDuplicates(["transaction_id"])

    # 5. TRANSFORM SANG DẠNG FACT_TRANSACTIONS NATIVELY
    @udf(returnType=StringType())
    def random_location_udf():
        return random.choice(LOCATION_IDS)
        
    fact_df = clean_df \
        .withColumnRenamed("nameOrig", "user_id") \
        .withColumnRenamed("nameDest", "merchant_id") \
        .withColumn("type_id", when(col("type") == "PAYMENT", "TYP_PAYMENT")
                             .when(col("type") == "TRANSFER", "TYP_TRANSFER")
                             .when(col("type") == "CASH_OUT", "TYP_CASH_OUT")
                             .when(col("type") == "DEBIT", "TYP_DEBIT")
                             .when(col("type") == "CASH_IN", "TYP_CASH_IN")
                             .otherwise("TYP_PAYMENT")) \
        .withColumn("location_id", random_location_udf()) \
        .withColumn("transaction_time", when(col("event_timestamp").isNull(), current_timestamp()).otherwise(col("event_timestamp"))) \
        .withColumn("date_key", date_format(col("transaction_time"), "yyyyMMdd").cast("long")) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("reward_points", (col("amount") * 0.01).cast("long")) \
        .select(
            "transaction_id",
            "user_id",
            "merchant_id",
            "type_id",
            "location_id",
            "date_key",
            "transaction_time",
            "amount",
            "reward_points"
        )

    # 6. OUTPUT
    if BQ_PROJECT_ID:
        # ─── BigQuery Sink (Spark BigQuery Connector) ────────────────────────
        query = fact_df.writeStream \
            .format("bigquery") \
            .outputMode("append") \
            .option("table", f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}") \
            .trigger(processingTime="15 seconds") \
            .option("checkpointLocation", "/tmp/spark_checkpoint_bq_native") \
            .start()
    else:
        # ─── Console Sink (debug / khi chưa có BigQuery) ────────
        final_display_df = fact_df.select(
            "transaction_id",
            "transaction_time",
            "type_id",
            "amount",
            "reward_points"
        )
        query = final_display_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="15 seconds") \
            .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
