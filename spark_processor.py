"""
Tuần 2: Spark Structured Streaming Processor
============================================
1. Kết nối Kafka topic `payment_events`.
2. Parse JSON schema (transaction_id, amount, event_timestamp,...).
3. Ép kiểu event_timestamp sang TimestampType.
4. Cài đặt Watermark trễ tối đa 5 phút (`withWatermark`).
5. Loại bỏ dữ liệu trùng lặp do Fault Injection (`dropDuplicates("transaction_id")`).
6. Tính toán điểm thưởng `reward = amount * 0.01` cho các bản ghi hợp lệ.
7. Đẩy kết quả xuất ra Console.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv

# ─── Load Configuration ──────────────────────────────────────────
load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events")

# ─── Định nghĩa Schema tương thích với dữ liệu PaySim sinh ra ─────
# Dữ liệu dạng JSON, ví dụ:
# {"step": 1, "type": "PAYMENT", "amount": 9839.64, "nameOrig": "...", "oldbalanceOrg": 170136.0, ...}
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
    # Dữ liệu event_timestamp được gen lúc sinh data dưới dạng chuẩn ISO8601 string
    StructField("event_timestamp", StringType(), True) 
])

def create_spark_session() -> SparkSession:
    """Tạo SparkSession local và tự động load package kafka-0-10."""
    # Lưu ý: Cần package pyspark-sql-kafka tương thích với phiên bản Spark đang chạy.
    # Package: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 (Cho Spark 3.5.x & Scala 2.12)
    # Tên version 3.5.0 mặc định, nếu máy khách dùng Spark bản khác, cần điều chỉnh version.
    
    # Do pyspark được cài mặc định là pyspark bản mới nhất (~3.5.1), ta sẽ set 3.5.1.
    return SparkSession.builder \
        .appName("PaySimStreamingProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def main():
    print("="*60)
    print("🚀 Khởi chạy Spark Structured Streaming Processor...")
    print(f"📡 Lắng nghe Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    print("="*60)

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
    # Giá trị message của payload luôn nằm trong cột "value" định dạng mảng byte (binary)
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), payment_schema).alias("data")) \
        .select("data.*")

    # 3. Ép kiểu event_timestamp từ dạng chuỗi ISO sang Timestamp Type
    # Vì lúc Kafka Producer gen data bằng: datetime.now(timezone.utc).isoformat()
    # Spark native TO_TIMESTAMP mặc định support format chuẩn.
    typed_df = parsed_df.withColumn("event_timestamp", expr("to_timestamp(event_timestamp)"))

    # 4. KHAI BÁO WATERMARK & DEDUPLICATION (Lọc Lỗi)
    # Khai báo độ trễ tối đa (delay threshold) là 5 phút.
    # Khi dữ liệu muộn hơn max_event_time - 5 phút đến, nó sẽ tự động rơi (drop).
    # Kết hợp với dropDuplicates theo transaction_id, giúp xử lý ngốn RAM (chỉ nhớ cache 5ph).
    clean_df = typed_df \
        .withWatermark("event_timestamp", "5 minutes") \
        .dropDuplicates(["transaction_id"])
    
    # 5. TÍNH ĐIỂM THƯỞNG (RULES-BASED REWARD)
    # Chỉ tính cho luồng sạch đã filter, thêm cột `reward` = 1% số lượng giao dịch.
    reward_df = clean_df.withColumn("reward", col("amount") * 0.01)

    # Lựa chọn các cột quan trọng nhất in ra cho dễ theo dõi
    final_display_df = reward_df.select(
        "transaction_id", 
        "event_timestamp", 
        "type", 
        "amount", 
        "reward"
    )

    # 6. Đẩy dữ liệu xuất Streaming ra dòng Console để Verify
    query = final_display_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
