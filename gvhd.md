# Câu hỏi dành cho Giáo viên Hướng dẫn (GVHD)
> **Đề tài:** Xây dựng hệ thống Streaming Pipeline phân tích giao dịch Crypto thời gian thực  
> **Stack:** Binance WebSocket → Kafka → Spark Structured Streaming → PostgreSQL + BigQuery → Power BI

---

## 1. Data Ingestion Layer (Kafka Producer)

### 1.1. Chiến lược Idempotent Deduplication bằng UUID5

**Trích dẫn:** [live_producer.py:L91-96](producer/live_producer.py#L91-L96)
```python
# 1. Natural Idempotent Deduplication
# When the WebSocket disconnects and reconnects, Binance may replay recent trades.
# Spark Streaming `.dropDuplicates(["transaction_id"])` requires a stable ID.
# We generate transaction_id from the original `trade_id` using UUID5.
transaction_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"binance.trade.{trade_id}"))
```

**Câu hỏi:**
- Em sử dụng `UUID5(NAMESPACE_DNS, "binance.trade.{trade_id}")` để tạo transaction_id deterministic. Cách tiếp cận này đảm bảo khi Binance replay trade cũ sau reconnect, UUID luôn giống nhau → Spark drop duplicate được. **Thầy/Cô đánh giá phương pháp này có đủ mạnh cho production không?** Hay nên dùng cơ chế khác như Kafka Exactly-Once Semantics (EOS) với `enable.idempotence=true`?

---

### 1.2. Phân loại giao dịch theo ngưỡng Volume (Chainalysis)

**Trích dẫn:** [live_producer.py:L44-62](producer/live_producer.py#L44-L62)
```python
def classify_transaction_volume(amount_usd: float) -> tuple[str, bool]:
    """
    Classify trade volume based on Chainalysis and Whale Alert thresholds.
    Reference:
      - < $10k: Retail Trade
      - $10k - $100k: Professional Trade
      - $100k - $1M: Institutional / Block Trade
      - > $1M: Whale -- triggers anomaly flag `is_anomaly = True`
    """
    if amount_usd >= 1_000_000:
        return "WHALE", True
```

**Câu hỏi:**
- Các ngưỡng phân loại (10k, 100k, 1M USD) em tham khảo từ Chainalysis Report. Tuy nhiên **các ngưỡng này có nên là tham số cấu hình (configurable) thay vì hardcode?** Trong thực tế, ngưỡng Whale có thể thay đổi theo thị trường. Thầy/Cô có gợi ý cách tiếp cận nào tốt hơn?

---

### 1.3. Kafka Producer — `acks=1` vs `acks=all`

**Trích dẫn:** [live_producer.py:L129-135](producer/live_producer.py#L129-L135)
```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks=1,
    retries=3,
    compression_type="gzip",
)
```

**Câu hỏi:**
- Em đang dùng `acks=1` (chỉ cần leader broker xác nhận). Với hệ thống chỉ có 1 broker (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — [docker-compose.yml:L29](docker-compose.yml#L29)), điều này có hợp lý. **Nhưng nếu mở rộng lên multi-broker cluster, em có nên chuyển sang `acks=all` không?** Trade-off giữa latency và durability trong context của real-time crypto data nên nghiêng về phía nào?

---

## 2. Stream Processing Layer (Spark Structured Streaming)

### 2.1. Watermark 5 phút — có quá dài/ngắn?

**Trích dẫn:** [spark_processor.py:L119-126](processor/spark_processor.py#L119-L126)
```python
# 2. Watermark & Drop Duplicates (Stateful Streaming)
# Natural deduplication when Binance replays old trades on reconnect.
clean_df = (
    typed_df
    .withWatermark("event_timestamp", "5 minutes")
    .dropDuplicates(["transaction_id"])
)
```

**Câu hỏi:**
- Watermark 5 phút nghĩa là Spark giữ state cho mỗi `transaction_id` trong 5 phút trước khi xoá. **Thầy/Cô cho ý kiến: 5 phút có phù hợp với tần suất reconnect của WebSocket không?** Nếu mạng mất kết nối lâu hơn 5 phút, trade replay sẽ không bị deduplicate. Ngược lại, nếu tăng watermark thì state memory sẽ tăng mạnh.

---

### 2.2. Trigger Interval 200ms — tối ưu cho tần suất nào?

**Trích dẫn:** [spark_processor.py:L345-354](processor/spark_processor.py#L345-L354)
```python
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "200 milliseconds")

query = (
    fact_df.writeStream
    .outputMode("append")
    .foreachBatch(dual_sink_batch)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .option("checkpointLocation", "/tmp/spark_checkpoint_binance_v4")
    .start()
)
```

Kết hợp với `maxOffsetsPerTrigger`:

**Trích dẫn:** [spark_processor.py:L329](processor/spark_processor.py#L329)
```python
.option("maxOffsetsPerTrigger", "500")   # Small limit -> small batches -> low latency
```

**Câu hỏi:**
- Em cấu hình trigger mỗi 200ms với tối đa 500 offsets/trigger. **Thầy/Cô cho ý kiến: với 5 cặp coin, throughput trung bình khoảng bao nhiêu trade/s thì cấu hình này bắt đầu gây backpressure?** Em có nên implement adaptive trigger interval không?

---

### 2.3. Dual-Sink: PostgreSQL (Primary) + BigQuery (Backup) — tại sao không chỉ dùng 1?

**Trích dẫn:** [spark_processor.py:L1-12](processor/spark_processor.py#L1-L12)
```
Streaming data pipeline designed for processing Binance WebSocket data:
  5. foreachBatch - Dual Sink:
       a. [PRIMARY]  Write to PostgreSQL  (UPSERT via JDBC, real-time).
       b. [BACKUP]   Write Parquet -> Load Job to BigQuery (batch, free Sandbox).
```

**Câu hỏi:**
- Kiến trúc Dual-Sink hiện tại dùng PostgreSQL cho real-time query và BigQuery cho analytics/Power BI. **Thầy/Cô có cho rằng việc giữ cả 2 sink là cần thiết?** Hay em nên chọn 1 sink chính và bỏ sink còn lại? Lý do em giữ cả 2: PG cho low-latency OLTP, BQ cho OLAP scale lớn. Nhưng điều này tạo ra vấn đề data consistency giữa 2 hệ thống.

---

### 2.4. BigQuery Async Buffer — race condition risk

**Trích dẫn:** [spark_processor.py:L208-251](processor/spark_processor.py#L208-L251)
```python
BQ_BUFFER = []
BQ_BUFFER_LOCK = threading.Lock()
BQ_UPLOAD_INTERVAL_SEC = 10  # Upload to BQ every 10 seconds
BQ_UPLOAD_ROWS_LIMIT = 5000  # Or when the buffer reaches 5000 rows

def dual_sink_batch(batch_df: DataFrame, batch_id: int):
    ...
    with BQ_BUFFER_LOCK:
        BQ_BUFFER.extend(bq_data)
    ...
    if current_buffer_size >= BQ_UPLOAD_ROWS_LIMIT or time_since_last_upload >= BQ_UPLOAD_INTERVAL_SEC:
        threading.Thread(
            target=_bq_async_upload,
            args=(upload_data, batch_id, len(upload_data)),
            daemon=True
        ).start()
```

**Câu hỏi:**
- Em dùng `threading.Lock` + daemon thread để upload BigQuery bất đồng bộ. **Nếu main process bị crash giữa lúc buffer chưa flush, data sẽ bị mất** (daemon thread bị kill theo main). Thầy/Cô có gợi ý cách nào an toàn hơn? Ví dụ: dùng `atexit` handler, hoặc persistent queue (Redis/file-based)?

---

## 3. Data Warehouse — Star Schema Design

### 3.1. `dim_exchange_rate` dùng simulated data (Random Walk)

**Trích dẫn:** [seed_dimensions_bq.py:L100-120](warehouse/seed_dimensions_bq.py#L100-L120)
```python
# Simulate exchange rate fluctuation (starting at 24,500 VND/USD)
current_rate = 24500.0

while current_date <= end_dt:
    # FX Rate row (USD/VND fluctuates +/- 15 VND per day)
    fluctuation = random.uniform(-15.0, 15.0)
    current_rate += fluctuation
    current_rate = max(23000.0, min(current_rate, 26500.0))

    fx_rows.append({
        "date_key": date_key,
        "currency_code": "VND",
        "vnd_rate": round(current_rate, 2)
    })
```

**Câu hỏi:**
- Hiện tại `dim_exchange_rate` dùng Random Walk simulation với `random.seed(42)` để có dữ liệu deterministic. **Thầy/Cô có yêu cầu em phải dùng tỷ giá thực từ API không?** (ví dụ: ExchangeRate-API, Vietcombank API). Nếu dùng real data, em cần thiết kế thêm scheduled job để cập nhật daily.

---

### 3.2. BigQuery không có Foreign Key — ảnh hưởng tính toàn vẹn?

**Trích dẫn:** So sánh giữa PostgreSQL Schema ([postgres_schema.py:L100-104](warehouse/postgres_schema.py#L100-L104)) và BigQuery Schema ([bigquery_schema.py:L87-102](warehouse/bigquery_schema.py#L87-L102)):

```python
# PostgreSQL — có Foreign Key constraints
CONSTRAINT fk_trade_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
CONSTRAINT fk_trade_time FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
CONSTRAINT fk_trade_category FOREIGN KEY (volume_category) REFERENCES dim_volume_category(volume_category),
CONSTRAINT fk_trade_symbol FOREIGN KEY (crypto_symbol) REFERENCES dim_crypto_pair(crypto_symbol)
```
```python
# BigQuery — KHÔNG có FK, chỉ có schema field
bigquery.SchemaField("date_key", "INT64"),
bigquery.SchemaField("crypto_symbol", "STRING"),
```

**Câu hỏi:**
- BigQuery là columnar warehouse, không enforce FK constraints. **Thầy/Cô có yêu cầu em implement data validation layer ở tầng application để đảm bảo referential integrity trước khi write vào BQ không?** Hay chấp nhận eventual consistency vì mục đích chính là analytics?

---

### 3.3. `fact_binance_trades` — Time Partitioning + Clustering strategy

**Trích dẫn:** [bigquery_schema.py:L110-115](warehouse/bigquery_schema.py#L110-L115)
```python
if table_name == "fact_binance_trades":
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="trade_time"
    )
    table.clustering_fields = ["crypto_symbol", "volume_category"]
```

**Câu hỏi:**
- Em partition theo `trade_time` (ngày) và cluster theo `crypto_symbol`, `volume_category`. **Thầy/Cô đánh giá: với các query Power BI thường dùng (filter theo ngày + coin), clustering order này đã tối ưu chưa?** Có nên thêm `date_key` vào clustering fields không?

---

## 4. Data Quality & Reconciliation

### 4.1. Reconciliation Threshold 90% — có quá thấp?

**Trích dẫn:** [bq_reconcile.py:L42](warehouse/bq_reconcile.py#L42) và [.env:L34](.env#L34)
```python
MATCH_THRESHOLD = float(os.getenv("RECONCILE_THRESHOLD", "0.90"))  # 90%
```

Kết hợp logic kiểm tra:

**Trích dẫn:** [bq_reconcile.py:L185-186](warehouse/bq_reconcile.py#L185-L186)
```python
if kafka_msgs > 0:
    match_pct = (bq_rows / kafka_msgs) * 100
```

**Câu hỏi:**
- Reconciliation so sánh `count(BigQuery rows)` vs `count(Kafka messages)`. Ngưỡng PASS hiện tại là 90%. **Thầy/Cô có cho rằng 90% quá thấp cho financial data?** Trong fintech thực tế, yêu cầu thường là 99.9%+. Em thiết lập 90% vì BigQuery Sandbox có rate limit nên async upload có thể miss rows. **Em nên giải thích khoảng cách 10% này như thế nào trong báo cáo?**

---

### 4.2. PostgreSQL UPSERT — `ON CONFLICT` chỉ update một phần columns

**Trích dẫn:** [spark_processor.py:L180-193](processor/spark_processor.py#L180-L193)
```python
insert_sql = """
    INSERT INTO fact_binance_trades (...)
    VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE SET
        price = EXCLUDED.price,
        quantity = EXCLUDED.quantity,
        amount_usd = EXCLUDED.amount_usd,
        is_anomaly = EXCLUDED.is_anomaly
"""
```

**Câu hỏi:**
- Khi xảy ra conflict (duplicate `transaction_id`), em chỉ update 4 cột: `price`, `quantity`, `amount_usd`, `is_anomaly`. **Thầy/Cô cho ý kiến: tại sao em không update các cột khác như `trade_time`, `volume_category`?** Assumption của em là các thuộc tính khác là immutable từ Binance. Lý luận này có chặt chẽ không?

---

## 5. Vận hành & DevOps

### 5.1. Checkpoint lưu ở `/tmp` — risk mất state khi restart

**Trích dẫn:** [spark_processor.py:L353](processor/spark_processor.py#L353)
```python
.option("checkpointLocation", "/tmp/spark_checkpoint_binance_v4")
```

**Câu hỏi:**
- Spark checkpoint đang lưu ở `/tmp`, sẽ bị xoá khi restart/reboot. **Thầy/Cô có yêu cầu em chuyển checkpoint sang persistent storage (HDFS, GCS, hoặc local volume)?** Nếu mất checkpoint, Spark sẽ đọc lại tất cả Kafka offsets từ `startingOffsets=earliest` → data bị duplicate lại.

---

### 5.2. Spark chạy `local[*]` — giới hạn scalability

**Trích dẫn:** [spark_processor.py:L87-106](processor/spark_processor.py#L87-L106)
```python
builder = (
    SparkSession.builder
    .appName("Binance_Crypto_Streaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
)
```

**Câu hỏi:**
- Hệ thống chạy trên single node (`local[*]`) với `shuffle.partitions=2` và `2g memory`. **Trong phạm vi KLTN, Thầy/Cô có yêu cầu em demo trên cluster (YARN/K8s) không?** Hay chỉ cần mô tả kiến trúc scale-out trong báo cáo là đủ?

---

### 5.3. Windows-specific workarounds — có nên đề cập trong báo cáo?

**Trích dẫn:** [spark_processor.py:L77-85](processor/spark_processor.py#L77-L85)
```python
# Set Hadoop Home for Windows environment explicitly
os.environ["HADOOP_HOME"] = r"C:\hadoop"
if sys.platform.startswith('win'):
    os.environ['PATH'] = os.environ['PATH'] + ';' + r'C:\hadoop\bin'
    # Fix BlockManagerId NullPointerException on Windows
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"
```

**Câu hỏi:**
- Em phải xử lý nhiều Windows-specific issues (Hadoop winutils, BlockManagerId bug, ConnectionResetError). **Thầy/Cô có muốn em dành một phần trong báo cáo để ghi chép các troubleshooting này không?** Hay nên tập trung vào kiến trúc tổng thể?

---

## 6. Mở rộng & Cải tiến tương lai

### 6.1. Schema Evolution — thêm trường mới cho `fact_binance_trades`

**Trích dẫn:** [bigquery_schema.py:L87-102](warehouse/bigquery_schema.py#L87-L102)

**Câu hỏi:**
- Hiện tại schema fact table là fixed. **Thầy/Cô có gợi ý em nên thêm trường nào để tăng giá trị phân tích?** Ví dụ:
  - `latency_ms`: thời gian từ trade trên Binance đến khi lưu xong vào DB → đo hiệu năng pipeline
  - `vnd_amount`: tự động quy đổi từ `amount_usd * vnd_rate` (join với `dim_exchange_rate`)
  - `moving_avg_price`: EMA/SMA tính trên Spark window function

---

### 6.2. Seed dimensions trực tiếp lên BigQuery — bỏ hẳn PostgreSQL?

**Trích dẫn:** [seed_dimensions_bq.py](warehouse/seed_dimensions_bq.py) (script mới) vs [seed_dimensions_pg.py](warehouse/seed_dimensions_pg.py) (script cũ)

**Câu hỏi:**
- Em vừa tạo `seed_dimensions_bq.py` để seed dim tables trực tiếp lên BigQuery mà không cần qua PostgreSQL. **Thầy/Cô có đồng ý em có thể bỏ hoàn toàn PostgreSQL khỏi kiến trúc không?** Nếu chỉ dùng BigQuery:
  - **Ưu điểm**: Đơn giản hoá kiến trúc, giảm operational overhead, không cần `pg_to_bq_sync.py`
  - **Nhược điểm**: BigQuery không phù hợp cho real-time UPSERT (latency cao), mất FK constraint enforcement

---

### 6.3. `pg_to_bq_sync.py` — batch sync strategy

**Trích dẫn:** [pg_to_bq_sync.py:L41-43](scripts/pg_to_bq_sync.py#L41-L43)
```python
def sync_table(table_name, db_url):
    print(f"\n[SYNC] Fetching table '{table_name}' from PostgreSQL...")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", db_url)
```

**Câu hỏi:**
- Script sync hiện đọc **toàn bộ** bảng từ PG rồi `WRITE_TRUNCATE` lên BQ. Với fact table lớn (millions rows), cách này gây tốn memory và thời gian. **Thầy/Cô có gợi ý em nên implement incremental sync** (ví dụ: chỉ sync rows có `trade_time > last_sync_time`)? Hay `WRITE_TRUNCATE` full load là chấp nhận được cho KLTN?

---

## 7. Business & Giá trị ứng dụng thực tiễn

### 7.1. Đối tượng người dùng mục tiêu — hệ thống này phục vụ ai?

**Bối cảnh:** Hệ thống hiện thu thập dữ liệu real-time từ 5 cặp crypto (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT) và phân loại giao dịch theo volume.

**Trích dẫn:** [live_producer.py:L35](producer/live_producer.py#L35)
```python
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]
```

**Trích dẫn:** [live_producer.py:L44-62](producer/live_producer.py#L44-L62) — Volume Classification (RETAIL → WHALE)

**Câu hỏi:**
- Hệ thống phân loại giao dịch thành 4 tầng (Retail, Professional, Institutional, Whale) và phát hiện anomaly. **Thầy/Cô cho ý kiến: đối tượng người dùng chính nên là ai?**
  - **(a)** Trader cá nhân muốn theo dõi Whale movement để ra quyết định giao dịch?
  - **(b)** Sàn giao dịch/broker cần giám sát rủi ro và phát hiện wash trading?
  - **(c)** Regulators / cơ quan quản lý cần giám sát dòng tiền bất thường (AML/KYC)?
  - **(d)** Nhà phân tích dữ liệu / quant cần data warehouse cho backtesting chiến lược?
  
  Em cần xác định rõ persona để viết phần **"Ý nghĩa thực tiễn"** trong báo cáo cho thuyết phục.

---

### 7.2. Giá trị kinh doanh của Anomaly Detection (Whale Alert)

**Trích dẫn:** [live_producer.py:L54-56](producer/live_producer.py#L54-L56)
```python
# > $1M: Whale -- triggers anomaly flag `is_anomaly = True`
if amount_usd >= 1_000_000:
    return "WHALE", True
```

**Trích dẫn:** [spark_processor.py:L188-192](processor/spark_processor.py#L188-L192) — UPSERT giữ nguyên `is_anomaly` khi conflict:
```python
ON CONFLICT (transaction_id) DO UPDATE SET
    ...
    is_anomaly = EXCLUDED.is_anomaly
```

**Câu hỏi:**
- Hiện tại `is_anomaly` chỉ đánh dấu True/False dựa trên ngưỡng 1M USD. **Thầy/Cô cho ý kiến: giá trị business thực sự của việc phát hiện Whale trades là gì?**
  - Có nên tạo thêm **alert mechanism** (Telegram bot, email notification) khi phát hiện Whale trade?
  - Từ góc độ nghiên cứu, liệu Whale trade có tương quan với biến động giá (price impact analysis) không? Em có nên thêm phân tích này vào báo cáo?
  - Trong thực tế fintech, anomaly detection thường phức tạp hơn (ML-based). **Ngưỡng cứng > 1M USD có đủ thuyết phục để gọi là "anomaly detection" trong KLTN không?**

---

### 7.3. Ứng dụng Power BI Dashboard — KPI nào cần hiển thị?

**Trích dẫn:** Fact table schema ([bigquery_schema.py:L87-102](warehouse/bigquery_schema.py#L87-L102)) chứa các trường:
```
transaction_id, trade_id, crypto_symbol, date_key, time_key, trade_time,
price, quantity, amount_usd, is_buyer_maker, volume_category, is_anomaly,
buyer_order_id, seller_order_id
```

Kết hợp Dim tables: `dim_date` (year/quarter/month/weekend), `dim_time` (time_of_day, business_hour), `dim_exchange_rate` (vnd_rate), `dim_volume_category`, `dim_crypto_pair`.

**Câu hỏi:**
- Em dự kiến tạo Power BI dashboard kết nối BigQuery. **Thầy/Cô mong đợi những KPI/biểu đồ nào trên dashboard?** Em đề xuất:
  - **Real-time Trading Volume** (total `amount_usd` theo giờ/ngày, filter by coin)
  - **Whale Activity Timeline** (số lượng `is_anomaly = True` theo thời gian)
  - **Buy vs Sell Pressure** (tỷ lệ `is_buyer_maker = True/False` — thể hiện áp lực mua/bán)
  - **Volume Distribution** (phân bố RETAIL / PROFESSIONAL / INSTITUTIONAL / WHALE)
  - **VND Conversion Report** (quy đổi `amount_usd × vnd_rate` từ `dim_exchange_rate`)
  - **Time-of-Day Analysis** (volume giao dịch theo `time_of_day`: Morning/Noon/Afternoon/Evening/Night)
  - **Weekend vs Weekday** (`dim_date.is_weekend` ảnh hưởng đến volume như thế nào?)

  **Thầy/Cô có bổ sung KPI nào khác không?**

---

### 7.4. So sánh với các giải pháp hiện tại trên thị trường

**Bối cảnh kiến trúc:** [README.md](README.md) — Lambda Architecture (Speed Layer)

**Câu hỏi:**
- Hiện nay có nhiều giải pháp phân tích crypto data real-time trên thị trường:
  - **Chainalysis** — giám sát on-chain, AML compliance (enterprise, trả phí)
  - **Whale Alert** — theo dõi giao dịch lớn, free tier có API public
  - **Glassnode / CryptoQuant** — on-chain analytics, dùng cho institutional research
  - **Dune Analytics** — SQL-based on-chain analytics, community-driven

  **Thầy/Cô cho ý kiến: em nên so sánh hệ thống của mình với giải pháp nào?** Và nên nhấn mạnh điểm khác biệt nào?
  - *Đề xuất*: Hệ thống em tập trung vào **off-chain CEX data** (Binance) thay vì on-chain, xây dựng từ đầu bằng open-source stack (Kafka + Spark), và có **tính mở** (thêm coin, thêm sàn dễ dàng).

---

### 7.5. Tính khả thi thương mại hóa — có thể deploy cho doanh nghiệp?

**Trích dẫn:** [docker-compose.yml](docker-compose.yml) — toàn bộ stack chạy trên Docker:
```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
  kafka:
    image: confluentinc/cp-kafka:7.5.0
  postgres:
    image: postgres:15
```

**Trích dẫn:** [spark_processor.py:L90](processor/spark_processor.py#L90) — chạy single node:
```python
.master("local[*]")
```

**Câu hỏi:**
- Toàn bộ hệ thống hiện chạy trên **single machine** (Docker Compose, Spark local, 1 broker Kafka). **Thầy/Cô có yêu cầu em mô tả lộ trình "từ KLTN → production-ready" trong báo cáo không?** Ví dụ:
  - Thay Kafka single broker → **Confluent Cloud** hoặc **Amazon MSK**
  - Thay Spark local → **Dataproc** / **EMR**
  - Thay PostgreSQL → **Cloud SQL** hoặc **AlloyDB**
  - Bổ sung monitoring: **Prometheus + Grafana** cho pipeline health
  - CI/CD: auto-deploy khi push code thông qua **Cloud Build / GitHub Actions**

---

### 7.6. Tính pháp lý — thu thập data Binance có vi phạm Terms of Service?

**Trích dẫn:** [live_producer.py:L9](producer/live_producer.py#L9) và [live_producer.py:L38-41](producer/live_producer.py#L38-L41)
```python
# Data source: wss://stream.binance.com:9443 (free, no API key required)

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)
```

**Câu hỏi:**
- Em sử dụng Binance WebSocket **public endpoint** (không cần API key, không cần đăng ký). **Thầy/Cô có yêu cầu em xác minh lại Binance Terms of Service xem việc thu thập data liên tục có vi phạm không?** Em cần trích dẫn điều khoản nào trong báo cáo để đảm bảo tính hợp pháp?
  - Theo hiểu biết của em: Public market data (trade streams) là free-to-use, nhưng Binance giới hạn tốc độ kết nối WebSocket (max 5 connections/IP). Hệ thống em dùng 1 multi-stream connection → nằm trong giới hạn cho phép.

---

### 7.7. Chi phí vận hành — BigQuery Sandbox vs Production

**Trích dẫn:** [.env:L23-25](.env#L23-L25)
```
BQ_PROJECT_ID=ecommerce-db2025
BQ_DATASET=paysim_dw
GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account.json
```

**Trích dẫn:** [spark_processor.py:L211-212](processor/spark_processor.py#L211-L212) — buffer interval:
```python
BQ_UPLOAD_INTERVAL_SEC = 10  # Upload to BQ every 10 seconds
BQ_UPLOAD_ROWS_LIMIT = 5000  # Or when buffer reaches 5000 rows
```

**Câu hỏi:**
- Em đang dùng **BigQuery Sandbox** (free tier, 10GB storage, 1TB query/month). Với tốc độ ingest hiện tại (~50-100 trades/s × 14 columns), **em ước tính chạy liên tục 24/7 thì bao lâu sẽ vượt free tier?** Thầy/Cô có muốn em đưa **phân tích chi phí (cost analysis)** vào báo cáo không?
  - *Ước tính sơ bộ*: 100 trades/s × 86,400s/ngày × ~500 bytes/row ≈ 4.3 GB/ngày → vượt 10GB Sandbox trong ~2 ngày.

---

### 7.8. Câu chuyện nghiên cứu — bài toán nào em đang giải quyết?

**Bối cảnh tổng hợp từ source code:**
- [live_producer.py](producer/live_producer.py): Thu thập + phân loại + dedup
- [spark_processor.py](processor/spark_processor.py): Stream processing + dual sink
- [bigquery_schema.py](warehouse/bigquery_schema.py): Star Schema warehouse
- [bq_reconcile.py](warehouse/bq_reconcile.py): Data quality reconciliation

**Câu hỏi:**
- Về mặt nghiên cứu, em câu chuyện (narrative) cho KLTN có thể theo nhiều hướng:
  - **(a) Data Engineering focus**: "Xây dựng pipeline streaming real-time with exactly-once semantics" → tập trung vào dedup, fault tolerance, dual-sink
  - **(b) Business Intelligence focus**: "Phân tích hành vi giao dịch crypto và phát hiện anomaly phục vụ ra quyết định đầu tư" → tập trung vào dashboard, KPI, insight
  - **(c) System Architecture focus**: "Thiết kế kiến trúc Lambda cho giám sát tài chính thời gian thực" → tập trung vào speed layer vs batch layer, CAP theorem tradeoff
  
  **Thầy/Cô khuyên em nên nghiêng về hướng nào?** Điều này ảnh hưởng đến cách em viết chương "Kết quả" và "Đánh giá" trong báo cáo.

---

## Tổng hợp — Checklist cho buổi gặp GVHD

| # | Nhóm | Câu hỏi chính | File trích dẫn |
|---|------|---------------|----------------|
| 1.1 | Ingestion | UUID5 dedup đủ mạnh? | `live_producer.py:L91-96` |
| 1.2 | Ingestion | Ngưỡng Volume nên configurable? | `live_producer.py:L44-62` |
| 1.3 | Ingestion | `acks=1` vs `acks=all` khi scale | `live_producer.py:L129-135` |
| 2.1 | Processing | Watermark 5 phút phù hợp? | `spark_processor.py:L119-126` |
| 2.2 | Processing | Trigger 200ms + 500 offsets | `spark_processor.py:L329,345-354` |
| 2.3 | Processing | Dual-Sink có cần thiết? | `spark_processor.py:L1-12` |
| 2.4 | Processing | BQ async buffer mất data? | `spark_processor.py:L208-251` |
| 3.1 | Warehouse | Simulated FX rate vs real API | `seed_dimensions_bq.py:L100-120` |
| 3.2 | Warehouse | BQ không có FK enforcement | `postgres_schema.py` vs `bigquery_schema.py` |
| 3.3 | Warehouse | Partition + Clustering tối ưu? | `bigquery_schema.py:L110-115` |
| 4.1 | Quality | 90% threshold quá thấp? | `bq_reconcile.py:L42,185-186` |
| 4.2 | Quality | UPSERT chỉ update 4 cột | `spark_processor.py:L180-193` |
| 5.1 | DevOps | Checkpoint ở `/tmp` | `spark_processor.py:L353` |
| 5.2 | DevOps | `local[*]` vs cluster demo | `spark_processor.py:L87-106` |
| 5.3 | DevOps | Windows workarounds | `spark_processor.py:L77-85` |
| 6.1 | Mở rộng | Thêm trường analytics | `bigquery_schema.py:L87-102` |
| 6.2 | Mở rộng | Bỏ PG, dùng BQ only? | `seed_dimensions_bq.py` vs `seed_dimensions_pg.py` |
| 6.3 | Mở rộng | Full sync vs incremental | `pg_to_bq_sync.py:L41-43` |
| **7.1** | **Business** | **Đối tượng người dùng mục tiêu** | `live_producer.py:L35,44-62` |
| **7.2** | **Business** | **Giá trị Whale Detection** | `live_producer.py:L54-56` |
| **7.3** | **Business** | **Power BI KPI cần hiển thị** | `bigquery_schema.py:L87-102` |
| **7.4** | **Business** | **So sánh giải pháp thị trường** | `README.md` |
| **7.5** | **Business** | **Lộ trình production-ready** | `docker-compose.yml`, `spark_processor.py:L90` |
| **7.6** | **Business** | **Pháp lý Binance ToS** | `live_producer.py:L9,38-41` |
| **7.7** | **Business** | **Chi phí BigQuery Sandbox** | `.env:L23-25`, `spark_processor.py:L211-212` |
| **7.8** | **Business** | **Hướng nghiên cứu chính** | Toàn bộ source code |
