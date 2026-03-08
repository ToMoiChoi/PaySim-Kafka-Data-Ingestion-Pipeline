# PaySim Kafka Data Ingestion Pipeline
**Graduation Thesis (KLTN) – Real-time Payment Transaction Processing & Reward Points System**

This project implements a real-time data pipeline that ingests simulated financial transactions from the [PaySim1](https://www.kaggle.com/datasets/ealaxi/paysim1) dataset, processes them through Apache Kafka and Spark Structured Streaming with fault tolerance (watermark & deduplication), computes reward points, and lands clean data into a Google BigQuery Star Schema data warehouse.

## Architecture & Data Flow

```
PaySim CSV → Kafka Producer → Kafka Topic → Spark Structured Streaming → BigQuery (Star Schema)
```

### Week 1: Data Ingestion (Kafka Producer)
1. **Read Data:** `kafka_producer.py` reads the PaySim CSV file.
2. **Transform:** Filters records with `type == "PAYMENT"`, adds `transaction_id` (UUID4) and `event_timestamp` (ISO-8601).
3. **Fault Injection:** Duplicates 10% of records randomly (same `transaction_id`) to simulate network retries / duplicate events.
4. **Produce to Kafka:** Streams JSON messages into the `payment_events` topic via Docker Compose.

### Week 2: Spark Structured Streaming (Core Processing)
1. **Kafka Consumer:** `spark_processor.py` reads real-time stream from topic `payment_events`.
2. **Watermark & Deduplication:** Declares a 5-minute watermark + `dropDuplicates(["transaction_id"])` to eliminate the 10% injected duplicate transactions.
3. **Reward Calculation:** Computes `reward_points = int(amount × 0.01)` for each valid transaction.
4. **BigQuery Sink:** Writes clean data into `fact_transactions` on BigQuery using `foreachBatch`.

### Week 3: Star Schema on BigQuery (Data Modeling)
1. **`bigquery_schema.py`**: Creates the `paysim_dw` dataset and 6 Star Schema tables.
2. **`seed_dimensions.py`**: Populates 5 Dimension tables with sample data.

#### Star Schema ERD
```
dim_users ──────────┐
dim_merchants ──────┤
dim_transaction_type┼──── fact_transactions
dim_location ───────┤
dim_date ───────────┘
```

| Table | Type | Description |
|-------|------|-------------|
| `fact_transactions` | Fact | Transaction history & reward points (partitioned by day, clustered by user/merchant) |
| `dim_users` | Dim | Customer information (derived from nameOrig) |
| `dim_merchants` | Dim | Merchant/store information (derived from nameDest) |
| `dim_transaction_type` | Dim | Transaction type configuration & reward rules |
| `dim_location` | Dim | Geographic information (10 Vietnamese cities) |
| `dim_date` | Dim | Standard date dimension (year 2026) |

### Week 4: Pipeline Sink – Spark → BigQuery UPSERT (Sprint 2)
1. **Micro-batch trigger:** `spark_processor.py` writes every **15 seconds** via `foreachBatch`.
2. **UPSERT via BigQuery MERGE:**
   - Each batch → written to `fact_transactions_staging` (overwrite).
   - A `MERGE` SQL runs: `WHEN MATCHED → UPDATE`, `WHEN NOT MATCHED → INSERT`.
   - Prevents duplicates even if Watermark misses a late event.
3. **Per-batch logging:** `batch_id`, `rows_in_batch`, `rows_affected`, `latency_ms`.
4. **Reconciliation:** `bq_reconcile.py` compares Kafka total messages vs BigQuery row count.

```
Kafka Offsets (end - begin) ──→ bq_reconcile.py ←── COUNT(*) BigQuery
                                       ↓
                              Match % ≥ 90% ? → PASS / FAIL
```

## Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Kaggle account (to download the dataset)
- Google Cloud account with BigQuery API enabled
- Service Account JSON with `BigQuery Data Editor` + `BigQuery Job User` roles
- *(Sprint 2)* A GCS bucket for spark-bigquery-connector temp staging

## Project Structure

```text
KLTN_2/
├── credentials/                                 # Service Account JSON (git ignored)
│   └── service-account.json
├── data/
│   └── PS_20174392719_1491204439457_log.csv     # Dataset file (git ignored)
├── .env                                         # Environment variables (git ignored)
├── docker-compose.yml                           # Kafka, Zookeeper, Spark Docker config
├── Dockerfile.spark                             # Spark container image
├── Makefile                                     # Sprint 2: Automation commands
├── kafka_producer.py                            # Week 1: CSV → Kafka Producer
├── spark_processor.py                           # Weeks 2-4: Kafka → Spark → BigQuery (UPSERT)
├── bigquery_schema.py                           # Week 3: Create Star Schema on BQ
├── seed_dimensions.py                           # Week 3: Seed Dimension tables
├── bq_reconcile.py                              # Week 4: Data reconciliation (Kafka vs BQ)
├── download_dataset.py                          # Script to download dataset from Kaggle
├── requirements.txt                             # Python dependencies
└── README.md
```

## Quickstart Guide

### Step 0: (Optional) Download Data from Kaggle
If you don't have the `PS_2017...log.csv` file yet:
1. Go to Kaggle → Account → Create New API Token (downloads `kaggle.json`).
2. Place `kaggle.json` in `~/.kaggle/` (Windows: `C:\Users\<Username>\.kaggle\kaggle.json`).
3. Run `python download_dataset.py` after setting up the Python environment (Step 2).

### Step 1: Start Kafka & Zookeeper

```bash
docker-compose up -d
```

Verify containers are running:
```bash
docker ps
```

### Step 2: Set Up Python Environment

```powershell
# Create virtual environment
python -m venv venv

# Activate (Windows PowerShell)
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables (`.env`)

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=payment_events
DUPLICATE_RATIO=0.10

# BigQuery
BQ_PROJECT_ID=your-gcp-project-id
BQ_DATASET=paysim_dw
GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account.json

# Sprint 2: GCS temp bucket for spark-bigquery-connector (no gs:// prefix)
BQ_TEMP_BUCKET=your-gcs-bucket-name
RECONCILE_THRESHOLD=0.90
```

> **Note:** Create a GCS bucket in the same region as your BigQuery dataset.
> Grant the service account `Storage Object Admin` on that bucket.

### Step 4: Set Up BigQuery Star Schema

```powershell
# Create dataset + 6 tables
python bigquery_schema.py

# Seed 5 Dimension tables with sample data
python seed_dimensions.py
```

### Step 5: Run Kafka Producer

```powershell
python kafka_producer.py
```

### Step 6: Run Spark Processor

```powershell
# Run locally (with BigQuery)
python spark_processor.py

# Or run via Docker
docker-compose up spark-processor
```

### Step 7: Reconcile & Verify Data

```powershell
# Check BigQuery rows vs Kafka messages
python bq_reconcile.py
```

Expected output:
```
┌───────────────────────────────────────────────────────────────┐
│  Kafka Messages Produced              │        52,384  │
│  BigQuery fact_transactions rows      │        49,102  │
│  Match Percentage                     │         93.72 %│
│  Status                              │  ✅ PASS         │
└───────────────────────────────────────────────────────────────┘
```

### Using Makefile (recommended)

```powershell
make help          # Show all available commands
make start-kafka   # Start Kafka + Zookeeper
make setup-bq      # Create BigQuery schema
make seed-bq       # Seed dimension tables
make run-producer  # Start Kafka producer
make run-spark     # Start Spark processor (BigQuery sink)
make reconcile     # Check data match
```

## Configuration

You can customize the pipeline behavior by editing the `.env` file:
- **CSV path:** Change `CSV_PATH`
- **Duplicate ratio:** Adjust `DUPLICATE_RATIO` (default: 10%)
- **Kafka throughput:** Tune `BATCH_SIZE`, `LINGER_MS`, `BUFFER_MEMORY`
- **BigQuery target:** Change `BQ_PROJECT_ID`, `BQ_DATASET`

## How to Query Data

Sau khi pipeline chạy xong, dữ liệu được lưu ở **2 nơi** với các cách truy vấn khác nhau.

---

### 🐘 PostgreSQL (Primary Sink – Real-time)

#### Cách 1: psql CLI (nhanh nhất)

```bash
# Kết nối vào PostgreSQL container
docker exec -it postgres psql -U paysim -d paysim_dw

# Xem tổng số giao dịch
SELECT COUNT(*) FROM fact_transactions;

# Xem 10 giao dịch gần nhất
SELECT * FROM fact_transactions ORDER BY transaction_time DESC LIMIT 10;

# Thống kê theo loại giao dịch
SELECT type_id, COUNT(*) as total, SUM(amount) as total_amount
FROM fact_transactions
GROUP BY type_id;

# Join với dim table
SELECT f.transaction_id, f.user_id, f.amount, f.reward_points
FROM fact_transactions f LIMIT 10;
```

#### Cách 2: Python – psycopg2

```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="paysim_dw", user="paysim", password="paysim123"
)
df = pd.read_sql("SELECT * FROM fact_transactions LIMIT 100", conn)
print(df.head())
conn.close()
```

#### Cách 3: pandas – connection string ngắn hơn

```python
import pandas as pd

CONN = "postgresql://paysim:paysim123@localhost:5432/paysim_dw"
df = pd.read_sql("SELECT * FROM fact_transactions", CONN)
print(df.describe())
```

#### Cách 4: SQLAlchemy

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://paysim:paysim123@localhost:5432/paysim_dw")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM fact_transactions"))
    print(result.fetchone())
```

---

### ☁️ BigQuery (Backup Sink – Analytics)

#### Cách 1: Google Cloud Console
Vào [console.cloud.google.com](https://console.cloud.google.com/) → BigQuery → `ecommerce-db2025.paysim_dw.fact_transactions`

#### Cách 2: Python – google-cloud-bigquery

```python
from google.cloud import bigquery

client = bigquery.Client(project="ecommerce-db2025")
query = """
    SELECT type_id, COUNT(*) as total, ROUND(SUM(amount), 2) as total_amount
    FROM `ecommerce-db2025.paysim_dw.fact_transactions`
    GROUP BY type_id
"""
df = client.query(query).to_dataframe()
print(df)
```

#### Cách 3: pandas-gbq

```python
import pandas_gbq

df = pandas_gbq.read_gbq(
    "SELECT * FROM paysim_dw.fact_transactions LIMIT 1000",
    project_id="ecommerce-db2025",
    credentials_path="credentials/service-account.json"
)
print(df.head())
```

---

### 📋 Các query phân tích hữu ích

```sql
-- Top 5 user giao dịch nhiều nhất
SELECT user_id, COUNT(*) as txn_count, SUM(amount) as total_amount
FROM fact_transactions
GROUP BY user_id ORDER BY txn_count DESC LIMIT 5;

-- Số giao dịch + doanh thu theo ngày
SELECT date_key, COUNT(*) as txn_count, SUM(amount) as daily_volume
FROM fact_transactions
GROUP BY date_key ORDER BY date_key;

-- Phân tích reward points
SELECT MIN(reward_points) as min, MAX(reward_points) as max,
       AVG(reward_points) as avg, SUM(reward_points) as total
FROM fact_transactions;
```
