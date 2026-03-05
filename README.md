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

## Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Kaggle account (to download the dataset)
- Google Cloud account with BigQuery API enabled
- Service Account JSON with `BigQuery Data Editor` + `BigQuery Job User` roles

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
├── kafka_producer.py                            # Week 1: CSV → Kafka Producer
├── spark_processor.py                           # Week 2: Kafka → Spark → BigQuery
├── bigquery_schema.py                           # Week 3: Create Star Schema on BQ
├── seed_dimensions.py                           # Week 3: Seed Dimension tables
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
```

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

### Step 7: Verify Data on BigQuery

Open [BigQuery Console](https://console.cloud.google.com/bigquery) → select dataset `paysim_dw`:
```sql
SELECT * FROM paysim_dw.fact_transactions LIMIT 10;
```

## Configuration

You can customize the pipeline behavior by editing the `.env` file:
- **CSV path:** Change `CSV_PATH`
- **Duplicate ratio:** Adjust `DUPLICATE_RATIO` (default: 10%)
- **Kafka throughput:** Tune `BATCH_SIZE`, `LINGER_MS`, `BUFFER_MEMORY`
- **BigQuery target:** Change `BQ_PROJECT_ID`, `BQ_DATASET`


