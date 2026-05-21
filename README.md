# Real-time Crypto Data Pipeline: Binance Trade Analysis & Anomaly Detection

A real-time streaming data pipeline for cryptocurrency market data, built on **Kimball Star Schema** methodology. The system ingests live trade data from Binance WebSocket, processes it through Apache Spark Structured Streaming with a 7-step transformation pipeline, applies dynamic statistical anomaly detection, and stores results in a dual-sink architecture (PostgreSQL + Google BigQuery).

---

## 📊 Dashboards & Analytics

*(PowerBI Visualizations powered by BigQuery Data Warehouse)*

### 1. Thanh Khoản & Dòng Tiền (Liquidity & Cash Flow)

![Thanh khoản & dòng tiền](img/Thanh%20kho%E1%BA%A3n%20&%20d%C3%B2ng%20ti%E1%BB%81n.png)

### 2. Hành Vi Cá Mập (Whale Behavior)

![Hành vi Cá Mập](img/H%C3%A0nh%20vi%20C%C3%A1%20M%E1%BA%ADp%20.png)

### 3. Cảnh Báo Rủi Ro & Bất Thường (Risk & Anomaly Warnings)

![Cảnh báo rủi ro & Bất thường](img/C%E1%BA%A3nh%20b%C3%A1o%20r%E1%BB%A7i%20ro%20&%20B%E1%BA%A5t%20th%C6%B0%E1%BB%9Dng.png)

### 4. Phát Hiện BOT Thao Túng (Wash Trade / Bot Manipulation)

![Phát hiện BOT thao túng](img/Ph%C3%A1t%20hi%E1%BB%87n%20BOT%20thao%20t%C3%BAng.png)

### 5. Hiệu Năng Pipeline (Pipeline Performance & Latency)

![Hiệu năng pipeline](img/Hi%E1%BB%87u%20n%C4%83ng%20pipeline.png)

---

## 🏗 Architecture

```mermaid
graph TD
    A["🔗 Binance WebSocket<br/>(5 pairs, public, no API key)"] -->|Raw JSON| B["📡 Kafka Producer<br/>(live_producer.py)"]
    B -->|"Topic: payment_events_v3<br/>(lz4 compressed)"| C["⚡ Spark Structured Streaming<br/>(spark_processor.py)"]

    subgraph SPARK ["Stage 2: Data Processing (7 Steps)"]
        direction TB
        S1["1. Type Casting<br/>string → numeric"] --> S2["2. Data Cleansing<br/>filter invalid records"]
        S2 --> S3["3. Deduplication<br/>Concat Key + 30s watermark"]
        S3 --> S4["4. Transformation<br/>price × quantity = amount_usd"]
        S4 --> S5["5. Volume Classification<br/>SK: 1=RETAIL, 2=PRO, 3=INST, 4=WHALE"]
        S5 --> S6["6. Dynamic Anomaly Detection<br/>Z-Score, Slippage, Wash Trade"]
        S6 --> S7["7. Star Schema Keys<br/>date_key, time_key, crypto_pair_key"]
    end

    C --> SPARK

    SPARK -->|"Primary Sink<br/>(sync UPSERT)"| E[("🐘 PostgreSQL<br/>fact_binance_trades")]
    SPARK -->|"Backup Sink<br/>(async Parquet + DLQ)"| F["☁️ Google BigQuery"]
    F --> G["📊 Power BI Dashboard"]
```

---

## ⚙ Data Processing Pipeline (7 Steps)

All processing is performed in **Spark Structured Streaming** (`spark_processor.py`).

| Step | Operation | Description |
|------|-----------|-------------|
| 1 | Type Casting | Convert Binance string fields (price, quantity) to numeric types. |
| 2 | Data Cleansing | Filter invalid records (price ≤ 0, quantity ≤ 0, nulls). |
| 3 | Deduplication | Concat crypto_symbol_trade_id + 30-second watermark + dropDuplicates. |
| 4 | Transformation | Calculate amount_usd = price × quantity. |
| 5 | Volume Classification | Map amount_usd → Surrogate Key (1=RETAIL, 2=PRO, 3=INSTITUTIONAL, 4=WHALE). |
| 6 | Anomaly Detection | Dynamic evaluation via foreachBatch using Window functions (Z-score, Wash trade clustering, Slippage). |
| 7 | Star Schema Keys | Generate date_key (yyyyMMdd), time_key (HHmm), and lookup crypto_pair_key. |

### Two-Layer Deduplication Strategy

1. **Layer 1 (Real-time in Spark):** String concatenation (`crypto_symbol` + `_` + `trade_id`) + 30-second watermark window to handle immediate WebSocket reconnect replays.
2. **Layer 2 (Storage-level):** PostgreSQL `INSERT ... ON CONFLICT (transaction_id) DO UPDATE` guarantees 100% deduplication even if duplicates arrive past the 30-second window.

### Dynamic Anomaly Detection Rules (foreachBatch)

Instead of static thresholds, the pipeline uses statistical Window functions per micro-batch:

- **Z-Score Outlier:** `z_score > 3.0` (Trade amount exceeds 3 standard deviations from the micro-batch mean for that symbol).
- **Wash Trade Bot:** `wash_cluster_size >= 4` (High-frequency sameness: 4+ trades occurring at the exact same millisecond timestamp).
- **Price Slippage:** `price_dev_pct > 0.01 & amount_usd > batch_mean` (Price deviates more than 1% from the batch average while having above-average volume).

---

## 🗄 Kimball Star Schema

The data warehouse follows **Kimball's Dimensional Modeling** methodology with **integer surrogate keys** for all dimension tables.

```mermaid
erDiagram
    dim_date {
        BIGINT date_key PK "Smart Key: yyyyMMdd"
        DATE full_date
    }
    dim_time {
        BIGINT time_key PK "Smart Key: HHmm"
        VARCHAR time_of_day
    }
    dim_crypto_pair {
        INT crypto_pair_key PK "Surrogate Key 1-5"
        VARCHAR crypto_symbol "Natural Key"
    }
    dim_volume_category {
        INT volume_category_key PK "Surrogate Key 1-4"
        VARCHAR volume_category "Natural Key"
    }
    fact_binance_trades {
        VARCHAR transaction_id PK "Degenerate Dimension"
        BIGINT trade_id
        BIGINT date_key FK
        BIGINT time_key FK
        INT crypto_pair_key FK
        INT volume_category_key FK
        NUMERIC price
        NUMERIC quantity
        NUMERIC amount_usd
        BOOLEAN is_anomaly
        NUMERIC z_score
        NUMERIC price_dev_pct
        INT wash_cluster_size
    }

    dim_date ||--o{ fact_binance_trades : "date_key"
    dim_time ||--o{ fact_binance_trades : "time_key"
    dim_crypto_pair ||--o{ fact_binance_trades : "crypto_pair_key"
    dim_volume_category ||--o{ fact_binance_trades : "volume_category_key"
```

---

## 🔄 Dual-Sink Storage with Fault Tolerance

| Feature | PostgreSQL (Primary) | BigQuery (Backup) |
|---------|---------------------|-------------------|
| Mode | Synchronous | Asynchronous (buffered) |
| Write Method | UPSERT via psycopg2 execute_values | Parquet Load Job via google-cloud-bigquery |
| Dedup | ON CONFLICT (transaction_id) DO UPDATE | WRITE_APPEND (periodic dedup if needed) |
| Failure Handling | Transaction rollback | DLQ (Dead-Letter Queue): failed Parquet files moved to dlq_bq_failed/ for manual retry |
| Buffer Strategy | Immediate per micro-batch | Flush every 10 seconds OR 5,000 rows |
| Latency Tracking | Logs to fact_pipeline_latency | Included in DLQ system |

---

## 🚀 How to Run

### Prerequisites

- Python 3.10+
- Docker Desktop running (for PostgreSQL, Kafka, Zookeeper)
- Google Cloud service account JSON for BigQuery (Optional)

### Quick Start

```powershell
# 1. Install dependencies
make install

# 2. Start infrastructure (Kafka + Zookeeper + PostgreSQL)
make start-kafka

# 3. Create Star Schema tables
make setup-pg       # PostgreSQL
make setup-bq       # BigQuery (optional)

# 4. Seed dimension tables
make seed-pg        # PostgreSQL
make seed-bq        # BigQuery (optional)

# 5. Run pipeline (open 2 terminals)
make run-live       # Terminal 1: Binance WebSocket → Kafka
make run-spark      # Terminal 2: Spark Processing → Dual Sink
```

---

## 🛠 Tech Stack

| Technology | Version | Purpose |
|------------|---------|---------|
| Python | 3.10+ | Core language |
| Apache Kafka | Confluent 7.5.0 | Message broker |
| Apache Spark | 3.5.0 | Stream processing engine |
| PostgreSQL | 15 | Primary data warehouse |
| Google BigQuery| — | Backup data warehouse |
| Power BI | — | Visualization layer |
| Docker | — | Container orchestration |

*This project is part of a graduation thesis — Real-time Crypto Data Pipeline with Kimball Star Schema.*