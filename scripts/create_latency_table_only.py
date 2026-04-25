import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB,
    user=PG_USER, password=PG_PASSWORD
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_pipeline_latency (
        latency_id       SERIAL PRIMARY KEY,
        batch_id         BIGINT,
        sink_name        VARCHAR(50),
        row_count        INTEGER,
        latency_ms       INTEGER,
        recorded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
print("Created fact_pipeline_latency table.")
conn.close()
