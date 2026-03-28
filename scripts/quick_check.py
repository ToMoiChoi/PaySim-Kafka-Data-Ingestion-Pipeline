import pandas as pd
from dotenv import load_dotenv
import os, sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()
import sqlalchemy as sa
url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','paysim')}:{os.getenv('POSTGRES_PASSWORD','paysim123')}@localhost:5432/{os.getenv('POSTGRES_DB','paysim_dw')}"
engine = sa.create_engine(url)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

print("=== TYPES ===")
df = pd.read_sql_query("SELECT type, COUNT(*) as cnt FROM fact_transactions GROUP BY type ORDER BY cnt DESC", engine)
print(df.to_string(index=False))

print("\n=== DATES (latest 10) ===")
df2 = pd.read_sql_query("SELECT DATE(transaction_time) as dt, COUNT(*) as cnt FROM fact_transactions GROUP BY DATE(transaction_time) ORDER BY dt DESC LIMIT 10", engine)
print(df2.to_string(index=False))

print("\n=== LIVE DATA (Binance types) ===")
df3 = pd.read_sql_query("SELECT type, COUNT(*) as cnt, MIN(transaction_time) as first_tx, MAX(transaction_time) as last_tx FROM fact_transactions WHERE type IN ('MICRO_TRADE','SPOT_BUY','SPOT_SELL','LARGE_TRADE','WHALE_ALERT') GROUP BY type ORDER BY cnt DESC", engine)
print(df3.to_string(index=False) if not df3.empty else "(NONE - no live data in Postgres)")
