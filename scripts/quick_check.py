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

print("=== CRYPTO PAIRS ===")
df = pd.read_sql_query("SELECT crypto_symbol, COUNT(*) as cnt FROM fact_binance_trades GROUP BY crypto_symbol ORDER BY cnt DESC", engine)
print(df.to_string(index=False))

print("\n=== DATES (latest 10) ===")
df2 = pd.read_sql_query("SELECT DATE(trade_time) as dt, COUNT(*) as cnt FROM fact_binance_trades GROUP BY DATE(trade_time) ORDER BY dt DESC LIMIT 10", engine)
print(df2.to_string(index=False))

print("\n=== VOLUME CATEGORIES ===")
df3 = pd.read_sql_query("SELECT volume_category, COUNT(*) as cnt, MIN(trade_time) as first_tx, MAX(trade_time) as last_tx FROM fact_binance_trades GROUP BY volume_category ORDER BY cnt DESC", engine)
print(df3.to_string(index=False) if not df3.empty else "(No data)")
