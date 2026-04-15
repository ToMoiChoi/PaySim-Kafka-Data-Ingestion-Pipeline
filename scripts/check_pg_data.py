"""
scripts/check_pg_data.py - PostgreSQL Data Warehouse Detailed Report (Binance Crypto)
=================================================================================
Displays a full overview: fact table, dimensions, crypto pairs, volume categories,
anomaly detection, maker/taker distribution, time analysis, and top whale trades.

Run: make check-db  or  python scripts/check_pg_data.py
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

W = 75  # Output width


def fetch(query: str) -> pd.DataFrame:
    """Run an SQL query and return a DataFrame."""
    try:
        return pd.read_sql_query(query, DATABASE_URL)
    except Exception as e:
        print(f"  [ERROR] Query failed: {e}")
        return pd.DataFrame()


def header(num: str, title: str):
    print(f"\n{'-' * W}")
    print(f" {num}  {title}")
    print(f"{'-' * W}")


def fmt_money(x):
    return f"${x:,.2f}" if pd.notnull(x) else "$0"


def fmt_int(x):
    return f"{int(x):,}" if pd.notnull(x) else "0"


def print_df(df: pd.DataFrame):
    if df.empty:
        print("  (No data available)")
        return
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 40)
    for line in df.to_string(index=False).split('\n'):
        print(f"  {line}")


def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

    print()
    print(f"{'=' * W}")
    print(f"  POSTGRESQL DATA WAREHOUSE -- DETAILED REPORT (BINANCE CRYPTO)")
    print(f"  Host: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"{'=' * W}")

    # =================================================================
    # 1. SYSTEM OVERVIEW
    # =================================================================
    header("[1]", "SYSTEM OVERVIEW (Fact + Dimension Tables)")

    overview_query = """
        SELECT 'fact_binance_trades' AS table_name, COUNT(*) AS row_count FROM fact_binance_trades
        UNION ALL SELECT 'dim_crypto_pair',           COUNT(*) FROM dim_crypto_pair
        UNION ALL SELECT 'dim_volume_category',       COUNT(*) FROM dim_volume_category
        UNION ALL SELECT 'dim_exchange_rate',         COUNT(*) FROM dim_exchange_rate
        UNION ALL SELECT 'dim_date',                  COUNT(*) FROM dim_date
        UNION ALL SELECT 'dim_time',                  COUNT(*) FROM dim_time
        ORDER BY row_count DESC;
    """
    df_overview = fetch(overview_query)
    if not df_overview.empty:
        df_overview['row_count'] = df_overview['row_count'].apply(fmt_int)
        print_df(df_overview)

    # Total volume
    df_vol = fetch("SELECT SUM(amount_usd) AS total, MIN(trade_time) AS first_tx, MAX(trade_time) AS last_tx FROM fact_binance_trades;")
    if not df_vol.empty and pd.notnull(df_vol['total'].iloc[0]):
        print(f"\n  Total volume      : {fmt_money(df_vol['total'].iloc[0])}")
        print(f"  First transaction : {df_vol['first_tx'].iloc[0]}")
        print(f"  Last transaction  : {df_vol['last_tx'].iloc[0]}")

    # =================================================================
    # 2. DIMENSION TABLE DETAILS
    # =================================================================
    header("[2]", "DIMENSION TABLES -- Details")

    print("\n  Crypto Pairs:")
    df_pairs = fetch("SELECT * FROM dim_crypto_pair ORDER BY crypto_symbol;")
    print_df(df_pairs)

    print(f"\n  Volume Categories:")
    df_cats = fetch("SELECT * FROM dim_volume_category ORDER BY min_usd;")
    if not df_cats.empty:
        df_cats['min_usd'] = df_cats['min_usd'].apply(fmt_money)
        df_cats['max_usd'] = df_cats['max_usd'].apply(lambda x: fmt_money(x) if pd.notnull(x) else 'MAX')
        print_df(df_cats)

    # =================================================================
    # 3. RECENT TRADES
    # =================================================================
    header("[3]", "10 MOST RECENT TRADES")

    df_recent = fetch("""
        SELECT
            f.transaction_id,
            f.trade_time,
            cp.crypto_symbol,
            f.price,
            f.quantity,
            f.amount_usd,
            vc.volume_category,
            f.is_buyer_maker,
            f.is_anomaly
        FROM fact_binance_trades f
        LEFT JOIN dim_crypto_pair cp ON f.crypto_pair_key = cp.crypto_pair_key
        LEFT JOIN dim_volume_category vc ON f.volume_category_key = vc.volume_category_key
        ORDER BY f.trade_time DESC
        LIMIT 10;
    """)
    if not df_recent.empty:
        df_recent['amount_usd'] = df_recent['amount_usd'].apply(fmt_money)
        df_recent['transaction_id'] = df_recent['transaction_id'].str[:12] + '...'
        df_recent['price'] = df_recent['price'].apply(lambda x: f"${x:,.4f}" if pd.notnull(x) else "")
        df_recent['quantity'] = df_recent['quantity'].apply(lambda x: f"{x:,.4f}")
        print_df(df_recent)

    # =================================================================
    # 4. ANALYSIS BY CRYPTO PAIR
    # =================================================================
    header("[4]", "ANALYSIS BY CRYPTO PAIR")

    df_by_pair = fetch("""
        SELECT
            cp.crypto_symbol,
            COUNT(*)                    AS tx_count,
            SUM(f.amount_usd)           AS total_volume,
            ROUND(AVG(f.amount_usd), 2) AS avg_amount,
            MIN(f.amount_usd)           AS min_amount,
            MAX(f.amount_usd)           AS max_amount
        FROM fact_binance_trades f
        JOIN dim_crypto_pair cp ON f.crypto_pair_key = cp.crypto_pair_key
        GROUP BY cp.crypto_symbol
        ORDER BY total_volume DESC;
    """)
    if not df_by_pair.empty:
        total_tx = df_by_pair['tx_count'].sum()
        df_by_pair['pct'] = (df_by_pair['tx_count'] / total_tx * 100).round(1).astype(str) + '%'
        df_by_pair['total_volume'] = df_by_pair['total_volume'].apply(fmt_money)
        df_by_pair['avg_amount'] = df_by_pair['avg_amount'].apply(fmt_money)
        df_by_pair['min_amount'] = df_by_pair['min_amount'].apply(fmt_money)
        df_by_pair['max_amount'] = df_by_pair['max_amount'].apply(fmt_money)
        df_by_pair['tx_count'] = df_by_pair['tx_count'].apply(fmt_int)
        print_df(df_by_pair)

    # =================================================================
    # 5. VOLUME CATEGORY STATISTICS
    # =================================================================
    header("[5]", "STATISTICS BY VOLUME CATEGORY")

    df_seg = fetch("""
        SELECT
            vc.volume_category,
            COUNT(f.transaction_id)       AS tx_count,
            SUM(f.amount_usd)             AS total_volume,
            ROUND(AVG(f.amount_usd), 2)   AS avg_amount
        FROM fact_binance_trades f
        JOIN dim_volume_category vc ON f.volume_category_key = vc.volume_category_key
        GROUP BY vc.volume_category
        ORDER BY total_volume DESC;
    """)
    if not df_seg.empty:
        df_seg['total_volume'] = df_seg['total_volume'].apply(fmt_money)
        df_seg['avg_amount'] = df_seg['avg_amount'].apply(fmt_money)
        df_seg['tx_count'] = df_seg['tx_count'].apply(fmt_int)
        print_df(df_seg)

    # =================================================================
    # 6. ANOMALY DETECTION
    # =================================================================
    header("[6]", "ANOMALY DETECTION ANALYSIS")

    df_anomaly = fetch("""
        SELECT
            COUNT(*) FILTER (WHERE is_anomaly = True)         AS anomaly_count,
            COUNT(*) FILTER (WHERE is_anomaly = False)        AS normal_count,
            COUNT(*)                                          AS total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly = True) / NULLIF(COUNT(*), 0), 4)  AS anomaly_rate_pct,
            SUM(CASE WHEN is_anomaly = True THEN amount_usd ELSE 0 END) AS anomaly_volume,
            SUM(amount_usd) AS total_volume
        FROM fact_binance_trades;
    """)
    if not df_anomaly.empty:
        r = df_anomaly.iloc[0]
        print(f"  Total trades      : {fmt_int(r['total'])}")
        print(f"  Normal            : {fmt_int(r['normal_count'])}")
        print(f"  Anomalies (true)  : {fmt_int(r['anomaly_count'])}  ({r['anomaly_rate_pct']}%)")
        print(f"  Anomaly volume    : {fmt_money(r['anomaly_volume'])}")
        print(f"  Total volume      : {fmt_money(r['total_volume'])}")

    # Anomalies by crypto pair
    df_anomaly_pair = fetch("""
        SELECT cp.crypto_symbol, COUNT(*) AS anomaly_count, SUM(f.amount_usd) AS anomaly_volume
        FROM fact_binance_trades f
        JOIN dim_crypto_pair cp ON f.crypto_pair_key = cp.crypto_pair_key
        WHERE f.is_anomaly = True
        GROUP BY cp.crypto_symbol ORDER BY anomaly_count DESC;
    """)
    if not df_anomaly_pair.empty:
        print(f"\n  Anomalies by Crypto Pair:")
        df_anomaly_pair['anomaly_volume'] = df_anomaly_pair['anomaly_volume'].apply(fmt_money)
        df_anomaly_pair['anomaly_count'] = df_anomaly_pair['anomaly_count'].apply(fmt_int)
        print_df(df_anomaly_pair)

    # =================================================================
    # 7. MAKER / TAKER DISTRIBUTION
    # =================================================================
    header("[7]", "MAKER / TAKER DISTRIBUTION (is_buyer_maker)")

    df_maker = fetch("""
        SELECT
            CASE WHEN is_buyer_maker THEN 'MAKER' ELSE 'TAKER' END AS trade_role,
            COUNT(transaction_id)       AS tx_count,
            SUM(amount_usd)             AS total_volume,
            ROUND(AVG(amount_usd), 2)   AS avg_amount
        FROM fact_binance_trades
        GROUP BY is_buyer_maker
        ORDER BY tx_count DESC;
    """)
    if not df_maker.empty:
        total_m = df_maker['tx_count'].sum()
        df_maker['pct'] = (df_maker['tx_count'] / total_m * 100).round(1).astype(str) + '%'
        df_maker['total_volume'] = df_maker['total_volume'].apply(fmt_money)
        df_maker['avg_amount'] = df_maker['avg_amount'].apply(fmt_money)
        df_maker['tx_count'] = df_maker['tx_count'].apply(fmt_int)
        print_df(df_maker)

    # =================================================================
    # 8. TIME ANALYSIS
    # =================================================================
    header("[8]", "TIME DISTRIBUTION ANALYSIS")

    # By date
    df_by_date = fetch("""
        SELECT
            DATE(trade_time)        AS tx_date,
            COUNT(*)                AS tx_count,
            SUM(amount_usd)         AS daily_volume
        FROM fact_binance_trades
        GROUP BY DATE(trade_time)
        ORDER BY tx_date DESC
        LIMIT 10;
    """)
    if not df_by_date.empty:
        print("  Trades by date (last 10 days):")
        df_by_date['daily_volume'] = df_by_date['daily_volume'].apply(fmt_money)
        df_by_date['tx_count'] = df_by_date['tx_count'].apply(fmt_int)
        print_df(df_by_date)

    # By time_of_day
    df_tod = fetch("""
        SELECT
            t.time_of_day,
            COUNT(f.transaction_id) AS tx_count,
            SUM(f.amount_usd)       AS total_volume
        FROM fact_binance_trades f
        LEFT JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.time_of_day
        ORDER BY tx_count DESC;
    """)
    if not df_tod.empty:
        print(f"\n  Trades by time of day:")
        df_tod['total_volume'] = df_tod['total_volume'].apply(fmt_money)
        df_tod['tx_count'] = df_tod['tx_count'].apply(fmt_int)
        print_df(df_tod)

    # =================================================================
    # 9. TOP WHALE TRADES
    # =================================================================
    header("[9]", "TOP 10 LARGEST TRADES (Whales)")

    df_top = fetch("""
        SELECT
            f.transaction_id,
            cp.crypto_symbol,
            f.price,
            f.amount_usd,
            vc.volume_category,
            f.is_anomaly
        FROM fact_binance_trades f
        LEFT JOIN dim_crypto_pair cp ON f.crypto_pair_key = cp.crypto_pair_key
        LEFT JOIN dim_volume_category vc ON f.volume_category_key = vc.volume_category_key
        ORDER BY f.amount_usd DESC
        LIMIT 10;
    """)
    if not df_top.empty:
        df_top['amount_usd'] = df_top['amount_usd'].apply(fmt_money)
        df_top['transaction_id'] = df_top['transaction_id'].str[:12] + '...'
        df_top['price'] = df_top['price'].apply(lambda x: f"${x:,.4f}" if pd.notnull(x) else "")
        print_df(df_top)

    # =================================================================
    # FOOTER
    # =================================================================
    print(f"\n{'=' * W}")
    print("  REPORT COMPLETE")
    print(f"  Tip: Run 'make run-live' to stream Binance data in real time")
    print(f"       Run 'make run-spark' to start the stream processing engine")
    print(f"{'=' * W}\n")


if __name__ == "__main__":
    main()
