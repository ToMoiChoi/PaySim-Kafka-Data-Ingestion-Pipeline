"""
scripts/check_pg_data.py - Kiểm tra chi tiết dữ liệu PostgreSQL Data Warehouse
=================================================================================
Hiển thị tổng quan đầy đủ: fact table, dimensions, fraud, rewards, channels,
phân tích theo thời gian, top users, so sánh dữ liệu batch vs live.

Chạy: make check-db  hoặc  python scripts/check_pg_data.py
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
    """Chạy SQL query và trả về DataFrame."""
    try:
        return pd.read_sql_query(query, DATABASE_URL)
    except Exception as e:
        print(f"  ❌ Lỗi query: {e}")
        return pd.DataFrame()


def header(num: str, title: str):
    print(f"\n{'─' * W}")
    print(f" {num}  {title}")
    print(f"{'─' * W}")


def fmt_money(x):
    return f"${x:,.2f}" if pd.notnull(x) else "$0"


def fmt_int(x):
    return f"{int(x):,}" if pd.notnull(x) else "0"


def print_df(df: pd.DataFrame):
    if df.empty:
        print("  (Không có dữ liệu)")
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
    print(f"{'═' * W}")
    print(f"  🔍  POSTGRESQL DATA WAREHOUSE — BÁO CÁO CHI TIẾT")
    print(f"  📍  Host: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"{'═' * W}")

    # ═══════════════════════════════════════════════════════════════
    # 1. TỔNG QUAN HỆ THỐNG
    # ═══════════════════════════════════════════════════════════════
    header("1️⃣", "TỔNG QUAN HỆ THỐNG (Fact + Dimension Tables)")

    overview_query = """
        SELECT 'fact_transactions' AS table_name, COUNT(*) AS row_count FROM fact_transactions
        UNION ALL SELECT 'dim_users',              COUNT(*) FROM dim_users
        UNION ALL SELECT 'dim_merchants',          COUNT(*) FROM dim_merchants
        UNION ALL SELECT 'dim_account',            COUNT(*) FROM dim_account
        UNION ALL SELECT 'dim_transaction_type',   COUNT(*) FROM dim_transaction_type
        UNION ALL SELECT 'dim_location',           COUNT(*) FROM dim_location
        UNION ALL SELECT 'dim_channel',            COUNT(*) FROM dim_channel
        UNION ALL SELECT 'dim_date',               COUNT(*) FROM dim_date
        UNION ALL SELECT 'dim_time',               COUNT(*) FROM dim_time
        ORDER BY row_count DESC;
    """
    df_overview = fetch(overview_query)
    if not df_overview.empty:
        df_overview['row_count'] = df_overview['row_count'].apply(fmt_int)
        print_df(df_overview)

    # Tổng volume
    df_vol = fetch("SELECT SUM(amount) AS total, MIN(transaction_time) AS first_tx, MAX(transaction_time) AS last_tx FROM fact_transactions;")
    if not df_vol.empty and pd.notnull(df_vol['total'].iloc[0]):
        print(f"\n  💰 Tổng volume     : {fmt_money(df_vol['total'].iloc[0])}")
        print(f"  📅 Giao dịch đầu   : {df_vol['first_tx'].iloc[0]}")
        print(f"  📅 Giao dịch cuối  : {df_vol['last_tx'].iloc[0]}")

    # ═══════════════════════════════════════════════════════════════
    # 2. DIMENSION TABLE DETAILS
    # ═══════════════════════════════════════════════════════════════
    header("2️⃣", "DIMENSION TABLES — Chi tiết")

    print("\n  📋 Transaction Types:")
    df_types_dim = fetch("SELECT type_id, type_name, is_reward_eligible, reward_multiplier FROM dim_transaction_type ORDER BY type_id;")
    print_df(df_types_dim)

    print(f"\n  📋 Channels:")
    df_channels_dim = fetch("SELECT channel_id, channel_name, device_os FROM dim_channel ORDER BY channel_id;")
    print_df(df_channels_dim)

    print(f"\n  📋 Locations (sample):")
    df_loc = fetch("SELECT location_id, city, region FROM dim_location ORDER BY region, city LIMIT 10;")
    print_df(df_loc)

    # ═══════════════════════════════════════════════════════════════
    # 3. GIAO DỊCH GẦN NHẤT
    # ═══════════════════════════════════════════════════════════════
    header("3️⃣", "10 GIAO DỊCH GẦN NHẤT")

    df_recent = fetch("""
        SELECT
            f.transaction_id,
            f.transaction_time,
            f.type,
            f.amount,
            u.user_segment,
            f.reward_points,
            f.channel_id,
            f."isFraud" AS is_fraud
        FROM fact_transactions f
        LEFT JOIN dim_users u ON f.user_id = u.user_id
        ORDER BY f.transaction_time DESC
        LIMIT 10;
    """)
    if not df_recent.empty:
        df_recent['amount'] = df_recent['amount'].apply(fmt_money)
        df_recent['reward_points'] = df_recent['reward_points'].apply(fmt_int)
        df_recent['transaction_id'] = df_recent['transaction_id'].str[:12] + '...'
        print_df(df_recent)

    # ═══════════════════════════════════════════════════════════════
    # 4. PHÂN TÍCH THEO LOẠI GIAO DỊCH
    # ═══════════════════════════════════════════════════════════════
    header("4️⃣", "PHÂN TÍCH THEO LOẠI GIAO DỊCH (Transaction Type)")

    df_by_type = fetch("""
        SELECT
            f.type,
            dt.type_name,
            COUNT(*)                    AS tx_count,
            SUM(f.amount)               AS total_volume,
            ROUND(AVG(f.amount), 2)     AS avg_amount,
            MIN(f.amount)               AS min_amount,
            MAX(f.amount)               AS max_amount,
            SUM(f.reward_points)        AS total_rewards,
            dt.reward_multiplier
        FROM fact_transactions f
        LEFT JOIN dim_transaction_type dt ON f.type_id = dt.type_id
        GROUP BY f.type, dt.type_name, dt.reward_multiplier
        ORDER BY tx_count DESC;
    """)
    if not df_by_type.empty:
        total_tx = df_by_type['tx_count'].sum()
        df_by_type['pct'] = (df_by_type['tx_count'] / total_tx * 100).round(1).astype(str) + '%'
        df_by_type['total_volume'] = df_by_type['total_volume'].apply(fmt_money)
        df_by_type['avg_amount'] = df_by_type['avg_amount'].apply(fmt_money)
        df_by_type['min_amount'] = df_by_type['min_amount'].apply(fmt_money)
        df_by_type['max_amount'] = df_by_type['max_amount'].apply(fmt_money)
        df_by_type['total_rewards'] = df_by_type['total_rewards'].apply(fmt_int)
        df_by_type['tx_count'] = df_by_type['tx_count'].apply(fmt_int)
        print_df(df_by_type)

    # ═══════════════════════════════════════════════════════════════
    # 5. PHÂN TÍCH THEO HẠNG USER + REWARD
    # ═══════════════════════════════════════════════════════════════
    header("5️⃣", "THỐNG KÊ THEO HẠNG USER (Segment & Rewards)")

    df_seg = fetch("""
        SELECT
            u.user_segment,
            COUNT(DISTINCT f.user_id)   AS unique_users,
            COUNT(f.transaction_id)     AS tx_count,
            SUM(f.amount)               AS total_volume,
            ROUND(AVG(f.amount), 2)     AS avg_amount,
            SUM(f.reward_points)        AS total_rewards,
            ROUND(AVG(f.reward_points), 0) AS avg_reward_per_tx
        FROM fact_transactions f
        LEFT JOIN dim_users u ON f.user_id = u.user_id
        GROUP BY u.user_segment
        ORDER BY total_volume DESC;
    """)
    if not df_seg.empty:
        df_seg['total_volume'] = df_seg['total_volume'].apply(fmt_money)
        df_seg['avg_amount'] = df_seg['avg_amount'].apply(fmt_money)
        df_seg['total_rewards'] = df_seg['total_rewards'].apply(lambda x: f"{int(x):,} pts" if pd.notnull(x) else "0 pts")
        df_seg['avg_reward_per_tx'] = df_seg['avg_reward_per_tx'].apply(lambda x: f"{int(x):,} pts" if pd.notnull(x) else "0 pts")
        df_seg['unique_users'] = df_seg['unique_users'].apply(fmt_int)
        df_seg['tx_count'] = df_seg['tx_count'].apply(fmt_int)
        print_df(df_seg)

    # ═══════════════════════════════════════════════════════════════
    # 6. PHÂN TÍCH FRAUD
    # ═══════════════════════════════════════════════════════════════
    header("6️⃣", "PHÂN TÍCH GIAN LẬN (Fraud Detection)")

    df_fraud = fetch("""
        SELECT
            COUNT(*) FILTER (WHERE "isFraud" = 1)        AS fraud_count,
            COUNT(*) FILTER (WHERE "isFraud" = 0)        AS legit_count,
            COUNT(*) FILTER (WHERE "isFlaggedFraud" = 1) AS flagged_count,
            COUNT(*)                                      AS total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE "isFraud" = 1) / NULLIF(COUNT(*), 0), 4)  AS fraud_rate_pct,
            SUM(CASE WHEN "isFraud" = 1 THEN amount ELSE 0 END) AS fraud_volume,
            SUM(amount) AS total_volume
        FROM fact_transactions;
    """)
    if not df_fraud.empty:
        r = df_fraud.iloc[0]
        print(f"  📊 Tổng giao dịch     : {fmt_int(r['total'])}")
        print(f"  ✅ Hợp lệ             : {fmt_int(r['legit_count'])}")
        print(f"  🚨 Gian lận (isFraud) : {fmt_int(r['fraud_count'])}  ({r['fraud_rate_pct']}%)")
        print(f"  🔴 Flagged            : {fmt_int(r['flagged_count'])}")
        print(f"  💸 Volume gian lận    : {fmt_money(r['fraud_volume'])}")
        print(f"  💰 Tổng volume        : {fmt_money(r['total_volume'])}")

    # Top fraud by type
    df_fraud_type = fetch("""
        SELECT type, COUNT(*) AS fraud_count, SUM(amount) AS fraud_volume
        FROM fact_transactions WHERE "isFraud" = 1
        GROUP BY type ORDER BY fraud_count DESC;
    """)
    if not df_fraud_type.empty:
        print(f"\n  📋 Fraud theo loại giao dịch:")
        df_fraud_type['fraud_volume'] = df_fraud_type['fraud_volume'].apply(fmt_money)
        df_fraud_type['fraud_count'] = df_fraud_type['fraud_count'].apply(fmt_int)
        print_df(df_fraud_type)

    # ═══════════════════════════════════════════════════════════════
    # 7. PHÂN TÍCH THEO CHANNEL
    # ═══════════════════════════════════════════════════════════════
    header("7️⃣", "PHÂN BỐ THEO KÊNH GIAO DỊCH (Channel)")

    df_channel = fetch("""
        SELECT
            c.channel_name,
            c.device_os,
            COUNT(f.transaction_id)     AS tx_count,
            SUM(f.amount)               AS total_volume,
            ROUND(AVG(f.amount), 2)     AS avg_amount
        FROM fact_transactions f
        LEFT JOIN dim_channel c ON f.channel_id = c.channel_id
        GROUP BY c.channel_name, c.device_os
        ORDER BY tx_count DESC;
    """)
    if not df_channel.empty:
        total_ch = df_channel['tx_count'].sum()
        df_channel['pct'] = (df_channel['tx_count'] / total_ch * 100).round(1).astype(str) + '%'
        df_channel['total_volume'] = df_channel['total_volume'].apply(fmt_money)
        df_channel['avg_amount'] = df_channel['avg_amount'].apply(fmt_money)
        df_channel['tx_count'] = df_channel['tx_count'].apply(fmt_int)
        print_df(df_channel)

    # ═══════════════════════════════════════════════════════════════
    # 8. PHÂN TÍCH THEO THỜI GIAN
    # ═══════════════════════════════════════════════════════════════
    header("8️⃣", "PHÂN BỐ THEO THỜI GIAN (Time Analysis)")

    # By date
    df_by_date = fetch("""
        SELECT
            DATE(transaction_time)  AS tx_date,
            COUNT(*)                AS tx_count,
            SUM(amount)             AS daily_volume
        FROM fact_transactions
        GROUP BY DATE(transaction_time)
        ORDER BY tx_date DESC
        LIMIT 10;
    """)
    if not df_by_date.empty:
        print("  📅 Giao dịch theo ngày (10 ngày gần nhất):")
        df_by_date['daily_volume'] = df_by_date['daily_volume'].apply(fmt_money)
        df_by_date['tx_count'] = df_by_date['tx_count'].apply(fmt_int)
        print_df(df_by_date)

    # By time_of_day
    df_tod = fetch("""
        SELECT
            t.time_of_day,
            COUNT(f.transaction_id) AS tx_count,
            SUM(f.amount)           AS total_volume
        FROM fact_transactions f
        LEFT JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.time_of_day
        ORDER BY tx_count DESC;
    """)
    if not df_tod.empty:
        print(f"\n  🕐 Giao dịch theo buổi trong ngày:")
        df_tod['total_volume'] = df_tod['total_volume'].apply(fmt_money)
        df_tod['tx_count'] = df_tod['tx_count'].apply(fmt_int)
        print_df(df_tod)

    # ═══════════════════════════════════════════════════════════════
    # 9. TOP USERS
    # ═══════════════════════════════════════════════════════════════
    header("9️⃣", "TOP 10 USERS (Theo volume giao dịch)")

    df_top = fetch("""
        SELECT
            f.user_id,
            u.user_segment,
            COUNT(f.transaction_id)     AS tx_count,
            SUM(f.amount)               AS total_volume,
            SUM(f.reward_points)        AS total_rewards,
            SUM(CASE WHEN f."isFraud" = 1 THEN 1 ELSE 0 END) AS fraud_txs
        FROM fact_transactions f
        LEFT JOIN dim_users u ON f.user_id = u.user_id
        GROUP BY f.user_id, u.user_segment
        ORDER BY total_volume DESC
        LIMIT 10;
    """)
    if not df_top.empty:
        df_top['total_volume'] = df_top['total_volume'].apply(fmt_money)
        df_top['total_rewards'] = df_top['total_rewards'].apply(lambda x: f"{int(x):,} pts" if pd.notnull(x) else "0 pts")
        df_top['tx_count'] = df_top['tx_count'].apply(fmt_int)
        print_df(df_top)

    # ═══════════════════════════════════════════════════════════════
    # 10. SO SÁNH DỮ LIỆU BATCH vs LIVE
    # ═══════════════════════════════════════════════════════════════
    header("🔟", "SO SÁNH DỮ LIỆU BATCH (CSV) vs LIVE (Binance)")

    # Batch types: PAYMENT, TRANSFER, CASH_OUT, DEBIT, CASH_IN
    # Live types:  SPOT_BUY, SPOT_SELL, LARGE_TRADE, MICRO_TRADE, WHALE_ALERT
    df_source = fetch("""
        SELECT
            CASE
                WHEN type IN ('SPOT_BUY','SPOT_SELL','LARGE_TRADE','MICRO_TRADE','WHALE_ALERT')
                THEN '🔴 LIVE (Binance)'
                ELSE '📦 BATCH (CSV)'
            END AS data_source,
            COUNT(*)            AS tx_count,
            SUM(amount)         AS total_volume,
            ROUND(AVG(amount), 2) AS avg_amount,
            MIN(transaction_time) AS first_tx,
            MAX(transaction_time) AS last_tx
        FROM fact_transactions
        GROUP BY
            CASE
                WHEN type IN ('SPOT_BUY','SPOT_SELL','LARGE_TRADE','MICRO_TRADE','WHALE_ALERT')
                THEN '🔴 LIVE (Binance)'
                ELSE '📦 BATCH (CSV)'
            END
        ORDER BY data_source;
    """)
    if not df_source.empty:
        df_source['total_volume'] = df_source['total_volume'].apply(fmt_money)
        df_source['avg_amount'] = df_source['avg_amount'].apply(fmt_money)
        df_source['tx_count'] = df_source['tx_count'].apply(fmt_int)
        print_df(df_source)

    # ═══════════════════════════════════════════════════════════════
    # FOOTER
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'═' * W}")
    print("  ✅ BÁO CÁO HOÀN TẤT")
    print(f"  💡 Mẹo: Chạy 'make run-live' để stream dữ liệu Binance real-time")
    print(f"          Chạy 'make run-producer' để load dữ liệu CSV batch")
    print(f"{'═' * W}\n")


if __name__ == "__main__":
    main()
