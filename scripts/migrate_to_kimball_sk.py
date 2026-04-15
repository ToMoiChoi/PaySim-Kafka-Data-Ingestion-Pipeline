"""
scripts/migrate_to_kimball_sk.py - Migrate Star Schema to Kimball Surrogate Keys
==================================================================================
This script PRESERVES ALL EXISTING DATA while migrating the schema:
  - Adds surrogate key columns to dim_volume_category and dim_crypto_pair
  - Adds FK columns (crypto_pair_key, volume_category_key) to fact_binance_trades
  - Populates new columns from existing natural key values
  - Drops old varchar FK columns from fact table
  - Updates constraints

Run this ONCE to migrate. After migration, use the updated spark_processor.py.

IMPORTANT: Backup your database before running this script!
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "paysim_dw")
PG_USER     = os.getenv("POSTGRES_USER", "paysim")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "paysim123")


# Surrogate Key mappings (must match seed_dimensions scripts)
VOLUME_SK_MAP = {
    "RETAIL": 1,
    "PROFESSIONAL": 2,
    "INSTITUTIONAL": 3,
    "WHALE": 4,
}

CRYPTO_SK_MAP = {
    "BTCUSDT": 1,
    "ETHUSDT": 2,
    "BNBUSDT": 3,
    "SOLUSDT": 4,
    "XRPUSDT": 5,
}


def migrate():
    print("=" * 65)
    print("  MIGRATION: Star Schema → Kimball Surrogate Keys")
    print("  DATA WILL BE PRESERVED — only format changes")
    print("=" * 65)
    print(f"  Host : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # ==============================================================
        # STEP 1: Migrate dim_volume_category
        # Add volume_category_key column, populate it, set as PK
        # ==============================================================
        print("[STEP 1/6] Migrating dim_volume_category...")

        # Check if column already exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'dim_volume_category' AND column_name = 'volume_category_key'
        """)
        if cur.fetchone():
            print("  > volume_category_key already exists, skipping.")
        else:
            # Drop old PK constraint
            cur.execute("""
                ALTER TABLE dim_volume_category DROP CONSTRAINT IF EXISTS dim_volume_category_pkey CASCADE;
            """)

            # Add surrogate key column
            cur.execute("""
                ALTER TABLE dim_volume_category ADD COLUMN volume_category_key INTEGER;
            """)

            # Populate from mapping
            for natural_key, sk in VOLUME_SK_MAP.items():
                cur.execute("""
                    UPDATE dim_volume_category SET volume_category_key = %s WHERE volume_category = %s
                """, (sk, natural_key))

            # Set NOT NULL + new PK
            cur.execute("""
                ALTER TABLE dim_volume_category ALTER COLUMN volume_category_key SET NOT NULL;
            """)
            cur.execute("""
                ALTER TABLE dim_volume_category ADD PRIMARY KEY (volume_category_key);
            """)
            # Add UNIQUE on natural key for reference integrity
            cur.execute("""
                ALTER TABLE dim_volume_category ADD CONSTRAINT uq_volume_category UNIQUE (volume_category);
            """)
            print("  > Added volume_category_key (SK 1-4) as new PK ✓")

        # ==============================================================
        # STEP 2: Migrate dim_crypto_pair
        # Add crypto_pair_key column, populate it, set as PK
        # ==============================================================
        print("[STEP 2/6] Migrating dim_crypto_pair...")

        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'dim_crypto_pair' AND column_name = 'crypto_pair_key'
        """)
        if cur.fetchone():
            print("  > crypto_pair_key already exists, skipping.")
        else:
            cur.execute("""
                ALTER TABLE dim_crypto_pair DROP CONSTRAINT IF EXISTS dim_crypto_pair_pkey CASCADE;
            """)

            cur.execute("""
                ALTER TABLE dim_crypto_pair ADD COLUMN crypto_pair_key INTEGER;
            """)

            for natural_key, sk in CRYPTO_SK_MAP.items():
                cur.execute("""
                    UPDATE dim_crypto_pair SET crypto_pair_key = %s WHERE crypto_symbol = %s
                """, (sk, natural_key))

            cur.execute("""
                ALTER TABLE dim_crypto_pair ALTER COLUMN crypto_pair_key SET NOT NULL;
            """)
            cur.execute("""
                ALTER TABLE dim_crypto_pair ADD PRIMARY KEY (crypto_pair_key);
            """)
            cur.execute("""
                ALTER TABLE dim_crypto_pair ADD CONSTRAINT uq_crypto_symbol UNIQUE (crypto_symbol);
            """)
            print("  > Added crypto_pair_key (SK 1-5) as new PK ✓")

        # ==============================================================
        # STEP 3: Add new FK columns to fact_binance_trades
        # ==============================================================
        print("[STEP 3/6] Adding surrogate key columns to fact_binance_trades...")

        # Add volume_category_key column if not exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fact_binance_trades' AND column_name = 'volume_category_key'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE fact_binance_trades ADD COLUMN volume_category_key INTEGER;
            """)
            print("  > Added volume_category_key column ✓")
        else:
            print("  > volume_category_key already exists, skipping.")

        # Add crypto_pair_key column if not exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fact_binance_trades' AND column_name = 'crypto_pair_key'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE fact_binance_trades ADD COLUMN crypto_pair_key INTEGER;
            """)
            print("  > Added crypto_pair_key column ✓")
        else:
            print("  > crypto_pair_key already exists, skipping.")

        # ==============================================================
        # STEP 4: Populate new FK columns from existing varchar values
        # ==============================================================
        print("[STEP 4/6] Populating surrogate keys from existing data...")

        # Populate volume_category_key from volume_category
        for natural_key, sk in VOLUME_SK_MAP.items():
            cur.execute("""
                UPDATE fact_binance_trades
                SET volume_category_key = %s
                WHERE volume_category = %s AND (volume_category_key IS NULL OR volume_category_key != %s)
            """, (sk, natural_key, sk))

        cur.execute("SELECT COUNT(*) FROM fact_binance_trades WHERE volume_category_key IS NOT NULL")
        vol_count = cur.fetchone()[0]
        print(f"  > Populated volume_category_key for {vol_count:,} rows ✓")

        # Populate crypto_pair_key from crypto_symbol
        for natural_key, sk in CRYPTO_SK_MAP.items():
            cur.execute("""
                UPDATE fact_binance_trades
                SET crypto_pair_key = %s
                WHERE crypto_symbol = %s AND (crypto_pair_key IS NULL OR crypto_pair_key != %s)
            """, (sk, natural_key, sk))

        cur.execute("SELECT COUNT(*) FROM fact_binance_trades WHERE crypto_pair_key IS NOT NULL")
        pair_count = cur.fetchone()[0]
        print(f"  > Populated crypto_pair_key for {pair_count:,} rows ✓")

        # ==============================================================
        # STEP 5: Drop old varchar FK columns & constraints
        # ==============================================================
        print("[STEP 5/6] Dropping old varchar FK columns...")

        # Drop old FK constraints first
        cur.execute("ALTER TABLE fact_binance_trades DROP CONSTRAINT IF EXISTS fk_trade_category;")
        cur.execute("ALTER TABLE fact_binance_trades DROP CONSTRAINT IF EXISTS fk_trade_symbol;")

        # Drop old varchar columns
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fact_binance_trades' AND column_name = 'volume_category'
        """)
        if cur.fetchone():
            cur.execute("ALTER TABLE fact_binance_trades DROP COLUMN volume_category;")
            print("  > Dropped volume_category (varchar) column ✓")

        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fact_binance_trades' AND column_name = 'crypto_symbol'
        """)
        if cur.fetchone():
            cur.execute("ALTER TABLE fact_binance_trades DROP COLUMN crypto_symbol;")
            print("  > Dropped crypto_symbol (varchar) column ✓")

        # Drop old indexes that reference removed columns
        cur.execute("DROP INDEX IF EXISTS idx_binance_symbol;")

        # ==============================================================
        # STEP 6: Add new FK constraints (integer surrogate keys)
        # ==============================================================
        print("[STEP 6/6] Adding new FK constraints...")

        cur.execute("""
            ALTER TABLE fact_binance_trades
            ADD CONSTRAINT fk_trade_pair FOREIGN KEY (crypto_pair_key)
            REFERENCES dim_crypto_pair(crypto_pair_key);
        """)
        cur.execute("""
            ALTER TABLE fact_binance_trades
            ADD CONSTRAINT fk_trade_category FOREIGN KEY (volume_category_key)
            REFERENCES dim_volume_category(volume_category_key);
        """)

        # Add new indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_binance_pair   ON fact_binance_trades(crypto_pair_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_binance_volcat ON fact_binance_trades(volume_category_key);")

        print("  > FK constraints added (integer surrogate keys) ✓")

        # ==============================================================
        # COMMIT
        # ==============================================================
        conn.commit()

        print(f"\n{'=' * 65}")
        print("[DONE] Migration completed successfully!")
        print("  - dim_volume_category: PK = volume_category_key (INT)")
        print("  - dim_crypto_pair:     PK = crypto_pair_key (INT)")
        print("  - fact_binance_trades: FK = integers only (Kimball-compliant)")
        print("  - ALL EXISTING DATA PRESERVED ✓")
        print("=" * 65)

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] Migration FAILED — rolled back all changes!")
        print(f"  Error: {e}")
        print("  Your data is safe, nothing was changed.")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    migrate()
