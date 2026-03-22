import psycopg2

conn = psycopg2.connect("postgresql://paysim:paysim123@localhost:5432/paysim_dw")
cur = conn.cursor()
staging = "fact_transactions_staging_1"

query = f"""
            INSERT INTO fact_transactions
                SELECT * FROM {staging}
            ON CONFLICT (transaction_id) DO UPDATE SET
                account_id       = EXCLUDED.account_id,
                merchant_id      = EXCLUDED.merchant_id,
                type_id          = EXCLUDED.type_id,
                location_id      = EXCLUDED.location_id,
                channel_id       = EXCLUDED.channel_id,
                date_key         = EXCLUDED.date_key,
                time_key         = EXCLUDED.time_key,
                transaction_time = EXCLUDED.transaction_time,
                amount           = EXCLUDED.amount,
                reward_points    = EXCLUDED.reward_points,
                type             = EXCLUDED.type,
                step             = EXCLUDED.step,
                "oldbalanceOrg"  = EXCLUDED."oldbalanceOrg",
                "newbalanceOrig" = EXCLUDED."newbalanceOrig",
                "oldbalanceDest" = EXCLUDED."oldbalanceDest",
                "newbalanceDest" = EXCLUDED."newbalanceDest",
                "isFraud"        = EXCLUDED."isFraud",
                "isFlaggedFraud" = EXCLUDED."isFlaggedFraud",
                ip_address       = EXCLUDED.ip_address
        """
try:
    cur.execute(query)
    conn.commit()
    print("SUCCESS")
except Exception as e:
    print(f"FAILED: {e}")
