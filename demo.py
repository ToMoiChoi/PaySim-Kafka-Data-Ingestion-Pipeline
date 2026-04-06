import pandas as pd

CONN = "postgresql://paysim:paysim123@localhost:5432/paysim_dw"
df = pd.read_sql("SELECT * FROM fact_binance_trades", CONN)
print(df)