import logging
import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

password = os.getenv("DB_PASSWORD")

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=betting_project;"
    "UID=sa;"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)

AGG_QUERY = """
SELECT b.user_id,
       b.total_bets,
       COALESCE(p.total_payments, 0) AS total_payments,
       b.total_bets - COALESCE(p.total_payments, 0) AS profit
FROM (
    SELECT user_id, SUM(amount) AS total_bets
    FROM bets
    GROUP BY user_id
) b
LEFT JOIN (
    SELECT user_id, SUM(amount) AS total_payments
    FROM payments
    GROUP BY user_id
) p ON b.user_id = p.user_id;
"""

def main():
    try:
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql_query(AGG_QUERY, conn)
    except Exception:
        logging.exception("Failed to load aggregated data from database")
        raise

    if df.empty:
        logging.info("No aggregated data returned.")
        return

    df["total_bets"] = pd.to_numeric(df["total_bets"], errors="coerce").fillna(0)
    df["total_payments"] = pd.to_numeric(df["total_payments"], errors="coerce").fillna(0)
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["profit"] = df["profit"].fillna(df["total_bets"] - df["total_payments"])

    print(df)

    print("\nTop user by bets:")
    top_idx = df["total_bets"].idxmax()
    top_user = df.loc[top_idx]
    print(top_user.to_string())

    print("\nUsers with negative profit:")
    neg = df[df["profit"] < 0]
    if neg.empty:
        print("None")
    else:
        print(neg.to_string(index=False))


if __name__ == "__main__":
    main()