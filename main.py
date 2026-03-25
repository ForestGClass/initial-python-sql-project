import argparse
import getpass
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "betting_project")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")

AGG_QUERY = """
SELECT 
    COALESCE(b.user_id, p.user_id) AS user_id,
    COALESCE(b.total_bets, 0) AS total_bets,
    COALESCE(p.total_payments, 0) AS total_payments,
    COALESCE(b.total_bets, 0) - COALESCE(p.total_payments, 0) AS profit
FROM (
    SELECT user_id, SUM(amount) AS total_bets
    FROM bets
    GROUP BY user_id
) b
FULL OUTER JOIN (
    SELECT user_id, SUM(amount) AS total_payments
    FROM payments
    GROUP BY user_id
) p ON b.user_id = p.user_id;
"""


def get_connection(password: str):
    return pyodbc.connect(
        driver="{ODBC Driver 18 for SQL Server}",
        server=f"{DB_HOST},{DB_PORT}",
        database=DB_NAME,
        uid=DB_USER,
        pwd=password,
        TrustServerCertificate="yes",
    )


def load_data(password: str) -> pd.DataFrame:
    try:
        with get_connection(password) as conn:
            df = pd.read_sql_query(AGG_QUERY, conn)
    except pyodbc.InterfaceError:
        logging.exception("Database connection failed. Check driver, credentials, and host settings.")
        raise
    except pyodbc.OperationalError:
        logging.exception("Operational database error. Verify SQL Server is running and credentials are correct.")
        raise
    except Exception:
        logging.exception("Failed to load aggregated data from database.")
        raise

    if df.empty:
        logging.info("No aggregated data returned.")
        return df

    df["total_bets"] = pd.to_numeric(df["total_bets"], errors="coerce").fillna(0)
    df["total_payments"] = pd.to_numeric(df["total_payments"], errors="coerce").fillna(0)
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["profit"] = df["profit"].fillna(df["total_bets"] - df["total_payments"])

    return df


def print_analysis(df: pd.DataFrame) -> None:
    print(f"Data shape: {df.shape}")
    print(df.head(50).to_string(index=False))

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

    print("\nBest profit user:")
    print(df.sort_values("profit", ascending=False).head(1).to_string(index=False))

    print("\nWorst loss user:")
    print(df.sort_values("profit").head(1).to_string(index=False))


def save_csv(df: pd.DataFrame, out_csv: Path) -> None:
    try:
        df.to_csv(out_csv, index=False)
        print(f"\nSaved to {out_csv}")
    except Exception:
        logging.exception("Failed to save CSV.")


def save_plot(df: pd.DataFrame, out_png: Path, top_n: int) -> None:
    try:
        import matplotlib.pyplot as plt

        plot_df = df.nlargest(top_n, "total_bets").set_index("user_id")[
            ["total_bets", "total_payments"]
        ]
        ax = plot_df.plot(kind="bar", figsize=(12, 6))
        ax.set_title("Top users: Bets vs Payments")
        ax.set_ylabel("Amount")
        plt.tight_layout()
        plt.savefig(out_png)
        print(f"Saved plot to {out_png}")
    except Exception:
        logging.exception("Failed to generate or save plot.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate bets and payments data and compute profit per user."
    )
    parser.add_argument("--password", help="Database password (overrides DB_PASSWORD from .env)")
    parser.add_argument("--out-csv", default="analysis_result.csv", help="Output CSV file path")
    parser.add_argument("--out-png", default="bets_vs_payments.png", help="Output PNG file path")
    parser.add_argument("--top-n", type=int, default=20, help="Top N users to include in chart")
    return parser.parse_args()


def get_password(cli_password: str | None) -> str:
    password = cli_password or DB_PASSWORD

    if not password and sys.stdin.isatty():
        try:
            password = getpass.getpass(f"DB password for user '{DB_USER}': ")
        except Exception:
            password = None

    if not password:
        logging.error("Database password not provided. Use .env, --password, or interactive input.")
        raise SystemExit(1)

    return password


def main() -> None:
    args = parse_args()
    out_csv = Path(args.out_csv)
    out_png = Path(args.out_png)

    password = get_password(args.password)
    df = load_data(password)

    if df.empty:
        logging.info("No data to save or plot. Exiting.")
        raise SystemExit(0)

    print_analysis(df)
    save_csv(df, out_csv)
    save_plot(df, out_png, args.top_n)


if __name__ == "__main__":
    main()
