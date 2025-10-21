from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import yfinance as yf
import pandas as pd

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data"

def fetch_ohlcv():
    engine = create_engine(DB_URL)
    query = """
        SELECT t.name AS ticker, fs.interval, fs.lookback_days
        FROM tickers t
        JOIN fetch_settings fs ON fs.ticker_id = t.id;
    """
    config_df = pd.read_sql(query, engine)

    for _, row in config_df.iterrows():
        ticker = row["ticker"]
        interval = row["interval"]
        lookback_days = row["lookback_days"]

        end = datetime.now()
        start = end - timedelta(days=lookback_days)

        print(f"Загружаем {ticker} [{interval}] за {lookback_days} дней")

        try:
            df = yf.download(ticker, start=start, end=end, interval=interval)
            if df.empty:
                print(f"Пустые данные для {ticker}")
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            df.reset_index(inplace=True)
            df["ticker"] = ticker
            df.rename(columns=str.lower, inplace=True)
            df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]
            df.to_sql("ohlcv", engine, if_exists="append", index=False)

        except Exception as e:
            print(f"Ошибка при загрузке {ticker}: {e}")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="fetch_ohlcv_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_ohlcv_task",
        python_callable=fetch_ohlcv,
    )