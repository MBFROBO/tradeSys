from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import apimoex
import requests
import os
import time

from plugins.sql_logger import log_task_message

DB_URL_DEFAULT = "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data"
DB_URL = os.getenv("DB_URL", DB_URL_DEFAULT)

log_task_conf = {
    "dag_id": "fetch_ohlcv_dag",
    "task_id": "fetch_ohlcv_task",
}


def fetch_moex_ohlcv(ticker: str, start: datetime, end: datetime, interval: str) -> pd.DataFrame:
    """Получает OHLCV данные с MOEX через библиотеку apimoex."""
    session = requests.Session()
    interval_map = {
        "1m": 1,
        "10m": 10,
        "1h": 60,
        "1d": 24,
    }

    interval_code = interval_map.get(interval, 24)

    try:
        data = apimoex.get_market_candles(
            session=session,
            security=ticker,
            market="shares",
            engine="stock",
            interval=interval_code,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log_task_message(
            dag_id=log_task_conf["dag_id"],
            task_id=log_task_conf["task_id"],
            log_level="ERROR",
            message=f"Ошибка запроса MOEX API для {ticker}: {e}",
        )
        raise RuntimeError(f"Ошибка запроса MOEX API для {ticker}: {e}")

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["ticker"] = ticker
    df.rename(columns={"begin": "date"}, inplace=True)
    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]
    return df


def fetch_ohlcv():
    engine = create_engine(DB_URL)
    query = """
        SELECT t.name AS ticker, fs.interval, fs.lookback_days
        FROM tickers t
        JOIN fetch_settings fs ON fs.ticker_id = t.id;
    """
    config_df = pd.read_sql(query, engine)

    # Очистим таблицу перед загрузкой
    with engine.begin() as conn:
        conn.execute("DELETE FROM ohlcv;")

    for _, row in config_df.iterrows():
        ticker = row["ticker"]
        interval = row["interval"]
        lookback_days = row["lookback_days"]

        end = datetime.now()
        start = end - timedelta(days=lookback_days)

        log_task_message(
            dag_id=log_task_conf["dag_id"],
            task_id=log_task_conf["task_id"],
            log_level="INFO",
            message=f"Загружаем {ticker} [{interval}] за {lookback_days} дней",
        )

        try:
            df = fetch_moex_ohlcv(ticker, start, end, interval)

            if df.empty:
                log_task_message(
                    dag_id=log_task_conf["dag_id"],
                    task_id=log_task_conf["task_id"],
                    log_level="WARNING",
                    message=f"Нет данных для {ticker}",
                )
                continue

            df.to_sql("ohlcv", engine, if_exists="append", index=False)
            time.sleep(0.5)  # мягкий лимит для API

        except Exception as e:
            log_task_message(
                dag_id=log_task_conf["dag_id"],
                task_id=log_task_conf["task_id"],
                log_level="ERROR",
                message=f"Ошибка при загрузке {ticker}: {e}",
            )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def log_task_start(context):
    log_task_message(
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
        log_level="INFO",
        message="Task started",
    )


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
        on_execute_callback=log_task_start,
    )