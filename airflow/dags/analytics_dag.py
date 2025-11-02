from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from plugins.sql_logger import log_task_message
import pandas as pd
import os

DB_URL_DEFAULT = "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data"
DB_URL = os.getenv("DB_URL", DB_URL_DEFAULT)

log_task_conf = {
    "dag_id": "analyze_ohlcv_dag",
    "task_id": "analyze_ohlcv_task",
}


def analyze_ohlcv():
    engine = create_engine(DB_URL)

    # Очистка таблицы перед заполнением
    query_drop = "DELETE FROM ohlcv_features;"
    with engine.begin() as conn:
        conn.execute(query_drop)


    df = pd.read_sql("SELECT * FROM ohlcv ORDER BY ticker, date", engine)
    if df.empty:
        log_task_message(
            dag_id=log_task_conf["dag_id"],
            task_id=log_task_conf["task_id"],
            log_level="WARNING",
            message="Нет данных для анализа",
        )
        raise ValueError("Нет данных для анализа")

    all_results = []

    for ticker, group in df.groupby("ticker"):
        try:
            group = group.sort_values("date").reset_index(drop=True)

            # Метрики
            group["sma_5"] = group["close"].rolling(window=5).mean()
            group["sma_20"] = group["close"].rolling(window=20).mean()
            group["daily_return"] = group["close"].pct_change()
            group["volatility"] = group["daily_return"].rolling(window=20).std()

            group["processed_at"] = datetime.utcnow()

            # Сохраняем только нужные колонки
            result = group[
                ["ticker", "date", "sma_5", "sma_20", "volatility", "daily_return", "processed_at"]
            ]
            all_results.append(result)
        except Exception as e:
            log_task_message(
                dag_id=log_task_conf["dag_id"],
                task_id=log_task_conf["task_id"],
                log_level="ERROR",
                message=f"Ошибка при анализе {ticker}: {e}",
            )
            raise ValueError(f"Ошибка при анализе {ticker}")

    try:
        features_df = pd.concat(all_results)
        features_df.to_sql("ohlcv_features", engine, if_exists="append", index=False)
        log_task_message(
            dag_id=log_task_conf["dag_id"],
            task_id=log_task_conf["task_id"],
            log_level="INFO",
            message=f"Записано {len(features_df)} строк в таблицу ohlcv_features",
        )
    except Exception as e:
        log_task_message(
            dag_id=log_task_conf["dag_id"],
            task_id=log_task_conf["task_id"],
            log_level="ERROR",
            message=f"Ошибка при сохранении данных: {e}",
        )
        raise ValueError("Ошибка при сохранении данных")

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
    dag_id="analyze_ohlcv_dag",
    default_args=default_args,
    schedule_interval="@daily", 
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["analytics", "ohlcv"],
) as dag:

    analyze_data = PythonOperator(
        task_id="analyze_ohlcv_task",
        python_callable=analyze_ohlcv,
        on_execute_callback=log_task_start, 
    )