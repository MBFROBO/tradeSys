from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from plugins.sql_logger import log_task_message
import pandas as pd
import pandas_ta as ta
import os

if not hasattr(pd.Series, "append"):
    pd.Series.append = lambda self, other: pd.concat([self, other])

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

            # === Базовые метрики ===
            group["daily_return"] = group["close"].pct_change()
            group["volatility"] = group["daily_return"].rolling(window=20).std()

            # --- Метрики SMA ---
            group["sma_5"] = group["close"].rolling(window=5).mean()
            group["sma_20"] = group["close"].rolling(window=20).mean()
            group["sma_50"] = group["close"].rolling(window=50).mean()
            group["sma_200"] = group["close"].rolling(window=200).mean()

            # --- Кумулятивная доходность ---
            group["cumulative_return"] = (1 + group["daily_return"]).cumprod() - 1

            # === Трендовые индикаторы ===
            group["ema_20"] = ta.ema(group["close"], length=20)
            group["ema_50"] = ta.ema(group["close"], length=50)

            macd = ta.macd(group["close"], fast=12, slow=26, signal=9)
            group["macd"] = macd["MACD_12_26_9"]
            group["macd_signal"] = macd["MACDS_12_26_9"]
            group["macd_hist"] = macd["MACDH_12_26_9"]

            group["adx"] = ta.adx(group["high"], group["low"], group["close"], length=14)["ADX_14"]

            # === Осцилляторы ===
            group["rsi"] = ta.rsi(group["close"], length=14)
            stoch = ta.stoch(group["high"], group["low"], group["close"], k=14, d=3)
            group["stoch_k"] = stoch.iloc[:, 2]  # slowK
            group["stoch_d"] = stoch.iloc[:, 3]  # slowD

            # === Волатильность ===
            group["atr"] = ta.atr(group["high"], group["low"], group["close"], length=14)

            bbands = ta.bbands(group["close"], length=20, std=2)
            group["bb_upper"] = bbands["BBU_20"]
            group["bb_middle"] = bbands["BBM_20"]
            group["bb_lower"] = bbands["BBL_20"]
            group["bb_width"] = group["bb_upper"] - group["bb_lower"]


            group["processed_at"] = datetime.utcnow()

            # Сохраняем только нужные колонки
            result = group[
                [
                    "ticker", "date", "sma_5", "sma_20", "sma_50", "sma_200",
                    "cumulative_return",
                    "ema_20", "ema_50", "macd", "macd_signal", "macd_hist",
                    "adx", "rsi", "stoch_k", "stoch_d",
                    "atr",
                    "bb_upper", "bb_middle", "bb_lower", "bb_width",
                    "daily_return", "volatility",
                    "processed_at"
                ]
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

    # === Запись результатов ===
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


# === Аргументы DAG ===
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