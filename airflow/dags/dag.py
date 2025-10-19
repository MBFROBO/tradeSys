from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import os
from sklearn.ensemble import RandomForestRegressor

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://airflow:airflow@postgres-data/airflow_data"
)
engine = create_engine(DATABASE_URL)
TICKER = "SBER.ME"
START_DATE = "2022-01-01"


def fetch_to_postgres():
    df = yf.download(TICKER, start=START_DATE)
    df.reset_index(inplace=True)
    df["ticker"] = TICKER
    df.to_sql("ohlcv", engine, if_exists="append", index=False)
    print(f"Загружено {len(df)} строк в PostgreSQL")


def train_and_predict():
    df = pd.read_sql(
        f"SELECT * FROM ohlcv WHERE ticker='{TICKER}' ORDER BY date", engine
    )
    df["Return"] = df["close"].pct_change()
    df.dropna(inplace=True)

    X = df[["open", "high", "low", "volume"]]
    y = df["close"].shift(-1).dropna()
    X = X.iloc[:-1]

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    preds = model.predict(X)

    out = pd.DataFrame(
        {
            "ticker": TICKER,
            "date": df["date"].iloc[:-1],
            "model": "RandomForest",
            "prediction": preds,
        }
    )
    out.to_sql("predictions", engine, if_exists="append", index=False)
    print("🌲 Прогнозы сохранены в PostgreSQL")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ml_finance_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Хранение данных и прогнозов в PostgreSQL",
) as dag:
    t1 = PythonOperator(task_id="fetch_to_postgres", python_callable=fetch_to_postgres)
    t2 = PythonOperator(task_id="train_and_predict", python_callable=train_and_predict)

    t1 >> t2
