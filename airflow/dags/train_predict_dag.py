from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from sklearn.linear_model import LinearRegression

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data"

def train_and_predict():
    engine = create_engine(DB_URL)
    tickers_df = pd.read_sql("SELECT name FROM tickers", engine)
    tickers = tickers_df["name"].tolist()

    for ticker in tickers:
        df = pd.read_sql(f"""
            SELECT * FROM ohlcv WHERE ticker='{ticker}' ORDER BY date ASC
        """, engine)

        if len(df) < 10:
            continue

        df["target"] = df["close"].shift(-1)
        df.dropna(inplace=True)

        X = df[["open", "high", "low", "close", "volume"]]
        y = df["target"]

        model = LinearRegression()
        model.fit(X, y)

        latest = df.iloc[-1][["open", "high", "low", "close", "volume"]].values.reshape(1, -1)
        pred = model.predict(latest)[0]

        result = pd.DataFrame([{
            "ticker": ticker,
            "date": datetime.now(),
            "model": "LinearRegression",
            "prediction": float(pred),
        }])

        result.to_sql("predictions", engine, if_exists="append", index=False)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(hours=12),
}

with DAG(
    dag_id="train_predict_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:
    train_task = PythonOperator(
        task_id="train_and_predict",
        python_callable=train_and_predict,
    )