import pandas as pd
from sqlalchemy import create_engine
import os  

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data")
engine = create_engine(DB_URL)

def save_forecast_to_db(df: pd.DataFrame, symbol: str):
    df["symbol"] = symbol
    df.to_sql("forecast", engine, if_exists="append", index=False)