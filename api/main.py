from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import os
import pandas as pd
from datetime import datetime

app = FastAPI(title="Finance ML API")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres-data/airflow_data")
engine = create_engine(DATABASE_URL)

@app.get("/tickers")
def get_tickers():
    query = "SELECT DISTINCT ticker FROM ohlcv;"
    tickers = pd.read_sql(query, engine)
    return tickers["ticker"].tolist()

@app.get("/ohlcv")
def get_ohlcv(
    ticker: str = Query(..., description="Тикер компании, например 'SBER.ME'"),
    limit: int = 100
):
    query = text("SELECT * FROM ohlcv WHERE ticker=:ticker ORDER BY date DESC LIMIT :limit")
    df = pd.read_sql(query, engine, params={"ticker": ticker, "limit": limit})
    return df.to_dict(orient="records")

@app.get("/predictions")
def get_predictions(
    ticker: str = Query(..., description="Тикер компании"),
    model: str | None = None,
    limit: int = 100
):
    base_query = "SELECT * FROM predictions WHERE ticker=:ticker"
    params = {"ticker": ticker}
    if model:
        base_query += " AND model=:model"
        params["model"] = model
    base_query += " ORDER BY date DESC LIMIT :limit"
    params["limit"] = limit
    df = pd.read_sql(text(base_query), engine, params=params)
    return df.to_dict(orient="records")

@app.get("/metrics")
def get_metrics(ticker: str):
    query = text("""
        SELECT model, AVG(prediction) as avg_pred
        FROM predictions
        WHERE ticker=:ticker
        GROUP BY model;
    """)
    df = pd.read_sql(query, engine, params={"ticker": ticker})
    return df.to_dict(orient="records")

@app.get("/")
def root():
    return {"message": "Finance ML API is running", "time": datetime.now()}