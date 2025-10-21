from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
import os
import pandas as pd
from datetime import datetime
from common import FetchSettings

app = FastAPI(title="Finance ML API")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres-data:5432/airflow_data")
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

@app.get("/settings")
def get_settings():
    query = """
        SELECT t.name AS ticker, fs.interval, fs.lookback_days
        FROM tickers t
        JOIN fetch_settings fs ON fs.ticker_id = t.id
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.post("/settings/update")
def update_settings(settings: FetchSettings):
    with engine.begin() as conn:
        res = conn.execute(text("SELECT id FROM tickers WHERE name=:t"), {"t": settings.ticker}).fetchone()
        if not res:
            raise HTTPException(status_code=404, detail="Ticker not found")
        conn.execute(
            text("""
                UPDATE fetch_settings
                SET interval=:i, lookback_days=:l
                WHERE ticker_id=:id
            """),
            {"i": settings.interval, "l": settings.lookback_days, "id": res[0]},
        )
    return {"status": "ok", "updated": settings.ticker}

@app.get("/")
def root():
    return {"message": "Finance ML API is running", "time": datetime.now()}