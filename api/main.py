from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
import os
import pandas as pd
from datetime import datetime
from common import TickersModel, FetchSettings, TaskLog
from typing import List, Optional
from sqlalchemy.orm import sessionmaker

app = FastAPI(title="Finance ML API")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres-data:5432/airflow_data")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


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
def update_settings(settings: dict):
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

@app.get("/logs", response_model=List[TaskLog])
def get_logs(
    dag_id: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    limit: int = Query(100),
):
    query = "SELECT id, dag_id, task_id, log_level, message, timestamp FROM task_logs"
    filters = []
    params = {}

    if dag_id:
        filters.append("dag_id = :dag_id")
        params["dag_id"] = dag_id
    if task_id:
        filters.append("task_id = :task_id")
        params["task_id"] = task_id

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY id DESC LIMIT :limit"
    params["limit"] = limit

    with SessionLocal() as session:
        rows = session.execute(text(query), params).fetchall()

    return [dict(r._mapping) for r in rows]

@app.get("/")
def root():
    return {"message": "Finance ML API is running", "time": datetime.now()}