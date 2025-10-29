
import pydantic
from pydantic import Field, BaseModel
from typing import List, Optional
from datetime import datetime



class FetchSettings(BaseModel):
    ticker: str
    interval: str
    lookback_days: int


class TickersModel(pydantic.BaseModel):
    tickers: list[str]


class TaskLog(BaseModel):
    id: int
    dag_id: Optional[str]
    task_id: Optional[str]
    log_level: Optional[str]
    message: Optional[str]
    timestamp: datetime