
import pydantic
from pydantic import Field, BaseModel

class FetchSettings(BaseModel):
    ticker: str
    interval: str
    lookback_days: int


class TickersModel(pydantic.BaseModel):
    tickers: list[str]