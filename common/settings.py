import yaml
import pydantic
from pydantic import Field
from datetime import datetime
from pathlib import Path


_path = Path(__file__).parent.parent / "settings.yaml"

with open(_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

tickers = config["tickers"]
settings = config["settings"]

class SettingsModel(pydantic.BaseModel):
    start: datetime = Field("2000-01-01", description="YYYY-MM-DD")
    end: datetime = Field("2000-01-01", description="YYYY-MM-DD")
    interval: str


class TickersModel(pydantic.BaseModel):
    tickers: list[str]