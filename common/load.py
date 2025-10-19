import yfinance as yf
import pandas as pd

def data_loader(tickers, settings) -> pd.DataFrame:
    data = yf.download(
        tickers,
        start=settings["start"],
        end=settings["end"],
        interval=settings["interval"],
        group_by='ticker',
        auto_adjust=True
    )
    return data