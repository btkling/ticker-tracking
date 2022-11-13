import pandas as pd
import yfinance as yf
import datetime as dt

def load_symbols(fp:str) -> list():
    '''Loads symbols to track'''
    sym_df = pd.read_csv(fp, header=None)
    symbols = sym_df[0].tolist()

    return symbols

def download_tickerdata(symbols:list(), ndays:int) -> pd.DataFrame:
    '''given a list of symbols and a time period, retrieve the
    latest stock information.'''

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days = ndays)

    end_date_str = end_date.isoformat()
    start_date_str = start_date.isoformat()

    tickerdata = yf.download(
        tickers = symbols,
        group_by = "Ticker",
        start = start_date_str,
        end = end_date_str
    )

    tickerdata = tickerdata.stack(level=0).rename_axis(["Date","Ticker"]).reset_index(level=1)

    return tickerdata
