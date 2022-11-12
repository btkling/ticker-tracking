import yfinance as yf
import pandas as pd

# list of tickers
symbols = pd.read_csv("symbols_to_track.csv", header=None)

tickers = symbols[0].tolist()

# import data
tickerdata = yf.download(
    tickers = tickers,
    group_by = "Ticker"
)

tickerdata = tickerdata.stack(level=0).rename_axis(["Date","Ticker"]).reset_index(level=1)

# combine data and export to csv
tickerdata.to_csv('test_data2.csv')