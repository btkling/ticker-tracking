import pandas as pd
import yfinance as yf

# list of tickers

def extract_symbollist(filename):
    symbols = pd.read_csv(
        filename,
        header = None
    )
   
    return symbols[0].tolist()

def import_tickerdata(symbols, startdate, enddate):
    dl = yf.download(
        tickers = symbols,
        group_by = "Ticker",
        start = startdate,
        end = enddate
    )

    return dl.stack(level=0).rename_axis(["Date","Ticker"]).reset_index(level=1)


def main():
    symbols = extract_symbollist("symbols_to_track.csv")

    # import data
    tickerdata = import_tickerdata(
        symbols = symbols,
        startdate = "2022-11-01",
        enddate = "2022-12-31"
    )

    # combine data and export to csv
    tickerdata.to_csv('test_data3.csv')

if __name__ == "__main__":
    main()