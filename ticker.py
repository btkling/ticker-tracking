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
    filename = "symbols_to_track.csv"
    directory = "/mnt/e/git/ticker-tracking/"
    fpath = f"{directory}{filename}"

    symbols = extract_symbollist(fpath)

    TODAY = pd.Timestamp.today()
    STDATE = TODAY + pd.Timedelta(-30, 'day')

    # import data
    tickerdata = import_tickerdata(
        symbols = symbols,
        startdate = STDATE,
        enddate = TODAY
    )

    # combine data and export to csv
    output_filename = "data/latest_ticker_output.csv"
    output_fpath = f"{directory}{output_filename}"
    tickerdata.to_csv(output_fpath)

if __name__ == "__main__":
    main()