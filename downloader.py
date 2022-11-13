import pandas as pd
import yfinance as yf

def load_symbols(fp:str) -> list():
    '''Loads symbols to track'''
    sym_df = pd.read_csv(fp, header=None)
    symbols = sym_df[0].tolist()

    return symbols

def download_tickerdata() -> pd.DataFrame:
