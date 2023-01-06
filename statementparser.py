import pandas as pd
import re


def main():
    base_fp = '/mnt/e/git/ticker-tracking/data/'
    schwabpath = f'{base_fp}schwab/'
    file_name = 'XXXXX911_Transactions_20230104-181416.csv'
    start_date = "2022-11-09"
    end_date = "2023-01-01"
    schwabdf = read_schwab_statement(schwabpath, file_name)

    schwabdf = clean_dates(schwabdf, "Date")

    schwab_actions = [
        'Buy',
        'Sell',
        'Reinvest Shares',
    ]
    schwabdf = filter_schwab_actions(schwabdf, schwab_actions)

    schwab_numbers = [
        "Quantity", 
        "Price", 
        "Fees & Comm", 
        "Amount"
    ]
    for col in schwab_numbers:
        schwabdf = clean_numbercol(schwabdf, col)

    schwab_symbols = [
        'LGILX',
        'SICNX',
        'SWISX',
        'SWLGX',
        'SWTSX'
    ]
    schwabdf = filter_symbols(schwabdf, "Symbol", schwab_symbols)

    schwabdf = filter_date(schwabdf, "Date", start_date, end_date)

    schwabdf.to_csv('/mnt/e/git/ticker-tracking/data/clean_statements/schwab.csv', index=False)





def read_schwab_statement(file_path, file_name):
    full_path = f"{file_path}{file_name}"
    schwabdf = pd.read_csv(
        full_path,
        skiprows=1
    )
    return schwabdf


def read_merrill_statement(file_path, file_name):
    full_path = f"{file_path}{file_name}"
    mdf = pd.read_csv(
        full_path,
        skiprows=1
    )
    return mdf


def clean_dates(df, datecol):
    df[datecol] = pd.to_datetime(df[datecol], errors="coerce")
    df = df.dropna(subset=[datecol])
    return df


def filter_schwab_actions(df, valid_actions):
    df = df[df["Action"].isin(valid_actions)]
    return df


def clean_numbercol(df, column):
    # Remove "$", ",", "(", and ")" characters
    df[column] = df[column].astype(str)
    df[column] = df[column].str.replace(r'[$,()]', "")
    df[column] = df[column].astype(float)
    return df


def filter_date(df, datecol, start_date, end_date):
    df = df[(df[datecol] >= start_date) & (df[datecol] <= end_date)]

    return df


def filter_symbols(df, symbolcol, symbollist):
    df = df[ df[symbolcol].isin(symbollist) ]
    return df


if __name__ == '__main__':
    main() 