import pandas as pd
import re


def main():
    file_path = 'E:/git/ticker-tracking/data/schwab/'
    file_name = 'XXXXX911_Transactions_20230104-181416.csv'
    schwabdf = read_schwab_statement(file_path, file_name)

    schwabdf = clean_dates(schwabdf, "Date")

    valid_actions = [
        'Buy',
        'Sell',
        'Short Term Cap Gain Reinvest',
        'Reinvest Shares',
        'Reinvest Dividend',
        'Long Term Cap Gain Reinvest'
    ]
    schwabdf = filter_schwab_actions(schwabdf, valid_actions)

    numbercols = [
        "Quantity", 
        "Price", 
        "Fees & Comm", 
        "Amount"
    ]
    for col in numbercols:
        schwabdf = clean_numbercol(schwabdf, col)

    schwabdf = filter_date(schwabdf, "Date", "2022-11-09", "2023-01-01")
    print(schwabdf.info())
    print(schwabdf.head())


def read_schwab_statement(file_path, file_name):
    full_path = f"{file_path}{file_name}"
    schwabdf = pd.read_csv(
        full_path,
        skiprows=1
    )
    return schwabdf


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


if __name__ == '__main__':
    main() 