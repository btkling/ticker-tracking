import pandas as pd
import numpy as np


def main():
    # variables shared across all brokers
    win = 'E:/'
    lin = 'mnt/e/'
    base_fp = f'{win}git/ticker-tracking/data/'
    output_fp = f"{base_fp}clean_statements/"
    start_date = "2022-11-09"
    end_date = "2023-01-01"

    
    # broker-specific variables that change
    schwab_input_file = 'XXXXX911_Transactions_20230104-181416.csv'
    merrill_input_file = 'ExportData06012023084449.csv'

    # static schwab variables
    schwabpath = f'{base_fp}schwab/'
    schwab_actions = [
        'Buy',
        'Sell',
        'Reinvest Shares',
    ]
    merrill_descriptions = [
        "Purchase",
        "Reinvestment"
    ]
    
    schwab_numbers = [
        "Quantity", 
        "Price", 
        "Fees & Comm", 
        "Amount"
    ]
    merrill_numbers = [
        "Quantity",
        "Price",
        "Amount"
    ]

    schwab_symbols = [
        'LGILX',
        'SICNX',
        'SWISX',
        'SWLGX',
        'SWTSX'
    ]
    merrill_symbols = [
        'CFAGX',
        'PREIX'
    ]

    # static merrill variables
    merrillpath = f'{base_fp}merrill/'

    # process schwab statement
    schwabdf = read_schwab_statement(schwabpath, schwab_input_file)
    schwabdf = clean_dates(schwabdf, "Date")
    schwabdf = filter_schwab_actions(schwabdf, schwab_actions)
    for col in schwab_numbers:
        schwabdf = clean_numbercol(schwabdf, col)
    schwabdf = filter_symbols(schwabdf, "Symbol", schwab_symbols)
    schwabdf = filter_date(schwabdf, "Date", start_date, end_date)
    schwabdf.to_csv(f'{output_fp}schwab.csv', index=False)

    # process merrill statement
    merrilldf = read_merrill_statement(merrillpath, merrill_input_file)
    merrilldf = clean_dates(merrilldf, "Trade Date")
    merrilldf = filter_merrill_descriptions(merrilldf, merrill_descriptions)
    for col in merrill_numbers:
        merrilldf = clean_numbercol(merrilldf, col)
    merrilldf = filter_symbols(merrilldf, 'Symbol', merrill_symbols)
    merrilldf = filter_date(merrilldf, 'Trade Date', start_date, end_date) 
    export_merrill(merrilldf, output_fp, 'merrill.csv')





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
        skiprows=[0,1,2,3,5,6],
        error_bad_lines = False
    )
    mdf = mdf.rename(str.strip, axis='columns')
    mdf = mdf.rename({'Symbol/ CUSIP':'Symbol'}, axis='columns')

    mdf = mdf.dropna(subset = ['Trade Date'])
    mdf['Quantity'].replace(' ', '', inplace=True)
    mdf['Quantity'].replace('', np.nan, inplace=True)
    mdf = mdf.dropna(subset = ['Quantity'])

    mdf['Symbol'] = mdf['Symbol'].str.strip()
    return mdf


def clean_dates(df, datecol):
    df[datecol] = pd.to_datetime(df[datecol], errors="coerce")
    df = df.dropna(subset=[datecol])
    return df


def filter_schwab_actions(df, valid_actions):
    df = df[df["Action"].isin(valid_actions)]
    return df


def filter_merrill_descriptions(df, valid_description_starts):
    filtered_df = df[df['Description'].str.startswith(tuple(valid_description_starts)) ]
    filtered_df['Transaction Type'] = None

    # iterate over the rows of the dataframe
    for index, row in filtered_df.iterrows():
        description = row['Description']
        # iterate over the list of valid descriptions
        for desc in valid_description_starts:
            if description.startswith(desc):
                filtered_df.at[index, 'Transaction Type'] = desc
    return filtered_df


def clean_numbercol(df, column):
    # Remove "$", ",", "(", and ")" characters
    df[column] = df[column].astype(str)
    df[column] = df[column].str.replace(r'[$,()]', "")
    df[column].replace(' ', '', inplace=True)
    df[column].replace('', np.nan, inplace=True)
    df[column] = df[column].astype(float)
    return df


def filter_date(df, datecol, start_date, end_date):
    df = df[(df[datecol] >= start_date) & (df[datecol] <= end_date)]

    return df


def filter_symbols(df, symbolcol, symbollist):
    df = df[ df[symbolcol].isin(symbollist) ]
    return df

def export_merrill(df: pd.DataFrame, output_filepath, output_filename):
    df = df[['Trade Date', 'Symbol', 'Quantity', 'Amount', 'Transaction Type']]
    df['Amount'] = df['Amount']
    df.to_csv(f'{output_filepath}{output_filename}', index=False)

if __name__ == '__main__':
    main() 