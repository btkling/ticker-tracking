import pandas as pd
import numpy as np


def main():
    # variables shared across all brokers
    # win = 'E:/'
    lin = '/mnt/e/'
    base_fp = f'{lin}git/ticker-tracking/data/'
    input_fp = f"{base_fp}raw_statements/"
    output_fp = f"{base_fp}clean_statements/"
    start_date = "2022-11-09"
    end_date = "2023-01-05"


    # broker-specific variables that change
    schwab_input_file = 'XXXXX911_Transactions_20230104-181416.csv'
    merrill_input_file = 'ExportData06012023084449.csv'
    vanguard_input_file = "ofxdownload.csv"

    # static schwab variables
    schwab_actions = [
        'Buy',
        'Sell',
        'Reinvest Shares',
    ]
    schwab_numbers = [
        "Quantity",
        "Price",
        "Fees & Comm",
        "Amount"
    ]
    schwab_symbols = [
        'LGILX',
        'SICNX',
        'SWISX',
        'SWLGX',
        'SWTSX'
    ]

    # static merrill variables
    merrill_descriptions = [
        "Purchase",
        "Reinvestment"
    ]
    merrill_numbers = [
        "Quantity",
        "Price",
        "Amount"
    ]
    merrill_symbols = [
        'CFAGX',
        'PREIX'
    ]

    # static vanguard variables
    vanguard_types = [
        "Buy",
        "Buy (exchange)",
        "Sell",
        "Sell (exchange)",
        "Reinvestment"
    ]
    vanguard_numbers = [
        "Shares",
        "Share Price",
        "Principal Amount",
        "Commission Fees",
        "Net Amount",
        "Accrued Interest"
    ]
    vanguard_symbols = [
        "VMVAX",
        "VSMAX",
        "VIMAX",
        "VTSAX"
    ]

    # process schwab statement
    schwabdf = read_schwab_statement(input_fp, schwab_input_file)
    schwabdf = clean_dates(schwabdf, "Date")
    schwabdf = filter_transaction_types(schwabdf, "Action", schwab_actions)
    for col in schwab_numbers:
        schwabdf = clean_numbercol(schwabdf, col)
    schwabdf = filter_symbols(schwabdf, "Symbol", schwab_symbols)
    schwabdf = filter_date(schwabdf, "Date", start_date, end_date)
    schwabdf.to_csv(f'{output_fp}schwab.csv', index=False)

    # process merrill statement
    merrilldf = read_merrill_statement(input_fp, merrill_input_file)
    merrilldf = clean_dates(merrilldf, "Trade Date")
    merrilldf = filter_merrill_descriptions(merrilldf, merrill_descriptions)
    for col in merrill_numbers:
        merrilldf = clean_numbercol(merrilldf, col)
    merrilldf = filter_symbols(merrilldf, 'Symbol', merrill_symbols)
    merrilldf = filter_date(merrilldf, 'Trade Date', start_date, end_date)
    export_merrill(merrilldf, output_fp, 'merrill.csv')

    # process vanguard statement
    vanguarddf = read_vanguard_statement(input_fp, vanguard_input_file)
    vanguarddf = clean_dates(vanguarddf, "Trade Date")
    vanguarddf = filter_transaction_types(
            vanguarddf,
            "Transaction Type",
            vanguard_types
    )
    for col in vanguard_numbers:
        vanguarddf = clean_numbercol(vanguarddf, col)
    vanguarddf = filter_symbols(vanguarddf, "Symbol", vanguard_symbols)
    vanguarddf = filter_date(vanguarddf, "Trade Date", start_date, end_date)
    export_vanguard(vanguarddf, output_fp, "vanguard.csv")


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
        skiprows=[0, 1, 2, 3, 5, 6],
        error_bad_lines=False
    )
    mdf = mdf.rename(str.strip, axis='columns')
    mdf = mdf.rename({'Symbol/ CUSIP': 'Symbol'}, axis='columns')

    mdf = mdf.dropna(subset=['Trade Date'])
    mdf['Quantity'].replace(' ', '', inplace=True)
    mdf['Quantity'].replace('', np.nan, inplace=True)
    mdf = mdf.dropna(subset=['Quantity'])

    mdf['Symbol'] = mdf['Symbol'].str.strip()
    return mdf


def read_vanguard_statement(file_path, file_name):
    full_path = f"{file_path}{file_name}"
    vdf = pd.read_csv(
        full_path,
        skiprows=10
    )
    return vdf


def clean_dates(df, datecol):
    df[datecol] = pd.to_datetime(df[datecol], errors="coerce")
    df = df.dropna(subset=[datecol])
    return df


def filter_transaction_types(df, trans_type_colname, valid_trans_types):
    df = df[df[trans_type_colname].isin(valid_trans_types)]
    return df


def filter_merrill_descriptions(df, valid_description_starts):
    filtered_df = df.loc[df['Description'].str.startswith(tuple(valid_description_starts))]
    filtered_df.loc[:, 'Transaction Type'] = None

    # iterate over the rows of the dataframe
    for index, row in filtered_df.iterrows():
        description = str(row['Description'])
        # iterate over the list of valid descriptions
        for desc in valid_description_starts:
            if description.startswith(desc):
                filtered_df.loc[index, 'Transaction Type'] = desc
    return filtered_df


def clean_numbercol(df: pd.DataFrame, column):
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
    df = df[df[symbolcol].isin(symbollist)]
    return df


def export_merrill(df: pd.DataFrame, output_filepath, output_filename):
    export_df = df.loc[:,
                       ['Trade Date',
                        'Symbol',
                        'Quantity',
                        'Amount',
                        'Transaction Type']]
    export_df.loc['Amount'] = -1 * df['Amount']
    export_df.dropna(inplace=True)
    export_df.to_csv(f'{output_filepath}{output_filename}', index=False)


def export_vanguard(df: pd.DataFrame, output_filepath, output_filename):
    export_df = df.loc[:,
                       ['Trade Date',
                        'Symbol',
                        'Shares',
                        'Net Amount',
                        'Transaction Type']]
    export_df.loc['Net Amount'] = -1 * df['Net Amount']
    export_df.dropna(inplace=True)
    export_df.to_csv(f'{output_filepath}{output_filename}', index=False)


if __name__ == '__main__':
    main()
