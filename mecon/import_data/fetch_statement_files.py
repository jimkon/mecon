import pathlib
import logging

import pandas as pd

STATEMENT_DIRPATH = pathlib.Path(r"C:\Users\dimitris\Downloads")


def get_csv_files():
    return list(STATEMENT_DIRPATH.glob('*.csv'))


def find_last_monzo_statement():
    csv_files = get_csv_files()
    monzo_files = [file for file in csv_files if 'MonzoDataExport_' in file.name]
    files_and_nrows = [(file, len(pd.read_csv(file))) for file in monzo_files]
    max_size = max([size for file, size in files_and_nrows])
    for file, size in files_and_nrows:
        if size == max_size:
            return file
    return None


def fetch_monzo_statement(dest_path):
    monzo_filepath = find_last_monzo_statement()

    df = pd.read_csv(monzo_filepath, index_col=None)
    df['Date'] = df['Date'].apply(transform_dates)
    # df.fillna('', inplace=True)

    min_date, max_date, size = df['Date'].min(), df['Date'].max(), len(df)

    dest_filepath = pathlib.Path(dest_path) / f"Monzo_Transactions_size_{size}_from_{min_date}_to_{max_date}.csv"
    df.to_csv(dest_filepath, index=False)
    logging.info(f"Monzo statement stored in {dest_filepath}")
    return dest_filepath


def transform_dates(date_str):
    if '-' in date_str:
        return date_str

    day = date_str[0:2]
    month = date_str[3:5]
    year = date_str[6:]
    return f"{year}-{month}-{day}"


# TODO add revolut and HSBC data
if __name__ == '__main__':
    fetch_monzo_statement(r"C:\Users\dimitris\PycharmProjects\mecon\datasets\v2\data\statements\Monzo")
