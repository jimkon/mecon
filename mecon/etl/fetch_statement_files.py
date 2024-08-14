import pathlib
import logging

import pandas as pd

from mecon.config import EXPECTED_MONZO_COLUMNS_IN_RAW_STATEMENT


def find_last_monzo_statement(from_path):
    # csv_files = get_csv_files(pathlib.Path(from_path))
    csv_files = list(pathlib.Path(from_path).glob('*.csv'))
    monzo_files = [file for file in csv_files if 'MonzoDataExport_' in file.name]
    if len(monzo_files) == 0:
        return None

    files_and_nrows = [(file, len(pd.read_csv(file))) for file in monzo_files]
    max_size = max([size for file, size in files_and_nrows])
    for file, size in files_and_nrows:
        if size == max_size:
            return file
    return None


def find_last_hsbc_statement(from_path):
    csv_files = list(pathlib.Path(from_path).glob('*.csv'))
    hsbc_files = [file for file in csv_files if 'TransactionHistory' in file.name]
    if len(hsbc_files) == 0:
        return None
    files_and_nrows = [(file, len(pd.read_csv(file))) for file in hsbc_files]
    max_size = max([size for file, size in files_and_nrows])
    for file, size in files_and_nrows:
        if size == max_size:
            return file
    return None


def find_last_revo_statement(from_path):
    csv_files = list(pathlib.Path(from_path).glob('*.csv'))
    # hsbc_files = [file for file in csv_files if 'TransactionHistory' in file.name]
    # files_and_nrows = [(file, len(pd.read_csv(file))) for file in hsbc_files]
    # max_size = max([size for file, size in files_and_nrows])
    # for file, size in files_and_nrows:
    #     if size == max_size:
    #         return file
    # return None


def grab_raw_statement_files(from_path, dest_path, matching_func):
    from_path, dest_path = pathlib.Path(from_path), pathlib.Path(dest_path)
    dest_path.mkdir(exist_ok=True, parents=True)
    csv_files = list(from_path.glob('*.csv'))
    monzo_files = [file for file in csv_files if matching_func(file)]
    logging.info(f"Found {len(csv_files)} from which {len(monzo_files)} are matching.")

    if len(monzo_files) == 0:
        return None

    for file in monzo_files:
        dest_file = dest_path / file.name
        if dest_file.exists():
            logging.warning(f"WARNING: Copying {file} will overwrite {dest_file}!")
            dest_file.unlink()

        logging.info(f"Copying {file} to {dest_file}...")
        dest_file.write_bytes(file.read_bytes())
        file.unlink()

    logging.info(f"Raw statements files fetched.")


def select_raw_monzo_statements(from_path,
                                dest_path,
                                delete_raw_files=False):
    from_path, dest_path = pathlib.Path(from_path), pathlib.Path(dest_path)
    csv_files = list(from_path.glob('*.csv'))

    dfs = [pd.read_csv(file, index_col=None) for file in csv_files]
    valid_dfs = [df for df in dfs if set(df.columns) == EXPECTED_MONZO_COLUMNS_IN_RAW_STATEMENT]
    if len(dfs) - len(valid_dfs) > 0:
        logging.warning(f"{len(dfs) - len(valid_dfs)} invalid raw Monzo statements.")
    if len(valid_dfs) == 0:
        logging.info(f"No raw Monzo statements to process.")
        return

    final_df = pd.concat(valid_dfs).drop_duplicates(subset=['Transaction ID'])

    final_df['Date'] = final_df['Date'].apply(transform_dates)
    # df.fillna('', inplace=True)

    min_date, max_date, size = final_df['Date'].min(), final_df['Date'].max(), len(final_df)
    output_filename = f"Monzo_Transactions_size_{size}_from_{min_date}_to_{max_date}.csv"
    dest_filepath = pathlib.Path(dest_path) / output_filename
    final_df.to_csv(dest_filepath, index=False)
    logging.info(f"Monzo statement stored in {dest_filepath}")

    if delete_raw_files:
        [file.unlink() for file in csv_files]


def fetch_and_merge_raw_monzo_statements(from_path, dest_path):
    grab_raw_statement_files(from_path,
                             dest_path / 'raw_statements',
                             matching_func=lambda _path: 'MonzoDataExport_' in _path.name
                             )
    select_raw_monzo_statements(from_path / 'raw_statements',
                                dest_path,
                                delete_raw_files=False)


def fetch_monzo_statement(from_path, dest_path):
    monzo_filepath = find_last_monzo_statement(from_path)
    logging.info(f"Monzo statement found in {monzo_filepath}")

    df = pd.read_csv(monzo_filepath, index_col=None)
    df['Date'] = df['Date'].apply(transform_dates)
    # df.fillna('', inplace=True)

    min_date, max_date, size = df['Date'].min(), df['Date'].max(), len(df)
    output_filename = f"Monzo_Transactions_size_{size}_from_{min_date}_to_{max_date}.csv"
    dest_filepath = pathlib.Path(dest_path) / output_filename
    df.to_csv(dest_filepath, index=False)
    logging.info(f"Monzo statement stored in {dest_filepath}")
    return dest_filepath


def fetch_hsbc_statement(from_path, dest_path):
    hsbc_filepath = find_last_hsbc_statement(from_path)

    ...


def fetch_revo_statement(from_path, dest_path):
    revo_filepath = find_last_revo_statement(from_path)

    ...


def transform_dates(date_str):
    if '-' in date_str:
        return date_str

    day = date_str[0:2]
    month = date_str[3:5]
    year = date_str[6:]
    return f"{year}-{month}-{day}"


# TODO add revolut and HSBC data
if __name__ == '__main__':
    fetch_hsbc_statement(r"C:\Users\dimitris\PycharmProjects\mecon\datasets\v2\data\statements\HSBC")
    # fetch_monzo_statement(r"C:\Users\dimitris\PycharmProjects\mecon\datasets\v2\data\statements\Monzo")
