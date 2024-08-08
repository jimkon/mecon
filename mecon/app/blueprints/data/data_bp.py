import logging
import pathlib
from typing import Dict
from datetime import datetime

import pandas as pd
from flask import Blueprint, redirect, url_for, render_template, request
from json2html import json2html

from mecon.monitoring import logging_utils
from mecon.data.datafields import NullDataframeInDataframeWrapper, \
    InvalidInputDataFrameColumns  # TODO: It doesn't work if it is "from mecon.data.datafields import ...". the exceptions cannot be caught by the try statement.
from mecon.app.data_manager import GlobalDataManager
from mecon.app.datasets import WorkingDatasetDir
from mecon.import_data import monzo_data
from mecon.import_data.statements import HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV
from mecon.import_data import fetch_statement_files

data_bp = Blueprint('data', __name__, template_folder='templates')

_data_manager = GlobalDataManager()
monzo_client = monzo_data.MonzoClient()


def _statement_files_info() -> Dict:
    current_dataset = WorkingDatasetDir.get_instance().working_dataset
    dirs_path = current_dataset.statements
    transformed_dict = current_dataset.statement_files().copy()

    for dir_name in transformed_dict:
        files_info = []
        for filename in transformed_dict[dir_name]:
            statement_filepath = pathlib.Path(dirs_path) / dir_name / filename
            try:
                df = pd.read_csv(statement_filepath)
                stats = len(df)
            except FileNotFoundError | ValueError:
                stats = 'error while reading file'

            files_info.append((statement_filepath, filename, stats))
        transformed_dict[dir_name] = files_info

    return transformed_dict


def _db_statements_info():
    res = {}
    hsbc_trans = _data_manager.get_hsbc_statements()
    if hsbc_trans is not None:
        # hsbc_trans['date'] = pd.to_datetime(hsbc_trans['date'], format="%d/%m/%Y") # ValueError: time data "2020-12-27 00:00:00.000000" doesn't match format "%d/%m/%Y", at position 0. You might want to try:
        hsbc_trans['date'] = pd.to_datetime(hsbc_trans['date'])
        res['HSBC'] = {
            'transactions': len(hsbc_trans),
            'days': hsbc_trans['date'].nunique(),
            'min_date': hsbc_trans['date'].min(),
            'max_date': hsbc_trans['date'].max(),
        }

    monzo_trans = _data_manager.get_monzo_statements()
    if monzo_trans is not None:
        monzo_trans['date'] = pd.to_datetime(monzo_trans['date'], format="%Y-%m-%d")
        res['Monzo'] = {
            'transactions': len(monzo_trans),
            'days': monzo_trans['date'].nunique(),
            'min_date': monzo_trans['date'].min(),
            'max_date': monzo_trans['date'].max(),
        }

    revo_trans = _data_manager.get_revo_statements()
    if revo_trans is not None:
        revo_trans['start_date'] = pd.to_datetime(revo_trans['start_date'], format="%Y-%m-%d %H:%M:%S")
        res['Revolut'] = {
            'transactions': len(revo_trans),
            'days': revo_trans['start_date'].nunique(),
            'min_date': revo_trans['start_date'].min(),
            'max_date': revo_trans['start_date'].max(),
        }

    return res


def _db_transactions_info():
    try:
        transactions = _data_manager.get_transactions()
        res = {
            'rows': transactions.size()
        }
    except (NullDataframeInDataframeWrapper, InvalidInputDataFrameColumns):
        res = {'No transaction data'}
    return res


def _reset_db():
    _data_manager.reset_statements()
    statements_dir = WorkingDatasetDir.get_instance().working_dataset.statements

    hsbc_dfs = HSBCStatementCSV.from_all_paths_in_dir(statements_dir / 'HSBC').dataframe()
    _data_manager.add_statement(hsbc_dfs, bank='HSBC')

    monzo_dfs = MonzoStatementCSV.from_all_paths_in_dir(statements_dir / 'Monzo').dataframe()
    _data_manager.add_statement(monzo_dfs, bank='Monzo')

    revo_dfs = RevoStatementCSV.from_all_paths_in_dir(statements_dir / 'Revolut').dataframe()
    _data_manager.add_statement(revo_dfs, bank='Revolut')

    _data_manager.reset_transactions()


@data_bp.route('/menu')
@logging_utils.codeflow_log_wrapper('#api')
def data_menu():
    dataset_name = WorkingDatasetDir.get_instance().working_dataset.name
    db_transactions_info = _db_transactions_info()
    db_statements_info = json2html.convert(json=_db_statements_info())
    files_info_dict = _statement_files_info()
    return render_template('data.html', **locals(), **globals())


@data_bp.post('/reload')
@logging_utils.codeflow_log_wrapper('#api')
def data_reload():
    _reset_db()
    return redirect(url_for('data.data_menu'))


@data_bp.post('/import')
@logging_utils.codeflow_log_wrapper('#api')
def data_import():
    return redirect(url_for('data.data_menu'))


@data_bp.route('/view/file/<path>')
@logging_utils.codeflow_log_wrapper('#api')
def datafile_view(path):
    df = pd.read_csv(path)
    tables_stats_html = df.describe(include='all').to_html()  #
    table_html = df.to_html()
    return f"""
    <h1>Stats</h1>
    {tables_stats_html}
    <h1>Table, shape: {df.shape}</h1>
    {table_html}
    """


@data_bp.route('/fetch', methods=['POST', 'GET'])
@logging_utils.codeflow_log_wrapper('#api')
def fetch_data():
    path_to_fetch_data_from = WorkingDatasetDir.get_instance().working_dataset.settings['AUTOFETCH_STATEMENTS_DIR_PATH']
    logging.info(f"Fetching raw statement files from {path_to_fetch_data_from}...")
    auth_message = f"Athenticated: {monzo_client.is_authenticated()}, Expiry: {monzo_client.token_expiry()}"
    if request.method == 'POST':
        if "auth_monzo_button" in request.form:
            if not monzo_client.is_authenticated():
                url = monzo_client.start_authentication()
                return redirect(url)
            else:
                auth_message = f"Already authenticated until {monzo_client.token_expiry()}"
        elif "fetch_data_from_bank_button" in request.form:
            if monzo_client.is_authenticated():
                dataset = WorkingDatasetDir.get_instance().working_dataset
                json_file = dataset.statements / 'Monzo' / f"json/raw_data.json"
                transactions_json = monzo_client.download_transactions(json_file)

                trans = monzo_data.MonzoDataTransformer.from_json_file(json_file)
                df = trans.to_dataframe()

                csv_file = f"Monzo_Transactions.csv"
                dataset.add_df_statement('Monzo', df, csv_file)
                monzo_fetch_bank_data_message = f" -> {datetime.now()}: {len(df)} transactions added to {csv_file} from {df['Date'].min()} to {df['Date'].max()}"
            else:
                monzo_fetch_bank_data_message = f" -> You must authenticate first!"
        elif "import_statement_button" in request.form:
            path_to_fetch_data_from = pathlib.Path(path_to_fetch_data_from)
            fetch_statement_files.fetch_and_merge_raw_monzo_statements(
                from_path=path_to_fetch_data_from,
                dest_path=WorkingDatasetDir.get_instance().working_dataset.statements / 'Monzo'
            )
            fetch_statement_files.fetch_hsbc_statement(
                from_path=path_to_fetch_data_from,
                dest_path=WorkingDatasetDir.get_instance().working_dataset.statements / 'HSBC'
            )
            fetch_statement_files.fetch_revo_statement(
                from_path=path_to_fetch_data_from,
                dest_path=WorkingDatasetDir.get_instance().working_dataset.statements / 'Revolut'
            )
    return render_template('fetch_data.html', **locals(), **globals())


@data_bp.route('/auth/monzo')
def auth_monzo():
    token = request.args.get('code', None)
    state = request.args.get('state', None)
    monzo_client.finish_authentication(token, state)
    return redirect(url_for('data.fetch_data'))
