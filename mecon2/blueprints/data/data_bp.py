import pathlib
from typing import Dict

import pandas as pd
from flask import Blueprint, redirect, url_for, render_template
from json2html import json2html

from mecon2.app.datasets import WorkingDatasetDir
from mecon2.data.db_controller import reset_transactions, import_data_access, data_access
from mecon2.data.statements import HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV
from mecon2.transactions import Transactions
from mecon2.datafields import ZeroSizeTransactionsError
from mecon2.monitoring import logs

data_bp = Blueprint('data', __name__, template_folder='templates')


def _statement_files_info() -> Dict:
    current_dataset = WorkingDatasetDir().get_dataset('v2')
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

            files_info.append((statement_filepath, filename, stats))  # TODO add more stats, maybe df.info()
        transformed_dict[dir_name] = files_info

    return transformed_dict


def _db_statements_info():
    res = {}
    hsbc_trans = import_data_access.hsbc_statements.get_transactions()
    if hsbc_trans is not None:
        hsbc_trans['date'] = pd.to_datetime(hsbc_trans['date'], format="%d/%m/%Y")
        res['HSBC'] = {
            'transactions': len(hsbc_trans),
            'days': hsbc_trans['date'].nunique(),
            'min_date': hsbc_trans['date'].min(),
            'max_date': hsbc_trans['date'].max(),
        }

    monzo_trans = import_data_access.monzo_statements.get_transactions()
    if monzo_trans is not None:
        monzo_trans['date'] = pd.to_datetime(monzo_trans['date'], format="%d/%m/%Y")
        res['Monzo'] = {
            'transactions': len(monzo_trans),
            'days': monzo_trans['date'].nunique(),
            'min_date': monzo_trans['date'].min(),
            'max_date': monzo_trans['date'].max(),
        }

    revo_trans = import_data_access.revo_statements.get_transactions()
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
        transactions = Transactions(data_access.transactions.get_transactions())
        res = {
            'rows': transactions.size()
        }
    except ZeroSizeTransactionsError:
        res = {'No transaction data'}
    return res


@data_bp.route('/menu')
@logs.codeflow_log_wrapper('#api')
def data_menu():
    db_transactions_info = _db_transactions_info()
    db_statements_info = json2html.convert(json=_db_statements_info())
    files_info_dict = _statement_files_info()
    return render_template('data.html', **locals(), **globals())


@data_bp.post('/reload')
@logs.codeflow_log_wrapper('#api')
def data_reload():
    reset_transactions()
    statements_info = _statement_files_info()
    for bank_name in statements_info:
        if bank_name == 'HSBC':  # TODO maybe use enums instead of literals
            dfs = [HSBCStatementCSV(filepath).dataframe() for filepath, *_ in statements_info[bank_name]]
            import_data_access.hsbc_statements.import_statement(dfs)
        elif bank_name == 'Monzo':  # TODO maybe use enums instead of literals
            dfs = [MonzoStatementCSV(filepath).dataframe() for filepath, *_ in statements_info[bank_name]]
            import_data_access.monzo_statements.import_statement(dfs)
        elif bank_name == 'Revolut':  # TODO maybe use enums instead of literals
            dfs = [RevoStatementCSV(filepath).dataframe() for filepath, *_ in statements_info[bank_name]]
            import_data_access.revo_statements.import_statement(dfs)

    data_access.transactions.load_transactions()

    return redirect(url_for('data.data_menu'))


@data_bp.post('/import')
@logs.codeflow_log_wrapper('#api')
def data_import():
    return redirect(url_for('data.data_menu'))


@data_bp.route('/view/file/<path>')
@logs.codeflow_log_wrapper('#api')
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


@data_bp.route('/view/<item>')
@logs.codeflow_log_wrapper('#api')
def data_view(item):
    # TODO get the df (either statement or the transactions) and return to_html
    return f"data view {item}"
