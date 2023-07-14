import pathlib
from typing import Dict

import pandas as pd
from flask import Blueprint, redirect, url_for, render_template
from json2html import json2html

from mecon2.app.datasets import current_dataset
from mecon2.data.db_controller import reset_db, import_data_access, data_access
from mecon2.data.statements import HSBCStatementCSV, MonzoStatementCSV, RevoStatementCSV

# from mecon2.data import file_system as fs

# data_bp = Blueprint('data', __name__, template_folder='templates')
data_bp = Blueprint('data', __name__)


def _statement_files_info() -> Dict:
    dirs_path = current_dataset.statements
    transformed_dict = current_dataset.statement_files().copy()

    for dir_name in transformed_dict:
        files_info = []
        for filename in transformed_dict[dir_name]:
            statement_filepath = pathlib.Path(dirs_path) / dir_name / filename
            try:
                df = pd.read_csv(statement_filepath)
                size = len(df)
            except FileNotFoundError:
                size = 'error while reading file'
            files_info.append((statement_filepath, filename, size))
        transformed_dict[dir_name] = files_info

    return transformed_dict


def _db_transactions_info():
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


@data_bp.route('/menu')
def data_menu():
    files_info_dict = _statement_files_info()
    db_transactions_info_dict = json2html.convert(json = _db_transactions_info())
    return render_template('data.html', **locals(), **globals())


@data_bp.post('/reload')
def data_reload():
    reset_db()
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
        print(f"{len(dfs)} {bank_name} statements imported")

    return redirect(url_for('data.data_menu'))

@data_bp.post('/import')
def data_import():
    return redirect(url_for('data.data_menu'))


@data_bp.route('/view/file/<path>')
def datafile_view(path):
    return pd.read_csv(path).to_html()


@data_bp.route('/view/<item>')
def data_view(item):
    # TODO get the df (either statement or the transactions) and return to_httml
    return f"data view {item}"
