import pathlib
from typing import Dict

import pandas as pd
from flask import Blueprint, redirect, url_for, render_template

from mecon2.app.datasets import current_dataset

# from mecon2.data import file_system as fs

# data_bp = Blueprint('data', __name__, template_folder='templates')
data_bp = Blueprint('data', __name__)


def _transform_statement_files_dict(files_dict: Dict, dirs_path) -> Dict:
    transformed_dict = files_dict.copy()

    for dir_name in transformed_dict:
        transformed_dict[dir_name] = [(pathlib.Path(dirs_path) / dir_name / filename, filename) for filename in transformed_dict[dir_name]]

    return transformed_dict


@data_bp.route('/menu')
def data_menu():
    menu_dict = _transform_statement_files_dict(current_dataset.statement_files(), current_dataset.statements)
    return render_template('data.html', menu_dict=menu_dict, **globals())


@data_bp.post('/import')
def data_import():
    return redirect(url_for('data.data_menu'))


@data_bp.route('/view/file/<path>')
def datafile_view(path):
    # TODO get the df (either statement or the transactions) and return to_httml
    return pd.read_csv(path).to_html()


@data_bp.route('/view/<item>')
def data_view(item):
    # TODO get the df (either statement or the transactions) and return to_httml
    return f"data view {item}"
