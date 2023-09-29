import pathlib

from flask import Blueprint, render_template, request

from mecon2.monitoring.logs import get_log_files, read_logs_as_df

from mecon2.monitoring.log_data import LogData

monitoring_bp = Blueprint('monitoring', __name__, template_folder='templates')


def get_logs(selected_log_file=None, start_date=None, end_date=None, tags_str=None, grouping=None, aggregation=None):
    if request.method == 'POST':
        selected_log_file = request.form['log_files']
    else:  # if request.method == 'GET':
        selected_log_file = 'logs\logs_raw.csv' if selected_log_file is None else selected_log_file

    log_files = get_log_files(historic_logs=True) if selected_log_file == 'All' else [pathlib.Path(selected_log_file)]
    df_logs = read_logs_as_df(log_files)

    logs = LogData.from_raw_logs(df_logs)

    return logs, selected_log_file


@monitoring_bp.route('/')
def home():
    all_log_filenames = [str(file) for file in get_log_files(historic_logs=True)]
    logs, selected_log_file = get_logs()
    table_html = logs.dataframe().to_html()
    return render_template('monitoring_home.html', **locals(), **globals())
