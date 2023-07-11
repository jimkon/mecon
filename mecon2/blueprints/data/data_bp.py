from flask import Blueprint, redirect, url_for, render_template

# data_bp = Blueprint('data', __name__, template_folder='templates')
data_bp = Blueprint('data', __name__)


@data_bp.route('/menu')
def data_menu():
    return render_template('data.html', **globals())


@data_bp.post('/import')
def data_import():
    return redirect(url_for('data.data_menu'))


@data_bp.route('/view/<item>')
def data_view(item):
    # TODO get the df (either statement or the transactions) and return to_httml
    return f"data view {item}"
