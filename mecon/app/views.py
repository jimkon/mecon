import json

from flask import Blueprint, redirect, url_for, render_template

from mecon.app.datasets import WorkingDatasetDir

main_bp = Blueprint('main', __name__, template_folder='../../templates')


@main_bp.route('/')
def index():
    return redirect(url_for('main.home'))


@main_bp.route('/home')
def home():
    dataset_path = WorkingDatasetDir.get_instance().working_dataset.path
    links = json.loads((dataset_path / 'links.json').read_text())
    return render_template('index.html', **locals())


