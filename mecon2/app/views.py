from flask import Blueprint, redirect, url_for

main_bp = Blueprint('main', __name__)
data_bp = Blueprint('data', __name__, url_prefix='/data')
tags_bp = Blueprint('tags', __name__, url_prefix='/tags')
reports_bp = Blueprint('reports', __name__, url_prefix='/reports')


@main_bp.route('/')
def index():
    return redirect(url_for('main.home'))  # TODO what?? why it works with main and not main_bp


@main_bp.route('/home')
def home():
    return 'Index'


@data_bp.route('/')
def data():
    return 'Data'


@reports_bp.route('/')
def reports():
    return 'reports'


@tags_bp.route('/')
def tags():
    return 'tags'


@tags_bp.route('/new')
def tags_new():
    return 'tags_new'


@tags_bp.get('/edit/<tag_name>')
def tags_edit_get(tag_name):
    return f'tags_edit_get {tag_name=}'


@tags_bp.post('/edit/<tag_name>')
def tags_edit_post(tag_name):
    return f'tags_edit_post {tag_name=}'

