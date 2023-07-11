from flask import Blueprint, redirect, url_for, render_template


# tags_bp = Blueprint('tags', __name__, url_prefix='/tags', template_folder='templates')
tags_bp = Blueprint('tags', __name__)


@tags_bp.route('/')
def tags_menu():
    return 'tags_menu'


@tags_bp.route('/new')
def tags_new():
    return 'tags_new'


@tags_bp.get('/edit/<tag_name>')
def tags_edit_get(tag_name):
    return f'tags_edit_get {tag_name=}'


@tags_bp.post('/edit/<tag_name>')
def tags_edit_post(tag_name):
    return f'tags_edit_post {tag_name=}'

