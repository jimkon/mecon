from flask import Blueprint, render_template, request, redirect, url_for


reports_bp = Blueprint('reports', __name__, template_folder='templates')


@reports_bp.route('/')
def reports_menu():
    return 'reports menu'


@reports_bp.route('/custom_graph')
def custom_graph():
    return render_template('custom_graph.html', **locals(), **globals())




