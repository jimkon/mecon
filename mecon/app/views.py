from flask import Blueprint, redirect, url_for, render_template

main_bp = Blueprint('main', __name__, template_folder='../../templates')


@main_bp.route('/')
def index():
    return redirect(url_for('main.home'))


@main_bp.route('/home')
def home():
    return render_template('index.html', **globals())


