from flask import Blueprint

monitoring_bp = Blueprint('monitoring', __name__, template_folder='templates')


@monitoring_bp.route('/')
def home():
    return "<h1>Console</h1>"
