import os
import logging

from flask import Flask

from mecon2.monitoring import logs  # DO NOT DELETE:important to initialise the logger
from mecon2.app.datasets import current_dataset
from mecon2.app.db_extension import db
from mecon2.app.views import main_bp
from mecon2.blueprints.data import data_bp
from mecon2.blueprints.reports import reports_bp
from mecon2.blueprints.tags import tags_bp

logs.print_logs_info()
logging.info('Starting app...')

app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{current_dataset.db}"
db.init_app(app)
app.app_context().push()
db.create_all()


app.register_blueprint(main_bp)
app.register_blueprint(data_bp.data_bp, url_prefix='/data')
app.register_blueprint(tags_bp.tags_bp, url_prefix='/tags')
app.register_blueprint(reports_bp.reports_bp, url_prefix='/reports')

logging.info('App initialised!')


# def create_app(config_file='settings.py'):
#     settings.py ->
#         import os
#
#         SECRET_KEY = os.getenv('SECRET_KEY')
#         SQLALCHEMY_DATABASE_URI = 'sqlite:///database.db'
#         FLASK_DEBUG = True
#         DEBUG = True
#
#     app = Flask(__name__, instance_relative_config=False)
#
#     app.config.from_pyfile(config_file)
#
#     db.init_app(app)
#     app.app_context().push()
#
#     db.create_all()
#
#     app.register_blueprint(views.main)
#
#     return app


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
