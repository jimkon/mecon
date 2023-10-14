import os
import logging

from flask import Flask

from mecon2.monitoring import logs
try:
    logs.setup_logging()
except Exception as e:
    print('WARNING: Logs setup failed.')

from mecon2.app.datasets import WorkingDatasetDir
from mecon2.app.db_extension import db
from mecon2.app.views import main_bp
from mecon2.blueprints.data import data_bp
from mecon2.blueprints.reports import reports_bp
from mecon2.blueprints.tags import tags_bp
from mecon2.blueprints.monitoring import monitoring_bp

logs.print_logs_info()
logging.info('Starting app...')

current_dataset = WorkingDatasetDir().get_dataset('v2')

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
app.register_blueprint(monitoring_bp.monitoring_bp, url_prefix='/monitoring')

logging.info('App initialised!')


