import os
import logging

from flask import Flask

from mecon.monitoring import logs
try:
    logs.setup_logging()
except FileNotFoundError as e:
    print('WARNING: Logs setup failed.')

from mecon.app.datasets import WorkingDatasetDir
from mecon.app.db_extension import db
from mecon.app.data_manager import CachedDBDataManager
from mecon.app.views import main_bp
from mecon.app.blueprints.data import data_bp
from mecon.app.blueprints.reports import reports_bp
from mecon.app.blueprints.tags import tags_bp
from mecon.app.blueprints.monitoring import monitoring_bp
from mecon.config import DEFAULT_DATASET_NAME

logs.print_logs_info()
logging.info('Starting app...')

current_dataset_dir = WorkingDatasetDir()
current_dataset_dir.set_working_dataset(DEFAULT_DATASET_NAME)
current_dataset = current_dataset_dir.working_dataset

app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{current_dataset.db}"
db.init_app(app)
app.app_context().push()
db.create_all()

CachedDBDataManager()

app.register_blueprint(main_bp)
app.register_blueprint(data_bp.data_bp, url_prefix='/data')
app.register_blueprint(tags_bp.tags_bp, url_prefix='/tags')
app.register_blueprint(reports_bp.reports_bp, url_prefix='/reports')
app.register_blueprint(monitoring_bp.monitoring_bp, url_prefix='/monitoring')

logging.info('App initialised!')


