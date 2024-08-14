# import logging
#
# from flask import Flask
#
# from mecon.monitoring import logs
# try:
#     logs.setup_logging()
# except FileNotFoundError as e:
#     print('WARNING: Logs setup failed.')
#
# from mecon.app.datasets import WorkingDatasetDir
# from mecon.app.db_extension import db
# from mecon.app import data_manager
# from blueprints.data import data_bp
# from blueprints.reports import reports_bp
# from blueprints.tags import tags_bp
# from blueprints.monitoring import monitoring_bp
#
# from views import main_bp
#
# logs.print_logs_info()
# logging.info('Starting app...')
#
# current_dataset_dir = WorkingDatasetDir.get_instance()
# current_dataset = current_dataset_dir.working_dataset
# logging.info(f"Current dataset: {current_dataset.name}")
#
#
# app = Flask(__name__)
# app.debug = True
# app.config['SECRET_KEY'] = 'secret key' #os.getenv('SECRET_KEY') TODO
# app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{current_dataset.db}"
# app.config['SESSION_TYPE'] = 'filesystem'
# db.init_app(app)
# app.app_context().push()
# db.create_all()
#
# data_manager.GlobalDataManager()
#
# app.register_blueprint(main_bp)
# app.register_blueprint(data_bp.data_bp, url_prefix='/data')
# app.register_blueprint(tags_bp.tags_bp, url_prefix='/tags')
# app.register_blueprint(reports_bp.reports_bp, url_prefix='/reports')
# app.register_blueprint(monitoring_bp.monitoring_bp, url_prefix='/monitoring')
#
# logging.info('App initialised!')
#
#
