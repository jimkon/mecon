import os
from datetime import datetime

from flask import Flask

from mecon2.app.extensions import db
from mecon2.app.views import main_bp
from mecon2.data.file_system import DatasetDir
from mecon2.data.io_framework import ImportDataAccess, DataAccess
from mecon2.data import db_controller
from mecon2.blueprints.data import data_bp
from mecon2.blueprints.tags import tags_bp

datasets_dir = DatasetDir(r"C:\Users\dimitris\PycharmProjects\mecon\datasets")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{datasets_dir.get_dataset('v2').db}"
db.init_app(app)
app.app_context().push()
db.create_all()

app.register_blueprint(main_bp)
app.register_blueprint(data_bp.data_bp, url_prefix='/data')
app.register_blueprint(tags_bp.tags_bp, url_prefix='/tags')

import_data = ImportDataAccess(db_controller.HSBCTransactionsDBAccessor(),
                               db_controller.MonzoTransactionsDBAccessor(),
                               db_controller.RevoTransactionsDBAccessor())
data_access = DataAccess(db_controller.TransactionsDBAccessor(),
                         db_controller.TagsDBAccessor())

DEPLOYMENT_DATETIME = datetime.now()


# def create_app(config_file='settings.py'):
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
    # app = create_app()
    app.run(host='0.0.0.0', debug=True)
