import os
from datetime import datetime

from flask import Flask

from mecon2.app.extensions import db
from mecon2.app import views
from mecon2.data.file_system import DatasetDir

datasets_dir = DatasetDir(r"C:\Users\dimitris\PycharmProjects\mecon\datasets")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{datasets_dir.get_dataset('v2').sqlite}"
db.init_app(app)
app.app_context().push()
db.create_all()
app.register_blueprint(views.main_bp)
app.register_blueprint(views.data_bp)
app.register_blueprint(views.reports_bp)
app.register_blueprint(views.tags_bp)

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
