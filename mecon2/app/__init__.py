from flask import Flask

from mecon2.app.extensions import db
from mecon2.app import views


def create_app(config_file='settings.py'):
    app = Flask(__name__, instance_relative_config=False)

    app.config.from_pyfile(config_file)

    db.init_app(app)
    app.app_context().push()

    db.create_all()

    app.register_blueprint(views.main)

    return app


if __name__ == '__main__':
    app = create_app()

    app.run(host='0.0.0.0', debug=True)
