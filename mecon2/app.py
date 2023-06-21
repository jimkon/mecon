from flask import Flask

from mecon2.data.db_controller import init_db_from_app

app = Flask(__name__)
init_db_from_app(app)

