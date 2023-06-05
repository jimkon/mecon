import abc

from flask import Flask

from mecon2.db_controller import init_db

app = Flask(__name__)
init_db(app)

