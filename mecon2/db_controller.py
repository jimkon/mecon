from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


def init_db(app):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'your_database_uri_here'
    db.init_app(app)


class TagsDBTable(db.Model):
    name = db.Column(db.String(50), primary_key=True, nullable=False)
    conditions_json = db.Column(db.String(1000), nullable=False)


class TransactionsDBTable(db.Model):
    datetime = db.Column(db.DateTime, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    amount_cur = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(200), nullable=True)


class HSBCTransactionsDBTable(db.Model):
    date = db.Column(db.Date, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(200), nullable=True)


class MonzoTransactionsDBTable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.String(50), nullable=False)
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.Time, nullable=False)
    transaction_type = db.Column(db.String(50), nullable=False)
    name = db.Column(db.String(100))
    emoji = db.Column(db.String(10))
    category = db.Column(db.String(50))
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    local_amount = db.Column(db.Float, nullable=False)
    local_currency = db.Column(db.String(10), nullable=False)
    notes_tags = db.Column(db.String(200))
    address = db.Column(db.String(200))
    receipt = db.Column(db.String(200))
    description = db.Column(db.String(200))
    category_split = db.Column(db.String(50))
    money_out = db.Column(db.Float)
    money_in = db.Column(db.Float)


class RevoTransactionsDBTable(db.Model):
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.Time, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    amount_cur = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(200), nullable=True)




