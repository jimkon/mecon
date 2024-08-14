from mecon.app.db_extension import db


class TagsDBTable(db.Model):
    name = db.Column(db.String(50), primary_key=True, nullable=False)
    conditions_json = db.Column(db.String(1000), nullable=False)

    def to_dict(self):
        tag_name = self.name
        return {'name': tag_name, 'conditions_json': self.conditions_json}


class HSBCTransactionsDBTable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(200), nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'date': self.date,
            'amount': self.amount,
            'description': self.description
        }


class MonzoTransactionsDBTable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(20), nullable=False)
    time = db.Column(db.String(20), nullable=False)
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

    def to_dict(self):
        return {
            'id': self.id,
            'date': self.date,
            'time': self.time,
            'transaction_type': self.transaction_type,
            'name': self.name,
            'emoji': self.emoji,
            'category': self.category,
            'amount': self.amount,
            'currency': self.currency,
            'local_amount': self.local_amount,
            'local_currency': self.local_currency,
            'notes_tags': self.notes_tags,
            'address': self.address,
            'receipt': self.receipt,
            'description': self.description,
            'category_split': self.category_split,
            'money_out': self.money_out,
            'money_in': self.money_in
        }


class RevoTransactionsDBTable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(10))
    product = db.Column(db.String(10))
    start_date = db.Column(db.String(20), nullable=False)
    completed_date = db.Column(db.String(20), nullable=True)
    description = db.Column(db.String(200), nullable=True)
    amount = db.Column(db.Float, nullable=False)
    fee = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    state = db.Column(db.String(10))
    balance = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'product': self.product,
            'start_date': self.start_date,
            'completed_date': self.completed_date,
            'description': self.description,
            'amount': self.amount,
            'fee': self.fee,
            'currency': self.currency,
            'state': self.state,
            'balance': self.balance,
        }


class TransactionsDBTable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    datetime = db.Column(db.DateTime, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    amount_cur = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(200), nullable=True)
    tags = db.Column(db.String(2000), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'datetime': self.datetime,
            'amount': self.amount,
            'currency': self.currency,
            'amount_cur': self.amount_cur,
            'description': self.description,
            'tags': self.tags,
        }

