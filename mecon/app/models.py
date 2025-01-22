from sqlalchemy import Column, String, Integer, Float, DateTime, func
from sqlalchemy.dialects.sqlite import JSON

from sqlalchemy.orm import declarative_base


# Initialize Base
Base = declarative_base()


class TagsDBTable(Base):
    __tablename__ = 'tags_db_table'

    name = Column(String(50), primary_key=True, nullable=False)
    conditions_json = Column(JSON, nullable=False)
    date_created = Column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        return {
            'name': self.name,
            'conditions_json': self.conditions_json,
            'date_created': self.date_created
        }


class TagsMetadataTable(Base):
    __tablename__ = 'tags_metadata_table'

    name = Column(String(50), primary_key=True, nullable=False)
    date_modified = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    total_money_in = Column(Float, nullable=False, default=0.0)
    total_money_out = Column(Float, nullable=False, default=0.0)
    count = Column(Integer, nullable=False, default=0)

    def to_dict(self):
        return {
            'name': self.name,
            'date_modified': self.date_modified,
            'total_money_in': self.total_money_in,
            'total_money_out': self.total_money_out,
            'count': self.count,
        }


class HSBCTransactionsDBTable(Base):
    __tablename__ = 'hsbc_transactions_db_table'

    id = Column(Integer, primary_key=True)
    date = Column(String(20), nullable=False)
    amount = Column(Float, nullable=False)
    description = Column(String(200), nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'date': self.date,
            'amount': self.amount,
            'description': self.description
        }


class MonzoTransactionsDBTable(Base):
    __tablename__ = 'monzo_transactions_db_table'

    id = Column(Integer, primary_key=True)
    date = Column(String(20), nullable=False)
    time = Column(String(20), nullable=False)
    transaction_type = Column(String(50), nullable=False)
    name = Column(String(100))
    emoji = Column(String(10))
    category = Column(String(50))
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    local_amount = Column(Float, nullable=False)
    local_currency = Column(String(10), nullable=False)
    notes_tags = Column(String(200))
    address = Column(String(200))
    receipt = Column(String(200))
    description = Column(String(200))
    category_split = Column(String(50))
    money_out = Column(Float)
    money_in = Column(Float)

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


class RevoTransactionsDBTable(Base):
    __tablename__ = 'revo_transactions_db_table'

    id = Column(Integer, primary_key=True)
    type = Column(String(10))
    product = Column(String(10))
    start_date = Column(String(20), nullable=False)
    completed_date = Column(String(20), nullable=True)
    description = Column(String(200), nullable=True)
    amount = Column(Float, nullable=False)
    fee = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    state = Column(String(10))
    balance = Column(Float, nullable=True)

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


class TransactionsDBTable(Base):
    __tablename__ = 'transactions_db_table'

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    amount_cur = Column(Float, nullable=False)
    description = Column(String(200), nullable=True)
    tags = Column(String(2000), nullable=False)

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

