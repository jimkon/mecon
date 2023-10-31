import abc

import pandas as pd

from mecon2.data import io_framework as io, db_controller
from mecon2 import config

class TagsDBAccessor(io.TagsIOABC, abc.ABC):
    def get_tag(self, name) -> dict:
        tag = db_controller.TagsDBTable.query.filter_by(name=name).first()
        return tag

    def set_tag(self, name, conditions_json):
        if len(conditions_json) > config.CONDITION_JSON_MAX_SIZE:
            raise ValueError(f"Tag's json string is bigger than 2000 characters ({len(conditions_json)=})."
                             f" Consider increasing the upper limit.")
        tag = db_controller.TagsDBTable(
            name=name,
            conditions_json=conditions_json
        )
        db_controller.db.session.add(tag)
        db_controller.db.session.commit()

    def all_tags(self) -> dict:
        tags = db_controller.TagsDBTable.query.all()
        return tags


class TransactionsDBAccessor(io.CombinedTransactionsIOABC, abc.ABC):
    def get_transactions(self) -> pd.DataFrame:
        pass

    def update_transactions(self, df):
        pass


class HSBCTransactionsDBAccessor(io.HSBCTransactionsIOABC, abc.ABC):
    def import_statement(self, df):
        pass


class MonzoTransactionsDBAccessor(io.MonzoTransactionsIOABC, abc.ABC):
    def import_statement(self, df):
        pass


class RevoTransactionsDBAccessor(io.RevoTransactionsIOABC, abc.ABC):
    def import_statement(self, df):
        pass
