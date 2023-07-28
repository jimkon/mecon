import pandas as pd

from mecon2.data import io_framework as io, db_controller


class TagsDBAccessor(io.TagsIOABC):
    def get_tag(self, name) -> dict:
        tag = db_controller.TagsDBTable.query.filter_by(name=name).first()
        return tag

    def set_tag(self, name, conditions_json):
        if len(conditions_json) > 2000:# TODO make this a constant config
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


class TransactionsDBAccessor(io.CombinedTransactionsIOABC):
    def get_transactions(self) -> pd.DataFrame:
        pass

    def update_transactions(self, df):
        pass


class HSBCTransactionsDBAccessor(io.HSBCTransactionsIOABC):
    def import_statement(self, df):
        pass


class MonzoTransactionsDBAccessor(io.MonzoTransactionsIOABC):
    def import_statement(self, df):
        pass


class RevoTransactionsDBAccessor(io.RevoTransactionsIOABC):
    def import_statement(self, df):
        pass