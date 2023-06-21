import json
from typing import Dict, Any, List

import pandas as pd

from mecon2.app.extensions import db
from mecon2.app import models
from mecon2.data import io_framework as io


class TagsDBAccessor(io.TagsIOABC):
    def get_tag(self, name) -> dict[str, Any] | None:
        tag = models.TagsDBTable.query.filter_by(name=name).first()

        if tag is None:
            return None

        return tag.to_dict()

    def set_tag(self, name: str, conditions_json: dict) -> None:
        if len(conditions_json) > 2000:# TODO make this a constant config
            raise ValueError(f"Tag's json string is bigger than 2000 characters ({len(conditions_json)=})."
                             f" Consider increasing the upper limit.")

        tag = db.session.query(models.TagsDBTable).filter_by(name=name).first()
        if tag is None:
            tag = models.TagsDBTable(
                name=name,
                conditions_json=str(conditions_json)
            )
            db.session.add(tag)
            db.session.commit()
        else:
            tag.conditions_json = str(conditions_json)
            db.session.commit()

    def delete_tag(self, name: str) -> bool:
        tag = db.session.query(models.TagsDBTable).filter_by(name=name).first()
        if tag:
            db.session.delete(tag)
            db.session.commit()
            return True
        return False

    def all_tags(self) -> list[dict]:
        tags = [tag.to_dict() for tag in models.TagsDBTable.query.all()]
        return tags


class HSBCTransactionsDBAccessor(io.HSBCTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.HSBCTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        transactions = models.HSBCTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df)>0 else None

    def delete_all(self) -> None:
        db.session.query(models.HSBCTransactionsDBTable).delete()


class MonzoTransactionsDBAccessor(io.MonzoTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.MonzoTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        transactions = models.MonzoTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        db.session.query(models.MonzoTransactionsDBTable).delete()


class RevoTransactionsDBAccessor(io.RevoTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.RevoTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        transactions = models.RevoTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        db.session.query(models.RevoTransactionsDBTable).delete()


# class TransactionsDBAccessor(io.TransactionsIOABC):
#     def get_transactions(self) -> pd.DataFrame:
#         pass
#
#     def update_transactions(self, df):
#         pass