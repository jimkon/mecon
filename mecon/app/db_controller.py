from typing import Any, List
import logging

import pandas as pd

from mecon.app import models
from mecon.app.db_extension import db
from mecon.etl import transformers
from mecon.etl import io_framework
from mecon.monitoring import logging_utils
from mecon.utils import currencies


class TagsDBAccessor(io_framework.TagsIOABC):
    @logging_utils.codeflow_log_wrapper('#db#tags')
    def get_tag(self, name: str) -> list[str, Any] | None:
        tag = models.TagsDBTable.query.filter_by(name=name).first()

        if tag is None:
            return None

        return tag.to_dict()

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def set_tag(self, name: str, conditions_json: list | dict) -> None:
        # if len(conditions_json) > 2000:  # TODO:v3 do we need this?
        #     raise ValueError(f"Tag's json string is bigger than 2000 characters ({len(conditions_json)=})."
        #                      f" Consider increasing the upper limit.")

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

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def delete_tag(self, name: str) -> bool:
        tag = db.session.query(models.TagsDBTable).filter_by(name=name).first()
        if tag:
            db.session.delete(tag)
            db.session.commit()
            return True
        return False

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def all_tags(self) -> list[dict]:
        tags = [tag.to_dict() for tag in models.TagsDBTable.query.all()]
        return tags


class HSBCTransactionsDBAccessor(io_framework.HSBCTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.HSBCTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        # TODO:v3 get and delete transactions is the same for all tables. deal with duplicated code
        transactions = models.HSBCTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        db.session.query(models.HSBCTransactionsDBTable).delete()
        db.session.commit()


class MonzoTransactionsDBAccessor(io_framework.MonzoTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.MonzoTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        transactions = models.MonzoTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        db.session.query(models.MonzoTransactionsDBTable).delete()
        db.session.commit()


class RevoTransactionsDBAccessor(io_framework.RevoTransactionsIOABC):
    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.RevoTransactionsDBTable.__tablename__, db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        transactions = models.RevoTransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        db.session.query(models.RevoTransactionsDBTable).delete()
        db.session.commit()


class InvalidTransactionsDataframeColumnsException(Exception):
    valid_cols = {'id', 'datetime', 'amount', 'currency', 'amount_cur', 'description'}

    def __init__(self, df):
        self.found_cols = set(list(df.columns))
        super().__init__(f"Transaction dataframe has to contain these fields: {self.valid_cols}. "
                         f"Found: {self.found_cols}, Missing: {self.missing_cols}, "
                         f"Extra: {self.extra_cols}")

    @property
    def missing_cols(self):
        return self.valid_cols - self.found_cols

    @property
    def extra_cols(self):
        return self.found_cols - self.valid_cols


class InvalidTransactionsDataframeDataTypesException(Exception):
    def __init__(self, invalid_types):
        self.invalid_types = invalid_types
        super().__init__(f"Invalid types: {self.invalid_types}")


class InvalidTransactionsDataframeDataValueException(Exception):
    def __init__(self, value_errors):
        self.value_errors = value_errors
        super().__init__(f"Invalid value range: {self.value_errors}")


class TransactionsDBAccessor(io_framework.CombinedTransactionsIOABC):
    @staticmethod
    def _transaction_df_validation(df):
        TransactionsDBAccessor._transaction_df_columns_validation(df)
        TransactionsDBAccessor._transaction_df_types_validation(df)
        TransactionsDBAccessor._transaction_df_values_validation(df)

    @staticmethod
    def _transaction_df_columns_validation(df):
        valid_cols = InvalidTransactionsDataframeColumnsException.valid_cols
        found_cols = set(list(df.columns))
        if found_cols != valid_cols:
            raise InvalidTransactionsDataframeColumnsException(df)

    @staticmethod
    def _transaction_df_types_validation(df):
        invalid_types = []
        if not pd.api.types.is_string_dtype(df['id']):
            invalid_types.append(f"invalid type for column 'id'. expected: string, got: {df['id'].dtype}")
        if not pd.api.types.is_datetime64_dtype(df['datetime']):
            invalid_types.append(f"invalid type for column 'datetime'. expected: datetime, got: {df['datetime'].dtype}")
        if not pd.api.types.is_numeric_dtype(df['amount']):
            invalid_types.append(f"invalid type for column 'amount'. expected: number, got: {df['amount'].dtype}")
        if not pd.api.types.is_string_dtype(df['currency']):
            invalid_types.append(f"invalid type for column 'currency'. expected: string, got: {df['currency'].dtype}")
        if not pd.api.types.is_numeric_dtype(df['amount_cur']):
            invalid_types.append(
                f"invalid type for column 'amount_cur'. expected: number, got: {df['amount_cur'].dtype}")
        if not pd.api.types.is_string_dtype(df['description']):
            invalid_types.append(
                f"invalid type for column 'description'. expected: string, got: {df['description'].dtype}")

        if len(invalid_types) > 0:
            raise InvalidTransactionsDataframeDataTypesException(invalid_types)

    @staticmethod
    def _transaction_df_values_validation(df):
        value_errors = []
        if df['amount'].isna().sum() > 0:
            value_errors.append(f"Amount column contains NaN values: {df['amount'].isna().sum()}")

        if len(value_errors):
            raise InvalidTransactionsDataframeDataValueException(value_errors)

    @staticmethod
    def _handle_duplicates(df: pd.DataFrame, cols_to_check=None):
        cols_to_check = df.columns if cols_to_check is None else cols_to_check

        dups = df.duplicated(subset=cols_to_check)

        if dups.sum() > 0:
            logging.warning(f"Duplicates found: {dups.sum()}")

        # if dups.sum() > 0: TODO handle duplicates
        #     logging.warning(f"Duplicate rows found with IDs: {df[dups]}")
        #     df.drop_duplicates(subset=cols_to_check, inplace=True)
        #     raise ValueError(f"Error due to duplicate rows in df.")

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def get_transactions(self) -> pd.DataFrame:
        transactions = models.TransactionsDBTable.query.all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def delete_all(self) -> None:
        db.session.query(models.TransactionsDBTable).delete()
        db.session.commit()

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def load_transactions(self) -> None:
        df_hsbc = HSBCTransactionsDBAccessor().get_transactions()
        df_monzo = MonzoTransactionsDBAccessor().get_transactions()
        df_revo = RevoTransactionsDBAccessor().get_transactions()

        currency_converter = currencies.HybridLookupCurrencyConverter()

        df_hsbc_transformed = transformers.HSBCStatementTransformer().transform(df_hsbc.copy())
        df_monzo_transformed = transformers.MonzoStatementTransformer().transform(df_monzo.copy())
        df_revo_transformed = transformers.RevoStatementTransformer(currency_converter).transform(df_revo.copy())

        logging.info(f"Checking HSBC transactions for duplicates...")
        self._handle_duplicates(df_hsbc_transformed,
                                ['datetime', 'amount', 'currency', 'amount_cur', 'description'])

        logging.info(f"Checking Monzo transactions for duplicates...")
        self._handle_duplicates(df_monzo_transformed,
                                ['datetime', 'amount', 'currency', 'amount_cur', 'description'])

        logging.info(f"Checking Revolut transactions for duplicates...")
        self._handle_duplicates(df_revo_transformed,
                                ['datetime', 'amount', 'currency', 'amount_cur'])  # TODO description is not considered

        self._transaction_df_validation(df_hsbc_transformed)
        self._transaction_df_validation(df_monzo_transformed)
        self._transaction_df_validation(df_revo_transformed)

        df_merged = pd.concat([df_hsbc_transformed, df_monzo_transformed, df_revo_transformed])
        df_merged = df_merged.sort_values(by='datetime')
        df_merged['tags'] = ''

        df_merged.to_sql(models.TransactionsDBTable.__tablename__, db.engine, if_exists='replace', index=False)

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def update_tags(self, df_tags) -> None:
        logging.info(f"Updating tags (shape: {df_tags.shape}) in DB.")
        # transaction_ids = df_tags['id'].to_list()
        update_values = df_tags.set_index('id')['tags'].to_dict()

        for transaction_id, tags in update_values.items():
            db.session.query(models.TransactionsDBTable).filter_by(id=transaction_id).update({'tags': tags})

        # from sqlalchemy.sql.expression import case  # more efficient but uses an extra dependency (sqlalchemy)
        # db.session.query(models.TransactionsDBTable).filter(models.TransactionsDBTable.id.in_(transaction_ids)).update(
        #     {models.TransactionsDBTable.tags: case(update_values, value=models.TransactionsDBTable.id)},
        #     synchronize_session=False
        # )
        db.session.commit()
