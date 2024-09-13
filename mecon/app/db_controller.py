import json
import logging
from typing import Any, List

import pandas as pd
from sqlalchemy.sql.expression import case

from mecon.app import models
from mecon.etl import io_framework
from mecon.etl import transformers
from mecon.monitoring import logging_utils
from mecon.utils import currencies


class TagsDBAccessor(io_framework.TagsIOABC):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def _format_received_tag(self, tag):
        conditions_json = json.loads(tag.conditions_json)

        return {
            'name': tag.name,
            'conditions_json': conditions_json
        }

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def get_tag(self, name: str) -> dict[str, Any] | None:
        new_session = self._db.session
        tag = new_session.query(models.TagsDBTable).filter_by(name=name).first()

        if tag is None:
            return None

        return self._format_received_tag(tag)


    @logging_utils.codeflow_log_wrapper('#db#tags')
    def set_tag(self, name: str, conditions_json: list | dict) -> None:
        # if len(conditions_json) > 2000:  # TODO:v3 do we need this?
        #     raise ValueError(f"Tag's json string is bigger than 2000 characters ({len(conditions_json)=})."
        #                      f" Consider increasing the upper limit.")
        new_session = self._db.session
        tag = new_session.query(models.TagsDBTable).filter_by(name=name).first()
        if tag is None:
            tag = models.TagsDBTable(
                name=name,
                conditions_json=json.dumps(conditions_json)  # str(conditions_json)
            )
            new_session.add(tag)
        else:
            tag.conditions_json = json.dumps(conditions_json)

        new_session.commit()

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def delete_tag(self, name: str) -> bool:
        new_session = self._db.session
        tag = new_session.query(models.TagsDBTable).filter_by(name=name).first()
        if tag:
            new_session.delete(tag)
            new_session.commit()  # Make sure to commit after deleting
            return True
        return False

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def all_tags(self) -> list[dict]:
        new_session = self._db.session
        tags = [self._format_received_tag(tag) for tag in new_session.query(models.TagsDBTable).all()]
        return tags


class HSBCTransactionsDBAccessor(io_framework.HSBCTransactionsIOABC):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.HSBCTransactionsDBTable.__tablename__, self._db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        # TODO:v3 get and delete transactions is the same for all tables. deal with duplicated code
        new_session = self._db.session
        transactions = new_session.query(models.HSBCTransactionsDBTable).all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        new_session = self._db.session
        new_session.query(models.HSBCTransactionsDBTable).delete()
        new_session.commit()


class MonzoTransactionsDBAccessor(io_framework.MonzoTransactionsIOABC):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.MonzoTransactionsDBTable.__tablename__, self._db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        new_session = self._db.session
        transactions = new_session.query(models.MonzoTransactionsDBTable).all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        new_session = self._db.session
        new_session.query(models.MonzoTransactionsDBTable).delete()
        new_session.commit()


class RevoTransactionsDBAccessor(io_framework.RevoTransactionsIOABC):
    def __init__(self, db):
        super().__init__()
        self._db = db

    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(models.RevoTransactionsDBTable.__tablename__, self._db.engine, if_exists='append', index=False)

    def get_transactions(self) -> pd.DataFrame:
        new_session = self._db.session
        transactions = new_session.query(models.RevoTransactionsDBTable).all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df if len(transactions_df) > 0 else None

    def delete_all(self) -> None:
        new_session = self._db.session
        new_session.query(models.RevoTransactionsDBTable).delete()
        new_session.commit()


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
    def __init__(self, db):
        super().__init__()
        self._db = db

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
        new_session = self._db.session
        transactions = new_session.query(models.TransactionsDBTable).all()
        transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
        return transactions_df

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def delete_all(self) -> None:
        new_session = self._db.session
        new_session.query(models.TransactionsDBTable).delete()
        new_session.commit()

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def load_transactions(self) -> None:
        df_hsbc = HSBCTransactionsDBAccessor(self._db).get_transactions()
        df_monzo = MonzoTransactionsDBAccessor(self._db).get_transactions()
        df_revo = RevoTransactionsDBAccessor(self._db).get_transactions()

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

        df_merged.to_sql(models.TransactionsDBTable.__tablename__, self._db.engine, if_exists='replace', index=False)

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def update_tags(self, df_tags) -> None:
        logging.info(f"Updating tags (shape: {df_tags.shape}) in DB.")

        session = self._db.session  # Access the session

        try:
            update_values = df_tags.set_index('id')['tags'].to_dict()
            # Iterate over the dictionary and update the rows in the database
            # for transaction_id, tags in update_values.items():
            #     session.query(models.TransactionsDBTable).filter_by(id=transaction_id).update({'tags': tags})
            # Use a `case` expression to perform a batch update
            session.query(models.TransactionsDBTable).filter(
                models.TransactionsDBTable.id.in_(update_values.keys())).update(
                {models.TransactionsDBTable.tags: case(update_values, value=models.TransactionsDBTable.id)},
                synchronize_session=False
            )

            # Commit the session after all updates
            session.commit()

        except Exception as e:
            logging.error(f"Failed to update tags: {e}")
            session.rollback()  # Rollback in case of error

        finally:
            session.close()  # Ensure the session is closed
