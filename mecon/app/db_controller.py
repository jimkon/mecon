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
        self._db = db

    def _format_received_tag(self, tag):
        conditions_json = json.loads(tag.conditions_json)
        return {
            'name': tag.name,
            'conditions_json': conditions_json
        }

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def get_tag(self, name: str) -> dict[str, Any] | None:
        session = self._db.new_session()
        try:
            tag = session.query(models.TagsDBTable).filter_by(name=name).first()
            return self._format_received_tag(tag) if tag else None
        except Exception as e:
            logging.error(f"Failed to retrieve tag: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def set_tag(self, name: str, conditions_json: list | dict) -> None:
        session = self._db.new_session()
        try:
            tag = session.query(models.TagsDBTable).filter_by(name=name).first()
            if tag is None:
                tag = models.TagsDBTable(
                    name=name,
                    conditions_json=json.dumps(conditions_json)
                )
                session.add(tag)
            else:
                tag.conditions_json = json.dumps(conditions_json)

            session.commit()
        except Exception as e:
            logging.error(f"Failed to set tag: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def delete_tag(self, name: str) -> bool:
        session = self._db.new_session()
        try:
            tag = session.query(models.TagsDBTable).filter_by(name=name).first()
            if tag:
                session.delete(tag)
                session.commit()
                return True
            return False
        except Exception as e:
            logging.error(f"Failed to delete tag: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def all_tags(self) -> list[dict]:
        session = self._db.new_session()
        try:
            tags = session.query(models.TagsDBTable).all()
            return [self._format_received_tag(tag) for tag in tags]
        except Exception as e:
            logging.error(f"Failed to retrieve all tags: {e}")
            session.rollback()
            raise
        finally:
            session.close()


class BaseTransactionsDBAccessor(io_framework.RawTransactionsIOABC):
    def __init__(self, db, model):
        self._db = db
        self._model = model  # Model reference passed in constructor

    def import_statement(self, dfs: List[pd.DataFrame] | pd.DataFrame):
        merged_df = pd.concat(dfs) if isinstance(dfs, list) else dfs
        merged_df.to_sql(self._model.__tablename__, self._db.engine, if_exists='append', index=False)

    @logging_utils.codeflow_log_wrapper('#db#transactions')
    def get_transactions(self) -> pd.DataFrame:
        session = self._db.new_session()
        try:
            transactions = session.query(self._model).all()
            transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
            return transactions_df if not transactions_df.empty else None
        except Exception as e:
            logging.error(f"Failed to retrieve transactions: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#transactions')
    def delete_all(self) -> None:
        session = self._db.new_session()
        try:
            session.query(self._model).delete()
            session.commit()
        except Exception as e:
            logging.error(f"Failed to delete transactions: {e}")
            session.rollback()
            raise
        finally:
            session.close()


class HSBCTransactionsDBAccessor(BaseTransactionsDBAccessor):
    def __init__(self, db):
        super().__init__(db, models.HSBCTransactionsDBTable)


class MonzoTransactionsDBAccessor(BaseTransactionsDBAccessor):
    def __init__(self, db):
        super().__init__(db, models.MonzoTransactionsDBTable)


class RevoTransactionsDBAccessor(BaseTransactionsDBAccessor):
    def __init__(self, db):
        super().__init__(db, models.RevoTransactionsDBTable)


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

    # ----------------- VALIDATION FUNCTIONS -----------------

    @staticmethod
    def _transaction_df_validation(df: pd.DataFrame):
        """
        Perform dataframe validation by calling column, type, and value validations.
        """
        TransactionsDBAccessor._transaction_df_columns_validation(df)
        TransactionsDBAccessor._transaction_df_types_validation(df)
        TransactionsDBAccessor._transaction_df_values_validation(df)

    @staticmethod
    def _transaction_df_columns_validation(df: pd.DataFrame):
        """
        Check if the dataframe columns are valid.
        """
        valid_cols = InvalidTransactionsDataframeColumnsException.valid_cols
        found_cols = set(df.columns)
        if found_cols != valid_cols:
            raise InvalidTransactionsDataframeColumnsException(df)

    @staticmethod
    def _transaction_df_types_validation(df: pd.DataFrame):
        """
        Validate the column types of the transaction dataframe.
        """
        invalid_types = []

        # Validate specific column data types
        if not pd.api.types.is_string_dtype(df['id']):
            invalid_types.append(f"Invalid type for column 'id'. Expected string, got: {df['id'].dtype}")
        if not pd.api.types.is_datetime64_dtype(df['datetime']):
            invalid_types.append(f"Invalid type for column 'datetime'. Expected datetime, got: {df['datetime'].dtype}")
        if not pd.api.types.is_numeric_dtype(df['amount']):
            invalid_types.append(f"Invalid type for column 'amount'. Expected numeric, got: {df['amount'].dtype}")
        if not pd.api.types.is_string_dtype(df['currency']):
            invalid_types.append(f"Invalid type for column 'currency'. Expected string, got: {df['currency'].dtype}")
        if not pd.api.types.is_numeric_dtype(df['amount_cur']):
            invalid_types.append(f"Invalid type for column 'amount_cur'. Expected numeric, got: {df['amount_cur'].dtype}")
        if not pd.api.types.is_string_dtype(df['description']):
            invalid_types.append(f"Invalid type for column 'description'. Expected string, got: {df['description'].dtype}")

        # Raise exception if any type errors are found
        if invalid_types:
            raise InvalidTransactionsDataframeDataTypesException(invalid_types)

    @staticmethod
    def _transaction_df_values_validation(df: pd.DataFrame):
        """
        Validate the values of the transaction dataframe.
        """
        value_errors = []
        if df['amount'].isna().sum() > 0:
            value_errors.append(f"Amount column contains NaN values: {df['amount'].isna().sum()}")

        if value_errors:
            raise InvalidTransactionsDataframeDataValueException(value_errors)

    # ----------------- DUPLICATE HANDLING -----------------

    @staticmethod
    def _handle_duplicates(df: pd.DataFrame, cols_to_check=None):
        """
        Check for duplicate rows in the dataframe based on specified columns.
        """
        cols_to_check = df.columns if cols_to_check is None else cols_to_check
        dups = df.duplicated(subset=cols_to_check)

        if dups.sum() > 0:
            logging.warning(f"Duplicates found: {dups.sum()}")
            # TODO: Handle duplicates as needed, e.g., drop duplicates or raise an exception

    # ----------------- TRANSACTION OPERATIONS -----------------

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def get_transactions(self) -> pd.DataFrame:
        """
        Retrieve all transactions from the database.
        """
        session = self._db.new_session()
        try:
            transactions = session.query(models.TransactionsDBTable).all()
            transactions_df = pd.DataFrame([trans.to_dict() for trans in transactions])
            return transactions_df
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def delete_all(self) -> None:
        """
        Delete all records from the Transactions table.
        """
        session = self._db.new_session()
        try:
            session.query(models.TransactionsDBTable).delete()
            session.commit()
        except Exception as e:
            logging.error(f"Failed to delete transactions: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#data#io')
    def load_transactions(self) -> None:
        """
        Load and merge transactions from multiple sources (HSBC, Monzo, Revo),
        validate, and store them in the database.
        """
        # Retrieve transaction data from various sources
        df_hsbc = HSBCTransactionsDBAccessor(self._db).get_transactions()
        df_monzo = MonzoTransactionsDBAccessor(self._db).get_transactions()
        df_revo = RevoTransactionsDBAccessor(self._db).get_transactions()

        currency_converter = currencies.HybridLookupCurrencyConverter()

        # Transform the data
        df_hsbc_transformed = transformers.HSBCStatementTransformer().transform(df_hsbc.copy())
        df_monzo_transformed = transformers.MonzoStatementTransformer().transform(df_monzo.copy())
        df_revo_transformed = transformers.RevoStatementTransformer(currency_converter).transform(df_revo.copy())

        # Check for duplicates
        self._handle_duplicates(df_hsbc_transformed, ['datetime', 'amount', 'currency', 'amount_cur', 'description'])
        self._handle_duplicates(df_monzo_transformed, ['datetime', 'amount', 'currency', 'amount_cur', 'description'])
        self._handle_duplicates(df_revo_transformed, ['datetime', 'amount', 'currency', 'amount_cur'])

        # Validate the transformed dataframes
        self._transaction_df_validation(df_hsbc_transformed)
        self._transaction_df_validation(df_monzo_transformed)
        self._transaction_df_validation(df_revo_transformed)

        # Merge, sort, and save the data
        df_merged = pd.concat([df_hsbc_transformed, df_monzo_transformed, df_revo_transformed])
        df_merged = df_merged.sort_values(by='datetime')
        df_merged['tags'] = ''  # Initialize 'tags' column

        # Store the merged data into the Transactions table
        df_merged.to_sql(models.TransactionsDBTable.__tablename__, self._db.engine, if_exists='replace', index=False)

    @logging_utils.codeflow_log_wrapper('#db#tags')
    def update_tags(self, df_tags: pd.DataFrame) -> None:
        """
        Update the 'tags' column for the transactions based on a dataframe containing the updated tags.
        """
        logging.info(f"Updating tags (shape: {df_tags.shape}) in DB.")

        session = self._db.new_session()
        try:
            update_values = df_tags.set_index('id')['tags'].to_dict()

            # Perform batch update using a SQLAlchemy case expression
            session.query(models.TransactionsDBTable).filter(
                models.TransactionsDBTable.id.in_(update_values.keys())
            ).update(
                {models.TransactionsDBTable.tags: case(update_values, value=models.TransactionsDBTable.id)},
                synchronize_session=False
            )

            # Commit after all updates
            session.commit()

        except Exception as e:
            logging.error(f"Failed to update tags: {e}")
            session.rollback()
            raise
        finally:
            session.close()
