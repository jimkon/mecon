# Example of adding a new DB table to the project

In this guide we will add a new table to the project as an example. The table we will add is a table for Tags Metadata.

1. We start by adding the DB model in mecon.app.models

```python
from sqlalchemy import Column, String, Integer, Float, DateTime, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

...

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
...
```

2. Next step is to decide how this data will be used in the project. In this case, we will use it to store metadata about tags used in transactions. Therefore, we will only need to update all the metadata at once and then get them all at once again. 

To do this we create an accessor class that wraps all the operations we need for that, but first we need to create a template interface for it. All accessors should implement a template interface that defines the methods that should be implemented regardless if they use a DB or a File System on the background. This allows us to decouple the actual data source from the logic of the app, and make it possible to have different data sources for each type of data.

All templates are defined in the `mecon.etl.io_framework` module

```python
import abc

import pandas as pd

...


class TagsMetadataIOABC(abc.ABC):
    """
    The interface of tags metadata io operations used by the app
    """

    @abc.abstractmethod
    def replace_all_metadata(self, metadata_df: pd.DataFrame) -> None:
        """
        Replace all data in the tags_metadata_table with the provided DataFrame.

        Args:
            metadata_df (pd.DataFrame): A DataFrame with columns matching the table schema.
        """
        pass

    @abc.abstractmethod
    def get_all_metadata(self) -> pd.DataFrame:
        """
        Retrieve all tag metadata as a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all metadata.
        """
        pass
```

And then, the actual implementation of the accessor class defined in `mecon.app.db_controller`

```python

import logging

import pandas as pd

from mecon.app import models
from mecon.etl import io_framework
from mecon.monitoring import logging_utils

...


class TagsMetadataDBAccessor(io_framework.TagsMetadataIOABC):
    def __init__(self, db):
        self._db = db

    def _format_received_metadata(self, metadata):
        return {
            'name': metadata.name,
            'date_modified': metadata.date_modified,
            'total_money_in': metadata.total_money_in,
            'total_money_out': metadata.total_money_out,
            'count': metadata.count,
        }

    @logging_utils.codeflow_log_wrapper('#db#tags_metadata')
    def replace_all_metadata(self, metadata_df: pd.DataFrame) -> None:
        session = self._db.new_session()
        try:
            # Clear existing metadata
            session.query(models.TagsMetadataTable).delete()

            # Insert new metadata
            for _, row in metadata_df.iterrows():
                metadata = models.TagsMetadataTable(
                    name=row['name'],
                    date_modified=row.get('date_modified'),  # Will use the current timestamp if not provided
                    total_money_in=row['total_money_in'],
                    total_money_out=row['total_money_out'],
                    count=row['count']
                )
                session.add(metadata)

            session.commit()
        except Exception as e:
            logging.error(f"Failed to replace all metadata: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    @logging_utils.codeflow_log_wrapper('#db#tags_metadata')
    def get_all_metadata(self) -> pd.DataFrame:
        session = self._db.new_session()
        try:
            metadata_records = session.query(models.TagsMetadataTable).all()
            metadata_list = [self._format_received_metadata(record) for record in metadata_records]
            return pd.DataFrame(metadata_list)
        except Exception as e:
            logging.error(f"Failed to retrieve all metadata: {e}")
            session.rollback()
            raise
        finally:
            session.close()
```

3. On top of all the accessors, we have a Mediator class combining the accessors for all the data types and doing all the data management. This gives the user a single point of access for the data, and an automated way of managing the operations in the right order.

The mediator class is defined in `mecon.data.data_management` and in this case we need to add only two methods to it. There is a BaseDataManager implementation plus a CachedDataManager implementation that caches the data in memory for faster access.

```python
import pandas as pd

from mecon.etl import io_framework
from mecon.tags.tag_helpers import tag_stats_from_transactions


class BaseDataManager:
    def __init__(self,
                 trans_io: io_framework.CombinedTransactionsIOABC,
                 tags_io: io_framework.TagsIOABC,
                 tags_metadata_io: io_framework.TagsMetadataIOABC,  # Dependency injection of the accessor object
                 hsbc_stats_io: io_framework.RawTransactionsIOABC,
                 monzo_stats_io: io_framework.RawTransactionsIOABC,
                 revo_stats_io: io_framework.RawTransactionsIOABC):
        self._transactions = trans_io
        self._tags = tags_io
        self._tags_metadata = tags_metadata_io  # Adding the accessor object
        self._hsbc_statements = hsbc_stats_io
        self._monzo_statements = monzo_stats_io
        self._revo_statements = revo_stats_io

    ...

    def reset_transaction_tags(self):
        transactions = self.get_transactions().reset_tags()
        all_tags = self.all_tags()

        for tag in all_tags:
            transactions = transactions.apply_tag(tag)

        data_df = transactions.dataframe()
        self._transactions.update_tags(data_df)

        tags_metadata = tag_stats_from_transactions(transactions)  # Adding this snippet to update tags metadata
        self.replace_tags_metadata(tags_metadata)

    def get_tags_metadata(self):  # new method to get all tags metadata
        tags_metadata_df = self._tags_metadata.get_all_metadata()
        return tags_metadata_df

    def replace_tags_metadata(self, metadata_df: pd.DataFrame):  # new method to replace tags metadata
        self._tags_metadata.replace_all_metadata(metadata_df)


...


# similarly
class CachedDataManager(BaseDataManager):
    def __init__(self,
                 trans_io: io_framework.CombinedTransactionsIOABC,
                 tags_io: io_framework.TagsIOABC,
                 tags_metadata_io: io_framework.TagsMetadataIOABC,
                 hsbc_stats_io: io_framework.RawTransactionsIOABC,
                 monzo_stats_io: io_framework.RawTransactionsIOABC,
                 revo_stats_io: io_framework.RawTransactionsIOABC):
        super().__init__(trans_io=trans_io,
                         tags_io=tags_io,
                         tags_metadata_io=tags_metadata_io,
                         hsbc_stats_io=hsbc_stats_io,
                         monzo_stats_io=monzo_stats_io,
                         revo_stats_io=revo_stats_io)
        self._cache = DataCache()

    ...

    def get_tags_metadata(self):
        if self._cache.tags_metadata is None:
            self._cache.tags_metadata = super().get_tags_metadata()
        return self._cache.tags_metadata

    def replace_tags_metadata(self, metadata_df: pd.DataFrame):
        super().replace_tags_metadata(metadata_df)
        self._cache.reset_tags_metadata()


# changes to the DataCache class
class DataCache:
    def __init__(self):
        self.transaction = None
        self.tags = {}
        self.tags_metadata = None
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_transactions(self):
        self.transaction = None

    def reset_tags(self):
        self.tags = {}

    def reset_tags_metadata(self):
        self.tags_metadata = None

    def reset_statements(self):
        self.hsbc_statements = None
        self.monzo_statements = None
        self.revo_statements = None

    def reset_all(self):
        self.reset_statements()
        self.reset_tags()
        self.reset_transactions()
        self.reset_tags_metadata()
```

Currently, the source for all the data is the DB, and we can modify the data managers as such.

`mecon.app.data_manager`:

```python
from mecon.app import db_controller
from mecon.data import data_management


class DBDataManager(data_management.DataManager):
    def __init__(self, db):
        super().__init__(
            trans_io=db_controller.TransactionsDBAccessor(db),
            tags_io=db_controller.TagsDBAccessor(db),
            tags_metadata_io=db_controller.TagsMetadataDBAccessor(db),
            hsbc_stats_io=db_controller.HSBCTransactionsDBAccessor(db),
            monzo_stats_io=db_controller.MonzoTransactionsDBAccessor(db),
            revo_stats_io=db_controller.RevoTransactionsDBAccessor(db),
        )


class CachedDBDataManager(data_management.CachedDataManager):
    def __init__(self, db):
        super().__init__(
            trans_io=db_controller.TransactionsDBAccessor(db),
            tags_io=db_controller.TagsDBAccessor(db),
            tags_metadata_io=db_controller.TagsMetadataDBAccessor(db),
            hsbc_stats_io=db_controller.HSBCTransactionsDBAccessor(db),
            monzo_stats_io=db_controller.MonzoTransactionsDBAccessor(db),
            revo_stats_io=db_controller.RevoTransactionsDBAccessor(db),
        )

```

4. Finally, we plug the new functionality to the app in two points.

First we calculate the metadata in `mecon.tags.tag_helpers.aggregate_data_for_each_tagged_row` for the given transactions.

Then we get the data to use them in the `services.edit_data.menu_tags` shiny app.


PS: All the changes, along with all the new tests can be found on the same commit of this file.
