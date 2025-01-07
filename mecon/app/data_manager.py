import pandas as pd

from mecon.app import db_controller
from mecon.data import data_management


class DBDataManager(data_management.DataManager):
    def __init__(self, db):
        super().__init__(
            trans_io=db_controller.TransactionsDBAccessor(db),
            tags_io=db_controller.TagsDBAccessor(db),
            hsbc_stats_io=db_controller.HSBCTransactionsDBAccessor(db),
            monzo_stats_io=db_controller.MonzoTransactionsDBAccessor(db),
            revo_stats_io=db_controller.RevoTransactionsDBAccessor(db),
        )


class CachedDBDataManager(data_management.CacheDataManager):
    def __init__(self, db):
        super().__init__(
            trans_io=db_controller.TransactionsDBAccessor(db),
            tags_io=db_controller.TagsDBAccessor(db),
            hsbc_stats_io=db_controller.HSBCTransactionsDBAccessor(db),
            monzo_stats_io=db_controller.MonzoTransactionsDBAccessor(db),
            revo_stats_io=db_controller.RevoTransactionsDBAccessor(db),
        )


