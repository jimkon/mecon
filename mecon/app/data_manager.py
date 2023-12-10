import pathlib
from typing import List

import pandas as pd

from mecon.data import statements
from mecon.data.data_management import DataManager
from mecon.utils.instance_management import Singleton
from mecon.data import db_controller


class DBDataManager(DataManager, Singleton):
    def __init__(self):
        super().__init__(
            trans_io=db_controller.TransactionsDBAccessor(),
            tags_io=db_controller.TagsDBAccessor(),
            hsbc_stats_io=db_controller.HSBCTransactionsDBAccessor(),
            monzo_stats_io=db_controller.MonzoTransactionsDBAccessor(),
            revo_stats_io=db_controller.RevoTransactionsDBAccessor(),
        )

    # def add_statement(self, df_statement: pd.DataFrame | List[pd.DataFrame], bank: str):
    #     super().add_statement(df_statement, bank)
    #
    #     df_statement = pd.concat(df_statement) if isinstance(df_statement, list) else df_statement
    #
    #     if bank == 'HSBC':
    #         df_transformed = statements.HSBCStatementCSV(df_statement).dataframe()
    #         start_date, end_date = df_transformed['date'].min(), df_transformed['date'].max()
    #     elif bank == 'Monzo':
    #         df_transformed = statements.MonzoStatementCSV(df_statement).dataframe()
    #         start_date, end_date = df_transformed['date'].min(), df_transformed['date'].max()
    #     elif bank == 'Revolut':
    #         df_transformed = statements.RevoStatementCSV(df_statement).dataframe()
    #         start_date, end_date = df_transformed['start_date'].min(), df_transformed['start_date'].max()
    #
    #     filename = pathlib.Path(bank) / f"statement_{start_date}_to_{end_date}_rows{len(df_transformed)}.csv"
    #     df_transformed.to_csv(filename)

