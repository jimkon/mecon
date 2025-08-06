import logging
import uuid
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Iterable

import pandas as pd

from mecon.data.transactions import Transactions
from mecon.etl.dataset import Dataset
from mecon.etl import transformers
# from mecon.etl.transformers import StatementTransformer, statement_transformers_factory, TrueLayerStatementTransformer, \
#     HSBCFileStatementTransformer
# from mecon.etl.true_layer import TrueLayerAccount, TrueLayerAPIHandler
from mecon.etl.true_layer_by_o3 import TrueLayerClient
from mecon.settings import DictFile
from mecon.utils.data_transformations import json_to_csv
from mecon.utils.data_transformations import normalise_df_column_names


class AccountStatementsSource:
    id = None
    dir_name = None

    def __init__(self,
                 working_dir: str | Path,
                 trans_transformer: transformers.StatementTransformer,
                 ):
        self.working_dir = Path(working_dir)
        self.trans_transformer = trans_transformer

    @property
    def name(self):
        return self.working_dir.name

    def read_statement_file(self, path):
        df = pd.read_csv(path, index_col=None)
        if len(df) == 0:
            return None
        df = normalise_df_column_names(df)
        return df

    @property
    def statement_filepaths(self) -> Iterable[Path]:
        return list(self.working_dir.rglob("*.csv"))

    @cached_property
    def statement_dataframes(self) -> Iterable[pd.DataFrame]:
        dfs = []
        for path in self.statement_filepaths:
            df = self.read_statement_file(path)
            if df is not None:
                dfs.append(df)

        logging.info(
            f"AccountStatements({self.name}) discovered {len(dfs)} statement files with {sum(len(df) for df in dfs)} total rows")
        return dfs

    def to_transactions(self) -> Transactions:
        statement_transactions = None
        for statement_dataframe in self.statement_dataframes:
            df_tx = self.trans_transformer.transform(statement_dataframe)
            if not df_tx['datetime'].is_monotonic_increasing:
                df_tx.sort_values(by='datetime', inplace=True)

            tx = Transactions(df_tx)
            statement_transactions = tx if statement_transactions is None else statement_transactions.merge(tx)
        logging.info(f"AccountStatements({self.name}) "
                     f"transformed {len(self.statement_dataframes)} files "
                     f"into {statement_transactions.size() if statement_transactions else 'NONE'} transactions.")
        return statement_transactions

    @classmethod
    def from_dir(cls, dir_path: Path) -> "AccountStatementsSource":
        source = dir_path.name
        transformer = transformers.statement_transformers_factory(source)
        return AccountStatementsSource(dir_path, transformer)

    @classmethod
    def from_dataset(cls, dataset: Dataset) -> list["AccountStatementsSource"]:
        statements_dirs = [p.name for p in dataset.statements.glob('*') if p.is_dir()]
        return [account_statements_factory(dataset, d) for d in statements_dirs]

    def __repr__(self):
        return f"{self.id} #AccountStatement({self.dir_name})"


class HSBCAccountStatementsSource(AccountStatementsSource):
    id = 'HSBC'
    dir_name = 'HSBC'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.HSBCFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)

    def read_statement_file(self, path):
        df = pd.read_csv(path, index_col=None, header=None)
        df.columns = ['date', 'description', 'amount']
        df = normalise_df_column_names(df)
        return df


class HSBCSaverAccountStatementsSource(AccountStatementsSource):
    id = 'HSBCSVR'
    dir_name = 'HSBCSVR'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.HSBCFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)

    def read_statement_file(self, path):
        # same as in HSBCAccountStatementsSource
        df = pd.read_csv(path, index_col=None, header=None)
        df.columns = ['date', 'description', 'amount']
        df = normalise_df_column_names(df)
        return df


class MonzoAccountStatementsSource(AccountStatementsSource):
    id = 'MZN'
    dir_name = 'Monzo'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.MonzoFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class RevolutAccountStatementsSource(AccountStatementsSource):
    id = 'REVO'
    dir_name = 'Revolut'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.RevoFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class InvestEngineAccountStatementsSource(AccountStatementsSource):
    id = 'INVENG'
    dir_name = 'INVENG'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.InvestEngineStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class Trading212AccountStatementsSource(AccountStatementsSource):
    id = 'TRD212'
    dir_name = 'TRD212'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.Trading212StatementTransformer()
        super().__init__(working_dir, trans_transformer)


class APIAccountStatementsSource(AccountStatementsSource):
    def __init__(self,
                 working_dir: str | Path,
                 trans_transformer: transformers.StatementTransformer,
                 api_handler,
                 auto_fetch: bool = False,
                 ):
        super().__init__(working_dir=working_dir, trans_transformer=trans_transformer)
        self.api_handler = api_handler
        if auto_fetch:
            self.fetch()

    def fetch(self):
        pass


class TrueLayerStatements(APIAccountStatementsSource):
    bank = None
    account_id = None

    def __init__(self,
                 working_dir: Path,
                 creds: DictFile,
                 ):
        self.creds = creds
        super().__init__(
            working_dir=working_dir,
            trans_transformer=transformers.TrueLayerStatementTransformer(
                source=self.id,
            ),
            api_handler=TrueLayerClient(creds)
        )

    def fetch(self):
        fetch_datetime = datetime.now().date()
        fetch_job_id = str(uuid.uuid4())

        json_transactions = self.api_handler.get_transactions(self.bank.lower(), self.account_id)
        df = json_to_csv(json_transactions)
        if len(df) == 0:
            logging.info(f"{self.__class__.__name__}: No transactions fetched for {self.bank}:{self.id} and {self.account_id}. No file added to {self.dir_name}.")
            return

        filepath = self.working_dir / f"transactions_{fetch_datetime}_{fetch_job_id}.csv"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index_label=None)


class TrueLayerHSBCStatements(TrueLayerStatements):
    id = 'TLHSBC'
    dir_name = 'TrueLayerHSBC'
    bank = 'ob-hsbc'
    account_id = 'd4aa58643585c1e3a5f7d3e24cf5e829'


class TrueLayerHSBCSSaverStatements(TrueLayerStatements):
    id = 'TLHSBCSVR'
    dir_name = 'TrueLayerHSBCSaver'
    bank = 'ob-hsbc'
    account_id = '875dba485407b435dfddccc5a91e772b'


class TrueLayerRevolutGBPStatements(TrueLayerStatements):
    id = 'TLREVOGBP'
    dir_name = 'TrueLayerRevolutGBP'
    bank = 'ob-revolut'
    account_id = '3b2038675f58008e4e58c43a5d8d103c'


class TrueLayerRevolutEURStatements(TrueLayerStatements):
    id = 'TLREVOEUR'
    dir_name = 'TrueLayerRevolutEUR'
    bank = 'ob-revolut'
    account_id = '5f2ed9feaf603a7a7a904469f37b260a'


class TrueLayerRevolutRONStatements(TrueLayerStatements):
    id = 'TLREVORON'
    dir_name = 'TrueLayerRevolutRON'
    bank = 'ob-revolut'
    account_id = 'fa5ddbfc7431ffd009445263b4259094'


class TrueLayerRevolutHUFStatements(TrueLayerStatements):
    id = 'TLREVOHUF'
    dir_name = 'TrueLayerRevolutHUF'
    bank = 'ob-revolut'
    account_id = '3ea5d7076b553a642d47c90ab5efec8b'


class TrueLayerMonzoStatements(TrueLayerStatements):
    id = 'TLMONZO'
    dir_name = 'TrueLayerMonzo'
    bank = 'ob-monzo'
    account_id = 'bee16ba99227a5079f78408115b05686'


class MonzoAPIStatements(APIAccountStatementsSource):
    id = 'MonzoAPI'
    dir_name = 'MonzoAPI'


class Trading212APIStatements(APIAccountStatementsSource):
    id = 'Trading212API'
    dir_name = 'Trading212API'


ACCOUNT_STATEMENT_SOURCES = [
    # HSBCAccountStatementsSource,
    # HSBCSaverAccountStatementsSource,
    # MonzoAccountStatementsSource,
    # RevolutAccountStatementsSource,
    # InvestEngineAccountStatementsSource,
    # Trading212AccountStatementsSource,
    TrueLayerHSBCStatements,
    TrueLayerHSBCSSaverStatements,
    TrueLayerRevolutGBPStatements,
    TrueLayerRevolutEURStatements,
    TrueLayerRevolutRONStatements,
    TrueLayerRevolutHUFStatements,
    TrueLayerMonzoStatements,
    # MonzoAPIStatements,
    # Trading212APIStatements,
]

ACCOUNT_STATEMENT_SOURCE_DIR_NAMES = [source_obj.dir_name for source_obj in ACCOUNT_STATEMENT_SOURCES]


def account_statements_factory(dataset, source) -> "AccountStatementsSource":
    dir_path = dataset.statements / source
    if source not in ACCOUNT_STATEMENT_SOURCE_DIR_NAMES:
        raise ValueError(
            f"Invalid or unknown transaction source name '{source}', must be one of {ACCOUNT_STATEMENT_SOURCE_DIR_NAMES}")

    search_index = ACCOUNT_STATEMENT_SOURCE_DIR_NAMES.index(source)

    acc_statement_class = ACCOUNT_STATEMENT_SOURCES[search_index]
    if issubclass(acc_statement_class, APIAccountStatementsSource):
        acc_statement_source = ACCOUNT_STATEMENT_SOURCES[search_index](dir_path, creds=dataset.creds)
    else:
        acc_statement_source = ACCOUNT_STATEMENT_SOURCES[search_index](dir_path)
    return acc_statement_source


class StatementsManager:
    def __init__(self,
                 dataset: Dataset,
                 ):
        self.dataset = dataset
        self.creds = dataset.creds
        self.sources = None

        self.discover_statement_sources()

    def discover_statement_sources(self):
        self.sources = [account_statements_factory(self.dataset, source_name) for source_name in
                        ACCOUNT_STATEMENT_SOURCE_DIR_NAMES]

    def all_transactions_from_all_sources(self, source_ids_to_exclude=None):
        source_ids_to_exclude = source_ids_to_exclude or []
        txs = []
        for source in self.sources:
            if source.id in source_ids_to_exclude:
                logging.info(f"Skipping {source.id} from fetching all transactions because it is found in the exclude list")
                continue
            try:
                tx = source.to_transactions()
                if tx is None:
                    continue
                txs.append(tx)
            except Exception as e:
                logging.error(f"{e.__class__} {e}: Error while fetching {source.name} ({source})")
                raise
        return txs

    def collect_and_merge_transactions(self):
        txs = self.all_transactions_from_all_sources()
        merges_tx = txs[0]
        for tx in txs[1:]:
            merges_tx.merge(tx)
        return merges_tx


if __name__ == '__main__':
    dt = Dataset(
        r"C:\Users\dimitris\PycharmProjects\datasets\v2_dataset_new_statements")


    sm = StatementsManager(dt)
    tx = sm.collect_and_merge_transactions()

    breakpoint()

    # sources = AccountStatementsSource.from_dataset(dataset)
    # sources = [account_statements_factory(dataset, source_name) for source_name in ACCOUNT_STATEMENT_SOURCE_DIR_NAMES]
    # t = account_statements_factory(dataset, source='TrueLayerHSBC')
    t = 0
    # t = TrueLayerHSBCStatements(dataset.statements / "TrueLayerHSBC",
    #                             dataset.creds,)
    # t.api_handler.get_accounts('hsbc')
