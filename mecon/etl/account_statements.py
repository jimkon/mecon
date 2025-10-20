import abc
import logging
import pathlib
import uuid
from abc import abstractclassmethod
import datetime as dt
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
from mecon.etl.true_layer_client_by_o3 import TrueLayerClient
from mecon.etl.trading212_client_by_o3 import Trading212Client
from mecon.etl.monzo_api_client import MonzoClient
from mecon.settings import DictFile
from mecon.utils.datatype_transformations import json_to_csv
from mecon.utils.datatype_transformations import normalise_df_column_names


# TODO Clear out sources and providers, id prefixes and banks mentioned in descriptions

class AccountStatementsSource:
    id = None
    dir_name = None
    original_provider = None

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

    def fetch_statement_dataframes(self) -> list[pd.DataFrame]:
        dfs = []
        for path in self.statement_filepaths:
            df = self.read_statement_file(path)
            if df is not None:
                dfs.append(df)

        logging.info(
            f"AccountStatements({self.name}) discovered {len(dfs)} statement files with {sum(len(df) for df in dfs)} total rows")
        return dfs

    def to_transactions(self) -> Transactions:
        all_dfs = self.fetch_statement_dataframes()

        if len(all_dfs) == 0:
            statement_transactions = Transactions.empty_transactions_factory()
        else:
            txs = []
            for statement_dataframe in all_dfs:
                df_tx = self.trans_transformer.transform(statement_dataframe)
                if not df_tx['datetime'].is_monotonic_increasing:
                    df_tx.sort_values(by='datetime', inplace=True)

                df_tx['tags'] = ''

                tx = Transactions(df_tx)
                txs.append(tx)
            statement_transactions = None if len(txs) == 0 else txs[0] if len(txs) == 1 else txs[0].merge(txs[1:],
                                                                                                          dedup_cols=[
                                                                                                              'id'])

        logging.info(f"AccountStatements({self.name}) "
                     f"transformed {len(all_dfs)} files "
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
    original_provider = 'HSBC'

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
    original_provider = 'HSBC'

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
    original_provider = 'Monzo'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.MonzoFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class RevolutAccountStatementsSource(AccountStatementsSource):
    id = 'REVO'
    dir_name = 'Revolut'
    original_provider = 'Revolut'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.RevoFileStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class InvestEngineAccountStatementsSource(AccountStatementsSource):
    id = 'INVENG'
    dir_name = 'INVENG'
    original_provider = 'InvestEngine'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.InvestEngineStatementTransformer()
        super().__init__(working_dir, trans_transformer)


class Trading212AccountStatementsSource(AccountStatementsSource):
    id = 'TRD212'
    dir_name = 'TRD212'
    original_provider = 'Trading212'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.Trading212StatementTransformer(self.id)
        super().__init__(working_dir, trans_transformer)


class Trading212CashISAAccountStatementsSource(AccountStatementsSource):
    id = 'TRD212_CASH_ISA'
    dir_name = 'TRD212_CASH_ISA'
    original_provider = 'Trading212'

    def __init__(self, working_dir: str | Path):
        trans_transformer = transformers.Trading212StatementTransformer(self.id)
        super().__init__(working_dir, trans_transformer)


class APIAccountStatementsSource(AccountStatementsSource, abc.ABC):
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

    def _log_fetch_banner(self) -> None:
        """Emit a visible log line so users can track remote fetches easily."""

        logging.info(f"##### {self.id} FETCHING DATA FROM THE INTERNET #####")

    @classmethod
    @abc.abstractmethod
    def from_path_and_creds(cls, working_dir: Path, creds: DictFile):
        pass

    @abc.abstractmethod
    def fetch(self, since: dt.datetime | None = None):
        """Fetch new statement data from the remote API.

        Args:
            since: Fetch transactions occurring after this datetime. If ``None``
                the implementation should fetch all available data.
        """
        pass

    def fetch_if_needed_and_transform(
            self, *, force_fetch: bool = False
    ) -> tuple[Transactions, bool]:
        """Fetch missing statement data and return transformed transactions.

        Returns a tuple ``(transactions, fetched)`` where ``transactions`` is
        the latest ``Transactions`` object and ``fetched`` indicates whether a
        remote fetch was performed. If ``force_fetch`` is ``True`` all data will
        be fetched regardless of what is already stored locally.
        """

        if force_fetch:
            self.fetch()
            return self.to_transactions(), True

        existing_transactions = self.to_transactions()
        _, last_date = existing_transactions.date_range()

        today = datetime.now().date()
        if last_date is None or last_date < today:
            since_dt = None
            if last_date is not None:
                since_dt = datetime.combine(
                    last_date + dt.timedelta(days=1),
                    datetime.min.time(),
                ).replace(tzinfo=dt.timezone.utc)
            self.fetch(since=since_dt)
            return self.to_transactions(), True
        return existing_transactions, False

class TrueLayerStatements(APIAccountStatementsSource):
    bank = None
    account_id = None

    # def __init__(self,
    #              working_dir: Path,
    #              creds: DictFile,
    #              ):
    #     self.creds = creds
    #     super().__init__(
    #         working_dir=working_dir,
    #         trans_transformer=transformers.TrueLayerStatementTransformer(
    #             source=self.id,
    #         ),
    #         api_handler=TrueLayerClient(creds)
    #     )

    @classmethod
    def from_path_and_creds(cls, working_dir: Path, creds: DictFile):
        return cls(
            working_dir=working_dir,
            trans_transformer=transformers.TrueLayerStatementTransformer(
                source=cls.id,
            ),
            api_handler=TrueLayerClient(creds)
        )

    def fetch(self, since: dt.datetime | None = None):
        fetch_datetime = datetime.now().date()
        fetch_job_id = str(uuid.uuid4())

        self._log_fetch_banner()

        from_date = since.date() if since else None
        json_transactions = self.api_handler.get_transactions(
            self.bank.lower(), self.account_id, from_date=from_date
        )
        df = json_to_csv(json_transactions)
        if len(df) == 0:
            logging.info(
                f"{self.__class__.__name__}: No transactions fetched for {self.bank}:{self.id} and {self.account_id}. No file added to {self.dir_name}.")
            return

        filepath = self.working_dir / f"transactions_{fetch_datetime}_{fetch_job_id}.csv"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index_label=None)
        logging.info(f"A statement file for {self.id} with {df.shape=} rows got added to the source dir: {filepath}")


class TrueLayerHSBCStatements(TrueLayerStatements):
    id = 'TLHSBC'
    dir_name = 'TrueLayerHSBC'
    original_provider = 'HSBC'
    bank = 'ob-hsbc'
    account_id = 'd4aa58643585c1e3a5f7d3e24cf5e829'


class TrueLayerHSBCSSaverStatements(TrueLayerStatements):
    id = 'TLHSBCSVR'
    dir_name = 'TrueLayerHSBCSaver'
    original_provider = 'HSBC'
    bank = 'ob-hsbc'
    account_id = '875dba485407b435dfddccc5a91e772b'


class TrueLayerRevolutGBPStatements(TrueLayerStatements):
    id = 'TLREVOGBP'
    dir_name = 'TrueLayerRevolutGBP'
    original_provider = 'Revolut'
    bank = 'ob-revolut'
    account_id = '3b2038675f58008e4e58c43a5d8d103c'


class TrueLayerRevolutEURStatements(TrueLayerStatements):
    id = 'TLREVOEUR'
    dir_name = 'TrueLayerRevolutEUR'
    original_provider = 'Revolut'
    bank = 'ob-revolut'
    account_id = '5f2ed9feaf603a7a7a904469f37b260a'


class TrueLayerRevolutRONStatements(TrueLayerStatements):
    id = 'TLREVORON'
    dir_name = 'TrueLayerRevolutRON'
    original_provider = 'Revolut'
    bank = 'ob-revolut'
    account_id = 'fa5ddbfc7431ffd009445263b4259094'


class TrueLayerRevolutHUFStatements(TrueLayerStatements):
    id = 'TLREVOHUF'
    dir_name = 'TrueLayerRevolutHUF'
    original_provider = 'Revolut'
    bank = 'ob-revolut'
    account_id = '3ea5d7076b553a642d47c90ab5efec8b'


class TrueLayerMonzoStatements(TrueLayerStatements):
    id = 'TLMONZO'
    dir_name = 'TrueLayerMonzo'
    original_provider = 'Monzo'
    bank = 'ob-monzo'
    account_id = 'bee16ba99227a5079f78408115b05686'


class Trading212APIStatements(APIAccountStatementsSource):
    id = 'Trading212API'
    dir_name = 'Trading212API'
    original_provider = 'Trading212'

    @classmethod
    def from_path_and_creds(cls, working_dir: Path, creds: DictFile):
        return cls(
            working_dir=working_dir,
            trans_transformer=transformers.Trading212StatementTransformer(cls.id),
            api_handler=Trading212Client(creds)
        )

    def fetch(self, since: dt.datetime | None = None):
        fetch_datetime = datetime.now().date()
        fetch_job_id = str(uuid.uuid4())

        self._log_fetch_banner()

        existing_report_ids, cached_chunk_tos = self._collect_cached_metadata()
        self._log_cached_report_ids(existing_report_ids)

        since = self._determine_since(since, cached_chunk_tos)
        if since is None:
            return

        df = self.api_handler.fetch_history_dataframe(
            since=since,
            request_ids_to_skip=sorted(str(rid) for rid in existing_report_ids),
        )
        if len(df) == 0:
            logging.info(
                f"{self.__class__.__name__}: No transactions fetched for Trading212 since {self}. No file added to {self.dir_name}.")
            return

        filepath = self.working_dir / f"transactions_{fetch_datetime}_{fetch_job_id}.csv"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index_label=None)
        logging.info(f"A statement file for {self.id} with {df.shape=} rows got added to the source dir: {filepath}")

    def _collect_cached_metadata(self) -> tuple[set[str], list[dt.datetime]]:
        logging.info("Trading212APIStatements: Collecting cached metadata from statement files.")

        existing_report_ids: set[str] = set()
        cached_chunk_tos: list[dt.datetime] = []
        columns_of_interest = {"_reportId", "_reportid", "_chunk_to"}
        for csv_path in self.statement_filepaths:
            try:
                df_meta = pd.read_csv(
                    csv_path,
                    dtype=str,
                    usecols=lambda col: col in columns_of_interest,
                )
            except ValueError:
                # None of the requested columns are present – nothing to reuse.
                continue
            except Exception as exc:
                logging.warning(
                    "Unable to inspect Trading212 statement file %s for cached metadata: %s",
                    csv_path,
                    exc,
                )
                continue

            for col in ("_reportId", "_reportid"):
                if col in df_meta:
                    existing_report_ids.update(
                        rid for rid in df_meta[col].dropna() if str(rid).strip()
                    )

            if "_chunk_to" in df_meta:
                chunk_tos = pd.to_datetime(df_meta["_chunk_to"], errors="coerce", utc=True)
                chunk_tos = chunk_tos.dropna()
                if not chunk_tos.empty:
                    cached_chunk_tos.append(chunk_tos.max().to_pydatetime())

        existing_report_ids = {str(rid).strip() for rid in existing_report_ids if str(rid).strip()}
        return existing_report_ids, cached_chunk_tos

    def _log_cached_report_ids(self, existing_report_ids: set[str]) -> None:
        logging.info("Trading212APIStatements: Logging cached report IDs.")
        if existing_report_ids:
            logging.info(
                f"Trading212API: detected {len(existing_report_ids)} cached export id(s) locally {existing_report_ids}."
            )

    def _determine_since(
        self,
        since: dt.datetime | None,
        cached_chunk_tos: list[dt.datetime],
    ) -> dt.datetime | None:
        logging.info("Trading212APIStatements: Determining since parameter.")

        if since is not None:
            if since.tzinfo is None:
                return since.replace(tzinfo=dt.timezone.utc)
            return since

        if not cached_chunk_tos:
            return dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)

        latest_chunk_to = max(cached_chunk_tos)
        if latest_chunk_to.tzinfo is None:
            latest_chunk_to = latest_chunk_to.replace(tzinfo=dt.timezone.utc)

        next_since = latest_chunk_to + dt.timedelta(seconds=1)
        now_utc = dt.datetime.now(dt.timezone.utc)
        if next_since >= now_utc:
            logging.info(
                "Trading212API: cached statements already cover up to %s; nothing to fetch.",
                latest_chunk_to,
            )
            return None

        logging.info(
            "Trading212API: defaulting fetch 'since' to %s based on cached chunk ending at %s.",
            next_since,
            latest_chunk_to,
        )
        return next_since


class MonzoAPIStatements(APIAccountStatementsSource):
    id = 'MonzoAPI'
    dir_name = 'MonzoAPI'
    original_provider = 'Monzo'

    @classmethod
    def from_path_and_creds(cls, working_dir: Path, creds: DictFile):
        return cls(
            working_dir=working_dir,
            trans_transformer=transformers.MonzoAPIFileStatementTransformer(),
            api_handler=MonzoClient(creds)
        )

    def fetch(self, since: dt.datetime | None = None):
        fetch_datetime = datetime.now().date()
        fetch_job_id = str(uuid.uuid4())

        self._log_fetch_banner()

        since_str = (
            since.isoformat().replace("+00:00", "Z")
            if since
            else "2019-01-01T00:00:00Z"
        )
        df = self.api_handler.download_full_history(since=since_str)
        if len(df) == 0:
            logging.info(
                f"{self.__class__.__name__}: No transactions fetched for Monzo-API since {self}. No file added to {self.dir_name}.")
            return

        filepath = self.working_dir / f"transactions_{fetch_datetime}_{fetch_job_id}.csv"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index_label=None)
        logging.info(f"A statement file for {self.id} with {df.shape=} rows got added to the source dir: {filepath}")


ACCOUNT_STATEMENT_SOURCES = [
    HSBCAccountStatementsSource,
    HSBCSaverAccountStatementsSource,
    MonzoAccountStatementsSource,
    RevolutAccountStatementsSource,
    InvestEngineAccountStatementsSource,
    Trading212AccountStatementsSource,
    Trading212CashISAAccountStatementsSource,
    TrueLayerHSBCStatements,
    TrueLayerHSBCSSaverStatements,
    TrueLayerRevolutGBPStatements,
    TrueLayerRevolutEURStatements,
    TrueLayerRevolutRONStatements,
    TrueLayerRevolutHUFStatements,
    TrueLayerMonzoStatements,
    Trading212APIStatements,
    MonzoAPIStatements,
]

ACCOUNT_STATEMENT_SOURCE_MAPPING = {source_obj.dir_name: source_obj for source_obj in ACCOUNT_STATEMENT_SOURCES}


def account_statements_factory(dataset, source) -> "AccountStatementsSource":
    dir_path = dataset.statements / source
    if source not in ACCOUNT_STATEMENT_SOURCE_MAPPING.keys():
        raise ValueError(
            f"Invalid or unknown transaction source name '{source}', must be one of {ACCOUNT_STATEMENT_SOURCE_MAPPING.keys()}")

    acc_statement_class = ACCOUNT_STATEMENT_SOURCE_MAPPING[source]
    if issubclass(acc_statement_class, APIAccountStatementsSource):
        acc_statement_source = acc_statement_class.from_path_and_creds(dir_path, creds=dataset.creds)
    else:
        acc_statement_source = acc_statement_class(dir_path)
    return acc_statement_source


class StatementsManager:
    def __init__(self, sources):
        self.sources = sources

    @classmethod
    def from_dataset(cls, dataset, source_names_to_look_for=None):
        sources = cls.discover_statement_sources(dataset, source_names_to_look_for)
        return cls(sources)

    # def __init__(self,
    #              dataset: Dataset,
    #              ):
    #     self.dataset = dataset
    #     self.creds = dataset.creds
    #     self.sources = None
    #
    #     self.discover_statement_sources()

    # @staticmethod
    # def discover_statement_sources(dataset):
    #     return [account_statements_factory(dataset, source_name) for source_name in
    #                     ACCOUNT_STATEMENT_SOURCE_MAPPING.keys()]

    @staticmethod
    def discover_statement_sources(dataset, source_names_to_look_for=None):
        source_names_to_look_for = list(
            ACCOUNT_STATEMENT_SOURCE_MAPPING.keys()) if source_names_to_look_for is None else source_names_to_look_for

        sub_dirs = set([p.name for p in dataset.statements.glob('*') if p.is_dir()])
        source_dir_names = set(
            [ACCOUNT_STATEMENT_SOURCE_MAPPING[source_name].dir_name for source_name in source_names_to_look_for])
        found_sources = [account_statements_factory(dataset, _dir) for _dir in sub_dirs.intersection(source_dir_names)]

        logging.info(
            f"Discovered {len(found_sources)} of {len(source_dir_names)} sources, {source_dir_names.difference(sub_dirs)} missing")
        if len(found_sources) < len(source_dir_names):
            logging.warning(
                f"Unknown source directory in {dataset} statements dir: {sub_dirs.difference(source_dir_names)}")
        return found_sources

    def get_sources_with_fetch_operation(self):
        f_sources = []
        for source in self.sources:
            if issubclass(source.__class__, APIAccountStatementsSource) and hasattr(source, 'fetch'):
                f_sources.append(source)
        logging.info(f"Fetched {len(f_sources)} sources that can fetch data, {f_sources}")
        return f_sources

    def fetch_from_apis(self):
        for source in self.get_sources_with_fetch_operation():
            source.fetch()

    def collect_statement_files(self):
        return {s.id: s.statement_filepaths for s in self.sources}

    def collect_statement_dataframes(self):
        return {s.id: s.fetch_statement_dataframes() for s in self.sources}

    def collect_transactions(self, source_ids_to_exclude=None, store_dir=None):
        source_ids_to_exclude = source_ids_to_exclude or []
        txs = {}
        for source in self.sources:
            if source.id in source_ids_to_exclude:
                logging.info(
                    f"Skipping {source.id} from fetching all transactions because it is found in the exclude list")
                continue
            try:
                tx = source.to_transactions()
                if store_dir is not None:
                    tx.to_csv(pathlib.Path(store_dir) / f"{source.id}.csv")
                if tx is None:
                    continue
                txs[source.name] = tx
            except Exception as e:
                logging.error(f"{e.__class__} {e}: Error while fetching {source.name} ({source})")
                raise
        return txs

    def collect_and_merge_transactions(self, store_dir=None):
        txs_dict = self.collect_transactions(store_dir=store_dir)
        txs = list(txs_dict.values())
        txs_totals = {tx_name: tx.size() for tx_name, tx in txs_dict.items()}
        logging.info(f"Merging transactions ({sum(txs_totals.values())}): {txs_totals}")
        merged_tx = None if len(txs) == 0 else txs[0] if len(txs) == 1 else txs[0].merge(txs[1:])
        return merged_tx
