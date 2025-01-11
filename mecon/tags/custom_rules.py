import abc
from typing import Set, Iterable

from mecon.data.transactions import Transactions
from mecon.tags import tagging


class IdMatchingCustomRule(tagging.CustomRule, abc.ABC):
    def __init__(self):
        self._ids = []
        super().__init__()

    def _compute(self, element):
        return element['id'] in self.matching_ids

    def fit(self, elements: Iterable):
        self._ids = None

    @property
    def matching_ids(self):
        return self._ids

    @abc.abstractmethod
    def calculate_matching_ids(self, df_wrapper: Transactions) -> Set[str]:
        pass


class TransferBetweenMyBanksRule(tagging.CustomRule):
    # def calculate_matching_ids(self, df_wrapper: Transactions) -> Set [str]:
    #     filter_dfw = df_wrapper.containing_tag(self._bank_tag_names)
    pass


class ZeroSumTransactionsRule(IdMatchingCustomRule):
    def __init__(self, time_diff: float = .5):
        self.time_diff = time_diff
        super().__init__()

    # @property
    # @abc.abstractmethod
    # def name(self) -> str:
    #     return 'ZeroSum'

    def calculate_matching_ids(self, df_wrapper: Transactions) -> Set [str]:
        t = 0

        df = df_wrapper.dataframe()
        df['time_diff'] = df['datetime'].diff()
        df['time_diff_secs'] = df['time_diff'].dt.total_seconds()
        df['time_diff_flag'] = df['time_diff_secs'] < self.time_diff

        df['amount_diff'] = df['amount'].diff()
        df['amount_diff_flag'] = df['amount_diff'] == 0

        df_flagged = df[(df['time_diff_flag'] == True) & (df['amount_diff_flag'] == True)]


class NightOutRule(IdMatchingCustomRule):
    pass

if __name__ == '__main__':
    from mecon import config
    from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    datasets_obj = WorkingDatasetDir()
    datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
    dataset = datasets_obj.working_dataset
    data_manager = WorkingDataManager()

    transaction = data_manager.get_transactions()

    rule = ZeroSumTransactionsRule()
    rule.calculate_matching_ids(transaction)