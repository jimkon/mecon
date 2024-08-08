import abc
from typing import Set, Iterable

from mecon.data.transactions import Transactions
from mecon.tag_tools import tagging


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
    # def calculate_matching_ids(self, df_wrapper: Transactions) -> Set[str]:
    #     filter_dfw = df_wrapper.containing_tag(self._bank_tag_names)
    pass
