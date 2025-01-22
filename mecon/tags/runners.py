import abc

from mecon.data.transactions import Transactions
from mecon.tags.tagging import Tag


class TaggingSession(abc.ABC):
    def __init__(self, tags: list[Tag]):
        self.tags = tags

    @abc.abstractmethod
    def tag(self, transactions: Transactions) -> Transactions:
        pass

class LinearTagging(TaggingSession):
    def tag(self, transactions: Transactions) -> Transactions:
        for tag in self.tags:
            transactions = transactions.apply_tag(tag)
        return transactions