import abc


class TagsIOABC(abc.ABC):
    """
    The interface of tags io operations used by the app
    """
    @abc.abstractmethod
    def get_tag(self, name):
        pass

    @abc.abstractmethod
    def set_tag(self, name, value):
        pass

    @abc.abstractmethod
    def all_tags(self):
        pass


class TransactionsIOABC(abc.ABC):
    """
    The interface of transactions io operations used by the app
    """
    @abc.abstractmethod
    def get_transactions(self):
        pass

    @abc.abstractmethod
    def update_transactions(self, df):
        pass


class HSBCTransactionsIOABC(abc.ABC):
    """
    HSBC bank statement files
    """
    @abc.abstractmethod
    def import_statement(self, df):
        pass


class MonzoTransactionsIOABC(abc.ABC):
    """
    Monzo bank statement files
    """
    @abc.abstractmethod
    def import_statement(self, df):
        pass


class RevoTransactionsIOABC(abc.ABC):
    """
    Revolut bank statement files
    """
    @abc.abstractmethod
    def import_statement(self, df):
        pass

