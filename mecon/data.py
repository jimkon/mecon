import abc
import os

from mecon.statements.tagged_statement import FullyTaggedData
from mecon.tagging.dict_tag import DictTag
from mecon.tagging.tags import ALL_TAGS
from mecon import configs


class DataAdapter(abc.ABC):
    @abc.abstractmethod
    def get_statements(self):
        pass

    @abc.abstractmethod
    def get_transactions(self):
        pass

    @abc.abstractmethod
    def get_tags(self):
        pass


class LocalFileAdapter(DataAdapter):
    def get_statements(self):
        pass

    def get_transactions(self):
        trans_dict = {'tagged_transactions': FullyTaggedData.instance().copy()}
        return trans_dict

    def get_tags(self):
        tags_dict = {
            'found': ALL_TAGS,
            'json': self._load_json_tags()
        }
        return tags_dict

    def _load_json_tags(self):
        result_taggers = []
        for tag_filepath in os.listdir(configs.TAGS_DIRECTORY):
            filepath = os.path.join(configs.TAGS_DIRECTORY, tag_filepath)
            try:
                tagger = DictTag.load_from_json_file(filepath)
                result_taggers.append(tagger)
            except ValueError as e:
                print(f"File {filepath} skipped because it is not a JSON.")
        return result_taggers


class DataObject:
    @staticmethod
    def local_data():
        do = DataObject(LocalFileAdapter())
        do.load_data()
        return do

    def __init__(self, data_adapter):
        self._data_adapter = data_adapter
        self._statements = None
        self._transactions = None
        self._tags = None

    def load_data(self):
        self._statements = self._data_adapter.get_statements()
        self._transactions = self._data_adapter.get_transactions()
        self._tags = self._data_adapter.get_tags()

    @property
    def statements(self):
        return self._statements

    @property
    def transactions(self):
        return self._transactions

    @property
    def tags(self):
        return self._tags


do = DataObject.local_data()

t = 0