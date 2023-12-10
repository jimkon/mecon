import abc
import json
import os
import pathlib
from collections import OrderedDict

import pandas as pd

from legacy import configs
from legacy.statements.tagged_statement import UntaggedData, FullyTaggedData
from legacy.tagging.json_tags import JsonTag


class OnDemandDataReader(abc.ABC):
    def __init__(self, address):
        self._address = address

    @property
    def address(self):
        return self._address

    @abc.abstractmethod
    def data(self):
        pass

class OnDemandTextFileReader(OnDemandDataReader):
    def data(self):
        with open(self.address, 'r') as fp:
            return fp.read()

class OnDemandCSVFileReader(OnDemandDataReader):
    def data(self):
        return pd.read_csv(self.address)


class ABCDataAdapter(abc.ABC):
    @abc.abstractmethod
    def load_statements(self):
        pass

    @abc.abstractmethod
    def load_transactions(self):
        pass

    @abc.abstractmethod
    def load_tagged_transactions(self):
        pass

    @abc.abstractmethod
    def load_tags(self):
        pass

    @abc.abstractmethod
    def set_tag(self, tag_name, _json):
        pass


class LocalFileDataAdapter(ABCDataAdapter):
    def __init__(self, data_dir):
        self._data_dir = data_dir
        self._statements = None

    def load_statements(self):
        if self._statements is None:
            dir_path = pathlib.Path(self._root_dir, 'statements')
            dir_path.r

    def load_transactions(self):
        trans_dict = {
            'untagged': UntaggedData.instance().copy(),
            'tagged': FullyTaggedData.instance().copy()
        }
        return trans_dict

    def load_tags(self):
        json_tags = self._load_json_tags()
        transactions = self.load_transactions()['tagged']
        data_tag_names = transactions.all_different_tags
        tags_dict = {}
        for tag_name in data_tag_names:
            tags_dict[tag_name] = {'tag_value': tag_name}

        for tag in json_tags:
            tag_name = tag.tag_name
            tags_dict[tag_name] = {'tag_value': tag_name, 'tagger': tag, 'json': tag.json}

        for tag_name in tags_dict:
            tags_dict[tag_name]['n_rows'] = len(transactions.get_rows_tagged_as(tag_name).dataframe())

        tags_dict = OrderedDict(sorted(tags_dict.items(), key=lambda x: x[1]['n_rows'], reverse=True))
        return tags_dict

    def _load_json_tags(self):
        result_taggers = []
        for tag_filepath in os.listdir(configs.TAGS_DIRECTORY):
            filepath = os.path.join(configs.TAGS_DIRECTORY, tag_filepath)
            try:
                tagger = JsonTag.from_json_file(filepath)
                result_taggers.append(tagger)
            except ValueError as e:
                print(f"File {filepath} skipped because it is not a JSON.")
        return result_taggers

    def set_tag(self, tag_name, _json):
        filepath = os.path.join(configs.TAGS_DIRECTORY, tag_name + ".json")
        with open(filepath, 'w') as fp:
            json.dump(_json, fp)

