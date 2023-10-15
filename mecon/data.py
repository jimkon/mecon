from mecon.data_strategies import LocalFileDataAdapter


class DataObject:
    @staticmethod
    def local_data():
        data_object = DataObject(LocalFileDataAdapter())
        data_object.load_data()
        return data_object

    def __init__(self, data_strategy):
        self._data_strategy = data_strategy
        self._statements = None
        self._transactions = None
        self._tags = None

    def load_data(self):
        self._statements = self._data_strategy.get_statements()
        self._tags = self._data_strategy.get_tags()
        self._transactions = self._data_strategy.get_transactions()
        self.calculate_tagged_data()

    def calculate_tagged_data(self):
        self._transactions['calculated'] = self.untagged_transactions.copy().apply_taggers(list(self.taggers.values()))

    @property
    def statements(self):
        raise NotImplementedError

    @property
    def transactions(self):
        return self._transactions

    @property
    def untagged_transactions(self):
        return self.transactions['untagged']

    @property
    def tagged_transactions(self):
        return self.transactions['tagged']

    @property
    def calculated_tagged_transactions(self):
        return self._transactions['calculated']

    @property
    def tags(self):
        return self._tags

    @property
    def taggers(self):
        return {k: v['tagger'] for k, v in self.tags.items() if 'tagger' in v.keys()}

    def set_tag(self, tag_name, _json):
        self._data_strategy.set_tag(tag_name, _json)
        self.load_data()
