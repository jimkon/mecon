import pandas as pd


class IdColumnMixin:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def id(self):
        pass


class DateTimeColumnMixin:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def datetime(self):
        pass

    def date(self):
        pass

    def time(self):
        pass


class AmountColumnMixin:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def amount(self):
        pass

    def currency(self):
        pass

    def amount_cur(self):
        pass


class TagsColumnMixin:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def tags(self):
        pass

    def add_tag(self, tag_obj):
        pass

    def remove_tag(self, tag_name):
        pass


class DescriptionColumnMixin:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def description(self):
        pass



