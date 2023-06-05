
class ColumnMixin:
    def __init__(self, df):
        pass


class DateTimeColumnMixin:
    def date(self):
        pass

    def time(self):
        pass


class AmountColumnMixin:
    def amount(self):
        pass

    def currency(self):
        pass

    def amount_cur(self):
        pass


class TagsColumnMixin:
    def tags(self):
        pass

    def add_tag(self, tag, computable):
        pass



