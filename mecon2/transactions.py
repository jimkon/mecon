import mecon2.datafields as fields


class Transactions(fields.DateTimeColumnMixin, fields.AmountColumnMixin, fields.TagsColumnMixin):
    def __init__(self, df):
        pass

    def dataframe(self):
        pass

    def copy(self):
        pass

    @staticmethod
    def from_csv(self, csv_stream):
        pass

    @staticmethod
    def from_json(json_stream):
        pass
