import mecon2.datafields as fields


class Transactions(fields.DateTimeColumnMixin, fields.AmountColumnMixin, fields.TagsColumnMixin, fields.DescriptionColumnMixin):
    """
    Responsible for holding the transactions dataframe and controlling the access to it, like a DataFrame Facade.
    Columns are specified and only accessed and modifies by the corresponding mixins. Key goal of this object
    is to solely represent sets of transactions and their app-level operations like the extraction of a subset (e.x
    filter out rows based on a field value) or a superset (e.x fill null dates to ensure non-empty groups)

    Not responsible for any IO operations.
    """
    def __init__(self, df):
        super().__init__(df)

    def dataframe(self):
        pass

    def copy(self):
        pass
