import abc

from legacy import logs


class TagsColumnDoesNotExistException(Exception):
    pass


class Tag(abc.ABC):
    def __init__(self, tag_name):
        self.tag_name = tag_name

    def tag(self, _df):
        return self._add_tag(_df)

    def _add_tag(self, _df, tag_column='tags'):
        if tag_column not in _df.columns:
            raise TagsColumnDoesNotExistException

        condition = self._calc_condition(_df)
        logs.log_calculation(f"{self.tag_name}: {sum(condition)}/{len(condition)} rows where tagged.")

        def _l(x):
            tags_array, cond = x
            if cond and self.tag_name not in tags_array:
                tags_array.append(self.tag_name)
            return x

        list(map(_l, zip(_df[tag_column], condition)))

    @abc.abstractmethod
    def _calc_condition(self, _df):
        pass


# if __name__ == '__main__':
#
#     df = TaggedStatement().dataframe()
#     print(df.head())

    # tagger = MonzoTag().tag(df)
    # print('monzo', count_tag_appearence(df, 'Monzo'))
    #
    # therapy_tag_json = {
    #     'description.lower': {'contains': 'karamanos'},
    # }
    #
    # test_df = df.copy()
    # at = AutoTag('Therapy', therapy_tag_json)
    # at.tag(test_df)
    # print('karamanos', get_tagged_rows(test_df, 'Therapy')['amount'].sum())  # get_tagged_rows




