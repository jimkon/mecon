import abc

from mecon.tagging.tag import Tag


class ManualTag(Tag, abc.ABC):
    def __init__(self, tag_name):
        super().__init__(tag_name)

    @abc.abstractmethod
    def condition(self, element):
        pass

    def _calc_condition(self, _df):
        res = _df.apply(lambda x: self.condition(x.to_dict()), axis=1)
        return res

