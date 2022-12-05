import abc

from mecon.tagging.tag import Tag


class ManualTag(Tag, abc.ABC):
    tag_value = 'default_tag_value'

    def __init__(self):
        super().__init__(self.tag_value)

    @abc.abstractmethod
    def condition(self, element):
        pass

    def _calc_condition(self, _df):
        res = _df.apply(lambda x: self.condition(x.to_dict()), axis=1)
        return res

