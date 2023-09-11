import json
from collections import Counter
from datetime import datetime
from itertools import chain

import pandas as pd

from mecon2.datafields import Aggregator
from mecon2.utils import calendar_utils


def concat_strings(items):
    return ','.join([str(item) for item in items])


def count_dict(items):
    return json.dumps(dict(sorted(Counter(items).items(), reverse=True)))


def aggregate_tags_set(tags):
    tags_split = [tags_row.split(',') for tags_row in tags if len(tags_row) > 0]
    tags_list = [tag for tag in chain.from_iterable(tags_split) if len(tag) > 0]
    return set(tags_list)


class TransactionAggregator(Aggregator):
    def __init__(self, id_agg, datetime_agg, amount_agg, currency_agg, description_agg, tags_agg):
        super().__init__({
            'id': id_agg,
            'datetime': datetime_agg,
            'amount': amount_agg,
            'currency': currency_agg,
            'amount_cur': amount_agg,
            'description': description_agg,
            'tags': tags_agg
        })


class CustomisedDefaultTransactionAggregator(TransactionAggregator):
    def __init__(self,
                 id_agg=None,
                 datetime_agg=None,
                 amount_agg=None,
                 currency_agg=None,
                 description_agg=None,
                 tags_agg=None):
        id_agg = (lambda ints: int(''.join([str(i) for i in ints]))) if id_agg is None else id_agg
        datetime_agg = min if datetime_agg is None else datetime_agg
        amount_agg = sum if amount_agg is None else amount_agg
        currency_agg = count_dict if currency_agg is None else currency_agg
        description_agg = concat_strings if description_agg is None else description_agg
        tags_agg = (lambda tags_set: ','.join(sorted(aggregate_tags_set(tags_set)))) if tags_agg is None else tags_agg

        super().__init__(
            id_agg=id_agg,
            datetime_agg=datetime_agg,
            amount_agg=amount_agg,
            currency_agg=currency_agg,
            description_agg=description_agg,
            tags_agg=tags_agg
        )


class CustomisedAmountTransactionAggregator(CustomisedDefaultTransactionAggregator):
    def __init__(self, amount_agg_key, date_group_unit):
        if amount_agg_key == 'min':  # TODO can be implemented with multiton
            amount_agg_f = min
        elif amount_agg_key == 'max':
            amount_agg_f = max
        elif amount_agg_key == 'sum':
            amount_agg_f = sum
        elif amount_agg_key == 'avg':
            amount_agg_f = pd.Series.mean
        elif amount_agg_key == 'count':
            amount_agg_f = len
        else:
            raise ValueError(f"Invalid amount aggregation function key '{amount_agg_key}'")

        super().__init__(
            datetime_agg=lambda dt_series: min(dt_series.apply(lambda dt: calendar_utils.date_floor(dt, date_group_unit))),
            amount_agg=amount_agg_f
        )
