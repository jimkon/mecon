from typing import List

import pandas as pd

from mecon.data.transactions import TransactionsHTMLTableFormat


class ManualTaggingHTMLTableFormat(TransactionsHTMLTableFormat):
    def __init__(self, tag_names: List, order_by='newest'):
        super().__init__()
        self._tag_names = tag_names
        self._order_by = order_by

    def _transform(self, df_in) -> pd.DataFrame:
        if self._order_by == 'newest':
            df_in = df_in.sort_values(['datetime'], ascending=False)
        elif self._order_by == 'least tagged':
            df_in['n_tags'] = df_in['tags'].apply(lambda s: len(s.split(',')))
            df_in = df_in.sort_values(['n_tags'], ascending=True)
            del df_in['n_tags']

        df_out = super()._transform(df_in)
        df_out['Edit'] = df_in.apply(lambda row: self._tags_form(self._tag_names, row), axis=1)
        df_out = df_out[[df_out.columns[-1]] + list(df_out.columns[:-1])]
        return df_out

    @staticmethod
    def _tags_form(all_tags, row):
        id = row[0]
        tags = row[6].split(',')
        tag_checkboxes = """<div class="labelContainer">"""
        for tag in all_tags:
            checkbox = f"""
            <label class="{'tagLabel_checked' if tag in tags else 'tagLabel_unchecked'}">{tag}<input type="checkbox"  name="{tag}_for_{id}" {'checked disabled' if tag in tags else ''}></label>
            """
            tag_checkboxes += checkbox
        tag_checkboxes = tag_checkboxes + "</div></div>"

        result = tag_checkboxes  # + tags_container
        return result.replace('\n', '')