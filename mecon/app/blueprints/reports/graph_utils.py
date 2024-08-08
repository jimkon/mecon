import numpy as np
import pandas as pd

from mecon.utils.dataframe_transformers import DataframeTransformer
from mecon.utils import calendar_utils


def calculated_histogram_and_contributions(amount):
    bin_counts, bin_edges = np.histogram(amount, bins='auto')

    bin_midpoints = (bin_edges[:-1] + bin_edges[1:]) / 2
    bin_width = bin_edges[1]-bin_edges[0] if len(bin_edges)>1 else None

    bin_edges_t = list(bin_edges)
    bin_edges_t[-1] = bin_edges_t[-1] + 1  # move it a bit to the right
    bin_indices = np.digitize(amount, bin_edges_t)
    df_bin_amounts = pd.DataFrame({'ind': bin_indices, 'amount': amount})
    fill_df = pd.DataFrame({'ind': range(min(bin_indices), max(bin_indices))})
    fill_df['amount'] = 0

    df_bin_totals = pd.concat([df_bin_amounts, fill_df], keys='ind').groupby('ind').agg('sum').reset_index()

    return bin_midpoints, bin_counts, df_bin_totals['amount'], bin_width


class FullLengthYearCalendarTransformer(DataframeTransformer):
    def __init__(self, amount_aggfunc):
        super().__init__()
        self._amount_aggfunc = amount_aggfunc

    def _construct_calendar_table(self, df: pd.DataFrame) -> pd.DataFrame:
        df['date'] = df['datetime'].dt.date
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].apply(lambda dt: dt.strftime("%B"))
        df['month_num'] = df['datetime'].apply(lambda dt: dt.strftime("%m"))
        df['year-month'] = df['date'].apply(calendar_utils.date_to_month_date)
        df['day_of_week'] = df['date'].apply(calendar_utils.day_of_week)
        df['day_of_month'] = df['date'].apply(calendar_utils.day_of_month)
        df['week_of_year'] = df['date'].apply(calendar_utils.week_of_year)
        df['week_of_month'] = df['date'].apply(calendar_utils.week_of_month)

        calendar_table = pd.pivot_table(df,
                                        values='amount',
                                        # index=['year', 'week_of_year', 'month', 'month_num'],
                                        index=['year-month', 'week_of_month', 'month'],
                                        columns='day_of_week',
                                        aggfunc={'amount': self._amount_aggfunc}
                                        ).reset_index()
        calendar_table = calendar_table.sort_values(['year-month', 'week_of_month', 'month'], ascending=False)
        calendar_table = calendar_table.merge(df.groupby(['year-month', 'week_of_month']).agg({'date': 'first'}),
                                              on=['year-month', 'week_of_month'])

        calendar_table['Date'], calendar_table['Month'] = calendar_table['date'], calendar_table['month']
        del calendar_table['date'], calendar_table['month']
        calendar_table = calendar_table[
            ['year-month', 'Date', 'Month', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
             'Saturday', 'Sunday']]

        return calendar_table

    def _add_html_elements(self, calendar_table: pd.DataFrame) -> pd.DataFrame:
        # calendar_table.set_index('Month', inplace=True)
        calendar_table = calendar_table.groupby(['year-month', 'Month', 'Date'], sort=False).first()
        # calendar_table['Date'] = calendar_table['Date'].apply(self._format_datetime)
        calendar_table['Monday'] = calendar_table['Monday'].apply(lambda am: self._format_amount(am, grey=True))
        calendar_table['Tuesday'] = calendar_table['Tuesday'].apply(lambda am: self._format_amount(am))
        calendar_table['Wednesday'] = calendar_table['Wednesday'].apply(lambda am: self._format_amount(am, grey=True))
        calendar_table['Thursday'] = calendar_table['Thursday'].apply(lambda am: self._format_amount(am))
        calendar_table['Friday'] = calendar_table['Friday'].apply(lambda am: self._format_amount(am, grey=True))
        calendar_table['Saturday'] = calendar_table['Saturday'].apply(lambda am: self._format_amount(am))
        calendar_table['Sunday'] = calendar_table['Sunday'].apply(lambda am: self._format_amount(am, grey=True))

        calendar_table = calendar_table.reset_index(level=0, drop=True)
        return calendar_table

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        calendar_table = self._construct_calendar_table(df)
        html_df = self._add_html_elements(calendar_table)
        return html_df

    @staticmethod
    def _format_datetime(dt): # TODO duplicated. make an HTMLDataframe class to be responsible for df.to_html with styles
        formatted_date = dt.strftime("%b %d, %Y")
        # html_representation = f"""<label class="datetime_label">{formatted_date}</label>"""
        html_representation = f"""<label class="datetime_label">{formatted_date}</label>"""
        return html_representation

    @staticmethod
    def _format_amount(amount, grey=False):
        if np.isnan(amount):
            amount_str = '-'
        else:
            amount_str = f'{amount:.2f}'
        text_color = 'orange' if amount < 0 else 'green' if amount > 0 else 'black'
        background_color = 'background-color: #eff' if grey else 'background-color: #fef'
        return f"""<h6 style="color: {text_color}; width: 150px; text-align: center; {background_color}; padding: 4px;">{amount_str}</h6>"""




