from datetime import time, timedelta
from enum import Enum
from math import ceil

import pandas as pd

from mecon import logs


class DayOfWeek(Enum):
    MONDAY = 'Monday'
    TUESDAY = 'Tuesday'
    WEDNESDAY = 'Wednesday'
    THURSDAY = 'Thursday'
    FRIDAY = 'Friday'
    SATURDAY = 'Saturday'
    SUNDAY = 'Sunday'


def dayofweek(s):
    return {
        0: DayOfWeek.MONDAY.value,
        1: DayOfWeek.TUESDAY.value,
        2: DayOfWeek.WEDNESDAY.value,
        3: DayOfWeek.THURSDAY.value,
        4: DayOfWeek.FRIDAY.value,
        5: DayOfWeek.SATURDAY.value,
        6: DayOfWeek.SUNDAY.value
    }[s.weekday()]


def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    https://stackoverflow.com/questions/3806473/week-number-of-the-month
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom / 7.0))


def date_to_month_date(date_series):
    return date_series.dt.year.astype(str) + '-' + date_series.dt.month.astype(str).apply(lambda x: f'{x:0>2}')


@logs.func_execution_logging
def fill_days(df_in):
    if len(df_in) == 0:
        return df_in

    existing_dates = set(pd.to_datetime(df_in['date'].unique()))
    all_dates = {df_in['date'].min()+timedelta(days=i) for i in range((df_in['date'].max()-df_in['date'].min()).days+1)}
    to_fill_dates = all_dates - existing_dates

    if len(to_fill_dates) == 0:
        return df_in

    logs.log_calculation(f"Filling {len(to_fill_dates)} days. Unique dates:{len(existing_dates)}, Time period in days: {len(all_dates)}, min day: {df_in['date'].min()}, max day: {df_in['date'].max()}")
    df_to_append = pd.DataFrame({'date': list(to_fill_dates)})
    df_to_append['month_date'] = date_to_month_date(df_to_append['date'])
    df_to_append['time'] = time(0, 0, 0)
    df_to_append['amount'] = .0
    df_to_append['currency'] = 'GBP'
    df_to_append['amount_curr'] = .0
    df_to_append['description'] = ''
    df_to_append['tags'] = [['FILLED'] for i in range(len(df_to_append))]
    return pd.concat([df_in.copy(), df_to_append]).sort_values(by=['date', 'time']).reset_index(drop=True)
