from datetime import time, timedelta, datetime
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


def get_closest_past_monday(dt):
    days_until_monday = (dt.weekday() - 0) % 7  # Calculate the number of days until Monday (0 represents Monday)
    closest_monday = dt - timedelta(days=days_until_monday)
    return closest_monday


def date_floor(dt: datetime, group_by_key: str):
    valid_values = ['day', 'week', 'month', 'year']
    if group_by_key not in valid_values:
        raise ValueError(f"Date grouping key must be one of {valid_values}. {group_by_key} was given instead.")

    if group_by_key == 'day':
        return datetime(dt.year, dt.month, dt.day, 0, 0, 0)
    elif group_by_key == 'week':
        past_monday = get_closest_past_monday(dt)
        return datetime(past_monday.year, past_monday.month, past_monday.day, 0, 0, 0)
    elif group_by_key == 'month':
        return datetime(dt.year, dt.month, 1, 0, 0, 0)
    elif group_by_key == 'year':
        return datetime(dt.year, 1, 1, 0, 0, 0)


def datetime_to_date_id(dt):
    return dt.strftime("%Y%m%d")


def datetime_to_date_id_str(dt):
    return str(datetime_to_date_id(dt))


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


def days_in_between(start_date, end_date):
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def part_of_day(hour):
    if 5 < hour <= 12:
        return 'Morning'
    elif 12 < hour <= 17:
        return 'Afternoon'
    elif 17 < hour <= 21:
        return 'Evening'
    else:
        return 'Night'


def hour_range_of_part_of_day(hour):
    if hour == 'Morning':
        return (5, 12)
    elif hour == 'Afternoon':
        return (12, 17)
    elif hour == 'Evening':
        return (17, 21)
    else:
        return (21, 5)


_fill_days_df = None


def _get_fill_dates(dates_to_fill):
    min_date, max_date = min(dates_to_fill), max(dates_to_fill)
    global _fill_days_df
    if _fill_days_df is None or min_date < _fill_days_df['date'].min() or max_date > _fill_days_df['date'].max():
        if _fill_days_df is not None:
            min_date, max_date = min(min_date, _fill_days_df['date'].min()), max(max_date, _fill_days_df['date'].max())

        logs.log_html(f"Creating prebuilt fill days table... ({min_date} to {max_date}")
        df = pd.DataFrame({'date': days_in_between(min_date, max_date)})
        df['month_date'] = date_to_month_date(df['date'])
        df['time'] = time(0, 0, 0)
        df['amount'] = .0
        df['currency'] = 'GBP'
        df['amount_curr'] = .0
        df['description'] = ''
        df['tags'] = [['FILLED'] for i in range(len(df))]

        _fill_days_df = df

    return _fill_days_df[_fill_days_df['date'].isin(dates_to_fill)]


@logs.func_execution_logging
def fill_days(df_in):
    if len(df_in) == 0:
        return df_in

    existing_dates = set(pd.to_datetime(df_in['date'].unique()))
    all_dates = days_in_between(df_in['date'].min(), df_in['date'].max())
    dates_to_fill = set(all_dates) - existing_dates

    if len(dates_to_fill) == 0:
        return df_in

    logs.log_calculation(
        f"Filling {len(dates_to_fill)} days. Unique dates:{len(existing_dates)}, Time period in days: {len(all_dates)}, min day: {df_in['date'].min()}, max day: {df_in['date'].max()}")
    df_to_append = _get_fill_dates(dates_to_fill)
    return pd.concat([df_in.copy(), df_to_append]).sort_values(by=['date', 'time']).reset_index(drop=True)
