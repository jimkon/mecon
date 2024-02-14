from datetime import timedelta, datetime, date
from enum import Enum
from math import ceil

import pandas as pd

from mecon import config


class InvalidDatetimeObjectType(Exception):
    pass


def to_date(date_arg):
    if isinstance(date_arg, str) and len(date_arg) == 10:  # magic number
        return datetime.strptime(date_arg, config.DATE_STRING_FORMAT).date()
    elif isinstance(date_arg, str) and len(date_arg) == 19:  # magic number
        return datetime.strptime(date_arg, config.DATETIME_STRING_FORMAT).date()
    elif isinstance(date_arg, datetime):
        return date_arg.date()
    elif isinstance(date_arg, date):
        return date_arg
    else:
        raise InvalidDatetimeObjectType(
            f"Datetime objects must be of type [str | datetime | date], got {type(date_arg)} instead with value '{date_arg}'.")


def to_datetime(date_arg):
    if isinstance(date_arg, str) and len(date_arg) == 10:  # magic number
        return datetime.strptime(date_arg, config.DATE_STRING_FORMAT)
    elif isinstance(date_arg, str) and len(date_arg) == 19:  # magic number
        return datetime.strptime(date_arg, config.DATETIME_STRING_FORMAT)
    elif isinstance(date_arg, datetime):
        return date_arg
    elif isinstance(date_arg, date):
        return datetime(date_arg.year, date_arg.month, date_arg.day, 0, 0, 0)
    else:
        raise InvalidDatetimeObjectType(
            f"Datetime objects must be of type [str | datetime | date], got {type(date_arg)} instead with value '{date_arg}'.")


class DayOfWeek(Enum):
    MONDAY = 'Monday'
    TUESDAY = 'Tuesday'
    WEDNESDAY = 'Wednesday'
    THURSDAY = 'Thursday'
    FRIDAY = 'Friday'
    SATURDAY = 'Saturday'
    SUNDAY = 'Sunday'


class DateRangeUnit(Enum):
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'


class InvalidDataRange(Exception):
    pass


def get_closest_past_monday(dt):
    days_until_monday = (dt.weekday() - 0) % 7  # Calculate the number of days until Monday (0 represents Monday)
    closest_monday = dt - timedelta(days=days_until_monday)
    return closest_monday


def date_floor(dt: datetime, group_by_key: str):
    valid_values = [dr.value for dr in DateRangeUnit]
    if group_by_key not in valid_values:
        raise InvalidDataRange(f"Date grouping key must be one of {valid_values}. {group_by_key} was given instead.")

    if group_by_key == DateRangeUnit.DAY.value:
        return datetime(dt.year, dt.month, dt.day, 0, 0, 0)
    elif group_by_key == DateRangeUnit.WEEK.value:
        past_monday = get_closest_past_monday(dt)
        return datetime(past_monday.year, past_monday.month, past_monday.day, 0, 0, 0)
    elif group_by_key == DateRangeUnit.MONTH.value:
        return datetime(dt.year, dt.month, 1, 0, 0, 0)
    elif group_by_key == DateRangeUnit.YEAR.value:
        return datetime(dt.year, 1, 1, 0, 0, 0)


def datetime_to_str(dt: datetime) -> str:
    return dt.strftime(format=config.DATETIME_STRING_FORMAT)


def datetime_from_str(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, config.DATETIME_STRING_FORMAT)


def datetime_to_date_id(dt):
    return int(dt.strftime("%Y%m%d"))


def datetime_to_date_id_str(dt):
    return str(datetime_to_date_id(dt))


def date_range(start_date: datetime, end_date: datetime, step: str):
    #  https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#period-aliases
    #  https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    start_date, end_date = min(start_date, end_date), max(start_date, end_date)

    if step == DateRangeUnit.DAY.value:
        result_date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    elif step == DateRangeUnit.WEEK.value:
        result_date_range = pd.date_range(start=start_date, end=end_date, freq='7D')
    elif step == DateRangeUnit.MONTH.value:
        month_start_date = start_date.replace(day=1)
        _date_range = pd.date_range(start=month_start_date, end=end_date, freq='MS')
        result_date_range = pd.Series([date.replace(day=start_date.day) for date in _date_range])
    elif step == DateRangeUnit.YEAR.value:
        year_start_date = start_date.replace(month=1, day=1)
        _date_range = pd.date_range(start=year_start_date, end=end_date, freq='YS')
        result_date_range = pd.Series(
            [date.replace(month=start_date.month, day=start_date.day) for date in _date_range])
    else:
        raise ValueError(
            f"Unknown step argument for date range ({step}). Acceptable values are {'day', 'week', 'month', 'year'}")

    return result_date_range


def date_range_group_beginning(start_date: datetime | date, end_date: datetime | date, step: str):
    if isinstance(start_date, date):
        start_date = datetime(start_date.year, start_date.month, start_date.day, hour=0, minute=0, second=0)
    if isinstance(end_date, date):
        end_date = datetime(end_date.year, end_date.month, end_date.day, hour=0, minute=0, second=0)

    if step == 'day':
        start_date = start_date
    elif step == 'week':
        start_date = get_closest_past_monday(start_date)
    elif step == 'month':
        start_date = start_date.replace(day=1)
    elif step == 'year':
        start_date = start_date.replace(month=1, day=1)
    else:
        raise InvalidDataRange(
            f"Unknown step argument for date range ({step}). Acceptable values are {[dr.value for dr in DateRangeUnit]}")

    return date_range(start_date, end_date, step)


def date_to_month_date(date_series):
    return date_series.dt.year.astype(str) + '-' + date_series.dt.month.astype(str).apply(lambda x: f'{x:0>2}')


def days_in_between(start_date, end_date):
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def hour_of_day(date_arg):
    _date = to_datetime(date_arg)
    return _date.hour


def day_of_week(date_arg) -> str:
    _date = to_date(date_arg)
    return {
        0: DayOfWeek.MONDAY.value,
        1: DayOfWeek.TUESDAY.value,
        2: DayOfWeek.WEDNESDAY.value,
        3: DayOfWeek.THURSDAY.value,
        4: DayOfWeek.FRIDAY.value,
        5: DayOfWeek.SATURDAY.value,
        6: DayOfWeek.SUNDAY.value
    }[_date.weekday()]


def day_of_month(date_arg) -> int:
    _date = to_date(date_arg)
    return _date.day


def day_of_year(date_arg) -> int:
    _date = to_date(date_arg)
    return _date.timetuple().tm_yday


def week_of_month(date_arg):
    """ Returns the week of the month for the specified date.
    https://stackoverflow.com/questions/3806473/week-number-of-the-month
    """

    _date = to_date(date_arg)
    first_day = _date.replace(day=1)

    dom = _date.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom / 7.0))


def week_of_year(date_arg):
    _date = to_date(date_arg)
    return _date.isocalendar()[1]


def month_of_year(date_arg):
    _date = to_date(date_arg)
    return _date.month


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
