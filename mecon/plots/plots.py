import matplotlib.pyplot as plt

from mecon.calendar_utils import fill_days, week_of_month
from mecon import grouping
from mecon.grouping import LocationGrouping

plt.style.use('bmh')

import pandas as pd

from mecon.statements.tagged_statement import TaggedData, FullyTaggedData


def calc_rolling_bin(time_unit):
    if time_unit == "date":
        return 30
    elif time_unit == "week":
        return 12
    elif time_unit == "month":
        return 4
    elif time_unit == 'working month':
        return 4
    elif time_unit == 'year':
        return 1


def plot_verticals(dates, months=True, years=True):
    if months:
        for date in pd.date_range(dates.min(), dates.max(), freq='M'):
            plt.axvline(date, color='r', alpha=0.2, linewidth=1, linestyle='--')

    if years:
        for date in pd.date_range(dates.min(), dates.max(), freq='Y'):
            plt.axvline(date, color='g', alpha=0.2, linewidth=1, linestyle='--')


def plot_rolling_hist(x, y, rolling_bin=30, actual_line_style='-', expanding_mean=False):
    if rolling_bin > 1:
        plt.plot(x, y.rolling(rolling_bin, center=True).mean(), label='mean', color='C1')
        plt.fill_between(x, y.rolling(rolling_bin, center=True).min(), y.rolling(rolling_bin, center=True).max(),
                         color='gray', alpha=.1,
                         label='min_max')
        if expanding_mean:
            plt.plot(x, y.expanding().mean(), label=f'expanding_mean (last value: {y.mean():.2f})', color='C2')

    if actual_line_style is not None:
        if rolling_bin == 1:
            plt.plot(x, y, ':.', label='actual', color='C0', linewidth=1)
        else:
            plt.plot(x, y, actual_line_style, label='actual', color='C0', linewidth=1)


def total_balance_timeline_fig(time_unit='day'):
    time_units = {
        'date': {'rolling_bin': 30, 'grouper': grouping.DailyGrouping},
        'week': {'rolling_bin': 12, 'grouper': grouping.WeeklyGrouping},
        'month': {'rolling_bin': 4, 'grouper': grouping.MonthlyGrouping},
        'working month': {'rolling_bin': 4, 'grouper': grouping.WorkingMonthGrouping},
        'year': {'rolling_bin': 1, 'grouper': grouping.YearlyGrouping},
    }
    assert time_unit in time_units

    data = FullyTaggedData.instance().fill_days()
    df = data.dataframe()

    grouper = time_units[time_unit]['grouper']
    rolling_bin = time_units[time_unit]['rolling_bin']

    df_agg = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index(drop=True)

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    plot_rolling_hist(df_agg['date'], df_agg['amount'].cumsum(), rolling_bin=rolling_bin)
    plot_verticals(df_agg['date'])

    plt.legend()
    plt.title(f"Total balance ({len(df_agg)} {time_unit}s)")
    plt.ylabel('Money (£)')
    plt.xlabel(f'Time ({time_unit})')
    plt.tight_layout()


def total_trips_timeline_fig():
    data = FullyTaggedData.instance().fill_days()
    df = data.dataframe()

    df_trips = LocationGrouping(df).agg({'date': ['min', 'max'], 'amount': 'sum', 'location': 'first'}).reset_index(drop=True)

    df_trips.columns = ["_".join(pair) for pair in df_trips.columns]

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    for ind, row in df_trips.iterrows():
        min_date, max_date, amount, loc = row.tolist()
        amount = -amount
        if loc == 'London':
            continue
        days = (max_date-min_date).days
        plt.fill_between([min_date, max_date], [0], [amount/days], label=f"{loc} (Total: £{amount:.0f})")

    plt.legend()
    plt.title(f"Trips timeline")
    plt.ylabel('Money per day (£)')
    plt.xlabel(f'Time (days)')
    plt.tight_layout()


def tag_amount_stats(tag, time_unit):
    tagged_stat = FullyTaggedData.instance()
    df = tagged_stat.get_rows_tagged_as(tag).fill_days().dataframe()

    # amount_per_day = df.groupby('date').agg({'amount': 'sum'}).reset_index()
    grouper = grouping.get_grouper(time_unit)
    amount_per_day = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index(drop=True)
    if time_unit == 'date':
        amount_per_day = fill_days(amount_per_day)[['date', 'amount']]

    plt.figure(figsize=(12, 3))
    plt.title(f"{tag} cost per {time_unit} {grouper}")
    plot_verticals(amount_per_day['date'])
    r = calc_rolling_bin(time_unit)
    plot_rolling_hist(amount_per_day['date'], -amount_per_day['amount'], actual_line_style='.',
                      expanding_mean=True, rolling_bin=calc_rolling_bin(time_unit))
    plt.ylabel('Amount')
    plt.legend()
    plt.tight_layout()


def tag_frequency_stats(tag, time_unit):
    # tag_amount_stats(tag, time_unit)
    tagged_stat = FullyTaggedData.instance()
    df = tagged_stat.get_rows_tagged_as(tag).dataframe()

    grouper = grouping.get_grouper(time_unit)
    count_per_day = grouper(df).agg({'date': 'first', 'amount': 'count'}).reset_index(drop=True)
    if time_unit == 'date':
        count_per_day = fill_days(count_per_day)[['date', 'amount']]

    plt.figure(figsize=(12, 3))
    plt.title(f"{tag} freq per {time_unit}")
    plot_rolling_hist(count_per_day['date'], count_per_day['amount'].abs(), actual_line_style='.',
                          expanding_mean=True, rolling_bin=calc_rolling_bin(time_unit))
    plot_verticals(df['date'])
    plt.ylabel('Count')
    plt.legend()
    plt.tight_layout()


def plot_tag_stats(tag):
    tagged_stat = FullyTaggedData.instance()
    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'amount']]

    amount_per_day = df.groupby('date').agg({'amount': 'sum'}).reset_index()
    amount_per_day = fill_days(amount_per_day)[['date', 'amount']]

    count_per_day = df.groupby('date').agg({'amount': 'count'}).reset_index()
    count_per_day = fill_days(count_per_day)[['date', 'amount']]

    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'month_date', 'amount']]

    amount_per_month = df.groupby('month_date').agg({'date': 'min', 'amount': 'sum'}).reset_index()

    count_per_month = df.groupby('month_date').agg({'date': 'min', 'amount': 'count'}).reset_index()

    plt.figure(figsize=(16, 9))
    plt.subplot(2, 2, 2)
    plt.title(f"Daily {tag} amount")
    plot_verticals(df['date'])
    plot_rolling_hist(amount_per_day['date'], amount_per_day['amount'].abs(), actual_line_style='.',
                      expanding_mean=True)
    plt.legend()

    plt.subplot(2, 2, 4)
    plt.title(f"Daily {tag} freq")
    plot_rolling_hist(count_per_day['date'], count_per_day['amount'].abs(), actual_line_style='.', expanding_mean=True)
    plot_verticals(df['date'])
    plt.legend()

    plt.subplot(2, 2, 1)
    plt.title(f"Monthly {tag} amount")
    plot_rolling_hist(amount_per_month['date'], amount_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plot_verticals(df['date'])
    plt.xticks(rotation=90)
    plt.legend()

    plt.subplot(2, 2, 3)
    plt.title(f"Monthly {tag} freq")
    plot_rolling_hist(count_per_month['date'], count_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plot_verticals(df['date'])
    plt.xticks(rotation=90)
    plt.legend()

    plt.tight_layout()
