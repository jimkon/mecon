import matplotlib.pyplot as plt

from mecon.calendar_utils import fill_dates, week_of_month
from mecon import grouping

plt.style.use('bmh')

import pandas as pd

from mecon.statements.tagged_statement import TaggedData, FullyTaggedData


def plot_vericals(dates, months=True, years=True):
    if months:
        for date in pd.date_range(dates.min(), dates.max(), freq='M'):
            plt.axvline(date, color='r', alpha=0.2, linewidth=1, linestyle='--')

    if years:
        for date in pd.date_range(dates.min(), dates.max(), freq='Y'):
            plt.axvline(date, color='g', alpha=0.2, linewidth=1, linestyle='--')


def plot_rolling_hist(x, y, rolling_bin=30, actual_line_style='-', expanding_mean=False, verticals=True):
    if actual_line_style is not None:
        plt.plot(x, y, actual_line_style, label='actual', color='C0')

    if rolling_bin == 1:
        return

    plt.plot(x, y.rolling(rolling_bin, center=True).mean(), label='mean', color='C1')
    plt.fill_between(x, y.rolling(rolling_bin, center=True).min(), y.rolling(rolling_bin, center=True).max(),
                     color='gray', alpha=.1,
                     label='min_max')

    if expanding_mean:
        plt.plot(x, y.expanding().mean(), label=f'expanding_mean (last value: {y.mean():.2f})', color='C2')


def total_balance_timeline_fig(time_unit='day'):
    time_units = {
        'day': {'rolling_bin': 30, 'grouper': grouping.DailyGrouping},
        'week': {'rolling_bin': 12, 'grouper': grouping.WeeklyGrouping},
        'month': {'rolling_bin': 4, 'grouper': grouping.MonthlyGrouping},
        'working month': {'rolling_bin': 4, 'grouper': grouping.WorkingMonthGrouping},
        'year': {'rolling_bin': 1, 'grouper': grouping.YearlyGrouping},
    }
    assert time_unit in time_units

    data = FullyTaggedData.instance().fill_dates()
    df = data.dataframe()

    grouper = time_units[time_unit]['grouper']
    rolling_bin = time_units[time_unit]['rolling_bin']

    df_agg = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index()

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    plot_rolling_hist(df_agg['date'], df_agg['amount'].cumsum(), rolling_bin=rolling_bin)
    plot_vericals(df_agg['date'])

    _x = df_agg[df_agg['date'].dt.day == 1]['date'].apply(pd.Timestamp)
    for date in _x:
        plt.axvline(date, color='r', alpha=0.2, linewidth=1, linestyle='--')

    _x = df_agg[(df_agg['date'].dt.day == 1) & (df_agg['date'].dt.month == 1)]['date'].apply(pd.Timestamp)
    for date in _x:
        plt.axvline(date, color='g', alpha=0.2, linewidth=1, linestyle='--')

    plt.legend()
    plt.title(f"Total balance ({len(df_agg)} {time_unit}s)")
    plt.xlabel('Money (£)')
    plt.xlabel(f'Time ({time_unit})')
    plt.tight_layout()


def plot_tag_stats(tag, show=True):
    tagged_stat = FullyTaggedData.instance()
    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'amount']]

    amount_per_day = df.groupby('date').agg({'amount': 'sum'}).reset_index()
    amount_per_day = fill_dates(amount_per_day)[['date', 'amount']]

    count_per_day = df.groupby('date').agg({'amount': 'count'}).reset_index()
    count_per_day = fill_dates(count_per_day)[['date', 'amount']]

    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'month_date', 'amount']]

    amount_per_month = df.groupby('month_date').agg({'date': 'min', 'amount': 'sum'}).reset_index()

    count_per_month = df.groupby('month_date').agg({'date': 'min', 'amount': 'count'}).reset_index()

    plt.figure(figsize=(16, 9))
    plt.subplot(2, 2, 2)
    plt.title(f"Daily {tag} amount")
    plot_vericals(df['date'])
    plot_rolling_hist(amount_per_day['date'], amount_per_day['amount'].abs(), actual_line_style='.',
                      expanding_mean=True)
    plt.legend()

    plt.subplot(2, 2, 4)
    plt.title(f"Daily {tag} freq")
    plot_rolling_hist(count_per_day['date'], count_per_day['amount'].abs(), actual_line_style='.', expanding_mean=True)
    plot_vericals(df['date'])
    plt.legend()

    plt.subplot(2, 2, 1)
    plt.title(f"Monthly {tag} amount")
    plot_rolling_hist(amount_per_month['date'], amount_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plot_vericals(df['date'])
    plt.xticks(rotation=90)
    plt.legend()

    plt.subplot(2, 2, 3)
    plt.title(f"Monthly {tag} freq")
    plot_rolling_hist(count_per_month['date'], count_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plot_vericals(df['date'])
    plt.xticks(rotation=90)
    plt.legend()

    plt.tight_layout()

    if show:
        plt.show()
