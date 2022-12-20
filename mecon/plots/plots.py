import matplotlib.pyplot as plt

from mecon.utils import fill_dates

plt.style.use('bmh')

import pandas as pd

from mecon.statements.tagged_statement import TaggedData, FullyTaggedData


def plot_rolling_hist(x, y, rolling_bin=30, actual_line_style='-', expanding_mean=False):
    if actual_line_style is not None:
        plt.plot(x, y, actual_line_style, label='actual', color='C0')

    plt.plot(x, y.rolling(rolling_bin).mean(), label='mean', color='C1')
    plt.fill_between(x, y.rolling(rolling_bin).min(), y.rolling(rolling_bin).max(), color='gray', alpha=.1,
                     label='min_max')

    if expanding_mean:
        plt.plot(x, y.expanding().mean(), label=f'expanding_mean (last value: {y.mean():.2f})', color='C2')


def total_balance_timeline_fig(show=True):
    df = FullyTaggedData.instance().dataframe()
    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90);

    df_agg = df.groupby('date').agg({'amount': 'sum'}).reset_index()
    plot_rolling_hist(df_agg['date'], df_agg['amount'].cumsum())

    _x = df_agg[df_agg['date'].dt.day == 1]['date'].apply(pd.Timestamp)
    for date in _x:
        plt.axvline(date, color='r', alpha=0.2, linewidth=1, linestyle='--')

    _x = df_agg[(df_agg['date'].dt.day == 1) & (df_agg['date'].dt.month == 1)]['date'].apply(pd.Timestamp)
    for date in _x:
        plt.axvline(date, color='g', alpha=0.2, linewidth=1, linestyle='--')

    plt.legend()
    plt.title(f"Total balance ({(df_agg['date'].max() - df_agg['date'].min()).days} days)")
    plt.xlabel('Money (£)')
    plt.xlabel('Time (daily)')
    plt.tight_layout()
    if show:
        plt.show()


def plot_tag_stats(tag, show=True):
    tagged_stat = FullyTaggedData.instance()
    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'amount']]

    amount_per_day = df.groupby('date').agg({'amount': 'sum'}).reset_index()
    amount_per_day = fill_dates(amount_per_day)[['date', 'amount']]

    count_per_day = df.groupby('date').agg({'amount': 'count'}).reset_index()
    count_per_day = fill_dates(count_per_day)[['date', 'amount']]

    df = tagged_stat.get_rows_tagged_as(tag).dataframe()[['date', 'month_date', 'amount']]

    amount_per_month = df.groupby('month_date').agg({'amount': 'sum'}).reset_index()

    count_per_month = df.groupby('month_date').agg({'amount': 'count'}).reset_index()

    plt.figure(figsize=(16, 9))
    plt.subplot(2, 2, 2)
    plt.title(f"Daily {tag} amount")

    plot_rolling_hist(amount_per_day['date'], amount_per_day['amount'].abs(), actual_line_style='.',
                      expanding_mean=True)
    plt.legend()

    plt.subplot(2, 2, 4)
    plt.title(f"Daily {tag} freq")
    plot_rolling_hist(count_per_day['date'], count_per_day['amount'].abs(), actual_line_style='.', expanding_mean=True)
    plt.legend()

    plt.subplot(2, 2, 1)
    plt.title(f"Monthly {tag} amount")

    plot_rolling_hist(amount_per_month['month_date'], amount_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plt.legend()
    plt.xticks(rotation=90);

    plt.subplot(2, 2, 3)
    plt.title(f"Monthly {tag} freq")
    plot_rolling_hist(count_per_month['month_date'], count_per_month['amount'].abs(), rolling_bin=3,
                      actual_line_style='.', expanding_mean=True)
    plt.legend()
    plt.xticks(rotation=90);

    plt.tight_layout()

    if show:
        plt.show()
