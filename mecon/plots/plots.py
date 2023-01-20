import matplotlib.pyplot as plt
from datetime import time

from mecon.calendar_utils import fill_days, week_of_month
from mecon import grouping
from mecon.grouping import TripGrouping, PlaceGrouping
from mecon.tagging.tags import TRIPS, RESIDENCE

plt.style.use('bmh')
plt.rc('legend',fontsize=6)
# plt.legend(fontsize="x-large")

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


def plot_timeline_fig(df, time_unit, cumsum=False, actual_line_style='-', title=''):
    grouper = grouping.get_grouper(time_unit)
    rolling_bin = calc_rolling_bin(time_unit)

    df_agg = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index(drop=True)

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    plot_rolling_hist(df_agg['date'],
                      df_agg['amount'].cumsum() if cumsum else -df_agg['amount'],
                      rolling_bin=rolling_bin,
                      actual_line_style=actual_line_style)
    plot_verticals(df_agg['date'])

    plt.legend()
    plt.title(title)
    plt.ylabel('Money (£)')
    plt.xlabel(f'Time ({len(df_agg)} {time_unit}s)')
    plt.tight_layout()


def plot_time_vs_date_fig(df, actual_line_style='-', title=''):
    from datetime import datetime
    import mecon.calendar_utils as utils

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    df['time_int'] = df['time'].\
        apply(lambda time_str: datetime.strptime(str(time_str), '%H:%M:%S').time()).\
        apply(lambda x: x.hour*100+x.minute)

    plot_rolling_hist(df['date'],
                      df['time_int'],
                      rolling_bin=30,
                      actual_line_style=actual_line_style)

    df['hour'] = df['time_int']//100
    df['part_of_day'] = df['hour'].apply(utils.part_of_day)
    df_agg = df.groupby(['date', 'part_of_day']).agg({'amount': 'sum', 'hour': [min, max]}).reset_index()
    df_agg.columns = ["_".join(pair) if len(pair[1])>0 else pair[0] for pair in df_agg.columns]
    # df_agg = fill_days(df_agg)

    d = df_agg[df_agg['hour_min'] != df_agg['hour_max']]

    idx = df_agg.groupby(['date'])['amount_sum'].transform(min) == df_agg['amount_sum']
    df_part = df_agg[idx]
    df_part['part_min'] = df_part['part_of_day'].apply(lambda x:utils.hour_range_of_part_of_day(x)[0])
    df_part['part_max'] = df_part['part_of_day'].apply(lambda x:utils.hour_range_of_part_of_day(x)[1])

    # plt.fill_between(df_part['date'], 100*df_part['hour_min'], 100*df_part['hour_max'], color='y', label='max', alpha=0.5)
    plt.fill_between(df_part['date'], 100*df_part['part_min'], 100*df_part['part_max'], color='y', label='max', alpha=0.5)

    plot_verticals(df['date'])
    for p in ['Monring', 'Afternoon', 'Evening', 'Night']:
        _min, _max = utils.hour_range_of_part_of_day(p)
        plt.axhline(100*_min, color='r', alpha=0.2, linewidth=1, linestyle='--')
        plt.axhline(100*_max, color='r', alpha=0.2, linewidth=1, linestyle='--')

    plt.legend()
    plt.title(title)
    plt.ylabel('Time')
    plt.xlabel(f"Dates ({df['date'].nunique()} days)")
    plt.yticks(range(0, 2401, 100), [f"{t//100}:00" for t in range(0, 2401, 100)])
    plt.tight_layout()


def total_cost_timeline_fig(df, time_unit):
    plot_timeline_fig(df, time_unit, actual_line_style='.')
    plt.title(f"Total daily costs")
    plt.tight_layout()


def total_trips_timeline_fig():
    data = FullyTaggedData.instance().get_rows_tagged_as(['Tap'])
    df = data.dataframe()

    # df_trips = PlaceGrouping(df).generate_grouping_column(df)
    df_trips = PlaceGrouping(df).agg({'date': ['min', 'max'], 'amount': 'sum', 'place': 'first'}).reset_index(drop=True)
    df_trips.columns = ["_".join(pair) for pair in df_trips.columns]
    df_trips['days'] = (df_trips['date_max']-df_trips['date_min']).dt.days

    df_trips['loc'] = df_trips['place_first'].apply(lambda s: s.split('(')[0])
    df_trips['loc_cat'] = df_trips['loc'].astype('category')
    df_trips['loc_idx'] = df_trips['loc_cat'].astype('category').cat.codes

    plt.figure(figsize=(12, 5))
    plt.xticks(rotation=90)

    locs_passed = set()
    for ind, row in df_trips.iterrows():
        plt.fill_between([row["date_min"], row["date_max"]],
                         [0],
                         [row["amount_sum"]/max(row["days"], 1)],
                         color=f"C{row['loc_idx']}",
                         label=f"{row['loc']}" if (row['loc'] not in locs_passed) else None)
        locs_passed.add(row['loc'])

    plt.legend()
    plt.title(f"Trips timeline")
    plt.ylabel('Money per day (£)')
    plt.xlabel(f'Time (days)')
    plt.tight_layout()


def tag_amount_stats(df, tag, time_unit):
    grouper = grouping.get_grouper(time_unit)
    amount_per_day = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index(drop=True)
    if time_unit == 'date':
        amount_per_day = fill_days(amount_per_day)[['date', 'amount']]

    plt.figure(figsize=(12, 3))
    plt.title(f"{tag} cost per {time_unit} {grouper}")
    plot_verticals(amount_per_day['date'])
    plot_rolling_hist(amount_per_day['date'], -amount_per_day['amount'], actual_line_style='.',
                      expanding_mean=True, rolling_bin=calc_rolling_bin(time_unit))
    plt.ylabel('Amount')
    plt.legend()
    plt.tight_layout()


def tag_frequency_stats(df, tag, time_unit):
    grouper = grouping.get_grouper(time_unit)
    count_per_day = grouper(df).agg({'date': 'min', 'amount': 'count'}).reset_index(drop=True)
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


def plot_multi_tags_timeline(tags, time_unit):
    plt.figure(figsize=(12, 5))

    for tag in tags:
        tagged_stat = FullyTaggedData.instance()
        df = tagged_stat.get_rows_tagged_as(tag).dataframe()
        grouper = grouping.get_grouper(time_unit)
        amount_per_date = grouper(df).agg({'date': 'min', 'amount': 'sum'}).reset_index(drop=True)

        plt.plot(amount_per_date['date'], -amount_per_date['amount'], '.', linewidth=1, alpha=0.75, label=tag)

    plot_verticals(df['date'])

    plt.title(f"{tags}")
    plt.ylabel('Amount')
    plt.legend()
    plt.tight_layout()
