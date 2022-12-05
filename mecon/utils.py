from datetime import time, timedelta

import pandas as pd


def date_to_month_date(date_series):
    return date_series.dt.year.astype(str) + '-' + date_series.dt.month.astype(str).apply(lambda x: f'{x:0>2}')


def fill_dates(df_in):
    if len(df_in) == 0:
        return df_in

    existing_dates = set(pd.to_datetime(df_in['date'].unique()))
    all_dates = {df_in['date'].min()+timedelta(days=i) for i in range((df_in['date'].max()-df_in['date'].min()).days+1)}
    print(f"Unique dates:{len(existing_dates)}, Time period in days: {len(all_dates)}, min day: {df_in['date'].min()}, max day: {df_in['date'].max()}")
    to_fill_dates = all_dates - existing_dates
    df_to_append = pd.DataFrame({'date': list(to_fill_dates)})
    df_to_append['month_date'] = date_to_month_date(df_to_append['date'])
    df_to_append['time'] = time(0, 0, 0)
    df_to_append['amount'] = .0
    df_to_append['currency'] = 'GBP'
    df_to_append['amount_curr'] = .0
    df_to_append['description'] = ''
    df_to_append['tags'] = [['FILLED'] for i in range(len(df_to_append))]
    return pd.concat([df_in.copy(), df_to_append]).sort_values(by=['date', 'time']).reset_index(drop=True)


def exchange_rate_func(curr, date=None):
    gbp_exchange_rate_dict = {
        'EUR': 0.88,
        'GBP': 1,
        'HUF': 0.0021,
        'USD': 0.89
    }
    if curr not in gbp_exchange_rate_dict.keys():
        print(f"Conversion rate for {curr} not found. Amount will be nullified")

    return gbp_exchange_rate_dict[curr]