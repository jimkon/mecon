import pandas as pd

def monthly_basic_agg_table(transactions):
    transactions = transactions.build_tags_lookup()
    monthly_basic_tags = ['Rent', 'Home Bills', 'Subscription', 'Super Market']
    merged_df = None
    for tag in monthly_basic_tags:
        filtered_tx = transactions.containing_tags(tag)
        tx_grouped = filtered_tx.group_and_fill_transactions(
            grouping_key='month',
            aggregation_key='sum',
            # fill_dates_after_groupagg=True,
        )
        df = tx_grouped.dataframe()[['datetime', 'amount']].rename(columns={'amount': tag})
        df['date'] = pd.to_datetime(df['datetime'].dt.date)
        del df['datetime']
        # transactions.containing_tags('Rent').select_date_range(start_date=dateparser.parse('12 months ago'), end_date=dateparser.parse('today')).dataframe()
        merged_df = df if merged_df is None else merged_df.merge(df, on='date', how='outer')

    merged_df.fillna(0, inplace=True)
    merged_df.sort_values('date', inplace=True, ascending=False)
    return merged_df[['date']+monthly_basic_tags]


if __name__ == '__main__':
    from mecon.app.current_data import WorkingDataManager

    data_manager = WorkingDataManager()
    dataset = data_manager.dataset
    transactions = data_manager.transactions
    monthly_basic_agg_table(transactions)
