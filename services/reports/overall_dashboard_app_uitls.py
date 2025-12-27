import pandas as pd

from mecon.data import graphs

def tag_sums_table(transactions, tags):
    transactions.build_tags_lookup()
    min_date, max_date = transactions.date_range()
    merged_df = None
    for tag in tags:
        filtered_tx = transactions.containing_tags(tag)
        if filtered_tx.is_empty():
            tx_grouped = filtered_tx.fill_values('month', min_date, max_date)
        else:
            tx_grouped = filtered_tx.group_and_fill_transactions(
                grouping_key='month',
                aggregation_key='sum',
                # fill_dates_before_groupagg=True,
                # fill_dates_after_groupagg=True,
            )
        df = tx_grouped.dataframe()[['datetime', 'amount']].rename(columns={'amount': tag})
        df['date'] = pd.to_datetime(df['datetime'].dt.date)
        del df['datetime']
        # transactions.containing_tags('Rent').select_date_range(start_date=dateparser.parse('12 months ago'), end_date=dateparser.parse('today')).dataframe()
        merged_df = df if merged_df is None else merged_df.merge(df, on='date', how='outer')

    merged_df.fillna(0, inplace=True)
    merged_df.sort_values('date', inplace=True, ascending=False)
    return merged_df[['date'] + tags]


def tag_sums_graph(tag_sums, cols, order=True):
    if order:
        from collections import OrderedDict
        sums = {col: tag_sums[col].abs().sum() for col in cols}
        ordered_sums = OrderedDict({el[0]: el[1] for el in sorted(sums.items(), key=lambda item: item[1], reverse=True)})
        cols = list(ordered_sums.keys())

    return graphs.stacked_bars_graph_html(
        times=[tag_sums['date']] * len(cols),
        lines=[tag_sums[c] for c in cols],
        names=cols,
        reverse_y_axis=True
    )


if __name__ == '__main__':
    from mecon.app.current_data import WorkingDataManager

    data_manager = WorkingDataManager()
    dataset = data_manager.dataset
    transactions = data_manager.transactions
    monthly_basic_agg_table(transactions)
