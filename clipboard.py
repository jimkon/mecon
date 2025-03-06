import datetime

from mecon.data import graphs

if __name__ == '__main__':
    from mecon import config
    from mecon.app.current_data import WorkingDataManager, WorkingDatasetDir

    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    datasets_obj = WorkingDatasetDir()
    datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
    dataset = datasets_obj.working_dataset
    data_manager = WorkingDataManager()

    transactions = data_manager.get_transactions()

    start_date = datetime.datetime(2020, 1, 8, 1, 0, 7)
    end_date = datetime.datetime(2025, 1, 21, 0, 0, 0)
    time_unit = 'month'
    compare_tags = {
        'Income',
        'Rent',
        'Super Market'
    }
    # transactions = filtered_transactions()

    all_trans = {}
    for tag in compare_tags:
        trans = transactions.containing_tags(tag)

        # if trans.size() == 0:
        #     raise ValueError(
        #         f"Transactions for {tag} is 0 for filter params=({start_date=}, {end_date=}, {filter_in_tags=}, {filter_out_tags=})")

        all_trans[tag] = trans

    min_date = min([trans.datetime.min() for tags, trans in all_trans.items()])
    max_date = max([trans.datetime.max() for tags, trans in all_trans.items()])

    synced_trans = {}
    for tag, trans in all_trans.items():
        grouped_trans = trans.group_and_fill_transactions(
            grouping_key='month',
            aggregation_key='sum',
            fill_dates_after_groupagg=True,
        )
        dt_r1 = grouped_trans.date_range()
        filled_trans = grouped_trans.fill_values(fill_unit='month', start_date=min_date,
                                                 end_date=max_date)
        dt_r2 = filled_trans.date_range()

        synced_trans[tag] = filled_trans.dataframe()


    plot = graphs.multiple_lines_graph_html(
        times=[trans.datetime for tags, trans in synced_trans.items()],
        lines=[trans.amount for tags, trans in synced_trans.items()],
        names=list(synced_trans.keys()),
        stacked=False
    )

    plot.show()