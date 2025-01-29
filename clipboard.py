from mecon.tags.rule_graphs import TagGraph

if __name__ == '__main__':
    from mecon import config
    from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
    datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
    if not datasets_dir.exists():
        raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

    datasets_obj = WorkingDatasetDir()
    datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
    dataset = datasets_obj.working_dataset
    data_manager = WorkingDataManager()

    transaction = data_manager.get_transactions()
    print(transaction.size())

    tags = data_manager.all_tags()
    tg = TagGraph.from_tags(tags)
    # tg.hierarchy()
    print(f"{tg.find_all_cycles()=}")
    tg2 = tg.remove_cycles()
    tg2.create_plotly_graph(k=.5, levels_col='level')

    t = 0