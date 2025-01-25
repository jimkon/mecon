from mecon.tags.rule_graphs import TagGraph
from tags.runners import ExtendedRuleTagging

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

    tags = data_manager.all_tags()
    ExtendedRuleTagging(tags).tag(data_manager.get_transactions())
    tg = TagGraph.from_tags(tags)
    #
    # tt = tg.tidy_table()