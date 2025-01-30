from mecon.tags.process import RuleExecutionPlanTagging
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

    tags = data_manager.all_tags()
    # LinearTagging(tags).tag(data_manager.get_transactions().copy())
    RuleExecutionPlanTagging(tags).tag(data_manager.get_transactions().copy())
    tg = TagGraph.from_tags(tags)
    #
    # tt = tg.tidy_table()