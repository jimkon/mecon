import pandas as pd

from mecon.tags.process import RuleExecutionPlanMonitor



if __name__ == '__main__':
    from mecon.etl.dataset import Dataset

    dataset = Dataset(r"C:\Users\dimitris\PycharmProjects\datasets\20250812")
    monitor = RuleExecutionPlanMonitor(dataset)
    monitor.load()
    all_monitored_tags = sorted(monitor.all_monitored_tag_names())
    tag_name = all_monitored_tags[0]
    df = monitor.get_tag_conditions(tag_name).copy()

    res = monitor.get_conditions_stats()

    stats = monitor.get_conditions_stats()

    df = df[:100]

    all_true = df.all()
    all_false = ~df.any()

    df_all = pd.DataFrame({
        'condition': all_true.index.tolist(),
        'all_true': all_true.values.tolist(),
        'all_false': all_false.values.tolist(),
    })

    breakpoint()