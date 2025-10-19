import logging

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app import shiny_app
from mecon.app.current_data import WorkingDatasetDir
from mecon.data.data_management import CachedFileDataManager
from mecon.etl import transformers
from mecon.tags.process import RuleExecutionPlanMonitor

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")
datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
dataset = datasets_obj.working_dataset

app_ui = shiny_app.app_ui_factory(
    ui.input_task_button(id='fetch_data_button', label='Fetch new transaction data from providers'),
    ui.input_task_button(id='reset_button', label='Reset data from statements'),
    ui.accordion(
        ui.accordion_panel('Sources', ui.card(
            ui.output_ui('statements_info_text'),
            ui.navset_tab(
                ui.nav_panel("Info", ui.output_data_frame('all_sources_info_text')),
                ui.nav_panel("Files", ui.output_data_frame("statements_info_dataframe")),
                # ui.nav_panel("HSBC", ui.card(ui.output_data_frame('hsbc_source_info_text'))),
                # ui.nav_panel("Monzo", ui.card(
                #     ui.h3("Monzo Export"),
                #     ui.output_data_frame('monzo_export_source_info_text'),
                # )),
                # ui.nav_panel("MonzoAPI", ui.card(
                #     ui.h3("Monzo API (*not integrated yet)"),
                #     ui.tags.a('Monzo authentication and fetching...', href="http://127.0.0.1:8000/auth/monzo/"),
                #     ui.output_data_frame('monzo_api_source_info_text'),
                # )),
                # ui.nav_panel("Revolut", ui.card(ui.output_data_frame('revo_source_info_text'))),
                # ui.nav_panel("HSBC Savings account", ui.card(ui.output_data_frame('hsbcsvr_source_info_text'))),
                # ui.nav_panel("Trading 212", ui.card(ui.output_data_frame('trd212_source_info_text'))),
                # ui.nav_panel("Invest Engine", ui.card(ui.output_data_frame('inveng_source_info_text'))),
            )
        )),
        ui.accordion_panel('Transactions', ui.card(
            ui.output_data_frame("transactions_info_dataframe")
        )),
        ui.accordion_panel('Tags', ui.card(
            ui.card(
                ui.h3("Tagging report"),
                ui.output_data_frame("tags_info_dataframe"),
            ),
            ui.card(
                ui.h3("Problematic Tagging conditions stats"),
                ui.input_checkbox('compact_tag_conditions_stats_dataframe_checkbox',
                                  label='Compact table (grouped by Tag)',
                                  value=True),
                ui.output_data_frame("tag_conditions_stats_dataframe"),
            )
        )),
        ui.accordion_panel('Tagged Transactions', ui.card(
            ui.input_selectize(
                "tagged_transactions_source_select",
                "Source tags",
                choices=[],
                multiple=True,
            ),
            ui.output_data_frame("tagged_transactions_info_dataframe")
        )),
        id='data_flow_acc',
        open=False
    )
)


def get_statement_files_info_dataframe(_dataset):
    return _dataset.statement_files_info_df()


def summarize_statement_files_info(df: pd.DataFrame) -> dict:
    return {
        'file_count': len(df),
        'row_count': df['rows'].sum(),
        'source_count': df['source'].nunique(),
    }


def aggregate_statement_sources_info(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('source').agg({'filename': 'count', 'rows': 'sum'}).reset_index()


def source_info_df(_dataset, source):
    df = get_statement_files_info_dataframe(_dataset)
    df_res = df[df['source'] == source]
    return df_res


def load_statement_dataframe(path: str) -> pd.DataFrame:
    return pd.read_csv(path, index_col=None)


def create_transactions_summary_dataframe(data_manager) -> pd.DataFrame:
    df_trans = data_manager.get_transactions().dataframe()
    return df_trans.describe(include='all').reset_index()


def get_tags_metadata_dataframe(data_manager) -> pd.DataFrame:
    return data_manager.get_tags_metadata()


def create_tagged_transactions_info_dataframe(data_manager) -> pd.DataFrame:
    df_tags_info = pd.DataFrame.from_dict(
        data_manager.get_tagged_transactions().all_tag_counts(),
        orient='index').reset_index()
    df_tags_info.columns = ['tag', 'name']
    return df_tags_info


def extract_source_tags(df: pd.DataFrame) -> list[str]:
    if 'tag' not in df.columns:
        return []

    return sorted([
        tag for tag in df['tag'].dropna().tolist()
        if isinstance(tag, str) and tag.startswith('Source [')
    ])


def fetch_statement_sources(data_manager):
    statement_manager = data_manager.get_statement_manager()
    fetchable_sources = statement_manager.get_sources_with_fetch_operation()
    logging.info(f"Fetching {len(fetchable_sources)} sources...")

    results = []
    for source in fetchable_sources:
        try:
            source.fetch()
            logging.info(f"Successfully fetched source {source}")
            results.append({'source': source, 'error': None})
        except Exception as exc:
            logging.exception(f"Failed to fetch source {source}: {exc}")
            results.append({'source': source, 'error': exc})

    logging.info("Fetching finished")
    return results


def build_sources_without_transformers_warning(sources_with_no_transformers, filepaths) -> str:
    message = f"No parser for sources: {sources_with_no_transformers}\\n"
    message += '\\n'.join(
        [
            f" -> Skipping {len(filepaths[source])} statement file from  source '{source}'"
            for source in sources_with_no_transformers
        ]
    )
    return message


def build_unparsed_sources_warning(unparsed_sources) -> str:
    return f"Source not parsed: {unparsed_sources}\\n"


def reset_dataset(data_manager):
    logging.info(f"Reset data")
    filepaths = data_manager.get_statement_filepaths()
    transformer_sources = set(transformers.StatementTransformer.SOURCES)

    statement_sources = set(filepaths.keys())
    warnings = []

    sources_with_no_transformers = statement_sources.difference(transformer_sources)
    if len(sources_with_no_transformers) > 0:
        message = build_sources_without_transformers_warning(sources_with_no_transformers, filepaths)
        logging.info(f"App warning while resetting the data: {sources_with_no_transformers}, {message=}")
        warnings.append(message)

    unparsed_sources = transformer_sources.difference(statement_sources)
    if len(unparsed_sources) > 0:
        message = build_unparsed_sources_warning(unparsed_sources)
        logging.info(f"App warning while resetting the data: {unparsed_sources}, {message=}")
        warnings.append(message)

    data_manager.reset()
    return warnings


def create_tag_conditions_stats_dataframe(_dataset, compact=True):
    monitor = RuleExecutionPlanMonitor(_dataset)
    monitor.load()

    dm = CachedFileDataManager(_dataset)
    df_stats = monitor.get_conditions_stats()
    df_types = dm.all_tags_df[['name', 'type']].rename(columns={'name': 'tag', 'type': 'tag_type'})
    df_merged = df_stats.merge(df_types, how='left', on='tag')
    df_merged['tag_type'].fillna('Unknown', inplace=True)

    df_sel = df_merged[
        (df_merged['tag_type'] != 'Built-in') & ((df_merged['all_true']) | (df_merged['all_false']))].copy()
    df_sel.replace([False, True], value=['False', 'True'], inplace=True)

    def make_link(tag_name: str):
        href = shiny_app.url_for_tag_edit(filter_in_tags=tag_name)
        return ui.HTML(f'<a href="{href}" target="_blank" rel="noopener">Edit \'{tag_name}\'</a>')

    df_sel["actions"] = df_sel["tag"].apply(lambda t: ui.HTML("") if t is None else make_link(t))

    if compact:
        def agg_strings_in_bulletpoints(arr):
            arr_str = [str(el).replace('<', '').replace('>', '') for el in arr]
            html_list = f"<ol><li>{'</li><li>'.join(arr_str)}</li></ol>"
            return ui.HTML(html_list)

        df_sel['all_true'] = df_sel['all_true'].replace({'True': 1, 'False': 0})
        df_sel['all_false'] = df_sel['all_false'].replace({'True': 1, 'False': 0})
        df_compact = df_sel.groupby('tag').agg({
            'type': lambda x: agg_strings_in_bulletpoints(
                [f"{v}x {k}{'s' if v > 1 else ''}" for k, v in pd.Series.value_counts(x).to_dict().items()]),
            'all_true': 'sum',
            'all_false': 'sum',
            'depending on': agg_strings_in_bulletpoints,
            'rule': agg_strings_in_bulletpoints,
            'priority': agg_strings_in_bulletpoints,
        }).reset_index()
        df_compact['actions'] = df_compact["tag"].apply(lambda t: ui.HTML("") if t is None else make_link(t))
        df_res = df_compact[['tag', 'actions', 'type', 'all_true', 'all_false', 'depending on', 'rule', 'priority']]
    else:
        df_res = df_sel

    logging.info(f"tag_conditions_stats_dataframe-> {df_res.shape=}, {compact=}")
    return df_res


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()

    @render.ui
    def statements_info_text():
        df = get_statement_files_info_dataframe(dataset)
        summary = summarize_statement_files_info(df)
        text = ui.HTML(
            f"""<p>Found <b>{summary['file_count']} files</b>, containing <b>{summary['row_count']} rows</b> (* rows might not be 100% accurate). in total and <b>{summary['source_count']} different sources</b></p>"""
        )
        return text

    @render.data_frame
    def all_sources_info_text():
        df = get_statement_files_info_dataframe(dataset)
        df_agg = aggregate_statement_sources_info(df)
        return shiny_app.render_table_standard(df_agg)

    @render.data_frame
    def hsbc_source_info_text():
        df = source_info_df(dataset, 'HSBC')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def monzo_export_source_info_text():
        df = source_info_df(dataset, 'Monzo')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def monzo_api_source_info_text():
        df = source_info_df(dataset, 'MonzoAPI')
        logging.info(f"Source: {df=}")
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def revo_source_info_text():
        df = source_info_df(dataset, 'Revolut')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def hsbcsvr_source_info_text():
        df = source_info_df(dataset, 'HSBCSVR')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def trd212_source_info_text():
        df = source_info_df(dataset, 'TRD212')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def inveng_source_info_text():
        df = source_info_df(dataset, 'INVENG')
        return shiny_app.render_table_standard(df)

    @render.data_frame
    def statements_info_dataframe():
        df = get_statement_files_info_dataframe(dataset)
        res = render.DataGrid(df, selection_mode="row")
        return res

    @reactive.effect
    def selected_statement():
        statements_selected = statements_info_dataframe.data_view(selected=True).to_dict('records')
        logging.info(f"Selected statement: {statements_selected}")

        if len(statements_selected) > 0:
            statement_dict = statements_selected[0]
            m = ui.modal(
                ui.output_data_frame(id='showing_statement_output_df'),
                title=f"Statement {statement_dict['filename']}",
                easy_close=True,
                size='xl'
            )
            ui.modal_show(m)

            @render.data_frame
            def showing_statement_output_df():
                df_stat = load_statement_dataframe(statement_dict['path'])
                return df_stat

    @render.data_frame
    def transactions_info_dataframe():
        df_info = create_transactions_summary_dataframe(data_manager)
        res = render.DataGrid(df_info, selection_mode="row")
        return res

    @render.data_frame
    def tags_info_dataframe():
        df_tags_info = get_tags_metadata_dataframe(data_manager)
        return shiny_app.render_table_standard(df_tags_info, format_columns=True)

    @render.data_frame
    def tag_conditions_stats_dataframe():
        df = create_tag_conditions_stats_dataframe(
            dataset,
            compact=input.compact_tag_conditions_stats_dataframe_checkbox())
        return shiny_app.render_table_standard(df, format_columns=True)

    @render.data_frame
    def tagged_transactions_info_dataframe():
        df_tags_info = create_tagged_transactions_info_dataframe(data_manager)
        source_tags = extract_source_tags(df_tags_info)

        if not source_tags:
            ui.update_selectize(
                "tagged_transactions_source_select",
                choices=[],
                selected=[],
            )
            res = render.DataGrid(df_tags_info, selection_mode="row")
            return res

        selected_sources = input.tagged_transactions_source_select()

        if not selected_sources:
            selected_sources = source_tags
        else:
            selected_sources = [tag for tag in selected_sources if tag in source_tags]
            if not selected_sources:
                selected_sources = source_tags

        ui.update_selectize(
            "tagged_transactions_source_select",
            choices=source_tags,
            selected=selected_sources,
        )

        df_tags_info = df_tags_info[df_tags_info['tag'].isin(selected_sources)]
        res = render.DataGrid(df_tags_info, selection_mode="row")
        return res

    @reactive.effect
    @reactive.event(input.fetch_data_button)
    def _():
        fetch_results = fetch_statement_sources(data_manager)
        for result in fetch_results:
            source = result['source']
            error = result['error']
            if error is None:
                ui.notification_show(
                    f"Successfully fetched source {source}",
                    type='message',
                    duration=5
                )
            else:
                ui.notification_show(
                    f"Failed to fetch source {source}: {error}",
                    type='error',
                    duration=None
                )

    @reactive.effect
    @reactive.event(input.reset_button)
    def _():
        warning_messages = reset_dataset(data_manager)
        for message in warning_messages:
            ui.notification_show(
                f"WARNING:\n{message}",
                type="warning",
                duration=10,
                close_button=True
            )


dataflow_app = App(app_ui, server)
