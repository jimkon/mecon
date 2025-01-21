# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon import config
from mecon.app.file_system import WorkingDataManager, WorkingDatasetDir
from mecon.tags.rule_graphs import TagGraph

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = config.DEFAULT_DATASETS_DIR_PATH
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

datasets_obj = WorkingDatasetDir()
datasets_dict = {dataset.name: dataset.name for dataset in datasets_obj.datasets()} if datasets_obj else {}
dataset = datasets_obj.working_dataset

if dataset is None:
    raise ValueError(f"Unable to locate working dataset: {datasets_obj.working_dataset=}")

dm = WorkingDataManager()
all_tags = dm.all_tags()

app_ui = ui.page_fluid(
    ui.tags.title("μEcon"),
    ui.navset_pill(
        ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
        ui.nav_control(ui.tags.a("Reports", href=f"http://127.0.0.1:8001/reports/")),
        ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/edit_data/")),
        ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
        ui.nav_control(ui.input_dark_mode(id="light_mode")),
    ),
    ui.hr(),

    ui.h5(ui.output_text('title_output')),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select(id="tags_graph_select", label='Tags group',
                            choices={'Groups': {'all': 'All'}},
                            selected='All'),
            ui.input_checkbox(id="tags_graph_remove_cycles", label='Remove cycles', value=True),
        ),
        ui.page_fluid(
            ui.navset_tab(
                ui.nav_panel("Dependency Graph",
                             ui.input_slider(id="tags_graph_k_slider", label='K spring value', min=.05, max=5, value=.5,
                                             step=.05),
                             ui.input_checkbox(id="tags_graph_levels", label='Display levels', value=False),
                             output_widget(id="tags_graph"),
                             ),
                ui.nav_panel("Hierarchy Graph",

                             )
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def title_output():
        return f"Tags: {len(all_tags)}"

    @reactive.calc
    def calc_tags_graph():
        tags_group_key = input.tags_graph_select()
        tg = TagGraph.from_tags(all_tags)
        if input.tags_graph_remove_cycles():
            tg = TagGraph.from_tags(all_tags).remove_cycles()

        ui.update_checkbox('tags_graph_levels', value=not tg.has_cycles())
        return tg

    @render_widget
    def tags_graph() -> object:
        logging.info('tags_graph')
        tg = calc_tags_graph()

        if input.tags_graph_remove_cycles():
            tg = TagGraph.from_tags(all_tags).remove_cycles()

        return tg.create_plotly_graph(
            k=input.tags_graph_k_slider(),
            levels_col='level' if input.tags_graph_levels() else None,
        )


tags_info_app = App(app_ui, server)
