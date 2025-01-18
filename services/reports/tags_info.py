# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, render, ui
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

    ui.page_fluid(
        ui.navset_tab(
            ui.nav_panel("Tags Graph",
                            ui.input_select(id="tags_graph_select", label='Tags group',
                                            choices={'Groups': {'all': 'All'}},
                                            selected='All'),
                            ui.input_slider(id="tags_graph_k_slider", label='K spring value', min=.05, max=5, value=.5, step=.05),
                            output_widget(id="tags_graph"),
                         ),

        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def title_output():
        return f"Tags: {len(all_tags)}"

    # @u

    @render_widget
    def tags_graph() -> object:
        logging.info('tags_graph')
        tg = TagGraph.from_tags(all_tags)
        return tg.create_plotly_graph(k=input.tags_graph_k_slider())


tags_info_app = App(app_ui, server)
