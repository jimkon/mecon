import logging
import pathlib

import shinywidgets
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app.datasets import WorkingDataManager
from mecon.settings import Settings
from mecon.tags import tagging

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = pathlib.Path(__file__).parent.parent.parent / 'datasets'
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

settings = Settings()
settings['DATASETS_DIR'] = str(datasets_dir)


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

    ui.page_fluid(
        ui.input_task_button(id='create_button', label='Create new tag'),
        ui.input_task_button(id='recalculate_button', label='Recalculate all tags', label_busy='Recalculating...'),
        ui.input_task_button(id='delete_button', label='Delete a tag', type='warning'),
        ui.h1("Tags menu"),
        ui.layout_column_wrap(
            ui.h3(ui.output_text(id='menu_title_text')),
            width=1 / 10,
        ),
    )
)


# def tag_to_ui(tag: tagging.Tag):
#     res_ui = ui.card(
#         ui.tags.a(f"{tag.name}", href=f"http://127.0.0.1:8002/edit_data/tags/edit/?tag_name={tag.name}")
#     )
#     return res_ui

def tag_to_ui(tag: tagging.Tag):
    _id = f"{tag.name.replace(' ', '_')}_value_box"
    res_ui = ui.value_box(
        ui.tags.a(f"{tag.name}", href=f"http://127.0.0.1:8002/edit_data/tags/edit/?filter_in_tags={tag.name}"),
        "[# transactions tagged] [total money in] [total money out] [date created]",
        showcase=shinywidgets.output_widget(_id + "_sparkline"),
        showcase_layout="bottom",
        theme='bg-green',
        # fill=False,
        id=_id,
    )
    return res_ui


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = WorkingDataManager()
    all_tags = data_manager.all_tags()

    @render.text
    def menu_title_text():
        return f"All tags: {len(all_tags)}"

    @reactive.effect
    def load():
        load_menu()

    def load_menu():
        logging.info(f"Loading menu.")
        for tag in all_tags:
            ui.remove_ui(
                selector=f"#{tag.name.replace(' ', '_')}_value_box",
            )
            ui.insert_ui(
                ui=tag_to_ui(tag),
                selector="#menu_title_text",
                where="afterEnd",
            )

    @reactive.effect
    @reactive.event(input.create_button)
    def _():
        m = ui.modal(
            ui.input_text(id='name_of_new_tag_text', label='New tag name'),
            title=f"Create a new tag",
            easy_close=False,
            footer=ui.input_task_button(id='confirm_create_button', label='Confirm', label_buzy='Creating...'),
            size='l'
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.confirm_create_button)
    def _():
        logging.info(f"Creating new tag.")
        new_tag = tagging.Tag.from_json_string(input.name_of_new_tag_text(), '{}')
        data_manager.update_tag(new_tag, update_tags=False)
        ui.modal_remove()
        load_menu()

    @reactive.effect
    @reactive.event(input.delete_button)
    def _():
        m = ui.modal(
            ui.input_select(id='name_of_tag_to_delete_select', label='New tag name',
                            choices={tag.name: tag.name for tag in all_tags}),
            title=f"Delete a new tag",
            easy_close=False,
            footer=ui.input_task_button(id='confirm_delete_button',
                                        label='Confirm',
                                        label_buzy='Deleting...',
                                        type='danger'),
            size='l'
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.confirm_delete_button)
    def _():
        logging.info(f"Deleting tag {input.name_of_tag_to_delete_select()}")
        data_manager.delete_tag(input.name_of_tag_to_delete_select())
        ui.modal_remove()
        load_menu()

    @reactive.effect
    @reactive.event(input.recalculate_button)
    def _():
        logging.info(f"Recalculating all tags.")
        data_manager.reset_transaction_tags()


menu_tags_app = App(app_ui, server)
