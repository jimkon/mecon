import logging

import pandas as pd
from htmltools import HTML
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon import config
from mecon.app.current_data import WorkingDataManager, WorkingDatasetDir
from mecon.tags import tagging

# from mecon.monitoring.logs import setup_logging
# setup_logging()

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

data_manager = WorkingDataManager()
all_tags = data_manager.all_tags()

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
        ui.h2(ui.output_text(id='menu_title_text')),
        ui.output_data_frame(id='menu_tags_table'),
    )
)



def tag_actions(tag_name):
    return HTML(
        f"""
        <a href="http://127.0.0.1:8001/reports/tags/?filter_in_tags={tag_name}" target="_blank">Info</a>
        &nbsp;|&nbsp;
        <a href="http://127.0.0.1:8002/edit_data/tags/edit/?filter_in_tags={tag_name}" target="_blank">Edit</a>
        """
    )


def server(input: Inputs, output: Outputs, session: Session):
    all_tags_reactive = reactive.Value(data_manager.all_tags())
    tags_metadata_reactive = reactive.Value(value=data_manager.get_tags_metadata().copy())


    @render.text
    def menu_title_text():
        return f"Tag menu: {len(all_tags_reactive.get())} tags"


    @render.data_frame
    def menu_tags_table():
        tags = data_manager.all_tags()
        tags_df = pd.DataFrame([tag.name for tag in tags], columns=['Name'])

        tag_stats_df = tags_metadata_reactive.get()
        tag_stats_df.columns = [col.capitalize().replace('_', ' ') for col in tag_stats_df.columns]
        tag_stats_df = tags_df.merge(tag_stats_df, on='Name', how='left')

        tag_stats_df['i'] = tag_stats_df.index
        tag_stats_df['Actions'] = tag_stats_df['Name'].apply(lambda tag_name: tag_actions(tag_name))

        tag_stats_df['Total money in'] = tag_stats_df['Total money in'].apply(lambda x: f"£ {float(x):.2f}")
        tag_stats_df['Total money out'] = tag_stats_df['Total money out'].apply(lambda x: f"£ {float(x):.2f}")


        # tags_df['Actions'] = tags_df['Name'].apply(lambda tag_name: tag_actions(tag_name))
        # tag_stats_df = tags_df.merge(tag_stats_df, on='Name', how='left')

        cols_to_show = ['i',
                        'Name',
                        'Count',
                        'Total money in',
                        'Total money out',
                        'Date created',
                        'Date modified',
                        'Actions']
        return tag_stats_df[cols_to_show]

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
        all_tags_reactive.set(data_manager.all_tags())
        tags_metadata_reactive.set(value=data_manager.get_tags_metadata().copy())
        ui.modal_remove()
        # load_menu()

    @reactive.effect
    @reactive.event(input.delete_button)
    def _():
        m = ui.modal(
            ui.input_select(id='name_of_tag_to_delete_select', label='New tag name',
                            choices=sorted([tag.name for tag in all_tags_reactive.get()])),
            title=f"Delete a new tag",
            easy_close=False,
            footer=ui.input_task_button(id='confirm_delete_button',
                                        label='DELETE',
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
        all_tags_reactive.set(data_manager.all_tags())
        tags_metadata_reactive.set(value=data_manager.get_tags_metadata().copy())
        ui.modal_remove()
        # load_menu()

    @reactive.effect
    @reactive.event(input.recalculate_button)
    def _():
        logging.info(f"Recalculating all tags.")
        data_manager.reset_transaction_tags()
        all_tags_reactive.set(data_manager.all_tags())
        tags_metadata_reactive.set(value=data_manager.get_tags_metadata().copy())


menu_tags_app = App(app_ui, server)
