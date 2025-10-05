import logging

import pandas as pd
from htmltools import HTML
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

from mecon.app import shiny_app
from mecon.app.current_data import WorkingDataManager
from mecon.tags import tagging

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


app_ui = shiny_app.app_ui_factory(
    ui.page_fluid(
        ui.input_task_button(id='create_button', label='Create new tag'),
        ui.input_task_button(id='recalculate_button', label='Recalculate all tags', label_busy='Recalculating...'),
        ui.input_task_button(id='delete_button', label='Delete a tag', type='warning'),
        ui.tags.a("Manual tagging", href=f"http://127.0.0.1:8002/edit_data/tags/manual/"),
        ui.h2(ui.output_text(id='menu_title_text')),
        ui.output_data_frame(id='menu_tags_table'),
    )
)



def tag_actions(tag_name, edit_enabled=True):
    tag_info_href = f"""<a href="{shiny_app.url_for_tag_report(filter_in_tags=tag_name)}" target="_blank">Info</a>"""

    if edit_enabled:
        tag_edit_href = f"""<a href="{shiny_app.url_for_tag_edit(filter_in_tags=tag_name)}" target="_blank">Edit</a>"""
    else:
        tag_edit_href = f"""Editing disabled"""

    return HTML(
        f"""
        {tag_info_href}
        &nbsp;|&nbsp;
        {tag_edit_href}
        """
    )


def build_tags_table(data_manager):
    tags_df = data_manager.all_tags_df
    tag_stats_df = data_manager.get_tags_metadata()
    tag_merged_info_df = tags_df.merge(tag_stats_df, on=['name', 'type', 'date_created'], how='left')
    tag_merged_info_df.columns = [col.capitalize().replace('_', ' ') for col in tag_merged_info_df.columns]
    tag_merged_info_df.sort_values(by=['Name'], ascending=True, inplace=True)

    tag_merged_info_df['i'] = list(range(len(tag_merged_info_df)))
    tag_merged_info_df['Actions'] = tag_merged_info_df.apply(
        lambda row: tag_actions(row['Name'], edit_enabled=row['Type'] == 'Custom'), axis=1)

    tag_merged_info_df['Total money in'] = tag_merged_info_df['Total money in'].apply(lambda x: f"£ {float(x):.2f}")
    tag_merged_info_df['Total money out'] = tag_merged_info_df['Total money out'].apply(lambda x: f"£ {float(x):.2f}")

    cols_to_show = ['i',
                    'Name',
                    'Count',
                    'Total money in',
                    'Total money out',
                    'Date created',
                    'Date modified',
                    'Type',
                    'Actions']
    return tag_merged_info_df[cols_to_show]


def create_tag_creation_modal():
    return ui.modal(
        ui.input_text(id='name_of_new_tag_text', label='New tag name'),
        title="Create a new tag",
        easy_close=True,
        footer=ui.input_task_button(id='confirm_create_button', label='Confirm', label_buzy='Creating...'),
        size='l'
    )


def create_tag_deletion_modal(all_tags):
    return ui.modal(
        ui.input_select(id='name_of_tag_to_delete_select', label='New tag name',
                        choices=sorted([tag.name for tag in all_tags])),
        title="Delete a new tag",
        easy_close=True,
        footer=ui.input_task_button(id='confirm_delete_button',
                                    label='DELETE',
                                    label_buzy='Deleting...',
                                    type='danger'),
        size='l'
    )


def create_tag_from_name(tag_name: str):
    return tagging.Tag.from_json_string(tag_name, '{}')


def refresh_tag_reactives(data_manager, all_tags_reactive, tags_metadata_reactive):
    all_tags_reactive.set(data_manager.all_tags())
    tags_metadata_reactive.set(value=data_manager.get_tags_metadata().copy())


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = WorkingDataManager()

    all_tags_reactive = reactive.Value(data_manager.all_tags())
    tags_metadata_reactive = reactive.Value(value=data_manager.get_tags_metadata().copy())


    @render.text
    def menu_title_text():
        return f"Tag menu: {len(all_tags_reactive.get())} tags"


    @render.data_frame
    def menu_tags_table():
        return build_tags_table(data_manager)

    @reactive.effect
    @reactive.event(input.create_button)
    def _():
        ui.modal_show(create_tag_creation_modal())

    @reactive.effect
    @reactive.event(input.confirm_create_button)
    def _():
        logging.info(f"Creating new tag.")
        new_tag = create_tag_from_name(input.name_of_new_tag_text())
        data_manager.update_tag(new_tag, update_tags=False)
        refresh_tag_reactives(data_manager, all_tags_reactive, tags_metadata_reactive)
        ui.modal_remove()
        # load_menu()

    @reactive.effect
    @reactive.event(input.delete_button)
    def _():
        ui.modal_show(create_tag_deletion_modal(all_tags_reactive.get()))

    @reactive.effect
    @reactive.event(input.confirm_delete_button)
    def _():
        logging.info(f"Deleting tag {input.name_of_tag_to_delete_select()}")
        data_manager.delete_tag(input.name_of_tag_to_delete_select())
        refresh_tag_reactives(data_manager, all_tags_reactive, tags_metadata_reactive)
        ui.modal_remove()
        # load_menu()

    @reactive.effect
    @reactive.event(input.recalculate_button)
    def _():
        logging.info(f"Recalculating all tags.")
        data_manager.reset_transaction_tags()
        refresh_tag_reactives(data_manager, all_tags_reactive, tags_metadata_reactive)


menu_tags_app = App(app_ui, server)
