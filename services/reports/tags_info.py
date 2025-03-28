import logging

from shiny import App, Inputs, Outputs, Session, render, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon.app import shiny_app
from mecon.tags.rule_graphs import TagGraph, AcyclicTagGraph

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# TODO all these will not refresh if i make any change to the data. data manager is not reseting
dataset = shiny_app.get_working_dataset()
data_manager = shiny_app.create_data_manager()
all_tags = data_manager.all_tags()

tag_graph_all = TagGraph.from_tags(all_tags).remove_cycles()
tag_roots = tag_graph_all.find_all_root_tags()
tag_root_groups = {f"root:{tag.name}": f"{tag.name} ({len(tag_graph_all.all_tags_affected_by(tag))})" for tag in tag_roots}
# TODO numbers in tag_root_groups are wrong sometimes
# TODO could sort from largest to smallest

app_ui = shiny_app.app_ui_factory(
    ui.h5(ui.output_text('title_output')),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select(id="tags_graph_select", label='Tags group',
                            choices={
                                'Groups': {'group:all': 'All'},
                                'Roots': tag_root_groups,
                            },
                            selected='root:All'),
        ),
        ui.navset_tab(
            ui.nav_panel("Dependency Graph",
                         # ui.input_slider(id="tags_graph_k_slider", label='K spring value', min=.05, max=5, value=.5,
                         #                 step=.05),
                         ui.input_checkbox(id="tags_graph_levels", label='Display levels', value=True),
                         output_widget(id="tags_graph"),
                         ),
            ui.nav_panel("Table",
                                      ui.output_data_frame('selected_tags_df')
                         ),
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()

    @render.text
    def title_output():
        return f"All tags: {len(all_tags)}"

    @reactive.calc
    def calc_tags_graph():
        category, tags_group_key = input.tags_graph_select().split(':')
        logging.info(f"calc_tags_graph: {category=}, {tags_group_key=}")
        if category == 'group':
            tg = TagGraph.from_tags(all_tags)
            logging.info(f"calc_tags_graph -) {tg.has_cycles()=}")
            if not tg.has_cycles():
                tg = tg.remove_cycles()
                tg.add_hierarchy_levels()
        elif category == 'root':
            tg_all = TagGraph.from_tags(all_tags).remove_cycles()
            subgraph_tags = tg_all.all_tags_affected_by(tags_group_key)
            logging.info(f"calc_tags_graph -) subgraphs for {tags_group_key} = {[tag for tag in subgraph_tags]=}")
            tg = AcyclicTagGraph.from_tags(subgraph_tags)
            tg.add_hierarchy_levels()
        else:
            raise ValueError(f"Invalid category: {category}")

        logging.info(f"calc_tags_graph-> {len(tg.tags_df)=},{tg.has_cycles()=}")
        return tg

    @render.data_frame
    def selected_tags_df() -> object:
        return calc_tags_graph().tidy_table()

    @render_widget
    def tags_graph() -> object:
        tg = calc_tags_graph()
        logging.info(f"tags_graph: {tg.tags=}")

        return tg.create_plotly_graph(
            k=.5,  # input.tags_graph_k_slider(),
            levels_col='level' if input.tags_graph_levels() else None,
        )


tags_info_app = App(app_ui, server)
