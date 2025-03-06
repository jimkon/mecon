# setup_logging()
import logging

from shiny import App, Inputs, Outputs, Session, ui, reactive
from shinywidgets import output_widget, render_widget

from mecon.app import shiny_app
from mecon.data import graphs

# from mecon.monitoring.logs import setup_logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

data_manager = shiny_app.create_data_manager()

# DEFAULT_PERIOD = 'Last year'
# DEFAULT_TIME_UNIT = 'month'

app_ui = shiny_app.app_ui_factory(
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filted_factory()
        ),
        ui.page_fluid(
            ui.input_selectize(
                id='compare_tags_select',
                label='Select tags to show',
                choices=sorted([tag.name for tag in data_manager.all_tags()]),  # sorted([tag_name for tag_name, cnt in all_transactions.all_tag_counts().items() if cnt > 0]),
                selected=None,
                multiple=True
            ),
            ui.navset_tab(
                ui.nav_panel("Timelines",
                             output_widget(id="timelines"),
                             ),
                ui.nav_panel("Histograms",
                             output_widget(id="histograms"),
                             ),
                ui.nav_panel("Balance",
                             output_widget(id="balances"),
                             ),
            )
        )
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()

    url_params = shiny_app.url_params_function_factory(
        input,
        output,
        session,
        data_manager)

    (get_filter_params,
     default_transactions,
     init,
     filtered_transactions) = shiny_app.filter_funcs_factory(
        input,
        output,
        session,
        data_manager)

    @reactive.calc
    def all_ungrouped_transactions():
        start_date, end_date, time_unit, filter_in_tags, filter_out_tags = get_filter_params()
        compare_tags = input.compare_tags_select() if len(input.compare_tags_select()) > 0 else ['All']
        logging.info(f"Calculating all transactions for {compare_tags}...")
        transactions = filtered_transactions()

        all_trans = {}
        for tag in compare_tags:
            trans = transactions.containing_tags(tag)

            logging.info(f"Ungrouped transactions for {tag}: {trans.size()}, date range {trans.date_range()}")
            if trans.size() == 0:
                raise ValueError(
                    f"Transactions for {tag} is 0 for filter params=({start_date=}, {end_date=}, {filter_in_tags=}, {filter_out_tags=})")

            all_trans[tag] = trans

        logging.info(f"Calculating all transactions... {len(all_trans)}")
        return all_trans

    def all_synced_and_grouped_transactions():
        all_trans = all_ungrouped_transactions()
        min_date = min([trans.datetime.min() for tags, trans in all_trans.items()])
        max_date = max([trans.datetime.max() for tags, trans in all_trans.items()])
        compare_tags = input.compare_tags_select() if len(input.compare_tags_select()) > 0 else ['All']
        logging.info(f"Calculating all transactions from {min_date} to {max_date} for {compare_tags} and {input.time_unit_select()}")

        synced_trans = {}
        for tag, trans in all_trans.items():
            grouped_trans = trans.group_and_fill_transactions(
                grouping_key=input.time_unit_select(),
                aggregation_key='sum',
                fill_dates_after_groupagg=True,
            )
            filled_trans = grouped_trans.fill_values(fill_unit=input.time_unit_select(), start_date=min_date,
                                                     end_date=max_date)
            logging.info(f"Filtered transactions for {tag}: {filled_trans.size()}, date range {filled_trans.date_range()}")

            synced_trans[tag] = filled_trans

        return synced_trans

    @render_widget
    def timelines():
        logging.info('timelines')

        if input.time_unit_select() == 'none':
            raise ValueError(f"This plot doesn't work for 'none' time unit")

        synced_trans = all_synced_and_grouped_transactions()

        plot = graphs.multiple_lines_graph_html(
            times=[trans.datetime for tags, trans in synced_trans.items()],
            lines=[trans.amount for tags, trans in synced_trans.items()],
            names=list(synced_trans.keys()),
            stacked=False
        )
        return plot

    @render_widget
    def histograms():
        logging.info('histograms')
        all_trans = all_ungrouped_transactions()

        grouped_trans = [trans.group_and_fill_transactions(
            grouping_key=input.time_unit_select(),
            aggregation_key='sum',
        ) for tag, trans in all_trans.items()]

        plot = graphs.multiple_histograms_graph_html(
            amounts=[trans.amount for trans in grouped_trans],
            names=list(all_trans.keys())
        )

        return plot

    @render_widget
    def balances():
        logging.info('balances')

        if input.time_unit_select() == 'none':
            raise ValueError(f"This plot doesn't work for 'none' time unit")

        synced_trans = all_synced_and_grouped_transactions()

        plot = graphs.stacked_bars_graph_html(
            times=[trans.datetime for tag, trans in synced_trans.items()],
            lines=[trans.amount.cumsum() for tag, trans in synced_trans.items()],
            names=list(synced_trans.keys()),
        )
        return plot


compare_tags_app = App(app_ui, server)
