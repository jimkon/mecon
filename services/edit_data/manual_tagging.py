import logging
import logging
import pathlib

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

import utils
from mecon.app.datasets import WorkingDataManager
from mecon.data.transactions import Transactions
from mecon.settings import Settings
from mecon.data import groupings

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

datasets_dir = pathlib.Path(__file__).parent.parent.parent / 'datasets'
if not datasets_dir.exists():
    raise ValueError(f"Unable to locate Datasets directory: {datasets_dir} does not exists")

settings = Settings()
settings['DATASETS_DIR'] = str(datasets_dir)



change_tracker = []
added_tags, removed_tags = {}, {}

shown_transactions = None

DEFAULT_TIME_UNIT = 'month'
PAGE_SIZE = 100

app_ui = ui.page_fluid(
    ui.tags.title("μEcon"),
    ui.navset_pill(
        ui.nav_control(ui.tags.a("Main page", href=f"http://127.0.0.1:8000/")),
        ui.nav_control(ui.tags.a("Reports", href=f"http://127.0.0.1:8001/")),
        ui.nav_control(ui.tags.a("Edit data", href=f"http://127.0.0.1:8002/edit_data/tags/")),
        ui.nav_control(ui.tags.a("Monitoring", href=f"http://127.0.0.1:8003/")),
        ui.nav_control(ui.input_dark_mode(id="light_mode")),
    ),
    ui.hr(),

    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select(
                id='transaction_order_select',
                label='Order by:',
                choices=['Newest transactions', 'Least tagged'],
                selected='Newest'
            ),
            ui.card(
                # ui.input_select(
                #     id='page_group_select',
                #     label='Groups:',
                #     choices=['100 transactions', '7 days', '30 days'],
                #     selected='100 transactions'
                # ),
                ui.input_select(
                    id='page_number_select',
                    label='Page number:',
                    choices={'0': '0'},
                    selected='0'
                ),
            ),
            ui.input_selectize(
                id='input_tags_select',
                label='Select tags',
                choices=[],
                selected=None,
                multiple=True
            ),
            ui.input_action_button(
                id='review_and_save_button',
                label='Review and save changes...',
            )
        ),
        ui.page_fluid(
            ui.output_text(id='transactions_header_text'),

        ),
    )
)


def new_transaction_row(transaction, all_tags):
    amount = transaction['amount']
    amount_str = f"{'⮝' if amount < 0 else '⮟'} {amount} GBP   " + \
                 (f"({transaction['amount_cur']} {transaction['currency']})" if transaction[
                                                                                    'currency'] != 'GBP' else '')
    _dt = transaction['datetime'].to_pydatetime()
    date_str, time = _dt.date().strftime('%a %d %B, %Y'), _dt.time()
    res_ui = ui.card(
        ui.row(
            ui.column(4, ui.h4(amount_str, style=f"color:{'red' if transaction['amount'] < 0 else 'green'}")),
            ui.column(6, ui.h3(f"📅{date_str} - 🕑{time}")),
            ui.column(2, ui.h6(transaction['id']), height='5px', style=f"background-color: grey")),
        ui.row(ui.column(12, ui.h4(ui.card(transaction['description'])))),
        ui.card_footer(ui.row(ui.h2(ui.input_selectize(id=f"tags_{transaction['id']}", label='Tags', multiple=True,
                                                       choices=[tag.name for tag in all_tags],
                                                       selected=transaction['tags'].split(','), width='100%')))),
        # max_height='10%',
        style="border-color: grey", id=transaction['id']
    )
    change_tracker.clear()
    return res_ui


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = WorkingDataManager()
    all_tags = data_manager.all_tags()

    transactions = data_manager.get_transactions()

    @reactive.effect
    def load():
        ui.update_selectize(
            id='input_tags_select',
            choices=sorted([tag.name for tag in all_tags])
        )

    @reactive.calc
    def tag_filtered_transactions() -> Transactions:
        logging.info(
            f"Filtering transactions, order: UNKNOWN, tags subset: {','.join(input.input_tags_select())}, page: , window: ")
        _tag_filtered_transactions = transactions.containing_tag(input.input_tags_select())
        return _tag_filtered_transactions

    @reactive.calc
    def filtered_transactions_df() -> pd.DataFrame:
        trans = tag_filtered_transactions()

        transactions_dfs = utils.sort_and_filter_transactions_df(trans,
                                                                 input.transaction_order_select(),
                                                                 int(input.page_number_select()),
                                                                 PAGE_SIZE)

        remove_transaction_rows()
        global shown_transactions
        shown_transactions = transactions_dfs

        for i, transaction in transactions_dfs.iterrows():
            ui.insert_ui(
                ui=new_transaction_row(transaction, all_tags),
                selector='#transactions_header_text',
                where="beforeEnd",
            )

        def selectize_change_handler(selectize_id):
            change_tracker.append(selectize_id)

        def create_selectize_change_handler_closure(id):
            reactive.effect(reactive.event(getattr(input, f"tags_{id}"))(lambda: selectize_change_handler(id)))

        for transaction_id in transactions_dfs['id']:
            create_selectize_change_handler_closure(transaction_id)

        logging.info(f"Filtering Done")

        return transactions_dfs

    def remove_transaction_rows():
        global shown_transactions
        if shown_transactions is None:
            return

        for _id in shown_transactions['id']:
            ui.remove_ui(selector=f"#{_id}")

    @reactive.effect
    @reactive.event(input.transaction_order_select)
    def _():
        trans = tag_filtered_transactions()
        if input.transaction_order_select() == 'Newest transactions':
            groups = trans.group(groupings.WEEK)
            label = 'Choose week'
        elif input.transaction_order_select() == 'Least tagged':
            groups = trans.group(groupings.IndexGrouping.equal_size_groups(PAGE_SIZE,
                                                                           trans.size()))
            label = f"Choose page (size: {PAGE_SIZE})"
        else:
            raise ValueError(f"Invalid ordering: {input.transaction_order_select()}")

        ranges = [(str(week.date.min()), str(week.date.max())) for week in groups]
        range_strings = {str(i): f"{i} ({rng[0], rng[1]})" for i, rng in enumerate(ranges)}
        ui.update_select(id='page_number_select', label=label, choices=range_strings)

    @reactive.effect
    @reactive.event(input.page_number_select)
    def _():
        logging.info(f"Changing page...")
        remove_transaction_rows()


    @render.text
    def transactions_header_text():
        return f"Transactions: {len(filtered_transactions_df())}," \
               f" page={input.page_number_select()}," \
               f" page_size={input.page_group_select()}"

    def fetch_changes():
        current_transactions_df = filtered_transactions_df()
        changed_transaction_ids = set(change_tracker[
                                      len(current_transactions_df):])  # filtering out the first len(listed transactions) because they get added on init
        old_changed_transactions_df = current_transactions_df[
            current_transactions_df['id'].isin(changed_transaction_ids)]
        new_tags = {_id: set(getattr(input, f"tags_{_id}")()) for _id in changed_transaction_ids}
        old_tags = old_changed_transactions_df[['id', 'tags']].set_index('id').to_dict('index')
        old_tags = {_id: set(tags['tags'].split(',')) for _id, tags in old_tags.items()}
        added_tags = {_id: new_tags[_id].difference(old_tags[_id]) for _id in changed_transaction_ids if
                      len(new_tags[_id].difference(old_tags[_id]))}
        removed_tags = {_id: old_tags[_id].difference(new_tags[_id]) for _id in changed_transaction_ids if
                        len(old_tags[_id].difference(new_tags[_id]))}
        return added_tags, removed_tags

    @reactive.effect
    @reactive.event(input.review_and_save_button)
    def _():
        global added_tags, removed_tags
        added_tags, removed_tags = fetch_changes()
        added_message = '\n'.join(
            [' * <span style="color:green">' + f"{', '.join(tags)} added to transaction \'{tid}\'</span>" for tid, tags
             in added_tags.items()])
        removed_message = '\n'.join(
            [' * <span style="color:red">' + f"{', '.join(tags)} removed from transaction \'{tid}\'</span>" for
             tid, tags in removed_tags.items()])

        m = ui.modal(
            ui.markdown(f'{added_message}\n'
                        f'{removed_message}'),  # , style=f"color: green"),
            # ui.markdown(removed_message),#, style=f"color: red"),
            title="Review changes before saving",
            easy_close=True,
            size='xl',
            footer=ui.input_action_button(id='save_button', label='Save'),
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.save_button)
    def save_changes():
        global added_tags, removed_tags
        utils.save_tag_changes(added_tags,
                               removed_tags,
                               data_manager)


manual_tagging_app = App(app_ui, server)
