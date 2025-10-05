import logging

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

import utils
from mecon.app import shiny_app
from mecon.data import groupings
from mecon.data.transactions import Transactions

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


change_tracker = []
added_tags, removed_tags = {}, {}

shown_transactions = None

DEFAULT_TIME_UNIT = 'month'
PAGE_SIZE = 100


app_ui = shiny_app.app_ui_factory(
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


def build_tag_choices(all_tags):
    return sorted([tag.name for tag in all_tags])


def filter_transactions_by_selected_tags(transactions: Transactions, selected_tags) -> Transactions:
    return transactions.containing_tags(selected_tags)


def paginate_transactions(transactions: Transactions, order_option: str, page_number: int, page_size: int):
    return utils.sort_and_filter_transactions_df(transactions, order_option, page_number, page_size)


def build_page_choices(transactions: Transactions, order_option: str, page_size: int):
    if order_option == 'Newest transactions':
        groups = transactions.group(groupings.WEEK)
        label = 'Choose week'
        ranges = [(str(week.date.min()), str(week.date.max())) for week in groups]
    elif order_option == 'Least tagged':
        groups = transactions.group(groupings.IndexGrouping.equal_size_groups(page_size, transactions.size()))
        label = f"Choose page (size: {page_size})"
        ranges = [(str(group.date.min()), str(group.date.max())) for group in groups]
    else:
        raise ValueError(f"Invalid ordering: {order_option}")

    range_strings = {str(i): f"{i} ({rng[0]}, {rng[1]})" for i, rng in enumerate(ranges)}
    return label, range_strings


def clear_rendered_transactions(_shown_transactions):
    if _shown_transactions is None:
        return
    for _id in _shown_transactions['id']:
        ui.remove_ui(selector=f"#{_id}")


def register_selectize_change_handler(input_obj, transaction_id):
    def on_change():
        change_tracker.append(transaction_id)

    reactive.effect(reactive.event(getattr(input_obj, f"tags_{transaction_id}"))(on_change))


def determine_tag_changes(current_transactions_df, input_obj):
    changed_transaction_ids = set(change_tracker[len(current_transactions_df):])
    old_changed_transactions_df = current_transactions_df[
        current_transactions_df['id'].isin(changed_transaction_ids)]
    new_tags = {_id: set(getattr(input_obj, f"tags_{_id}")()) for _id in changed_transaction_ids}
    old_tags = old_changed_transactions_df[['id', 'tags']].set_index('id').to_dict('index')
    old_tags = {_id: set(tags['tags'].split(',')) for _id, tags in old_tags.items()}
    added = {_id: new_tags[_id].difference(old_tags[_id]) for _id in changed_transaction_ids if
             len(new_tags[_id].difference(old_tags[_id]))}
    removed = {_id: old_tags[_id].difference(new_tags[_id]) for _id in changed_transaction_ids if
               len(old_tags[_id].difference(new_tags[_id]))}
    return added, removed


def build_review_changes_modal(added, removed):
    added_message = '\n'.join(
        [' * <span style="color:green">' + f"{', '.join(tags)} added to transaction \'{tid}\'</span>" for tid, tags in added.items()])
    removed_message = '\n'.join(
        [' * <span style="color:red">' + f"{', '.join(tags)} removed from transaction \'{tid}\'</span>" for tid, tags in removed.items()])

    return ui.modal(
        ui.markdown(f'{added_message}\n{removed_message}'),
        title="Review changes before saving",
        easy_close=True,
        size='xl',
        footer=ui.input_action_button(id='save_button', label='Save'),
    )


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()
    all_tags = data_manager.all_tags()

    transactions = data_manager.get_transactions()

    @reactive.effect
    def load():
        ui.update_selectize(
            id='input_tags_select',
            choices=build_tag_choices(all_tags)
        )

    @reactive.calc
    def tag_filtered_transactions() -> Transactions:
        logging.info(
            f"Filtering transactions, order: UNKNOWN, tags subset: {','.join(input.input_tags_select())}, page: , window: ")
        return filter_transactions_by_selected_tags(transactions, input.input_tags_select())

    @reactive.calc
    def filtered_transactions_df() -> pd.DataFrame:
        trans = tag_filtered_transactions()

        transactions_dfs = paginate_transactions(
            trans,
            input.transaction_order_select(),
            int(input.page_number_select()),
            PAGE_SIZE,
        )

        global shown_transactions
        clear_rendered_transactions(shown_transactions)
        shown_transactions = transactions_dfs

        for i, transaction in transactions_dfs.iterrows():
            ui.insert_ui(
                ui=new_transaction_row(transaction, all_tags),
                selector='#transactions_header_text',
                where="beforeEnd",
            )

        for transaction_id in transactions_dfs['id']:
            register_selectize_change_handler(input, transaction_id)

        logging.info(f"Filtering Done")

        return transactions_dfs

    def remove_transaction_rows():
        global shown_transactions
        clear_rendered_transactions(shown_transactions)
        shown_transactions = None

    @reactive.effect
    @reactive.event(input.transaction_order_select)
    def _():
        label, choices = build_page_choices(tag_filtered_transactions(), input.transaction_order_select(), PAGE_SIZE)
        ui.update_select(id='page_number_select', label=label, choices=choices)

    @reactive.effect
    @reactive.event(input.page_number_select)
    def _():
        logging.info(f"Changing page...")
        remove_transaction_rows()


    @render.text
    def transactions_header_text():
        return f"Transactions: {len(filtered_transactions_df())}," \
               f" page={input.page_number_select()}," \
               f" page_size={PAGE_SIZE}"

    @reactive.effect
    @reactive.event(input.review_and_save_button)
    def _():
        global added_tags, removed_tags
        added_tags, removed_tags = determine_tag_changes(filtered_transactions_df(), input)
        ui.modal_show(build_review_changes_modal(added_tags, removed_tags))

    @reactive.effect
    @reactive.event(input.save_button)
    def save_changes():
        global added_tags, removed_tags
        utils.save_tag_changes(added_tags,
                               removed_tags,
                               data_manager)


manual_tagging_app = App(app_ui, server)
