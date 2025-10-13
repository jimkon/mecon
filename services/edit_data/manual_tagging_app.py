import logging
import re

import pandas as pd
from shiny import App, Inputs, Outputs, Session, render, ui, reactive

import utils
from mecon.app import shiny_app
from mecon.data import groupings
from mecon.data.transactions import Transactions
import mecon.utils.calendar_utils as cu

# from mecon.monitoring.logs import setup_logging
# setup_logging()

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

change_tracker = []
added_tags, removed_tags = {}, {}

shown_transactions = None

DEFAULT_TIME_UNIT = 'month'
PAGE_SIZE = 100

TRANSACTION_TABLE_COLUMN_WIDTHS = {
    'Tx_ID': '10ch',
    'amount': '14ch',
    'date': '12ch',
    'time': '10ch',
    'week_id': '16ch',
    'n_tags': '8ch',
    'select_tags': '26ch',
    'short_desc': '28ch',
}

TRANSACTION_TABLE_BASE_STYLES = [
    {
        "location": "header",
        "style": {
            "backgroundColor": "#212529",
            "color": "#f8f9fa",
            "fontWeight": "600",
            "fontSize": "13px",
            "textTransform": "uppercase",
        },
    },
    {
        "location": "body",
        "style": {
            "backgroundColor": "#f8f9fa",
            "borderBottom": "1px solid #dee2e6",
            "fontSize": "14px",
            "color": "#212529",
        },
    },
]


def build_column_width_styles(column_names: list[str], width_overrides: dict[str, str | int], *, include_header: bool = True):
    if not width_overrides:
        return []

    name_to_index = {name: idx for idx, name in enumerate(column_names)}
    styles: list[dict] = []

    for column_name, width in width_overrides.items():
        if column_name not in name_to_index or width is None:
            continue

        column_index = name_to_index[column_name]
        width_value = str(width)
        style_payload = {
            "cols": [column_index],
            "style": {
                "minWidth": width_value,
                "maxWidth": width_value,
                "whiteSpace": "nowrap",
            },
        }

        styles.append({"location": "body", **style_payload})
        if include_header:
            styles.append({"location": "header", **style_payload})

    return styles


def combine_table_styles(*style_groups: list[dict] | dict | None):
    combined: list[dict] = []
    for styles in style_groups:
        if not styles:
            continue
        if isinstance(styles, list):
            combined.extend(styles)
        else:
            combined.append(styles)
    return combined


def render_table_customised_width(
    df: pd.DataFrame,
    *,
    width: str | float | None = "100%",
    height: str | float | None = "600px",
    column_widths: dict[str, str | int] | None = None,
    base_styles: list[dict] | None = None,
):
    column_width_styles = build_column_width_styles(df.columns.tolist(), column_widths or {})
    resolved_base_styles = TRANSACTION_TABLE_BASE_STYLES if base_styles is None else base_styles
    table_styles = combine_table_styles(resolved_base_styles, column_width_styles)

    return render.DataTable(
        df,
        width=width,
        height=height,
        filters=True,
        selection_mode="none",
        styles=table_styles,
    )

app_ui = shiny_app.app_ui_factory(
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filtered_factory(),
        ),
        ui.page_fluid(
            ui.card(
                ui.card_header(ui.output_text(id='transactions_header_text')),
                ui.card_body(ui.output_data_frame(id='transactions_output_df'))
            ),
        ),
    )
)


# def _transform_id(id_str):
#     return id_str.replace('.', '[dot]').replace('-', '_')
#
#
# def new_transaction_row(transaction, all_tags):
#     amount = transaction['amount']
#     amount_str = f"{'⮝' if amount < 0 else '⮟'} {amount} GBP   " + \
#                  (f"({transaction['amount_cur']} {transaction['currency']})" if transaction[
#                                                                                     'currency'] != 'GBP' else '')
#     _dt = transaction['datetime'].to_pydatetime()
#     date_str, time = _dt.date().strftime('%a %d %B, %Y'), _dt.time()
#     res_ui = ui.card(
#         ui.row(
#             ui.column(4, ui.h4(amount_str, style=f"color:{'red' if transaction['amount'] < 0 else 'green'}")),
#             ui.column(6, ui.h3(f"📅{date_str} - 🕑{time}")),
#             ui.column(2, ui.h6(transaction['id']), height='5px', style=f"background-color: grey")),
#         ui.row(ui.column(12, ui.h4(ui.card(transaction['description'])))),
#         ui.card_footer(ui.row(ui.h2(ui.input_selectize(id=f"tags_{transaction['id']}", label='Tags', multiple=True,
#                                                        choices=[tag.name for tag in all_tags],
#                                                        selected=transaction['tags'].split(','), width='100%')))),
#         # max_height='10%',
#         style="border-color: grey", id=transaction['id']
#     )
#     change_tracker.clear()
#     return res_ui
#
#
# def build_tag_choices(all_tags):
#     return sorted([tag.name for tag in all_tags])
#
#
# def filter_transactions_by_selected_tags(transactions: Transactions, selected_tags) -> Transactions:
#     return transactions.containing_tags(selected_tags)
#
#
# def paginate_transactions(transactions: Transactions, order_option: str, page_number: int, page_size: int):
#     return utils.sort_and_filter_transactions_df(transactions, order_option, page_number, page_size)
#
#
# def build_page_choices(transactions: Transactions, order_option: str, page_size: int):
#     if order_option == 'Newest transactions':
#         groups = transactions.group(groupings.WEEK)
#         label = 'Choose week'
#         ranges = [(str(week.date.min()), str(week.date.max())) for week in groups]
#     elif order_option == 'Least tagged':
#         groups = transactions.group(groupings.IndexGrouping.equal_size_groups(page_size, transactions.size()))
#         label = f"Choose page (size: {page_size})"
#         ranges = [(str(group.date.min()), str(group.date.max())) for group in groups]
#     else:
#         raise ValueError(f"Invalid ordering: {order_option}")
#
#     range_strings = {str(i): f"{i} ({rng[0]}, {rng[1]})" for i, rng in enumerate(ranges)}
#     return label, range_strings
#
#
# def clear_rendered_transactions(_shown_transactions):
#     if _shown_transactions is None:
#         return
#     for _id in _shown_transactions['id']:
#         ui.remove_ui(selector=f"#{_id}")
#
#
# def register_selectize_change_handler(input_obj, transaction_id):
#     def on_change():
#         change_tracker.append(transaction_id)
#
#     reactive.effect(reactive.event(getattr(input_obj, f"tags_{transaction_id}"))(on_change))
#
#
# def determine_tag_changes(current_transactions_df, input_obj):
#     changed_transaction_ids = set(change_tracker[len(current_transactions_df):])
#     old_changed_transactions_df = current_transactions_df[
#         current_transactions_df['id'].isin(changed_transaction_ids)]
#     new_tags = {_id: set(getattr(input_obj, f"tags_{_id}")()) for _id in changed_transaction_ids}
#     old_tags = old_changed_transactions_df[['id', 'tags']].set_index('id').to_dict('index')
#     old_tags = {_id: set(tags['tags'].split(',')) for _id, tags in old_tags.items()}
#     added = {_id: new_tags[_id].difference(old_tags[_id]) for _id in changed_transaction_ids if
#              len(new_tags[_id].difference(old_tags[_id]))}
#     removed = {_id: old_tags[_id].difference(new_tags[_id]) for _id in changed_transaction_ids if
#                len(old_tags[_id].difference(new_tags[_id]))}
#     return added, removed
#
#
# def build_review_changes_modal(added, removed):
#     added_message = '\n'.join(
#         [' * <span style="color:green">' + f"{', '.join(tags)} added to transaction \'{tid}\'</span>" for tid, tags in
#          added.items()])
#     removed_message = '\n'.join(
#         [' * <span style="color:red">' + f"{', '.join(tags)} removed from transaction \'{tid}\'</span>" for tid, tags in
#          removed.items()])
#
#     return ui.modal(
#         ui.markdown(f'{added_message}\n{removed_message}'),
#         title="Review changes before saving",
#         easy_close=True,
#         size='xl',
#         footer=ui.input_action_button(id='save_button', label='Save'),
#     )


def construct_amount_str(transaction_series):
    transaction_dict = transaction_series.to_dict()
    is_amount_positive = transaction_dict['amount']>0
    amount_str = f"{abs(transaction_dict['amount']):.1f}"
    amount_full_info_str = f"{'⮝' if is_amount_positive else '⮟'} {amount_str} GBP   " + \
                 (f"({transaction_dict['amount_cur']} {transaction_dict['currency']})" if transaction_dict[
                                                                                    'currency'] != 'GBP' else '')

    return amount_full_info_str.strip()


def ui_id_transformation(id_str, short_str_len=5):
    id_str_short = id_str[:min(short_str_len, len(id_str)) - 1]+'...'
    res = ui.tooltip(ui.HTML(f"<label>{id_str_short}</label>"), id_str, placement='top')
    return res

def ui_description_transformation(desc_str, short_str_len=10):
    desc_str_short = desc_str[:min(short_str_len, len(desc_str)) - 1]+'...'
    res = ui.tooltip(ui.HTML(f"<label>{desc_str_short}</label>"), desc_str, placement='top')
    return res

def sanitize_input_id(raw_id: str) -> str:
    """Convert transaction identifiers into valid Shiny input ids."""

    sanitized = re.sub(r'[^0-9a-zA-Z_]', '_', str(raw_id))
    if not sanitized:
        sanitized = 'tags_input'
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def ui_tags_transformation(transaction_id: str, tags_str: str, tag_choices: list[str]):
    tags_list = [tag.strip() for tag in tags_str.split(',') if tag.strip()] if tags_str else []
    selectize_id = sanitize_input_id(f"tags_{transaction_id}")
    selectize_input = ui.input_selectize(
        id=selectize_id,
        label=None,
        choices=tag_choices,
        selected=tags_list,
        multiple=True,
        width='100%',
        remove_button=True,
        options={
            'placeholder': 'Search or add tags…',
        }
    )

    container = ui.div(
        selectize_input,
        {'class': 'transaction-tags-selectize', 'data-transaction-id': str(transaction_id)}
    )

    return container


def enhance_transactions_df(df_tx, tag_choices):
    df_ench = df_tx.copy()
    df_ench['date'] = df_tx['datetime'].apply(lambda dt: dt.date().strftime('%Y-%m-%d'))
    df_ench['time'] = df_tx['datetime'].apply(lambda dt: dt.time().strftime('%H:%M:%S'))
    df_ench['amount'] = df_tx.apply(lambda row: construct_amount_str(row), axis=1)
    df_ench['week_id'] = df_tx['datetime'].apply(lambda dt: f"{dt.date().strftime('%Y-%m-%d')}/{cu.week_of_year(dt)}")
    df_ench['n_tags'] = df_tx['tags'].apply(lambda tags: len(tags.split(',')))

    df_ench['Tx_ID'] = df_ench['id'].apply(ui_id_transformation)
    df_ench['short_desc'] = df_tx['description'].apply(ui_description_transformation)
    df_ench['select_tags'] = df_tx.apply(
        lambda row: ui_tags_transformation(row['id'], row['tags'], tag_choices),
        axis=1
    )


    cols_to_keep = ['Tx_ID', 'amount', 'date', 'time', 'week_id', 'n_tags', 'select_tags', 'short_desc']
    df_res = df_ench[cols_to_keep]
    return df_res


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()
    all_tags = data_manager.all_tags()

    transactions = data_manager.get_transactions()

    filter_url_params = shiny_app.filter_url_params_function_factory(
        input,
        output,
        session,
        data_manager)

    (get_filter_params,
     default_transactions,
     init,
     filtered_transactions_calc) = shiny_app.filter_funcs_factory(
        input,
        output,
        session,
        data_manager)

    @render.text
    def transactions_header_text():
        filtered_transactions_tx: Transactions = filtered_transactions_calc()
        filtered_transactions_df = filtered_transactions_tx.dataframe()
        start_date, end_date = filtered_transactions_tx.date_range()
        unique_tags = filtered_transactions_tx.all_tags()

        title = f"{len(filtered_transactions_df)} transactions from {start_date} to {end_date} containing {len(unique_tags)}" \
                # f" page={input.page_number_select()}," \
                # f" page_size={PAGE_SIZE}"
        return title

    @render.data_frame
    def transactions_output_df():
        tag_choices = sorted({tag.name for tag in all_tags})
        df = enhance_transactions_df(filtered_transactions_calc().dataframe(), tag_choices)
        return render_table_customised_width(
            df,
            width='100%',
            height='600px',
            column_widths=TRANSACTION_TABLE_COLUMN_WIDTHS,
        )

    # @reactive.effect
    # def load():
    #     ui.update_selectize(
    #         id='input_tags_select',
    #         choices=build_tag_choices(all_tags)
    #     )
    #
    # @reactive.calc
    # def tag_filtered_transactions() -> Transactions:
    #     logging.info(
    #         f"Filtering transactions, order: UNKNOWN, tags subset: {','.join(input.input_tags_select())}, page: , window: ")
    #     return filter_transactions_by_selected_tags(transactions, input.input_tags_select())
    #
    # @reactive.calc
    # def filtered_transactions_df() -> pd.DataFrame:
    #     # trans = tag_filtered_transactions()
    #     trans = filtered_transactions()
    #
    #     transactions_dfs = paginate_transactions(
    #         trans,
    #         input.transaction_order_select(),
    #         int(input.page_number_select()),
    #         PAGE_SIZE,
    #     )
    #     transactions_dfs['id'] = transactions_dfs['id'].apply(_transform_id)
    #
    #     global shown_transactions
    #     clear_rendered_transactions(shown_transactions)
    #     shown_transactions = transactions_dfs
    #
    #     for i, transaction in transactions_dfs.iterrows():
    #         ui.insert_ui(
    #             ui=new_transaction_row(transaction, all_tags),
    #             selector='#transactions_header_text',
    #             where="beforeEnd",
    #         )
    #
    #     for transaction_id in transactions_dfs['id']:
    #         register_selectize_change_handler(input, transaction_id)
    #
    #     logging.info(f"Filtering Done")
    #
    #     return transactions_dfs
    #
    # def remove_transaction_rows():
    #     global shown_transactions
    #     clear_rendered_transactions(shown_transactions)
    #     shown_transactions = None
    #
    # @reactive.effect
    # @reactive.event(input.transaction_order_select)
    # def _():
    #     label, choices = build_page_choices(tag_filtered_transactions(), input.transaction_order_select(), PAGE_SIZE)
    #     ui.update_select(id='page_number_select', label=label, choices=choices)
    #
    # @reactive.effect
    # @reactive.event(input.page_number_select)
    # def _():
    #     logging.info(f"Changing page...")
    #     remove_transaction_rows()
    #
    # @render.text
    # def transactions_header_text():
    #     return f"Transactions: {len(filtered_transactions_df())}," \
    #            f" page={input.page_number_select()}," \
    #            f" page_size={PAGE_SIZE}"
    #
    # @reactive.effect
    # @reactive.event(input.review_and_save_button)
    # def _():
    #     global added_tags, removed_tags
    #     added_tags, removed_tags = determine_tag_changes(filtered_transactions_df(), input)
    #     ui.modal_show(build_review_changes_modal(added_tags, removed_tags))
    #
    # @reactive.effect
    # @reactive.event(input.save_button)
    # def save_changes():
    #     global added_tags, removed_tags
    #     utils.save_tag_changes(added_tags,
    #                            removed_tags,
    #                            data_manager)


manual_tagging_app = App(app_ui, server)
