import logging
import re
from itertools import chain

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

app_ui = shiny_app.app_ui_factory(
    ui.layout_sidebar(
        ui.sidebar(
            shiny_app.transactions_intersection_filtered_factory(
                fixed_time_unit=True,
                default_time_unit='none',
            ),
        ),
        ui.page_fluid(
            ui.card(
                ui.card_header(
                    ui.output_text(id='transactions_header_text'),
                    ui.input_action_button(id='see_all_changes_button', label='See all changes')
                ),
                ui.card_body(ui.output_data_frame(id='transactions_output_df'))
            ),
        ),
    )
)

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


def build_column_width_styles(column_names: list[str], width_overrides: dict[str, str | int], *,
                              include_header: bool = True):
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

    # TODO solve bug and re-enable
    # return render.DataTable(
    #     df,
    #     width=width,
    #     height=height,
    #     filters=True,
    #     selection_mode="none",
    #     styles=table_styles,
    # )
    return df


def build_selectize_id(transaction_id: str) -> str:
    return f"selectize_{sanitize_tx_id(transaction_id)}"


def construct_amount_str(transaction_series):
    transaction_dict = transaction_series.to_dict()
    is_amount_positive = transaction_dict['amount'] > 0
    amount_str = f"{abs(transaction_dict['amount']):.1f}"
    amount_full_info_str = f"{'⮝' if is_amount_positive else '⮟'} {amount_str} GBP   " + \
                           (f"({transaction_dict['amount_cur']} {transaction_dict['currency']})" if transaction_dict[
                                                                                                        'currency'] != 'GBP' else '')

    return amount_full_info_str.strip()


def ui_id_transformation(id_str, short_str_len=5):
    id_str_short = id_str[:min(short_str_len, len(id_str)) - 1] + '...'
    res = ui.tooltip(ui.HTML(f"<label>{id_str_short}</label>"), id_str, placement='top')
    return res


def ui_description_transformation(desc_str, short_str_len=10):
    desc_str_short = desc_str[:min(short_str_len, len(desc_str)) - 1] + '...'
    res = ui.tooltip(ui.HTML(f"<label>{desc_str_short}</label>"), desc_str, placement='top')
    return res


def sanitize_tx_id(raw_tx_id: str) -> str:
    """Convert transaction identifiers into valid Shiny input ids."""
    sanitized = raw_tx_id.replace('-', '_hyphen_').replace('.', '_dot_')
    return sanitized


def desanitize_tx_id(sanitized_tx_id: str) -> str:
    """Convert sanitized identifiers into transaction ids."""
    desanitized = sanitized_tx_id.replace('_hyphen_', '-').replace('_dot_', '.')
    return desanitized


def ui_tags_transformation(transaction_id: str, current_tags: set[str], tag_choices: set[str], selectize_id: str):
    selected_tags = []
    available_tags = tag_choices.difference(current_tags)
    selectize_input = ui.input_selectize(
        id=selectize_id,
        label=None,
        choices=list(available_tags),
        selected=selected_tags,
        multiple=True,
        width='100%',
        remove_button=True,
        options={
            'placeholder': 'Add tags...',
        }
    )

    container = ui.div(
        selectize_input,
        {'class': 'transaction-tags-selectize', 'data-transaction-id': str(transaction_id)}
    )

    return container

def ui_view_tx_button(tx_row: dict, input):
    _id = f"{tx_row['selectize_id']}_view_tx_button"
    button = ui.input_action_button(
        id=_id,
        label='View',
    )

    @reactive.effect
    @reactive.event(getattr(input, _id))
    def _():
        m = ui.modal(
            tx_row['description'],
            title=f"Transaction {tx_row['id']}",
            easy_close=True,
            size='xl',
        )
        ui.modal_show(m)


    return button



def transform_tag_diffs(tag_diffs):
    tag_diffs_df = pd.DataFrame(tag_diffs)
    tag_diffs_df['id'] = tag_diffs_df['selectize_id'].apply(lambda sid: desanitize_tx_id(sid.replace('selectize_', '')))
    tags = set(chain(*tag_diffs_df['changes'].tolist()))
    dfs = []
    for tag in tags:
        tag_df = tag_diffs_df[tag_diffs_df['changes'].apply(lambda changes: tag in changes)]
        tag_df['tag'] = tag
        dfs.append(tag_df[['tag', 'id']])
    df_merged = pd.concat(dfs)

    transformed_df = df_merged.groupby('tag').agg({'id': list}).reset_index()
    transformed_dict = {k: v['id'] for k, v in transformed_df.set_index('tag').to_dict('index').items()}
    return transformed_dict


def format_changes_markdown(changes_per_tag: dict[str, list[str]]) -> str:
    if not changes_per_tag:
        return "_No pending tag changes to display._"

    header = "| Tag | Transactions |\n| --- | --- |"
    rows = []
    for tag_name in sorted(changes_per_tag.keys()):
        transaction_ids = ", ".join(
            f"`{tx_id}`" for tx_id in sorted(changes_per_tag[tag_name])
        ) or "—"
        rows.append(f"| `{tag_name}` | {transaction_ids} |")

    return "\n".join([header, *rows])


def build_tag_diffs_summary(tag_diffs):
    changes_per_tag = transform_tag_diffs(tag_diffs) if len(tag_diffs) != 0 else {}

    total_assignments = sum(len(ids) for ids in changes_per_tag.values())
    summary_line = (
        f"**Pending changes:** {total_assignments} tag assignment{'s' if total_assignments != 1 else ''}"
        f" across {len(changes_per_tag)} tag{'s' if len(changes_per_tag) != 1 else ''}."
        if changes_per_tag
        else "No pending tag changes were detected."
    )

    modal_contents = [ui.markdown(summary_line)]
    if changes_per_tag:
        modal_contents.append(ui.markdown(format_changes_markdown(changes_per_tag)))

    return modal_contents


def server(input: Inputs, output: Outputs, session: Session):
    data_manager = shiny_app.create_data_manager()
    # all_tags = data_manager.all_tags()

    custom_tags = set(data_manager.custom_tags_df['name'])

    addable_tags_set = {tag for tag in custom_tags}

    # transactions = data_manager.get_transactions()
    #
    # filter_url_params = shiny_app.filter_url_params_function_factory(
    #     input,
    #     output,
    #     session,
    #     data_manager)

    (get_filter_params,
     default_transactions,
     init,
     filtered_transactions_calc) = shiny_app.filter_funcs_factory(
        input,
        output,
        session,
        data_manager)

    def enhance_transactions_df(df_tx, all_tags: set[str]):
        logging.info(f"Enhancing transactions df with {df_tx.shape[0]} rows")
        df_ench = df_tx.copy().sort_values(by=['datetime'], ascending=False)
        df_ench['current_tags'] = df_ench['tags'].apply(lambda tags: ','.join(sorted(tags.split(','))))
        df_ench['tags'] = df_ench['tags'].apply(lambda tags: set(tags.split(',')))
        df_ench['date'] = df_tx['datetime'].apply(lambda dt: dt.date().strftime('%Y-%m-%d'))
        df_ench['time'] = df_tx['datetime'].apply(lambda dt: dt.time().strftime('%H:%M:%S'))
        df_ench['amount'] = df_tx.apply(lambda row: construct_amount_str(row), axis=1)
        df_ench['week_id'] = df_tx['datetime'].apply(
            lambda dt: f"{dt.date().strftime('%Y-%m-%d')}/{cu.week_of_year(dt)}")
        df_ench['n_tags'] = df_tx['tags'].apply(lambda tags: len(tags.split(',')))

        df_ench['Tx_ID'] = df_ench['id'].apply(ui_id_transformation)
        # df_ench['short_desc'] = df_tx['description'].apply(ui_description_transformation)
        df_ench['selectize_id'] = df_tx['id'].apply(build_selectize_id)
        df_ench['add_tags'] = df_ench.apply(
            lambda row: ui_tags_transformation(row['id'], row['tags'], all_tags, row['selectize_id']),
            axis=1
        )
        df_ench['view_full'] = df_ench.apply(lambda row: ui_view_tx_button(row, input), axis=1)

        cols_to_keep = ['Tx_ID', 'amount', 'date', 'time', 'week_id', 'n_tags', 'current_tags', 'add_tags',
                        # 'short_desc',
                        'view_full']
        df_res = df_ench[cols_to_keep]
        return df_res

    @render.text
    def transactions_header_text():
        filtered_transactions_tx: Transactions = filtered_transactions_calc()
        filtered_transactions_df = filtered_transactions_tx.dataframe()
        start_date, end_date = filtered_transactions_tx.date_range()
        unique_tags = filtered_transactions_tx.all_tags()

        title = f"{len(filtered_transactions_df)} transactions from {start_date} to {end_date} containing {len(unique_tags)} unique tags" \
            # f" page={input.page_number_select()}," \
        # f" page_size={PAGE_SIZE}"
        return title

    @render.data_frame
    def transactions_output_df():
        df = enhance_transactions_df(filtered_transactions_calc().dataframe(), addable_tags_set)
        return render_table_customised_width(
            df,
            width='100%',
            height='600px',
            column_widths=TRANSACTION_TABLE_COLUMN_WIDTHS,
        )

    def fetch_tag_diffs():
        ids = filtered_transactions_calc().dataframe()['id'].apply(build_selectize_id).tolist()
        tag_diffs = []
        logging.info(f"{dir(input)=}")
        for selectize_id in ids:
            func = getattr(input, selectize_id)
            try:
                added_tags_info = {'selectize_id': selectize_id, 'changes': func()}
                if len(added_tags_info['changes']) > 0:
                    tag_diffs.append(added_tags_info)
            except Exception as e:
                logging.error(e)
        return tag_diffs

    @reactive.effect
    @reactive.event(input.see_all_changes_button)
    def see_all_changes_button():
        tag_diffs = fetch_tag_diffs()

        m = ui.modal(
            *build_tag_diffs_summary(tag_diffs),
            title="Review changes before saving",
            easy_close=True,
            size='xl',
            footer=ui.input_action_button(id='save_button', label='Save', disabled=len(tag_diffs) == 0),
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.save_button)
    def save_changes():
        tag_diffs = fetch_tag_diffs()
        changes_per_tag = transform_tag_diffs(tag_diffs)
        utils.save_tag_changes(changes_per_tag, data_manager)


manual_tagging_app = App(app_ui, server)
