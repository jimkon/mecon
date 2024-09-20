import json
import datetime

import pandas as pd

from mecon.data import transactions


def transactions_stats_json(trans: transactions.Transactions, grouping='none'):
    df = trans.dataframe()

    avg_amount_per_unit = trans.fill_values(grouping).amount.mean() if grouping and grouping != 'none' else pd.Series([-1.])
    info_dict = {
        'General': {
            '#': trans.size(),
            'avg Amount (per event)': trans.amount.mean().round(2),
            f'avg Amount (per {grouping})': avg_amount_per_unit.round(2),
            'avg Frequency': trans.datetime.diff().mean().round('1s')
        },
        'Amount': {
            'Total': df['amount'].sum().round(2),
            'Pos': df[df['amount'] > 0]['amount'].sum().round(2),
            'Neg': df[df['amount'] < 0]['amount'].sum().round(2),
        },
        'Dates': {
            'min': trans.date.min(),
            'max': trans.date.max(),
            '# unique': trans.date.nunique(),
        },
        'All tags': trans.all_tags(),
        'Currencies': trans.all_currencies()
    }

    return info_dict


def transactions_stats_markdown(trans: transactions.Transactions, grouping='none'):
    stats_json = transactions_stats_json(trans, grouping)
    general = stats_json.get('General', {})
    general_md = (
        f"### General Information\n"
        f"- **Total number of transactions (#)**: {general.get('#', 'N/A')}\n"
        f"- **Average amount per event**: {general.get('avg Amount (per event)', 'N/A')}\n"
        f"- **Average amount per non-event**: {general.get('avg Amount (per none)', 'N/A')}\n"
        f"- **Average frequency**: {general.get('avg Frequency', 'N/A')}\n"
    )

    # Extract the "Amount" section
    amount = stats_json.get('Amount', {})
    amount_md = (
        f"\n### Amount Summary\n"
        f"- **Total transaction amount**: {amount.get('Total', 'N/A')}\n"
        f"- **Total positive transactions**: {amount.get('Pos', 'N/A')}\n"
        f"- **Total negative transactions**: {amount.get('Neg', 'N/A')}\n"
    )

    # Extract the "Dates" section
    dates = stats_json.get('Dates', {})
    min_date = dates.get('min', 'N/A')
    max_date = dates.get('max', 'N/A')
    if isinstance(min_date, datetime.date):
        min_date = min_date.isoformat()
    if isinstance(max_date, datetime.date):
        max_date = max_date.isoformat()

    dates_md = (
        f"\n### Date Range\n"
        f"- **Earliest transaction date**: {min_date}\n"
        f"- **Latest transaction date**: {max_date}\n"
        f"- **Number of unique dates**: {dates.get('# unique', 'N/A')}\n"
    )

    # Extract the "All tags" section
    tags = stats_json.get('All tags', {})
    tags_md = "\n### Tag Breakdown\nThe following table shows the number of transactions associated with each tag:\n\n"
    tags_md += "| Tag               | Count |\n|-------------------|-------|\n"
    for tag, count in tags.items():
        tags_md += f"| {tag:<17} | {count} |\n"

    # Extract the "Currencies" section
    currencies = json.loads(stats_json.get('Currencies', '{}'))
    currencies_md = "\n### Currencies Used\nThe following currencies were used in the transactions:\n"
    for currency, count in currencies.items():
        currencies_md += f"- **{currency}**: {count} transactions\n"

    # Combine all sections into a final markdown string
    markdown_output = f"## Aggregated Transaction Data\n\n{general_md}{amount_md}{dates_md}{tags_md}{currencies_md}"

    return markdown_output