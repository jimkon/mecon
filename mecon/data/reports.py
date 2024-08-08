from mecon.data import transactions


def transactions_stats(trans: transactions.Transactions, grouping='none'):
    df = trans.dataframe()

    avg_amount_per_unit = trans.fill_values(grouping).amount.mean() if grouping and grouping != 'none' else 'Undefined'
    return {
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

