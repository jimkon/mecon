from data import transactions


def transactions_stats(trans: transactions.Transactions, grouping='none'):
    df = trans.dataframe()

    avg_amount_per_unit = trans.fill_values(grouping).amount.mean() if grouping and grouping != 'none' else 'Undefined'
    return {
        'General': {
            '#': trans.size(),
            'avg Amount (per event)': trans.amount.mean(),
            f'avg Amount (per {grouping})': avg_amount_per_unit,
            'avg Frequency': trans.datetime.diff().mean()
        },
        'Amount': {
            'Total': df['amount'].sum(),
            'Pos': df[df['amount'] > 0]['amount'].sum(),
            'Neg': df[df['amount'] < 0]['amount'].sum(),
        },
        'Dates': {
            'min': trans.date.min(),
            'max': trans.date.max(),
            '# unique': trans.date.nunique(),
        },
        'All tags': trans.all_tags(),
        'Currencies': trans.all_currencies()
    }

