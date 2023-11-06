from data import transactions


def transactions_stats(trans: transactions.Transactions):
    df = trans.dataframe()
    return {
        'General': {
            '#': trans.size(),
            'avg Amount': trans.amount.mean(),
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

