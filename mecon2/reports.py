from mecon2 import transactions


def transactions_stats(trans: transactions.Transactions):
    df = trans.dataframe()
    return {
        '#': trans.size(),
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
        'All tags': list(trans.all_tags()),
        'Currencies': trans.all_currencies()
    }


def plot(transactions,
         date_range=None,
         grouping=None,
         fill_values=None,
         cumsum=False,
         tags=None,):
    pass
