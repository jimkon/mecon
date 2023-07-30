from mecon2 import transactions


def transactions_stats(transactions: transactions.Transactions):
    df = transactions.dataframe()
    return {
        '#': transactions.size(),
        'All tags': list(transactions.all_tags()),
        'Amount': {
            'Total': df['amount'].sum(),
            'Pos': df[df['amount'] > 0]['amount'].sum(),
            'Neg': df[df['amount'] < 0]['amount'].sum(),
        },
        'Dates': {
            'min': df['datetime'].min(),
            'max': df['datetime'].max(),
            '# unique': df['datetime'].nunique(),
        },
        'Currencies': transactions.all_currencies()
    }


def plot(transactions,
         date_range=None,
         grouping=None,
         fill_values=None,
         cumsum=False,
         tags=None,):
    pass
