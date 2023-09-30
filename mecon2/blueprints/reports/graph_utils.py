import numpy as np
import pandas as pd


def calculated_histogram_and_contributions(amount):
    bin_counts, bin_edges = np.histogram(amount, bins='auto')

    bin_midpoints = (bin_edges[:-1] + bin_edges[1:]) / 2

    bin_edges_t = list(bin_edges)
    bin_edges_t[-1] = bin_edges_t[-1] + 1  # move it a bit to the right
    bin_indices = np.digitize(amount, bin_edges_t)
    df_bin_amounts = pd.DataFrame({'ind': bin_indices, 'amount': amount})
    fill_df = pd.DataFrame({'ind': range(min(bin_indices), max(bin_indices))})
    fill_df['amount'] = 0

    df_bin_totals = pd.concat([df_bin_amounts, fill_df], keys='ind').groupby('ind').agg('sum').reset_index()

    return bin_midpoints, bin_counts, df_bin_totals['amount']
