import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot

from mecon2.blueprints.reports import graph_utils


def amount_and_freq_timeline_html(time, amount, freq):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=time, y=-amount, name="amount", line=dict(width=1)))
    if freq is not None:
        fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='Amount paid [£]'),
        yaxis2=dict(title='Freq [#/time]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


def balance_graph_html(time, amount: pd.Series):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=time[::-1], y=amount[::-1].cumsum(), name="balance", line=dict(width=3)))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='£'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


def histogram_and_cumsum_graph_html(amount: pd.Series):
    fig = go.Figure()

    hist = go.Histogram(x=amount, name="balance")
    fig.add_trace(hist)

    amount_sorted = amount.sort_values()
    amount_cumsum = amount_sorted.cumsum()

    fig.add_trace(go.Scatter(x=amount_sorted, y=-amount_cumsum, name="cumsum", yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='cumsum [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


def histogram_and_contributions(amounts: pd.Series):
    bin_centers, counts, contributions = graph_utils.calculated_histogram_and_contributions(amounts)

    # Create a custom histogram using Plotly
    fig = go.Figure()

    # Plot the bars for counts
    fig.add_trace(go.Bar(
        x=bin_centers,
        y=counts,
        name="Counts"
    ))

    # Plot the bars for contributions
    fig.add_trace(go.Scatter(
        x=bin_centers,
        y=-contributions,
        name="Contributions",
        yaxis='y2'
    ))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='cumsum [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )

    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html
