import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot


def amount_and_freq_timeline_html(time, amount, freq):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=time, y=-amount, name="amount", line=dict(width=1)))
    if freq is not None:
        fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='Cost[£]'),
        yaxis2=dict(title='#', overlaying='y', side='right'),
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


def histogram_graph_html(amount: pd.Series):
    fig = go.Figure()

    fig.add_trace(go.Histogram(x=amount, name="balance"))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html
