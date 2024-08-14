import datetime
from typing import List

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.offline import plot
from plotly.subplots import make_subplots

from mecon.monitoring import logging_utils
from services.main.blueprints.reports import graph_utils

pio.templates["custom_template"] = go.layout.Template(
    layout_colorway=px.colors.qualitative.Antique
)
pio.templates.default = "custom_template"


@logging_utils.codeflow_log_wrapper('#graphs')
def lines_graph_html(time: List | pd.Series, lines: [List | pd.Series]):
    fig = go.Figure()
    for line in lines:
        fig.add_trace(go.Scatter(x=time, y=line, name=line.name, line=dict(width=3), fill='tozeroy'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='£'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def stacked_bars_graph_html(time, lines: [pd.Series]):
    fig = go.Figure()

    for i, bar in enumerate(lines):
        fig.add_trace(go.Bar(x=time, y=bar, name=bar.name))

    fig.update_layout(
        barmode='stack',  # Stacked bar mode
        autosize=True,
        hovermode='closest',
        yaxis=dict(title='£'),
        # xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())
    )

    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def amount_and_freq_timeline_html(time: List | pd.Series,
                                  amount: List | pd.Series,
                                  freq: List | pd.Series,
                                  grouping='month'):
    fig = go.Figure()

    rolling_window = {
        'none': 10,
        'day': 7,
        'week': 4,
        'month': 3,
        'year': 1
    }.get(grouping, 1)

    amount_pos = amount.clip(lower=0)
    amount_neg = amount.clip(upper=0)
    amount_pos, amount_neg = amount_pos.round(2), amount_neg.round(2)

    amount_axis_range = [1.6 * amount_neg.min(), 1.0 * amount_pos.max()]
    fig.add_trace(go.Scatter(x=time, y=amount_pos, name="in", line=dict(width=1, color='green'), fill='tozeroy'))
    fig.add_trace(go.Scatter(x=time, y=amount_neg, name="out", line=dict(width=1, color='red'), fill='tozeroy'))

    smoothed_total = amount.rolling(rolling_window).mean()
    smoothed_total = smoothed_total.round(2)
    fig.add_trace(go.Scatter(x=time, y=smoothed_total, name="total", line=dict(width=5, color='rgba(100,0,100,0.5)')))
    # fig.add_trace(go.Scatter(x=time, y=amount, name="amount", line=dict(width=1), fill='tozeroy'))
    freq_axis_range = None
    if freq is not None:
        freq_axis_range = [0, 5 * freq.max()]
        fig.add_trace(go.Bar(
            x=time,
            y=freq,
            name="freq",
            yaxis='y2',
            marker={'color': 'rgba(60,60,60,250)', 'opacity': 0.5},
        ))
        # fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1, color='black'), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='[£]', range=amount_axis_range),
        yaxis2=dict(title='# transactions', overlaying='y', side='right', range=freq_axis_range),
        xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
    )

    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def balance_graph_html(time, amount: pd.Series, fit_line=False):
    amount = amount.round(2)

    fig = go.Figure()

    balance = amount.cumsum()
    fig.add_trace(go.Scatter(x=time, y=balance, name="balance", line=dict(width=3), fill='tozeroy'))

    if fit_line:
        x = np.arange(len(time))
        a, b = np.polyfit(x, balance, deg=1)
        y = a * x + b
        y = y.round(2)
        fig.add_trace(go.Scatter(x=time, y=y, name=f"fit: {a=:.1f}*x + {b=:.1f}",
                                 line=dict(width=5, color='rgba(100,100,0,0.5)')))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='£'),
        xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def histogram_and_cumsum_graph_html(amount: pd.Series):
    fig = go.Figure()

    hist = go.Histogram(x=amount, name="balance")
    fig.add_trace(hist)

    amount_sorted = amount.sort_values()
    amount_cumsum = amount_sorted.cumsum()

    fig.add_trace(go.Scatter(x=amount_sorted, y=-amount_cumsum, name="total", yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='total [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def histogram_and_contributions(amounts: pd.Series):
    bin_centers, counts, contributions, bin_width = graph_utils.calculated_histogram_and_contributions(amounts)
    bin_centers, contributions = bin_centers.round(2), contributions.round(2)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=bin_centers,
        y=counts,
        name="Counts",
    ))

    fig.add_trace(go.Scatter(
        x=bin_centers,
        y=-contributions,
        name="Contributions",
        yaxis='y2'
    ))

    fig.update_layout(
        bargap=0,
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='cumsum [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
        xaxis_title=f"Bin width: £{bin_width:.2f}"
    )

    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def codeflow_timeline_graph_html(functions, start_datetime, end_datetime):
    data = list(zip(functions, start_datetime, end_datetime))

    non_zero_data = [(f, s, e) for f, s, e in data if s != e]
    # Convert data to a DataFrame
    df = pd.DataFrame(non_zero_data, columns=['Task', 'Start', 'Finish'])

    # Convert 'Start' and 'Finish' columns to datetime
    df['Start'] = pd.to_datetime(df['Start'])
    df['Finish'] = pd.to_datetime(df['Finish'])

    # Create a Gantt chart
    fig = px.timeline(df, x_start='Start', x_end='Finish', y='Task', color='Task')

    # Update axis labels
    fig.update_xaxes(title_text='Timeline')
    fig.update_yaxes(title_text='Task')

    # Set chart title
    fig.update_layout(title='Task Execution Timeline',
                      autosize=True,
                      )

    # Show the plot
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def performance_stats_barplot_graph_html(perf_data_stats: dict):
    stats = perf_data_stats

    # Create a subplots figure with two y-axes
    fig = make_subplots(rows=1, cols=1, shared_xaxes=True)

    # Create bars for each function
    funcs = [func for func, times in stats.items()]
    mins = [data['Min'] for func, data in stats.items()]
    avgs = [data['Average'] for func, data in stats.items()]
    maxs = [data['Max'] for func, data in stats.items()]
    percs = [data['Percentage'] for func, data in stats.items()]

    bar = go.Bar(
        x=funcs,
        y=mins,
        name='Min',
    )
    fig.add_trace(bar)

    bar = go.Bar(
        x=funcs,
        y=avgs,
        name='Average',
    )
    fig.add_trace(bar)

    bar = go.Bar(
        x=funcs,
        y=maxs,
        name='Max',
    )
    fig.add_trace(bar)

    bar = go.Bar(
        x=funcs,
        y=percs,
        name='Percentage',
        yaxis='y2',  # Use the second y-axis
        marker=dict(color='rgba(100, 100, 100, 0.5)'),  # Set the color to transparent
        showlegend=False,  # Hide the legend for this bar
        legendgroup='Percentage',  # Group the Percentage bars together
        # offsetgroup=func,  # Offset bars by function name
    )
    fig.add_trace(bar)

    # Define layout
    layout = {
        'autosize': True,  # TODO:v3 it does not autosize
        'title': 'Function Execution Statistics',
        'xaxis': {
            'title': 'Functions',
            'showticklabels': True,
        },
        'yaxis': {
            'title': 'Time (milliseconds)',
            # 'type': 'log',  # Set the y-axis to log scale
        },
        'yaxis2': {
            'title': 'Total perc (%)',
            # 'type': 'log',  # Set the y-axis to log scale
            'overlaying': 'y',
            'side': 'right',
            'showticklabels': True,
            'range': [0, 100],  # Set the percentage y-axis range to [0, 100]
        },
    }

    # Update layout
    fig.update_layout(layout,
                      updatemenus=[
                          dict(
                              type="buttons",
                              direction="left",
                              buttons=list([
                                  dict(
                                      args=[{'yaxis': {'type': 'linear'}}],
                                      label="Linear Scale",
                                      method="relayout"
                                  ),
                                  dict(
                                      args=[{'yaxis': {'type': 'log'}}],
                                      label="Log Scale",
                                      method="relayout"
                                  )
                              ])
                          ),
                      ])

    graph_html = plot(fig, output_type='div',
                      include_plotlyjs='cdn')  # , config={'autosizable': True, 'responsive': True})
    return graph_html


@logging_utils.codeflow_log_wrapper('#graphs')
def performance_stats_scatterplot_graph_html(perf_data_stats: dict):
    fig = go.Figure()

    for tag_name, tag_data in perf_data_stats.items():
        fig.add_trace(go.Scatter(x=tag_data['datetime'], y=tag_data['execution_time'], name=tag_name, mode='markers'))

    layout = {
        'autosize': True,  # TODO:v3 it does not autosize
        'title': 'Function Execution Statistics',
        'xaxis': {
            'title': 'Functions',
            'showticklabels': True,
        },
        'yaxis': {
            'title': 'Time (milliseconds)',
            # 'type': 'log',  # Set the y-axis to log scale
        },
    }
    fig.update_layout(layout,
                      updatemenus=[
                          dict(
                              type="buttons",
                              direction="left",
                              buttons=list([
                                  dict(
                                      args=[{'yaxis': {'type': 'linear'}}],
                                      label="Linear Scale",
                                      method="relayout"
                                  ),
                                  dict(
                                      args=[{'yaxis': {'type': 'log'}}],
                                      label="Log Scale",
                                      method="relayout"
                                  )
                              ])
                          ),
                      ])

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='Total time [ms]'),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html
