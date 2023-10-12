import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
from plotly.subplots import make_subplots
import plotly.express as px

from mecon2.blueprints.reports import graph_utils
from mecon2.monitoring import logs


@logs.codeflow_log_wrapper('#graphs')
def amount_and_freq_timeline_html(time, amount, freq):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=time, y=amount, name="amount", line=dict(width=1)))
    if freq is not None:
        fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='[£]'),
        yaxis2=dict(title='Freq [#/time]', overlaying='y', side='right'),
        xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logs.codeflow_log_wrapper('#graphs')
def balance_graph_html(time, amount: pd.Series):
    fig = go.Figure()

    balance = amount.cumsum()

    fig.add_trace(go.Scatter(x=time, y=balance, name="balance", line=dict(width=3)))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='£'),
        xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )
    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logs.codeflow_log_wrapper('#graphs')
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


@logs.codeflow_log_wrapper('#graphs')
def histogram_and_contributions(amounts: pd.Series):
    # TODO:v2 maybe add x_ticks of bar left and right limits
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
        bargap=0,
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='cumsum [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
    )

    graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    return graph_html


@logs.codeflow_log_wrapper('#graphs')
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


@logs.codeflow_log_wrapper('#graphs')
def performance_stats_graph_html(perf_data_stats: dict):
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
        'autosize': True,  # TODO:v2 it does not autosize
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
            'title': 'Percentage (%)',
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
