import datetime
import warnings
from typing import List

import networkx as nx
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from mecon.data import graph_utils
from mecon.monitoring import logging_utils

# from plotly.offline import plot
# from plotly.subplots import make_subplots

warnings.simplefilter("ignore", category=FutureWarning)

pio.templates["custom_template"] = go.layout.Template(
    layout_colorway=px.colors.qualitative.Antique
)
pio.templates.default = "plotly_dark"


@logging_utils.codeflow_log_wrapper('#graphs')
def amount_and_freq_timeline_fig(time_pos: List | pd.Series,
                                 amount_pos: List | pd.Series,
                                 time_neg: List | pd.Series,
                                 amount_neg: List | pd.Series,
                                 time_freg: List | pd.Series,
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

    # amount_pos = amount.clip(lower=0)
    # amount_neg = amount.clip(upper=0)
    amount_pos, amount_neg = amount_pos.round(2), amount_neg.round(2)

    amount_axis_range = [1.6 * amount_neg.min(), 1.0 * amount_pos.max()]
    if grouping != 'none':
        fig.add_trace(go.Scatter(x=time_pos, y=amount_pos, name="in", line=dict(width=1), fill='tozeroy'))
        fig.add_trace(go.Scatter(x=time_neg, y=amount_neg, name="out", line=dict(width=1), fill='tozeroy'))
    else:
        fig.add_trace(go.Scatter(x=time_pos, y=amount_pos, name="in", mode='markers'))
        fig.add_trace(go.Scatter(x=time_neg, y=amount_neg, name="out", mode='markers'))

    amount = amount_pos + amount_neg
    smoothed_total = amount.rolling(rolling_window, min_periods=1).mean()
    smoothed_total = smoothed_total.round(2)
    fig.add_trace(go.Scatter(x=time_pos, y=smoothed_total, name="total (rolling)", line=dict(width=5)))
    # fig.add_trace(go.Scatter(x=time_pos, y=amount, name="amount", line=dict(width=1), fill='tozeroy'))
    freq_axis_range = None
    if freq is not None:
        freq_axis_range = [0, 5 * freq.max()]
        fig.add_trace(go.Bar(
            x=time_freg,
            y=freq,
            name=f"count (total {freq.sum()})",
            yaxis='y2',
            marker={'opacity': 0.5},
        ))
        # fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1, color='black'), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='[£]', range=amount_axis_range),
        yaxis2=dict(title=f"# transactions{(' per ' + grouping) if grouping != 'None' else ''}", overlaying='y',
                    side='right', range=freq_axis_range),
        # xaxis=dict(title=f"({len(time_freg)} points)"),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
    )

    return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def balance_graph_fig(time, amount: pd.Series, fit_line=False):
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
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
    )
    # graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    # return graph_html
    return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def histogram_and_contributions_fig(amounts: pd.Series, show_bin_edges=False):
    bin_centers, counts, contributions, bin_width, edges = graph_utils.calculated_histogram_and_contributions(amounts)
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
        name="Total contributions",
        yaxis='y2'
    ))

    fig.update_layout(
        bargap=0,
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='#'),
        yaxis2=dict(title='cumsum [£]', overlaying='y', side='right'),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
        xaxis_title=f"Bin width: £{bin_width:.2f}",
        xaxis=dict(
            tickmode='array',
            tickvals=np.round(edges, decimals=1),
        ) if show_bin_edges else None
    )

    return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def amount_and_freq_timeline_fig_old(time: List | pd.Series,
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
    fig.add_trace(go.Scatter(x=time, y=amount_pos, name="in", line=dict(width=1), fill='tozeroy'))
    fig.add_trace(go.Scatter(x=time, y=amount_neg, name="out", line=dict(width=1), fill='tozeroy'))

    smoothed_total = amount.rolling(rolling_window).mean()
    smoothed_total = smoothed_total.round(2)
    fig.add_trace(go.Scatter(x=time, y=smoothed_total, name="total", line=dict(width=5)))
    # fig.add_trace(go.Scatter(x=time, y=amount, name="amount", line=dict(width=1), fill='tozeroy'))
    freq_axis_range = None
    if freq is not None:
        freq_axis_range = [0, 5 * freq.max()]
        fig.add_trace(go.Bar(
            x=time,
            y=freq,
            name="count",
            yaxis='y2',
            marker={'opacity': 0.5},
        ))
        # fig.add_trace(go.Scatter(x=time, y=freq, name="freq", line=dict(width=1, color='black'), yaxis='y2'))

    fig.update_layout(
        autosize=True,  # Automatically adjust the size of the plot
        hovermode='closest',  # Define hover behavior
        yaxis=dict(title='[£]', range=amount_axis_range),
        yaxis2=dict(title=f"# transactions{(' per ' + grouping) if grouping != 'None' else ''}", overlaying='y',
                    side='right', range=freq_axis_range),
        xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now()),  # Set a unique value to trigger the layout change
    )

    # graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    # return graph_html
    return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def time_aggregated_amount_and_frequency_fig(time: pd.Series, amount: pd.Series = None):
    if amount is None:
        amount = pd.Series(np.ones(time.size))

    # Group data for each plot
    hourly_data = amount.groupby(time.dt.hour).sum()
    weekly_data = amount.groupby(time.dt.dayofweek).sum()
    daily_data = amount.groupby(time.dt.day).sum()
    monthly_data = amount.groupby(time.dt.month).sum()

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "Hourly Totals", "Daily Totals by Weekday",
            "Daily Totals by Day of Month", "Monthly Totals"
        ]
    )

    # Hourly plot
    fig.add_trace(go.Bar(
        x=hourly_data.index,
        y=hourly_data.values,
        name="Hourly Totals"
    ), row=1, col=1)

    # Weekly plot
    fig.add_trace(go.Bar(
        x=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        y=weekly_data.values,
        name="Daily Totals by Weekday"
    ), row=1, col=2)

    # Daily of month plot
    fig.add_trace(go.Bar(
        x=daily_data.index,
        y=daily_data.values,
        name="Daily Totals by Day of Month"
    ), row=2, col=1)

    # Monthly plot
    fig.add_trace(go.Bar(
        x=['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November',
           'December'],
        y=monthly_data.values,
        name="Monthly Totals"
    ), row=2, col=2)

    # Update layout with titles and settings
    fig.update_layout(
        title="Aggregated Amounts by Time Period",
        autosize=True,
        hovermode='closest',
        uirevision=str(pd.Timestamp.now()),  # To preserve UI state on updates
        showlegend=False
    )

    # Set axis labels
    fig.update_yaxes(title_text="£", row=1, col=1)
    fig.update_yaxes(title_text="£", row=1, col=2)
    fig.update_yaxes(title_text="£", row=2, col=1)
    fig.update_yaxes(title_text="£", row=2, col=2)

    # Return the figure
    return fig


# -------------------------------------------------------------------

# @logging_utils.codeflow_log_wrapper('#graphs')
# def lines_graph_html(times: List[pd.Series], lines: List[pd.Series], names: List[str]):
#     fig = go.Figure()
#
#     for time, line, name in zip(times, lines, names):
#         fig.add_trace(go.Scatter(x=time, y=line, name=name, line=dict(width=3), fill='tozeroy'))
#
#     fig.update_layout(
#         autosize=True,  # Automatically adjust the size of the plot
#         hovermode='closest',  # Define hover behavior
#         yaxis=dict(title='£'),
#         uirevision=str(datetime.datetime.now())  # Set a unique value to trigger the layout change
#     )
#     # graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
#     # return graph_html
#     return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def stacked_bars_graph_html(times: List[pd.Series], lines: List[pd.Series], names: List[str]):
    fig = go.Figure()

    for time, line, name in zip(times, lines, names):
        fig.add_trace(go.Bar(x=time, y=line, name=name))

    fig.update_layout(
        barmode='stack',  # Stacked bar mode
        autosize=True,
        hovermode='closest',
        yaxis=dict(title='£'),
        # xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())
    )

    # graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    # return graph_html
    return fig


@logging_utils.codeflow_log_wrapper('#graphs')
def multiple_histograms_graph_html(amounts: List[pd.Series], names: List[str]):
    fig = go.Figure()

    for amount, name in zip(amounts, names):
        fig.add_trace(go.Histogram(x=amount, name=name, autobinx=True))

    fig.update_layout(
        # barmode='stack',  # Stacked bar mode
        autosize=True,
        hovermode='closest',
        yaxis=dict(title='£'),
        # xaxis=dict(title=f"({len(time)} points)"),
        uirevision=str(datetime.datetime.now())
    )

    # graph_html = plot(fig, output_type='div', include_plotlyjs='cdn')
    # return graph_html
    return fig


def create_plotly_graph(df, from_col=None, to_col=None, info_col=None):
    """
    Generate an interactive directed graph using Plotly and networkx.

    Args:
        df (pd.DataFrame): A DataFrame with columns for nodes and edges.
        from_col (str): Column representing the source nodes.
        to_col (str): Column representing the target nodes.
        info_col (str): Optional column for custom hover text.
    """
    from_col = df.columns[0] if from_col is None else from_col
    to_col = df.columns[1] if to_col is None else to_col

    # Create hover text
    node_hover_text = (
        df[info_col] if info_col is not None else df[from_col] + " depends on " + df[to_col].fillna("nothing")
    )

    # Create a directed graph
    G = nx.DiGraph()

    # Add nodes and edges based on the DataFrame
    for _, row in df.iterrows():
        tag = row[from_col]
        depends_on = row[to_col]

        # Add the tag node even if it has no dependencies
        G.add_node(tag)

        # Add edge if there is a dependency
        if pd.notna(depends_on):
            G.add_edge(depends_on, tag)

    # Compute positions for nodes
    try:
        pos = nx.spring_layout(G, k=0.5, seed=42)  # Adjust `k` for spacing
    except Exception as e:
        raise ValueError("Error generating graph layout: " + str(e))

    # Handle missing or invalid positions
    pos = {node: (coords[0] if not pd.isna(coords[0]) else 0, coords[1] if not pd.isna(coords[1]) else 0)
           for node, coords in pos.items()}

    # Extract node positions
    x_nodes = [pos[node][0] for node in G.nodes()]
    y_nodes = [pos[node][1] for node in G.nodes()]

    # Extract edge positions
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])  # None separates edges
        edge_y.extend([y0, y1, None])

    # Create the edge trace
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=1, color="#888"),
        hoverinfo="none",
        mode="lines"
    )

    # Create the node trace
    node_trace = go.Scatter(
        x=x_nodes,
        y=y_nodes,
        mode="markers+text",
        text=[str(node) for node in G.nodes()],
        textposition="top center",
        marker=dict(
            size=20,
            color="skyblue",
            line=dict(width=2, color="darkblue")
        ),
        hoverinfo="text",
        hovertext=node_hover_text
    )

    # Create the figure
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title="Interactive Dependency Graph",
                        title_x=0.5,
                        showlegend=False,
                        hovermode="closest",
                        margin=dict(b=0, l=0, r=0, t=40),
                        xaxis=dict(showgrid=False, zeroline=False),
                        yaxis=dict(showgrid=False, zeroline=False),
                    ))

    return fig
