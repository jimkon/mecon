import datetime
import logging
import warnings
from typing import List, Literal

import networkx as nx
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from mecon.data import graph_utils
from mecon.monitoring import logging_utils

warnings.simplefilter("ignore", category=FutureWarning)

# -----
# pio.templates["custom_template"] = go.layout.Template(
#     layout_colorway=px.colors.qualitative.Antique
# )
# pio.templates.default = "plotly_dark"

# -----
# Define a custom dark theme template
# dark_theme = pio.templates["plotly_dark"]  # Start with built-in dark theme
#
# # Customize the theme
# dark_theme.layout.update(
#     font=dict(color="white"),           # White text for better contrast
#     paper_bgcolor="black",              # Black background
#     plot_bgcolor="black",               # Black plot area
#     title=dict(x=0.5, font=dict(size=18)),  # Centered titles with adjusted size
#     xaxis=dict(showgrid=False, zeroline=False),  # Remove gridlines
#     yaxis=dict(showgrid=False, zeroline=False),
# )
#
# # Set the template as the default globally
# pio.templates["custom_dark"] = dark_theme
# pio.templates.default = "custom_dark"

# -----
# Define a custom dark theme template
dark_theme = pio.templates["plotly_dark"]  # Start with built-in dark theme

# Customize the theme
dark_theme.layout.update(
    font=dict(color="white"),                 # White text for better contrast
    paper_bgcolor="black",                    # Black background
    plot_bgcolor="black",                     # Black plot area
    title=dict(x=0.5, font=dict(size=18)),    # Center title and adjust size
    xaxis=dict(showgrid=False, zeroline=False),  # Remove x-axis gridlines
    yaxis=dict(showgrid=False, zeroline=False),  # Remove y-axis gridlines
    legend=dict(
        font=dict(color="white", size=12),    # White text for the legend
        bgcolor="rgba(0, 0, 0, 0.5)",         # Semi-transparent black background
        bordercolor="white",                  # White border for the legend box
        borderwidth=1                         # Thickness of the legend border
    )
)

# Set the template as the default globally
pio.templates["custom_dark"] = dark_theme
pio.templates.default = "custom_dark"


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
    fig.add_trace(go.Scatter(x=time_pos, y=amount, name="total", line=dict(width=2)))
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
def multiple_lines_graph_html(
        times: List[pd.Series],
        lines: List[pd.Series],
        names: List[str],
        stacked: bool = False,
        order: Literal["desc", "asc", "none"] = "desc",  # "desc", "asc", or "none"
        rolling_window: int = None  # Window size for rolling max/min
):
    fig = go.Figure()

    if rolling_window is None:
        rolling_window = int(.1*len(times[0]))

    # Validate input lengths
    time_lens, lines_lens = [len(time) for time in times], [len(line) for line in lines]
    if len(set(time_lens)) != 1 or len(set(lines_lens)) != 1:
        raise ValueError(f"All time series must have the same length, {time_lens=} != {lines_lens=}")

    # Convert to DataFrame for easier handling
    df = pd.DataFrame({name: line.tolist() for name, line in zip(names, lines)})
    df["time"] = times[0].tolist()  # Assume all times are identical

    # Order lines if needed
    if order in ("desc", "asc"):
        line_sums = df[names].abs().sum()
        sorted_names = line_sums.sort_values(ascending=(order == "asc")).index.tolist()
        df = df[["time"] + sorted_names]  # Reorder columns

    total_line = None
    cols = sorted(df.columns.tolist())
    cols.remove("time")
    for name in cols:  # Exclude "time" column
        line = df[name]
        if not stacked or total_line is None:
            total_line = line
        else:
            total_line += line

        fig.add_trace(go.Scatter(
            x=df["time"], y=total_line if stacked else line, name=name,
            line=dict(width=1), opacity=0.7  # Transparency for visibility
        ))

    # Compute total line (sum of all lines)
    total_line = df[names].sum(axis=1)
    fig.add_trace(go.Scatter(
        x=df["time"], y=total_line, name="Total",
        line=dict(width=1, color="#FFD700"), opacity=.5  # Gold color for total
    ))

    # Compute rolling max/min and add shaded area
    rolling_max = df[names].rolling(window=rolling_window, min_periods=1).max().sum(axis=1)
    rolling_min = df[names].rolling(window=rolling_window, min_periods=1).min().sum(axis=1)
    rolling_avg = df[names].rolling(window=rolling_window, min_periods=1).mean().sum(axis=1)
    fig.add_trace(go.Scatter(
        x=df["time"], y=rolling_avg, name=f"Rolling Avg (w={rolling_window})",
        line=dict(width=3, color="#FFD700"), opacity=.25  # Hide in legend
    ))
    fig.add_trace(go.Scatter(
        x=df["time"], y=rolling_max, name="Rolling Max",
        line=dict(width=0), showlegend=False  # Hide in legend
    ))
    fig.add_trace(go.Scatter(
        x=df["time"], y=rolling_min, name="Rolling Min",
        fill="tonexty", fillcolor="rgba(255, 215, 0, 0.1)",
        line=dict(width=0), showlegend=False  # Semi-transparent gold shade
    ))


    # Update layout for dark theme
    fig.update_layout(
        autosize=True,
        hovermode="closest",
        yaxis=dict(title="£", gridcolor="gray"),
        xaxis=dict(title=f"({len(df['time'])} points)", gridcolor="gray"),
        plot_bgcolor="#1E1E1E",  # Dark background
        paper_bgcolor="#1E1E1E",
        font=dict(color="white")  # White font
    )

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


def create_plotly_graph(df, from_col=None, to_col=None, info_col=None, k=0.5, levels_col=None):
    """
    https://chatgpt.com/c/678aa579-efac-8006-a2db-beba6dedb7e3
    Generate an interactive directed graph using Plotly and networkx, with optional node coloring by levels.

    Args:
        df (pd.DataFrame): A DataFrame with columns for nodes and edges.
        from_col (str): Column representing the source nodes.
        to_col (str): Column representing the target nodes.
        info_col (str): Optional column for custom hover text.
        k (float): Spring layout spacing parameter.
        levels_col (str): Optional column for node levels (0 or positive integer values).
    """
    from_col = df.columns[0] if from_col is None else from_col
    to_col = df.columns[1] if to_col is None else to_col

    if levels_col is not None and levels_col not in df.columns:
        raise ValueError(f"Level information not found in the DataFrame. {levels_col} does not exist: {df.columns=}.")

    # Create a directed graph
    G = nx.DiGraph()

    # Add nodes and edges based on the DataFrame
    for _, row in df.iterrows():
        tag = row[from_col]
        depends_on = row[to_col]
        G.add_node(tag)
        if pd.notna(depends_on):
            G.add_edge(depends_on, tag)

    if levels_col and levels_col in df.columns:
        # Compute node positions manually based on levels
        levels = df[[from_col, levels_col]].drop_duplicates()
        levels[levels_col] = levels[levels_col].fillna(0).astype(int)

        # Sort levels and invert so the highest level is on top
        unique_levels = sorted(levels[levels_col].unique(), reverse=True)
        level_mapping = {level: i for i, level in enumerate(unique_levels)}

        # Assign y-coordinates based on levels
        pos = {}
        x_offsets = {level: 0 for level in unique_levels}  # Track x positions per level
        for node in G.nodes():
            level_row = levels.loc[levels[from_col] == node]
            level = level_row[levels_col].iloc[0] if not level_row.empty else 0
            y = level_mapping[level]  # Higher levels have higher y-values
            x = x_offsets[level]
            pos[node] = (x, y)
            x_offsets[level] += 1  # Spread nodes horizontally
    else:
        # Use networkx spring layout if no levels_col is provided
        pos = nx.spring_layout(G, k=k, seed=42)

    # Extract node positions
    x_nodes = [pos[node][0] for node in G.nodes()]
    y_nodes = [pos[node][1] for node in G.nodes()]

    # Extract edge positions and arrows
    edge_x = []
    edge_y = []
    arrow_x = []
    arrow_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

        # Calculate arrowhead position
        arrow_x.append((x0 + x1 * 2) / 3)
        arrow_y.append((y0 + y1 * 2) / 3)

    # Determine node colors and hover text
    node_colors = []
    node_hover_text = []
    if levels_col and levels_col in df.columns:
        start_color = "ffdf69"
        end_color = "8b008b"

        def interpolate_color(value):
            ratio = (value - min(unique_levels)) / (max(unique_levels) - min(unique_levels)) if max(
                unique_levels) > min(unique_levels) else 0
            start_rgb = [int(start_color[i:i + 2], 16) for i in (0, 2, 4)]
            end_rgb = [int(end_color[i:i + 2], 16) for i in (0, 2, 4)]
            interp_rgb = [int(start + ratio * (end - start)) for start, end in zip(start_rgb, end_rgb)]
            return f"#{''.join(f'{c:02x}' for c in interp_rgb)}"

        color_mapping = {level: interpolate_color(level) for level in unique_levels}

        for node in G.nodes():
            level_row = levels.loc[levels[from_col] == node]
            level = level_row[levels_col].iloc[0] if not level_row.empty else 0
            color = color_mapping.get(level, "#cccccc")
            hover_text = f"{node} (Level: {level})"
            node_colors.append(color)
            node_hover_text.append(hover_text)
    else:
        node_colors = ["skyblue"] * len(G.nodes())
        node_hover_text = [f"{node}" for node in G.nodes()]

    # Create the edge trace
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=1, color="#888"),
        hoverinfo="none",
        mode="lines"
    )

    # Create the arrow markers
    arrow_trace = go.Scatter(
        x=arrow_x,
        y=arrow_y,
        mode="markers",
        marker=dict(
            size=10,
            color="#888",
            symbol="triangle-down"
        ),
        hoverinfo="none"
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
            color=node_colors,
            line=dict(width=2, color="darkblue")
        ),
        hoverinfo="text",
        hovertext=node_hover_text,
    )

    # Create the figure
    fig = go.Figure(data=[edge_trace, arrow_trace, node_trace],
                    layout=go.Layout(
                        title="Interactive Dependency Graph",
                        title_x=0.5,
                        showlegend=False,
                        hovermode="closest",
                        margin=dict(b=0, l=0, r=0, t=40),
                        xaxis=dict(showgrid=False, zeroline=False),
                        yaxis=dict(showgrid=False, zeroline=False, autorange="reversed"),
                    ))

    return fig
