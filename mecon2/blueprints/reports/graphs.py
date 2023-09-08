import mpld3
import matplotlib.pyplot as plt

from mecon2.blueprints.reports import graph_utils
from mecon2.utils.html_pages import TabsHTML
from mecon2.utils import html_pages


def timeline(transactions, ax):
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    # TODO https://matplotlib.org/stable/gallery/subplots_axes_and_figures/two_scales.html
    # add counts in the second y axis
    ax.plot(x, y, '.')
    ax.bar(x, y)
    # ax.invert_yaxis()
    ax.set_title('Timeline')

    # ax2 = ax.twinx()
    # ax2.plot(x, -y, ':')

    # print(ax.get_ylim())  # Print the y-axis limits
    # print(ax2.get_ylim())  # Print the y-axis limits for the twin axes
    # ax.set_xticklabels(x.to_list(), rotation=45)


def balance(transactions, ax):
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    ax.plot(x, y.cumsum(), '-')
    # ax.bar(x, y.cumsum())
    # ax.invert_yaxis()
    ax.set_title('Balance')


def histogram(transactions, ax):
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    ax.hist(y, bins='auto')
    ax.invert_xaxis()
    ax.set_title('Histogram')


def timeline_fig(transactions, figsize=(15, 8)):
    fig = plt.figure(figsize=figsize)
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    # TODO https://matplotlib.org/stable/gallery/subplots_axes_and_figures/two_scales.html
    # add counts in the second y axis
    plt.plot(x, y, 'r:')
    plt.bar(x, y)
    # ax.invert_yaxis()
    plt.title('Timeline')
    # plt.grid(True, which='minor', linestyle=':', linewidth=0.5, color="black")
    # ax2 = ax.twinx()
    # ax2.plot(x, -y, ':')

    # print(ax.get_ylim())  # Print the y-axis limits
    # print(ax2.get_ylim())  # Print the y-axis limits for the twin axes
    # ax.set_xticklabels(x.to_list(), rotation=45)
    # html_image = html_pages.ImageHTML.from_matplotlib()
    html_image = mpld3.fig_to_html(fig)
                                   # d3_url=r"file://C:\Users\dimitris\miniconda3\envs\mecon\Lib\site-packages\mpld3\js\d3.v5.min.js",
                                   # mpld3_url=r"file://C:\Users\dimitris\miniconda3\envs\mecon\Lib\site-packages\mpld3\js\mpld3.v0.5.9.js")
                                   # d3_url=r"/mecon/d3.v3.min.js",
                                   # mpld3_url=r"C:\Users\dimitris\miniconda3\envs\mecon\Lib\site-packages\mpld3\js\mpld3.v0.5.9.js")

    return html_image


def balance_fig(transactions, figsize=(15, 8)):
    fig = plt.figure(figsize=figsize)
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    plt.plot(x, y.cumsum(), '-')
    # ax.bar(x, y.cumsum())
    # ax.invert_yaxis()
    plt.title('Balance')
    # html_image = html_pages.ImageHTML.from_matplotlib()
    html_image = mpld3.fig_to_html(fig)
    return html_image


def histogram_fig(transactions, figsize=(15, 8)):
    fig = plt.figure(figsize=figsize)
    x, y = transactions.datetime[::-1], transactions.amount[::-1]
    plt.hist(y, bins='auto')
    # plt.invert_xaxis()
    plt.title('Histogram')
    # html_image = html_pages.ImageHTML.from_matplotlib()
    html_image = mpld3.fig_to_html(fig)
    return html_image




def general_cost_stats_html_img(transactions):
    # x, y = transactions.datetime[::-1], transactions.amount[::-1]

    # results = graph_utils.async_multiplot([
    #     (graph_utils.create_html_plot, (timeline, transactions)),
    #     (graph_utils.create_html_plot, (balance, transactions)),
    #     (graph_utils.create_html_plot, (histogram, transactions))
    # ])

    results = [
        timeline_fig(transactions),
        balance_fig(transactions),
        histogram_fig(transactions)
    ]

    html = TabsHTML() \
        .add_tab('Timeline', results[0]) \
        .add_tab('Balance', results[1]) \
        .add_tab('Histogram', results[2]) \
        .html()

    return html
