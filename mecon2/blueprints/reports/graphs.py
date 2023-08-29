import pandas as pd

from mecon2.blueprints.reports import graph_utils
from mecon2.utils.html_pages import TabsHTML


# plt.style.use('bmh')


def timeline(x, y, ax, **kwargs):
    # ax.plot(x, y, '.')
    ax.bar(x, y)
    ax.invert_yaxis()
    ax.set_title('Timeline')
    # ax.set_xticklabels(x.to_list(), rotation=45)


def balance(x, y, ax, **kwargs):
    ax.plot(x, y.cumsum(), '-')
    # ax.bar(x, y.cumsum())
    # ax.invert_yaxis()
    ax.set_title('Balance')


def histogram(x, y, ax, **kwargs):
    ax.hist(y, bins='auto')
    ax.invert_xaxis()
    ax.set_title('Histogram')


def general_cost_stats_html_img(x, y):
    x, y = x[::-1], y[::-1]

    results = graph_utils.async_multiplot([
        (graph_utils.create_html_plot, (timeline, x, y), {}),
        (graph_utils.create_html_plot, (balance, x, y), {}),
        (graph_utils.create_html_plot, (histogram, x, y), {})
    ])

    html = TabsHTML()\
        .add_tab('Timeline', results[0])\
        .add_tab('Balance', results[1])\
        .add_tab('Histogram', results[2])\
        .html()

    return html


