import matplotlib.pyplot as plt
import mpld3

# plt.style.use('bmh')


def general_cost_stats_html_img(x, y):
    x, y = x[::-1], y[::-1]
    fig, axes = plt.subplots(1, 3, figsize=(10, 4))

    timeline_ax, balance_ax, hist_ax = axes
    timeline_ax.plot(x, y, '.')
    timeline_ax.invert_yaxis()
    timeline_ax.set_title('Timeline')
    # timeline_ax.set_xticklabels(x.to_list(), rotation=45)

    balance_ax.plot(x, y.cumsum(), '-.')
    # balance_ax.invert_yaxis()
    balance_ax.set_title('Balance')

    hist_ax.hist(y, bins='auto')
    hist_ax.invert_xaxis()
    hist_ax.set_title('Histogram')

    fig.tight_layout()
    # Convert the figure to an HTML-encoded image using mpld3
    html_image = mpld3.fig_to_html(fig)
    return html_image