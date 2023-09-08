from concurrent.futures import ThreadPoolExecutor

import matplotlib.pyplot as plt
import mpld3

from mecon2.utils import html_pages

# plt.style.use('bmh')
# plt.style.use('Solarize_Light2')
# plt.style.use('seaborn-v0_8-whitegrid')


def create_plot(func, x, y, ax):
    return func(x, y, ax)


def create_html_plot(func, transactions, fig_size=(10, 4)):
    fig, ax = plt.subplots(1, 1, figsize=fig_size)
    func(transactions, ax)

    fig.tight_layout()
    # Convert the figure to an HTML-encoded image using mpld3
    # html_image = mpld3.fig_to_html(fig)
    html_image = html_pages.ImageHTML.from_matplotlib()
    return html_image


class ThreadPoolExecutorWrapper:
    def __init__(self):
        self._tasks = []

    def append_task(self, func, args):
        self._tasks.append((func, args))

    def run(self):
        futures, results = [], []
        with ThreadPoolExecutor(max_workers=len(self._tasks)) as executor:
            for func, args in self._tasks:
                future = executor.submit(func, *args)
                futures.append(future)

            for future in futures:
                results.append(future.result())

        return results


def async_multiplot(tasks):
    executor = ThreadPoolExecutorWrapper()
    for func, args in tasks:
        executor.append_task(func, args)
    results = executor.run()
    return results




