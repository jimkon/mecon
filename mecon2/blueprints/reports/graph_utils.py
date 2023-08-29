from concurrent.futures import ThreadPoolExecutor

import matplotlib.pyplot as plt
import mpld3


def create_plot(func, x, y, ax):
    return func(x, y, ax)


def create_html_plot(func, x, y, fig_size=(10, 4)):
    fig, ax = plt.subplots(1, 1, figsize=fig_size)
    func(x, y, ax)

    fig.tight_layout()
    # Convert the figure to an HTML-encoded image using mpld3
    html_image = mpld3.fig_to_html(fig)
    return html_image


class ThreadPoolExecutorWrapper:
    def __init__(self):
        self._tasks = []

    def append_task(self, func, args, kwargs):
        self._tasks.append((func, args, kwargs))

    def run(self):
        futures, results = [], []
        with ThreadPoolExecutor(max_workers=len(self._tasks)) as executor:
            for func, args, kwargs in self._tasks:
                future = executor.submit(func, *args, **kwargs)
                futures.append(future)

            for future in futures:
                results.append(future.result())

        return results


def async_multiplot(tasks):
    executor = ThreadPoolExecutorWrapper()
    for func, args, kwargs in tasks:
        executor.append_task(func, args, kwargs)
    results = executor.run()
    return results




