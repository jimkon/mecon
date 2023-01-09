from datetime import datetime
import multiprocessing

from mecon.configs import LINEAR_EXECUTION
from mecon.html_pages import html_pages

from mecon.plots import plots
from mecon.statements.tagged_statement import TaggedData, FullyTaggedData
from mecon.tagging.tags import ALL_TAGS, SERVICE_TAGS, TRIPS
from mecon import logs


def iterarate_over_time_units(plot_func, *args, time_units=None, **kwargs):
    if not time_units:
        time_units = ['date', 'week', 'month', 'working month', 'year']

    time_window_tabs = html_pages.TabsHTML()
    for time_unit in time_units:
        page = plot_func(*args, time_unit=time_unit, **kwargs)
        time_window_tabs.add_tab(time_unit.capitalize(), page)
    return time_window_tabs


def create_total_balance_timeline_graph_page(time_unit):
    page = html_pages.HTMLPage()
    plots.total_balance_timeline_fig(time_unit)
    page.add_element(html_pages.ImageHTML.from_matplotlib())
    return page


def create_total_cost_timeline_graph_page(time_unit):
    page = html_pages.HTMLPage()
    plots.total_cost_timeline_fig(time_unit)
    page.add_element(html_pages.ImageHTML.from_matplotlib())
    return page


def create_week_graph(time_unit):
    page = html_pages.HTMLPage()
    plots.plot_multi_tags_timeline(tags=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                                   time_unit=time_unit)
    page.add_element(html_pages.ImageHTML.from_matplotlib())
    return page


def overview_plot_page():
    page = html_pages.HTMLPage()

    page.add_element(iterarate_over_time_units(create_total_balance_timeline_graph_page))

    page.add_element(iterarate_over_time_units(create_total_cost_timeline_graph_page))

    page.add_element(iterarate_over_time_units(create_week_graph,
                                               time_units=['week', 'month', 'year']))

    plots.total_trips_timeline_fig()
    page.add_element(html_pages.ImageHTML.from_matplotlib())

    return page


def create_service_df_tag_description(data):
    df = data.dataframe()
    all_tags = data.all_different_tags
    return f"""<h2>{'General info':*^50}</h2>
    <b>Count</b>: {len(df)}, <b>Total</b>: {-df['amount'].sum():.2f}, <b>min</b>: {-df['amount'].max():.2f}, <b>avg</b>: {-df['amount'].mean():.2f}, <b>max</b>: {-df['amount'].min():.2f}<br> 
    <b>Dates</b> from {df['date'].min()} to {df['date'].min()} <br>
    <b>All tags appeared</b>: {all_tags} <br>
    {df['currency'].value_counts().to_frame().to_html()} <br>
    """


def create_df_table_page(df, title=''):
    return html_pages.SimpleHTMLTable(df, f"{title} ({df.shape[0]} rows)")


def create_plot_tag_stats(tag, time_unit):
    page = html_pages.HTMLPage()
    # plots.plot_tag_stats(tag)
    plots.tag_amount_stats(tag, time_unit)
    page.add_element(html_pages.ImageHTML.from_matplotlib())

    plots.tag_frequency_stats(tag, time_unit)
    page.add_element(html_pages.ImageHTML.from_matplotlib())
    return page


def create_service_report_for_tag(service_tag):
    logs.log_html(f"Creating service report for tag #{service_tag}# ...")
    data = FullyTaggedData.instance().get_rows_tagged_as(service_tag)
    df = data.dataframe()
    if len(df) == 0:
        return html_pages.HTMLPage().add_element(f"<h1>No rows are tagged as '{service_tag}' </h1>")

    page = html_pages.HTMLPage()

    page.add_element(f"<h1>{service_tag}</h1>")
    page.add_element(create_service_df_tag_description(data))

    plot_html = iterarate_over_time_units(create_plot_tag_stats, service_tag)
    page.add_element(plot_html)

    page.add_element(create_df_table_page(df.sort_values('date', ascending=False), title='Full table'))
    page.add_element(create_df_table_page(df.describe(include='all', datetime_is_numeric=True).reset_index(), title=service_tag))

    logs.log_html(f"Service report for tag #{service_tag}# created.")
    return page


@logs.func_execution_logging
def create_services_reports():
    tag_names = [tagger.tag_name for tagger in SERVICE_TAGS]

    if LINEAR_EXECUTION:
        return [(tag, create_service_report_for_tag(tag)) for tag in tag_names]
    else:
        with multiprocessing.Pool() as pool:
            service_reports = pool.map(create_service_report_for_tag, tag_names)

        return [(tag, rep_html_page) for tag, rep_html_page in zip(tag_names, service_reports)]


@logs.func_execution_logging
def create_trips_reports():
    tag_names = [tagger.tag_name for tagger in TRIPS]

    if LINEAR_EXECUTION:
        return [(tag, create_service_report_for_tag(tag)) for tag in tag_names]
    else:
        with multiprocessing.Pool() as pool:
            service_reports = pool.map(create_service_report_for_tag, tag_names)

        return [(tag, rep_html_page) for tag, rep_html_page in zip(tag_names, service_reports)]


@logs.func_execution_logging
def create_report():
    logs.log_html("Creating full report...")
    report_html = html_pages.HTMLPage()
    report_html.add_element(f"<title>Report ({datetime.now()})</title>")

    tabs_html = html_pages.TabsHTML()
    tabs_html.add_tab("Balance", overview_plot_page())

    services_tabs = html_pages.TabsHTML()
    for tag, rep_html_page in create_services_reports():
        services_tabs.add_tab(tag, rep_html_page)
    tabs_html.add_tab("Services", services_tabs)

    trips_tabs = html_pages.TabsHTML()
    plots.total_trips_timeline_fig()
    trips_tabs.add_tab('Overview', html_pages.ImageHTML.from_matplotlib())
    for tag, rep_html_page in create_trips_reports():
        trips_tabs.add_tab(tag, rep_html_page)
    tabs_html.add_tab("Trips", trips_tabs)


    report_html.add_element(tabs_html)
    report_html.save('report.html')
    logs.log_html(f"Full report stored in 'report.html'. Size:{len(report_html.html())} ({len(report_html.html())/1024**2:.1f}MB)")


if __name__ == "__main__":
    create_report()
