from datetime import datetime
import multiprocessing

from mecon.html_pages import html_pages

from mecon.plots import plots
from mecon.statements.tagged_statement import TaggedData, FullyTaggedData
from mecon.tagging.tags import ALL_TAGS, SERVICE_TAGS
from mecon import logs


def balance_plot_page():
    plots.total_balance_timeline_fig(show=False)
    return html_pages.ImageHTML.from_matplotlib()


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


def create_service_report_for_tag(service_tag):
    logs.log_html(f"Creating service report for tag #{service_tag}# ...")
    data = FullyTaggedData.instance().get_rows_tagged_as(service_tag)
    df = data.dataframe()
    if len(df) == 0:
        return html_pages.HTMLPage().add_element(f"<h1>No rows are tagged as '{service_tag}' </h1>")

    page = html_pages.HTMLPage()

    page.add_element(f"<h1>{service_tag}</h1>")
    page.add_element(create_service_df_tag_description(data))

    plots.plot_tag_stats(service_tag, show=False)
    page.add_element(html_pages.ImageHTML.from_matplotlib())

    page.add_element(create_df_table_page(df.sort_values('date', ascending=False), title='Full table'))
    page.add_element(create_df_table_page(df.describe(include='all', datetime_is_numeric=True).reset_index(), title=service_tag))

    logs.log_html(f"Service report for tag #{service_tag}# created.")
    return page


@logs.func_execution_logging
def create_service_reports():
    # linear executions
    # return [(tag, create_service_report_for_tag(tag)) for tag in [tagger.tag_name for tagger in SERVICE_TAGS]]

    tag_names = [tagger.tag_name for tagger in SERVICE_TAGS]
    with multiprocessing.Pool() as pool:
        service_reports = pool.map(create_service_report_for_tag, tag_names)

    return [(tag, rep_html_page) for tag, rep_html_page in zip(tag_names, service_reports)]


@logs.func_execution_logging
def create_report():
    logs.log_html("Creating full report...")
    report_html = html_pages.HTMLPage()
    report_html.add_element(f"<title>Report ({datetime.now()})</title>")

    tabs_html = html_pages.TabsHTML()
    tabs_html.add_tab("Balance", balance_plot_page())

    service_tabs = html_pages.TabsHTML()
    for tag, rep_html_page in create_service_reports():
        service_tabs.add_tab(tag, rep_html_page)
    tabs_html.add_tab("Services", service_tabs)

    report_html.add_element(tabs_html)
    report_html.save('report.html')
    logs.log_html(f"Full report stored in 'report.html'. Size:{len(report_html.html())} ({len(report_html.html())/1024**2:.1f}MB)")


if __name__ == "__main__":
    create_report()
