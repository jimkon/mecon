from datetime import datetime
import multiprocessing

from mecon.html_pages import html_pages

from mecon.plots import plots
from mecon.statements.tagged_statement import TaggedData
from mecon.tagging.tags import ALL_TAGS, SERVICE_TAGS

df = TaggedData.fully_tagged_data().dataframe()
df.to_csv("fully_tagged_statement.csv", index_label=None)


def balance_plot_page():
    plots.total_balance_timeline_fig(show=False)
    return html_pages.ImageHTML.from_matplotlib()


def create_service_df_tag_description(df):
    return f"""<h2>{'General info':*^50}</h2>
    <b>Count</b>: {len(df)}, <b>Total</b>: {-df['amount'].sum():.2f}, <b>min</b>: {-df['amount'].max():.2f}, <b>avg</b>: {-df['amount'].mean():.2f}, <b>max</b>: {-df['amount'].min():.2f}<br> 
    <b>Dates</b> from {df['date'].min()} to {df['date'].min()} <br>
    <b>All tags appeared</b>: # <br>
    {df['currency'].value_counts().to_frame().to_html()} <br>
    """


def create_df_table_page(df, title=''):
    return html_pages.SimpleHTMLTable(df, f"{title} ({df.shape[0]} rows)")


def create_service_report_for_tag(service_tag):
    df = TaggedData.fully_tagged_data().get_rows_tagged_as(service_tag).dataframe()
    if len(df) == 0:
        return html_pages.HTMLPage().add_element(f"<h1>No rows are tagged as '{service_tag}' </h1>")

    page = html_pages.HTMLPage()

    page.add_element(f"<h1>{service_tag}</h1>")
    page.add_element(create_service_df_tag_description(df))

    plots.plot_tag_stats(service_tag, show=False)
    page.add_element(html_pages.ImageHTML.from_matplotlib())

    page.add_element(create_df_table_page(df.sort_values('date', ascending=False), title='Full table'))
    page.add_element(create_df_table_page(df.describe(include='all', datetime_is_numeric=True).reset_index(), title=service_tag))

    return page


def create_service_reports():
    # linear executions
    # for tag in [tagger.tag_name for tagger in SERVICE_TAGS]:
    #     report_html.add_tab(tag, create_service_report(tag))

    tag_names = [tagger.tag_name for tagger in SERVICE_TAGS]
    with multiprocessing.Pool() as pool:
        service_reports = pool.map(create_service_report_for_tag, tag_names)

    return [(tag, rep_html_page) for tag, rep_html_page in zip(tag_names, service_reports)]


def create_report():
    report_html = html_pages.HTMLPage()
    report_html.add_element(f"<title>Report ({datetime.now()})</title>")

    tabs_html = html_pages.TabsHTML()
    tabs_html.add_tab("Balance", balance_plot_page())

    for tag, rep_html_page in create_service_reports():
        tabs_html.add_tab(tag, rep_html_page)

    report_html.add_element(tabs_html)
    report_html.save('report.html')


if __name__ == "__main__":
    import timeit
    print(timeit.timeit(create_report, number=1))
