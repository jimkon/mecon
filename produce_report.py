import pandas as pd

from mecon.html_pages import html_pages

from mecon.plots import plots
from mecon.statements.tagged_statement import TaggedStatement

df = TaggedStatement.fully_tagged_statement().dataframe()
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


def create_service_report(service_tag):
    df = TaggedStatement.fully_tagged_statement().get_tagged_rows(service_tag)
    if len(df) == 0:
        return f"<h1>No rows are tagged as '{service_tag}' </h1>"

    page = html_pages.HTMLPage()

    page.add_element(f"<h1>{service_tag}</h1>")
    page.add_element(create_service_df_tag_description(df))

    plots.plot_tag_stats(service_tag, show=False)
    page.add_element(html_pages.ImageHTML.from_matplotlib())

    page.add_element(create_df_table_page(df.sort_values('date', ascending=False), title='Full table'))
    page.add_element(create_df_table_page(df.describe(include='all').reset_index(), title=service_tag))

    return page


def create_report():
    report_html = html_pages.TabsHTML()

    report_html.add_tab("Balance", balance_plot_page())

    for tag in ['TFL (excluding cycle hire)', 'Giffgaff', 'Spotify', 'SantaderBikes',
                'Therapy', 'Super Market', 'Flight tickets', 'Rent', 'Bills', 'Online orders',
                'Too Good To Go', 'Cash']:
        report_html.add_tab(tag, create_service_report(tag))

    report_html.save('report.html')


if __name__ == "__main__":
    create_report()

