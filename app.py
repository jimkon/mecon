import datetime
import json
from functools import wraps

from flask import Flask, url_for, request, redirect, render_template
import redis

import mecon.produce_report as reports

app = Flask(__name__)
# cache = None
cache = redis.StrictRedis(host='redis', port=6379, db=0)
# print("REDIS ping:", cache.ping())
DEPLOYMENT_DATETIME = datetime.datetime.now()


def cached_html(func):
    """
    Decorator that caches the results of the function call.

    We use Redis in this example, but any cache (e.g. memcached) will work.
    We also assume that the result of the function can be seralized as JSON,
    which obviously will be untrue in many situations. Tweak as needed.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Generate the cache key from the function's arguments.
        key_parts = [func.__name__] + list(args)
        key = 'cached_html:'+'-'.join(key_parts)+json.dumps(kwargs)
        app.logger.info(f'{key=}')
        result = cache.get(key)

        if result is None:
            # Run the function and cache the result for next time.
            html_page = func(*args, **kwargs)
            cache.set(key, html_page)
            app.logger.info(f'{key=} SET')
        else:
            # Skip the function entirely and use the cached value instead.
            app.logger.info(f'{key=} FOUND')
            html_page = result.decode('utf-8')

        return html_page

    return wrapper


def reset_cache():
    for key in cache.scan_iter("cached_html:*"):
        cache.delete(key)


@app.route('/')
def main():
    return render_template('index.html', **globals())


@app.route('/overview')
def overview():
    overview_page = reports.overview_page().html()
    return render_template('overview.html', overview_page=overview_page)


@app.route('/tags/')
def tags():
    import json
    from mecon.tagging.tags import ALL_TAGS
    from mecon.tagging.dict_tag import DictTag

    tag_names = [tag.tag_name for tag in ALL_TAGS]
    urls_for_tag_reports = [url_for('tag_report', tag=_tag.tag_name) for _tag in ALL_TAGS]
    urls_for_tag_tables = [url_for('query_data',
                                  tag_name=_tag.tag_name,
                                  tag_json_str=json.dumps(_tag.json)) if isinstance(_tag, DictTag) else None
                           for _tag in ALL_TAGS]
    links = list(zip(tag_names, urls_for_tag_reports, urls_for_tag_tables))
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('tags_menu.html', **kwargs)


@app.route('/tags/<tag>')
@cached_html
def tag_report(tag):
    tag_report_page = reports.create_report_for_tag(tag).html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('tag_report.html', **kwargs)


@app.route('/data/query/table/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def query_data(tag_name, tag_json_str):
    import json
    from mecon.statements.tagged_statement import FullyTaggedData
    from mecon.tagging.dict_tag import DictTag

    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    data = FullyTaggedData.instance().apply_taggers(tagger).get_rows_tagged_as(tag_name)
    table = data.dataframe().to_html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('table.html', **kwargs)


@app.get('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_get(tag_name, tag_json_str):
    import json
    from mecon.statements.tagged_statement import FullyTaggedData
    from mecon.tagging.dict_tag import DictTag

    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    data = FullyTaggedData.instance().copy().apply_taggers(tagger).get_rows_tagged_as(tag_name)
    df = data.dataframe()
    table = df.to_html()
    number_of_rows = len(df)
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('query_edit.html', **kwargs)


@app.post('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_post(tag_name, tag_json_str):
    new_tag_name = request.form['tag_name_input']
    new_query_str = request.form['query_text_input']
    return redirect(url_for('edit_query_get', tag_name=new_tag_name, tag_json_str=new_query_str))


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
