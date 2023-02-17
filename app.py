import datetime
import json
from functools import wraps

from flask import Flask, url_for, request, redirect, render_template, flash
import redis

import mecon.produce_report as reports
from mecon.data import DataObject
from mecon.tagging.dict_tag import DictTag

app = Flask(__name__)
app.secret_key = b'secret_key'
# cache = None
cache = redis.StrictRedis(host='redis', port=6379, db=0)
# print("REDIS ping:", cache.ping())
DEPLOYMENT_DATETIME = datetime.datetime.now()
data_object = DataObject.local_data()


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
    all_tags = data_object.tags['found']

    tag_info = [{
        'name': tag.tag_name,
        'is_json_tag': issubclass(tag.__class__, DictTag)
    } for tag in all_tags]

    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('tags_menu.html', **kwargs)


@app.route('/tags/<tag_name>')
def tag_page(tag_name):
    is_json_tag = any([issubclass(tag.__class__, DictTag) for tag in data_object.tags['found']])
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('tag_page.html', **kwargs)


@app.route('/tags/report/<tag_name>')
@cached_html
def tag_report(tag_name):
    tag_report_page = reports.create_report_for_tag(tag_name).html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('tag_report.html', **kwargs)


@app.route('/tags/table/<tag_name>')
def tag_table(tag_name):
    tagged_data = data_object.transactions['tagged_transactions']
    data = tagged_data.get_rows_tagged_as(tag_name)
    df = data.dataframe()
    table_html = df.to_html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('table.html', **kwargs)

@app.route('/tags/edit/<tag_name>')
def tag_edit(tag_name):
    tag_json = [tag.json for tag in data_object.tags['found'] if issubclass(tag.__class__, DictTag)]
    tag_json_str = json.dumps(tag_json, indent=4)

    tagged_data = data_object.transactions['tagged_transactions']
    data = tagged_data.get_rows_tagged_as(tag_name)
    df = data.dataframe()
    table_html = df.to_html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('table_edit.html', **kwargs)


@app.route('/data/query/table/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def query_data(tag_name, tag_json_str):
    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    tagged_data = data_object.transactions['tagged_transactions']
    data = tagged_data.apply_taggers(tagger).get_rows_tagged_as(tag_name)
    table_html = data.dataframe().to_html()
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('table.html', **kwargs)


@app.get('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_get(tag_name, tag_json_str):
    import json
    from mecon.tagging.dict_tag import DictTag

    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    tagged_data = data_object.transactions['tagged_transactions']
    data = tagged_data.apply_taggers(tagger).get_rows_tagged_as(tag_name)
    df = data.dataframe()
    table_html = df.to_html()
    number_of_rows = len(df)
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('query_edit.html', **kwargs)


@app.post('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_post(tag_name, tag_json_str):
    new_tag_name = request.form['tag_name_input']
    new_query_str = request.form['query_text_input']
    return redirect(url_for('edit_query_get', tag_name=new_tag_name, tag_json_str=new_query_str))


@app.route('/statement/upload/<bank_name>/', methods=['GET', 'POST'])
def upload_statement(bank_name):
    if request.method == 'POST':
        if 'file' not in request.files:
            message = f"No file part"
            flash(message)
            reset_cache()
            return render_template('upload_file.html',
                                   file_info=bank_name,
                                   message=message)
        file = request.files['file']
        if file.filename == '':
            message = f"No selected file"
            flash(message)
            reset_cache()
            return render_template('upload_file.html',
                                   file_info=bank_name,
                                   message=message)
        message = f"Uploading files in not implemented yet. File {file.filename} was not uploaded."
        flash(message)
        reset_cache()
        return render_template('upload_file.html',
                               file_info=bank_name,
                               message=message)

    file_info = bank_name
    kwargs = globals().copy()
    kwargs.update(locals())
    return render_template('upload_file.html', **kwargs)



if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
