import datetime

from flask import Flask, url_for, request, redirect
from markupsafe import escape  # https://flask.palletsprojects.com/en/2.2.x/quickstart/#html-escaping
import redis

import mecon.produce_report as reports

app = Flask(__name__)
cache = None
# cache = redis.StrictRedis(host='redis', port=6379, db=0)
# print("REDIS ping:", cache.ping())
_start_datetime = datetime.datetime.now()


# next https://flask.palletsprojects.com/en/2.2.x/quickstart/#http-methods

@app.route('/')
def main():
    html = f"""
    <h1>Mecon app</h1>
    <p>Deployed: {_start_datetime}</p>
    Links:
    <ul>
      <li><a href={url_for('overview')}>Overview</a></li>
      <li><a href={url_for('tags')}>Tags</a></li>
      <li><a href={url_for('edit_query_get', tag_name='new_tag', tag_json_str='[{}]')}>Edit tag</a></li>
    </ul>
    """
    return html


@app.route('/overview')
def overview():
    overview_page = reports.overview_page().html()
    html = f"""
        <a href={url_for('main')}>Home</a>
        {overview_page}
        """
    return html


@app.route('/tags/')
def tags():
    import json
    from mecon.tagging.tags import ALL_TAGS
    from mecon.tagging.dict_tag import DictTag

    urls_for_tag_reports = {_tag.tag_name: url_for('tag_report', tag=_tag.tag_name) for _tag in ALL_TAGS}
    urls_for_tag_tables = {_tag.tag_name: url_for('query_data',
                                                  tag_name=_tag.tag_name,
                                                  tag_json_str=json.dumps(_tag.json)) if isinstance(_tag, DictTag) else ''
                           for _tag in ALL_TAGS}

    html = f"""
    <a href={url_for('main')}>Home</a>
    <h1>Tags</h1>
    <ul>
        <li>{'</li><li>'.join([
        '<a href=http://localhost:5000/'+urls_for_tag_reports[tag.tag_name]+'>'+tag.tag_name+'</a>, '+
        '<a href=http://localhost:5000/'+urls_for_tag_tables[tag.tag_name]+'>'+('Table' if urls_for_tag_tables[tag.tag_name]!='' else '')+'</a>'
        for tag 
        in ALL_TAGS
    ])}</li>
    </ul>
    """
    return html


@app.route('/tags/<tag>')
def tag_report(tag):
    tag_report_html = reports.create_report_for_tag(tag).html()
    html = f"""
    <a href={url_for('main')}>Home</a>
    <h1>Tag "{escape(tag)}"</h1>
    {tag_report_html}
    """
    return html


@app.route('/data/query/table/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def query_data(tag_name, tag_json_str):
    import json
    from mecon.statements.tagged_statement import FullyTaggedData
    from mecon.tagging.dict_tag import DictTag

    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    data = FullyTaggedData.instance().apply_taggers(tagger).get_rows_tagged_as(tag_name)
    tag_table = reports.create_df_table_page(data.dataframe()).html()
    html = f"""
        <a href={url_for('main')}>Home</a>
        <h1>Tag "{escape(tag_name)}"</h1>
        {tag_table}
        """
    return html


@app.get('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_get(tag_name, tag_json_str):
    import json
    from mecon.statements.tagged_statement import FullyTaggedData
    from mecon.tagging.dict_tag import DictTag

    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    data = FullyTaggedData.instance().copy().apply_taggers(tagger).get_rows_tagged_as(tag_name)
    tag_table = reports.create_df_table_page(data.dataframe()).html()
    html = f"""
    <a href={url_for('main')}>Home</a>
    <h1>Edit a tag</h1>
        <form method="POST">
          <label for="tag_name">Tag name</label>
          <input type="text" id="tag_name" name="tag_name_input" value={tag_name}><br><br>
          <label for="query_text">Query JSON</label>
          <textarea id="query_text" name="query_text_input" rows="4" cols="50">{tag_json_str}</textarea><br><br>
          <a href={url_for('edit_query_get', tag_name='new_tag', tag_json_str='[{}]')}><input type="button" value="Reset"></a>
          <input type="submit" value="Refresh">
        </form>
        <h1>Tag "{escape(tag_name)}"</h1>
        {tag_table}
        """
    return html


@app.post('/tag/edit/tag_name=<tag_name>:tag_json_str=<tag_json_str>')
def edit_query_post(tag_name, tag_json_str):
    new_tag_name = request.form['tag_name_input']
    new_query_str = request.form['query_text_input']
    return redirect(url_for('edit_query_get', tag_name=new_tag_name, tag_json_str=new_query_str))



if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
