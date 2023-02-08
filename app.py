import datetime

from flask import Flask
from markupsafe import escape  # https://flask.palletsprojects.com/en/2.2.x/quickstart/#html-escaping
import redis

import mecon.produce_report as reports

app = Flask(__name__)
cache = None
# cache = redis.StrictRedis(host='redis', port=6379, db=0)
# print("REDIS ping:", cache.ping())
_start_datetime = datetime.datetime.now()

@app.route('/')
def main():
    html = f"""
    <h1>Mecon app</h1>
    <p>Deployed: {_start_datetime}</p>
    Links:
    <ul>
      <li><a href=http://localhost:5000/overview>Overview</a></li>
      <li><a href=http://localhost:5000/tags>Tags</a></li>
    </ul>
    """
    return html


@app.route('/overview')
def overview():
    overview_page = reports.overview_page().html()
    html = f"""
        <a href=http://localhost:5000/>Home</a>
        {overview_page}
        """
    return html


@app.route('/tags/')
def tags():
    from mecon.tagging.tags import ALL_TAGS
    html = f"""
    <a href=http://localhost:5000/>Home</a>
    <h1>Tags</h1>
    <ul>
        <li>{'</li><li>'.join(['<a href=http://localhost:5000/tags/'+_tag.tag_name+'>'+_tag.tag_name+'</a>' for _tag in ALL_TAGS])}</li>
    </ul>
    """
    return html


@app.route('/tags/<tag>')
def tag(tag):
    tag_report = reports.create_report_for_tag(tag).html()
    html = f"""
    <a href=http://localhost:5000/>Home</a>
    <h1>Tag "{escape(tag)}"</h1>
    {tag_report}
    """
    return html


@app.route('/tags/json/<name_and_tag_json_str>')
def calc_tag(name_and_tag_json_str):
    import json
    from mecon.statements.tagged_statement import FullyTaggedData
    from mecon.tagging.dict_tag import DictTag

    tag_name = name_and_tag_json_str.split(':')[0]
    tag_json_str = name_and_tag_json_str[len(tag_name)+1:]
    tag_json = json.loads(tag_json_str)
    tagger = DictTag(tag_name, tag_json)
    data = FullyTaggedData.instance().apply_taggers(tagger).get_rows_tagged_as(tag_name)
    tag_table = reports.create_df_table_page(data.dataframe()).html()
    html = f"""
        <a href=http://localhost:5000/>Home</a>
        <h1>Tag "{escape(tag_name)}"</h1>
        {tag_table}
        """
    return html


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
