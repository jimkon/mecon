from flask import Blueprint

main = Blueprint('main', __name__)


@main.route('/')
def index():
    return 'Index'


@main.route('/test')
def test():
    from mecon2.db import TagsDBAccessor

    tags = TagsDBAccessor()
    tags.set_tag('test_tag1', "{'a': 1}")
    t = tags.get_tag('test_tag1')
    return f"{t=}"
