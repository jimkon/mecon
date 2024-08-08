from typing import List

import pandas as pd
from flask import Flask, Blueprint, request

from mecon.ui_elements import graphs


# tables_bp = Blueprint('tables', __name__, template_folder='templates')
graphs_bp = Blueprint('graphs', __name__, template_folder='templates')


def create_app():
    app = Flask(__name__)
    # app.register_blueprint(tables_bp, url_prefix='/tables')
    app.register_blueprint(graphs_bp, url_prefix='/graphs')

    return app


def _call_graph_function_by_name(func_name, **kwargs):
    # TODO maybe restict which of the functions can be called? i e the ones ending with _graph_html
    result_html = graphs.lines_graph_html(**kwargs)
    #
    # try:
    #     # graph_func = getattr(graphs, func_name)
    #     # result_html = graph_func(**kwargs)
    #     breakpoint()
    #     result_html = graphs.lines_graph_html(**kwargs)
    # except AttributeError:
    #     result_html = f"""<p>Graph function '{func_name}' does not exist.</p>"""
    # except Exception as e:
    #     result_html = f"""<p>Something went wrong while calling {func_name} with {kwargs}</p><br><p>Exception {e}</p>"""
    # return result_html

@graphs_bp.get('/<graph_func_name>')
def general_graph_func_call(graph_func_name: str):
    result_html = _call_graph_function_by_name(graph_func_name, **request.args.to_dict(flat=False))
    return result_html, 200


# @graphs_bp.get('/amount_and_freq')
# def amount_and_freq_timeline():
#     time = pd.Series(request.args['time'])
#     amount = pd.Series(request.args['amount'])
#     freq = pd.Series(request.args['freq'])
#     grouping = request.args['grouping'] if 'grouping' in request.args else 'month'
