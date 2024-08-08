import unittest
from unittest import mock

import requests

import flask

from mecon.ui_elements import app


class UtilsTestCase(unittest.TestCase):
    def test_call_graph_function_by_name(self):
        app.graphs.a_graph_function = mock.MagicMock()
        app._call_graph_function_by_name('a_graph_function',
                                         time=[0, 1, 2],
                                         amount=[0, 100, 200],
                                         example_flag=True,
                                         string_parameter='example')
        app.graphs.a_graph_function.assert_has_calls(
            [mock.call(time=[0, 1, 2], amount=[0, 100, 200], example_flag=True, string_parameter='example')]
        )


# class GraphsBlueprintTestCase(unittest.TestCase):
#     # def setUp(self) -> None:
#     #     self.app = app.create_app()
#     #     self.app.run(host='127.0.0.1', port=1234)
#
#     def test_general_graph_func_call(self):
#         # response = requests.get('127.0.0.1:1234/graphs/test')
#         # ...
#         # with mock.patch('mecon.app.blueprints.reports.graphs') as mock_graphs_module:
#         with mock.patch.object(app, "request") as mock_request:
#             mock_request.args = {'a':1}
#             app.graphs.a_graph_function = mock.MagicMock()
#             # mock_graphs_module.a_graph_function = mock.MagicMock()
#             res, status_code = app.general_graph_func_call('a_graph_function')
#             app.graphs.a_graph_function.assert_called_once()
