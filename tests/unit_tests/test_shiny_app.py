import unittest


from mecon.app import shiny_app


class TestShinyApp(unittest.TestCase):
    def test__parse_params(self):
        params = shiny_app._parse_params("?start_date=2024-03-01&end_date=2025-02-21&time_unit=month&filter_in_tags=All&filter_out_tags=My transfers")
        assert params == {'end_date': ['2025-02-21'], 'filter_in_tags': ['All'], 'filter_out_tags': ['My transfers'],
                          'start_date': ['2024-03-01'], 'time_unit': ['month']}

    def test__parse_params_ensure_exists_success(self):
        shiny_app._parse_params("?start_date=2024-03-01&end_date=2025-02-21&time_unit=month&filter_in_tags=All&filter_out_tags=My transfers",
                                     ensure_exists=['start_date'])

    def test__parse_params_ensure_exists_fail(self):
        with self.assertRaises(ValueError):
            shiny_app._parse_params("?start_date=2024-03-01&end_date=2025-02-21&time_unit=month&filter_in_tags=All&filter_out_tags=My transfers",
                                        ensure_exists=['another params'])
