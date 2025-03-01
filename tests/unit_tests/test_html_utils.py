from unittest import TestCase

from mecon.utils import html

class Test(TestCase):
    def test_build_url(self):
        # Case 1: No params
        assert html.build_url("https://example.com") == "https://example.com"

        # Case 2: Single param
        assert html.build_url("https://example.com", {"key1": "value1"}) == "https://example.com?key1=value1"

        # Case 3: Multiple params
        assert html.build_url("https://example.com", {"key1": "value1", "key2": "value2"}) == \
               "https://example.com?key1=value1&key2=value2"

        # Case 4: List values
        assert html.build_url("https://example.com", {"key": ["val1", "val2"]}) == "https://example.com?key=val1%2Cval2"

        # Case 5: Base URL with trailing slash
        assert html.build_url("https://example.com/", {"key": "value"}) == "https://example.com?key=value"

        # Case 6: Empty params dictionary
        assert html.build_url("https://example.com", {}) == "https://example.com"

