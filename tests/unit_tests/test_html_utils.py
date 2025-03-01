from datetime import date
from unittest import TestCase

from mecon.utils import html

class Test(TestCase):
    def test_build_url(self):
        # Case 1: No params
        self.assertEqual(html.build_url("https://example.com"), "https://example.com")

        # Case 2: Single param
        self.assertEqual(html.build_url("https://example.com", {"key1": "value1"}), "https://example.com?key1=value1")

        # Case 3: Multiple params
        self.assertEqual(html.build_url("https://example.com", {"key1": "value1", "key2": "value2"}),
               "https://example.com?key1=value1&key2=value2")

        # Case 4: List values - Collapse
        self.assertEqual(html.build_url("https://example.com", {"key": ["val1", "val2"]}), "https://example.com?key=val1,val2")

        # Case 5: List values - First only
        self.assertEqual(html.build_url("https://example.com", {"key": ["val1", "val2"]}, list_handling="first"),
               "https://example.com?key=val1")

        # Case 6: List values - No handling (keeps the list as is, but not valid for URLs)
        self.assertEqual(html.build_url("https://example.com", {"key": ["val1", "val2"]}, list_handling="none"),
               "https://example.com?key=['val1', 'val2']")

        # Case 7: Base URL with trailing slash
        self.assertEqual(html.build_url("https://example.com/", {"key": "value"}), "https://example.com?key=value")

        # Case 8: Empty params dictionary
        self.assertEqual(html.build_url("https://example.com", {}), "https://example.com")

        self.assertEqual(html.build_url("https://example.com"), "https://example.com")

        # Case 2: Dates should be converted properly
        self.assertEqual(html.build_url("https://example.com", {"date": date(2024, 3, 1)}), "https://example.com?date=2024-03-01")

        # Case 3: Tuple behaves like a list
        self.assertEqual(html.build_url("https://example.com", {"key": ("val1", "val2")}), "https://example.com?key=val1,val2")

        # Case 4: List handling - First value only
        self.assertEqual(html.build_url("https://example.com", {"key": ["val1", "val2"]}, list_handling="first"),
               "https://example.com?key=val1")

        # Case 5: Empty tuple should be removed
        self.assertEqual(html.build_url("https://example.com", {"key": ()}), "https://example.com")

        self.assertEqual(html.build_url("https://example.com/", {
            'start_date': date(2024, 3, 1),
            'end_date': date(2025, 2, 21),
            'time_unit': 'month',
            'filter_in_tags': ('All', 'Accomodation'),  # Tuple should behave like a list
            'filter_out_tags': ()  # Empty tuple should be removed
        }), "https://example.com?start_date=2024-03-01&end_date=2025-02-21&time_unit=month&filter_in_tags=All,Accomodation")

