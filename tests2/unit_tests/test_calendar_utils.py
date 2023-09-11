import unittest
from datetime import datetime

from mecon2.utils import calendar_utils as cu


class TestCalendarUtils(unittest.TestCase):
    def test_get_closest_past_monday(self):
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 11, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 12, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 13, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 14, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 15, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 16, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 17, 12, 23, 34)),
            datetime(2023, 9, 11, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_past_monday(datetime(2023, 9, 18, 12, 23, 34)),
            datetime(2023, 9, 18, 12, 23, 34)
        )





if __name__ == '__main__':
    unittest.main()
