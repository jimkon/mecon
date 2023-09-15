import unittest
from datetime import datetime

from mecon2.utils import calendar_utils as cu


class TestCalendarUtils(unittest.TestCase):
    def test_datetime_to_date_id(self):
        self.assertEqual(cu.datetime_to_date_id(datetime(2023, 9, 11, 12, 23, 34)), 20230911)
        self.assertEqual(cu.datetime_to_date_id(datetime(2023, 1, 1, 0, 23, 1)), 20230101)
        self.assertEqual(cu.datetime_to_date_id(datetime(2023, 10, 31, 12, 23, 34)), 20231031)
        self.assertEqual(cu.datetime_to_date_id(datetime(2023, 12, 31, 12, 23, 34)), 20231231)

    def test_datetime_to_date_id_str(self):
        self.assertEqual(cu.datetime_to_date_id_str(datetime(2023, 9, 11, 12, 23, 34)), "20230911")
        self.assertEqual(cu.datetime_to_date_id_str(datetime(2023, 1, 1, 0, 23, 1)), "20230101")
        self.assertEqual(cu.datetime_to_date_id_str(datetime(2023, 10, 31, 12, 23, 34)), "20231031")
        self.assertEqual(cu.datetime_to_date_id_str(datetime(2023, 12, 31, 12, 23, 34)), "20231231")


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

    def test_date_floor(self):
        self.assertEqual(
            cu.date_floor(datetime(2023, 9, 12, 12, 23, 34), 'day'),
            datetime(2023, 9, 12, 0, 0, 0)
        )
        self.assertEqual(
            cu.date_floor(datetime(2023, 9, 11, 12, 23, 34), 'week'),
            datetime(2023, 9, 11, 0, 0, 0)
        )
        self.assertEqual(
            cu.date_floor(datetime(2023, 9, 12, 12, 23, 34), 'month'),
            datetime(2023, 9, 1, 0, 0, 0)
        )
        self.assertEqual(
            cu.date_floor(datetime(2023, 9, 12, 12, 23, 34), 'year'),
            datetime(2023, 1, 1, 0, 0, 0)
        )


if __name__ == '__main__':
    unittest.main()
