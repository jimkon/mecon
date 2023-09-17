import unittest
from datetime import datetime

from pandas import Timestamp

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


class TestDateRange(unittest.TestCase):
    def test_date_range_day(self):
        self.assertEqual(
            cu.date_range(
                datetime(2023, 9, 1, 12, 23, 34),
                datetime(2023, 9, 7, 12, 23, 34),
                step='day').tolist(),
            [
                Timestamp('2023-09-01 12:23:34'),
                Timestamp('2023-09-02 12:23:34'),
                Timestamp('2023-09-03 12:23:34'),
                Timestamp('2023-09-04 12:23:34'),
                Timestamp('2023-09-05 12:23:34'),
                Timestamp('2023-09-06 12:23:34'),
                Timestamp('2023-09-07 12:23:34')
            ]
        )

    def test_date_range_week(self):
        self.assertEqual(
            cu.date_range(
                datetime(2023, 9, 1, 12, 23, 34),
                datetime(2023, 9, 17, 12, 23, 34),
                step='week').tolist(),
            [
                Timestamp('2023-09-01 12:23:34'),
                Timestamp('2023-09-08 12:23:34'),
                Timestamp('2023-09-15 12:23:34'),
            ]
        )

    def test_date_range_month(self):
        self.assertEqual(
            cu.date_range(
                datetime(2023, 9, 1, 12, 23, 34),
                datetime(2024, 2, 1, 12, 23, 34),
                step='month').tolist(),
            [
                Timestamp('2023-09-01 12:23:34'),
                Timestamp('2023-10-01 12:23:34'),
                Timestamp('2023-11-01 12:23:34'),
                Timestamp('2023-12-01 12:23:34'),
                Timestamp('2024-01-01 12:23:34'),
                Timestamp('2024-02-01 12:23:34'),
            ]
        )
        self.assertEqual(
            cu.date_range(
                datetime(2023, 9, 2, 12, 23, 34),
                datetime(2024, 2, 17, 12, 23, 34),
                step='month').tolist(),
            [
                Timestamp('2023-09-02 12:23:34'),
                Timestamp('2023-10-02 12:23:34'),
                Timestamp('2023-11-02 12:23:34'),
                Timestamp('2023-12-02 12:23:34'),
                Timestamp('2024-01-02 12:23:34'),
                Timestamp('2024-02-02 12:23:34'),
            ]
        )

    def test_date_range_year(self):
        self.assertEqual(
            cu.date_range(
                datetime(2022, 9, 2, 12, 23, 34),
                datetime(2024, 2, 17, 12, 23, 34),
                step='year').tolist(),
            [
                Timestamp('2022-09-02 12:23:34'),
                Timestamp('2023-09-02 12:23:34'),
                Timestamp('2024-09-02 12:23:34'),
            ]
        )

    def test_date_range_start_date_greater_than_end_date(self):
        self.assertEqual(
            cu.date_range(
                datetime(2023, 9, 17, 12, 23, 34),
                datetime(2023, 9, 1, 12, 23, 34),
                step='week').tolist(),
            [
                Timestamp('2023-09-01 12:23:34'),
                Timestamp('2023-09-08 12:23:34'),
                Timestamp('2023-09-15 12:23:34'),
            ]
        )

    def test_date_range_invalid_step_value(self):
        with self.assertRaises(ValueError):
            self.assertEqual(
                cu.date_range(
                    datetime(2023, 9, 17, 12, 23, 34),
                    datetime(2023, 9, 1, 12, 23, 34),
                    step='not a valid step value').tolist(),
                [
                    Timestamp('2023-09-01 12:23:34'),
                    Timestamp('2023-09-08 12:23:34'),
                    Timestamp('2023-09-15 12:23:34'),
                ]
            )


class TestDateRangeGroupBeginning(unittest.TestCase):
    def test_date_range_group_beginning_day(self):
        self.assertEqual(
            cu.date_range_group_beginning(
                datetime(2023, 9, 1, 12, 23, 34),
                datetime(2023, 9, 7, 12, 23, 34),
                step='day').tolist(),
            [
                Timestamp('2023-09-01 00:00:00'),
                Timestamp('2023-09-02 00:00:00'),
                Timestamp('2023-09-03 00:00:00'),
                Timestamp('2023-09-04 00:00:00'),
                Timestamp('2023-09-05 00:00:00'),
                Timestamp('2023-09-06 00:00:00'),
                Timestamp('2023-09-07 00:00:00')
            ]
        )

    def test_date_range_group_beginning_week(self):
        self.assertEqual(
            cu.date_range_group_beginning(
                datetime(2023, 9, 1, 12, 23, 34),
                datetime(2023, 9, 18, 12, 23, 34),
                step='week').tolist(),
            [
                Timestamp('2023-08-28 00:00:00'),
                Timestamp('2023-09-04 00:00:00'),
                Timestamp('2023-09-11 00:00:00'),
                Timestamp('2023-09-18 00:00:00'),
            ]
        )

    def test_date_range_group_beginning_month(self):
        self.assertEqual(
            cu.date_range_group_beginning(
                datetime(2023, 9, 2, 12, 23, 34),
                datetime(2024, 2, 17, 12, 23, 34),
                step='month').tolist(),
            [
                Timestamp('2023-09-01 00:00:00'),
                Timestamp('2023-10-01 00:00:00'),
                Timestamp('2023-11-01 00:00:00'),
                Timestamp('2023-12-01 00:00:00'),
                Timestamp('2024-01-01 00:00:00'),
                Timestamp('2024-02-01 00:00:00'),
            ]
        )

    def test_date_range_year(self):
        self.assertEqual(
            cu.date_range_group_beginning(
                datetime(2022, 9, 2, 12, 23, 34),
                datetime(2024, 2, 17, 12, 23, 34),
                step='year').tolist(),
            [
                Timestamp('2022-01-01 00:00:00'),
                Timestamp('2023-01-01 00:00:00'),
                Timestamp('2024-01-01 00:00:00'),
            ]
        )


if __name__ == '__main__':
    unittest.main()
