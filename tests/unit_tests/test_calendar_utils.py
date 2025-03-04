import unittest
from datetime import datetime, date

from pandas import Timestamp

from mecon.utils import calendar_utils as cu


class TestCalendarUtils(unittest.TestCase):
    def test_to_date(self):
        self.assertEqual(cu.to_date("2023-09-11"), date(2023, 9, 11))
        self.assertEqual(cu.to_date("2023-09-11 12:34:56"), date(2023, 9, 11))
        self.assertEqual(cu.to_date(datetime(2023, 9, 11, 12, 23, 34)), date(2023, 9, 11))
        self.assertEqual(cu.to_date(date(2023, 9, 11)), date(2023, 9, 11))

        with self.assertRaises(cu.InvalidDatetimeObjectType):
            cu.to_date(5)

    def test_to_datetime(self):
        self.assertEqual(cu.to_datetime("2023-09-11"), datetime(2023, 9, 11, 0, 0, 0))
        self.assertEqual(cu.to_datetime("2023-09-11 12:34:56"), datetime(2023, 9, 11, 12, 34, 56))
        self.assertEqual(cu.to_datetime(datetime(2023, 9, 11, 12, 23, 34)), datetime(2023, 9, 11, 12, 23, 34))
        self.assertEqual(cu.to_datetime(date(2023, 9, 11)), datetime(2023, 9, 11, 0, 0, 0))

        with self.assertRaises(cu.InvalidDatetimeObjectType):
            cu.to_datetime(5)


    def test_datetime_to_str(self):
        self.assertEqual(cu.datetime_to_str(datetime(2023, 9, 11, 12, 23, 34)), "2023-09-11 12:23:34")

    def test_datetime_from_str(self):
        self.assertEqual(cu.datetime_from_str("2023-09-11 12:23:34"), datetime(2023, 9, 11, 12, 23, 34))

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

    def test_datetime_to_hour_id_str(self):
        self.assertEqual(cu.datetime_to_hour_id_str(datetime(2023, 9, 11, 12, 23, 34)), "2023091112")
        self.assertEqual(cu.datetime_to_hour_id_str(datetime(2023, 1, 1, 0, 23, 1)), "2023010100")
        self.assertEqual(cu.datetime_to_hour_id_str(datetime(2023, 10, 31, 12, 23, 34)), "2023103112")
        self.assertEqual(cu.datetime_to_hour_id_str(datetime(2023, 12, 31, 3, 23, 34)), "2023123103")

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

    def test_get_closest_future_sunday(self):
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 11, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 12, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 13, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 14, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 15, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 16, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 17, 12, 23, 34)),
            datetime(2023, 9, 17, 12, 23, 34)
        )
        self.assertEqual(
            cu.get_closest_future_sunday(datetime(2023, 9, 18, 12, 23, 34)),
            datetime(2023, 9, 24, 12, 23, 34)
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

        with self.assertRaises(cu.InvalidDataRange):
            cu.date_floor(datetime(2023, 9, 12, 12, 23, 34), 'quarter')

    def test_date_ceil(self):
        self.assertEqual(
            cu.date_ceil(datetime(2023, 9, 12, 12, 23, 34), 'day'),
            datetime(2023, 9, 12, 23, 59, 59)
        )
        self.assertEqual(
            cu.date_ceil(datetime(2023, 9, 11, 12, 23, 34), 'week'),
            datetime(2023, 9, 17, 23, 59, 59)  # Closest future Sunday
        )
        self.assertEqual(
            cu.date_ceil(datetime(2023, 9, 12, 12, 23, 34), 'month'),
            datetime(2023, 9, 30, 23, 59, 59)  # Last day of September
        )
        self.assertEqual(
            cu.date_ceil(datetime(2023, 9, 12, 12, 23, 34), 'year'),
            datetime(2023, 12, 31, 23, 59, 59)  # Last day of the year
        )

        with self.assertRaises(cu.InvalidDataRange):
            cu.date_ceil(datetime(2023, 9, 12, 12, 23, 34), 'quarter')

    def test_week_of_month(self):
        # Test week_of_month function
        dt1 = date(2023, 10, 1)  # Week 1
        dt2 = date(2023, 10, 8)  # Week 2
        dt3 = date(2023, 10, 15)  # Week 3

        self.assertEqual(cu.week_of_month(dt1), 1)
        self.assertEqual(cu.week_of_month(dt2), 2)
        self.assertEqual(cu.week_of_month(dt3), 3)

    def test_date_to_month_date(self):
        self.assertEqual(cu.date_to_month_date(datetime(2023, 10, 5)), '2023-10')
        self.assertEqual(cu.date_to_month_date(datetime(2023, 11, 15)), '2023-11')
        self.assertEqual(cu.date_to_month_date(datetime(2023, 12, 25)), '2023-12')

    def test_days_in_between(self):
        # Test days_in_between function
        start_date = date(2023, 10, 1)
        end_date = date(2023, 10, 5)

        result = cu.days_in_between(start_date, end_date)

        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], start_date)
        self.assertEqual(result[-1], end_date)

    def test_part_of_day(self):
        # Test part_of_day function
        morning_hours = [6, 7, 8, 9, 10, 11]
        afternoon_hours = [13, 14, 15, 16, 17]
        evening_hours = [18, 19, 20, 21]
        night_hours = [0, 1, 2, 3, 4, 22, 23]

        for hour in morning_hours:
            self.assertEqual(cu.part_of_day(hour), 'Morning')

        for hour in afternoon_hours:
            self.assertEqual(cu.part_of_day(hour), 'Afternoon')

        for hour in evening_hours:
            self.assertEqual(cu.part_of_day(hour), 'Evening')

        for hour in night_hours:
            self.assertEqual(cu.part_of_day(hour), 'Night')

    def test_hour_range_of_part_of_day(self):
        # Test hour_range_of_part_of_day function
        self.assertEqual(cu.hour_range_of_part_of_day('Morning'), (5, 12))
        self.assertEqual(cu.hour_range_of_part_of_day('Afternoon'), (12, 17))
        self.assertEqual(cu.hour_range_of_part_of_day('Evening'), (17, 21))
        self.assertEqual(cu.hour_range_of_part_of_day('Night'), (21, 5))

    def test_hour_of_day(self):
        self.assertEqual(cu.hour_of_day(datetime(2024, 2, 14, 12, 23, 34)), 12)
        self.assertEqual(cu.hour_of_day(date(2024, 2, 14)), 0)

    def test_day_of_week(self):
        self.assertEqual(cu.day_of_week(datetime(2024, 2, 14, 12, 23, 34)), cu.DayOfWeek.WEDNESDAY.value)
        self.assertEqual(cu.day_of_week(date(2024, 2, 14)), cu.DayOfWeek.WEDNESDAY.value)

    def test_day_of_month(self):
        self.assertEqual(cu.day_of_month(datetime(2024, 2, 14, 12, 23, 34)), 14)
        self.assertEqual(cu.day_of_month(date(2024, 2, 14)), 14)

    def test_day_of_year(self):
        self.assertEqual(cu.day_of_year(datetime(2024, 1, 1, 12, 23, 34)), 1)
        self.assertEqual(cu.day_of_year(datetime(2024, 2, 14, 12, 23, 34)), 45)
        self.assertEqual(cu.day_of_year(date(2024, 2, 14)), 45)

    def test_week_of_year(self):
        self.assertEqual(cu.week_of_year(datetime(2024, 1, 1, 12, 23, 34)), 1)
        self.assertEqual(cu.week_of_year(datetime(2024, 2, 14, 12, 23, 34)), 7)
        self.assertEqual(cu.week_of_year(date(2024, 2, 14)), 7)

    def test_month_of_year(self):
        self.assertEqual(cu.month_of_year(datetime(2024, 1, 1, 12, 23, 34)), 1)
        self.assertEqual(cu.month_of_year(datetime(2024, 2, 14, 12, 23, 34)), 2)
        self.assertEqual(cu.month_of_year(date(2024, 2, 14)), 2)




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
    def test_date_range_group_beginning_start_date_is_date_obj(self):
        self.assertEqual(
            cu.date_range_group_beginning(
                date(2023, 9, 1),
                date(2023, 9, 18),
                step='week').tolist(),
            [
                Timestamp('2023-08-28 00:00:00'),
                Timestamp('2023-09-04 00:00:00'),
                Timestamp('2023-09-11 00:00:00'),
                Timestamp('2023-09-18 00:00:00'),
            ]
        )

        self.assertEqual(
            cu.date_range_group_beginning(
                datetime(2023, 9, 1, 12, 23, 34),
                date(2023, 9, 18),
                step='week').tolist(),
            [
                Timestamp('2023-08-28 00:00:00'),
                Timestamp('2023-09-04 00:00:00'),
                Timestamp('2023-09-11 00:00:00'),
                Timestamp('2023-09-18 00:00:00'),
            ]
        )

        self.assertEqual(
            cu.date_range_group_beginning(
                date(2023, 9, 1),
                datetime(2023, 9, 18, 12, 23, 34),
                step='week').tolist(),
            [
                Timestamp('2023-08-28 00:00:00'),
                Timestamp('2023-09-04 00:00:00'),
                Timestamp('2023-09-11 00:00:00'),
                Timestamp('2023-09-18 00:00:00'),
            ]
        )

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

    def test_date_range_wrong_step(self):
        with self.assertRaises(cu.InvalidDataRange):
            cu.date_range_group_beginning(
                datetime(2022, 9, 2, 12, 23, 34),
                datetime(2024, 2, 17, 12, 23, 34),
                step='quarter')


if __name__ == '__main__':
    unittest.main()
