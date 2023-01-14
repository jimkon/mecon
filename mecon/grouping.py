import abc

from pandas.core.groupby.generic import DataFrameGroupBy

from mecon.calendar_utils import week_of_month
from mecon.tagging.tags import LOCATIONS


def get_grouper(unit):
    for grouper in [DailyGrouping, WeeklyGrouping, MonthlyGrouping, WorkingMonthGrouping, WeeklyGrouping, YearlyGrouping, TripGrouping]:
        if unit == grouper.col_name:
            return grouper


class DataGrouping(DataFrameGroupBy):
    col_name = None

    def __init__(self, df):
        df[self.col_name] = self.generate_grouping_column(df)
        super().__init__(df.copy(), keys=self.col_name)

    @abc.abstractmethod
    def generate_grouping_column(self, df):
        pass


class DailyGrouping(DataGrouping):
    col_name = 'date'

    def generate_grouping_column(self, df):
        return df[self.col_name]


class WeeklyGrouping(DataGrouping):
    col_name = 'week'

    def generate_grouping_column(self, df):
        df['year'] = df['date'].dt.year.astype(str)
        df['month'] = df['date'].dt.month.apply(lambda x: f"{str(x):0>2}")
        df['week'] = df['date'].apply(week_of_month).apply(lambda x: f"{str(x):0>2}")
        df[self.col_name] = df['year'] + '-' + df['month'] + '/' + df['week']
        return df[self.col_name]


class MonthlyGrouping(DataGrouping):
    col_name = 'month'

    def generate_grouping_column(self, df):
        df['year'] = df['date'].dt.year.astype(str)
        df['month'] = df['date'].dt.month.apply(lambda x: f"{str(x):0>2}")
        df[self.col_name] = df['year'] + '-' + df['month']
        return df[self.col_name]


class YearlyGrouping(DataGrouping):
    col_name = 'year'

    def generate_grouping_column(self, df):
        df[self.col_name] = df['date'].dt.year.astype(str)
        return df[self.col_name]


class WorkingMonthGrouping(DataGrouping):
    col_name = 'working month'

    def generate_grouping_column(self, df):
        df['is_income'] = df['tags'].apply(lambda tags: 1 if ('Income' in tags) else 0)
        df[self.col_name] = df['is_income'].cumsum()
        return df[self.col_name]


class TripGrouping(DataGrouping):
    col_name = 'location'

    def generate_grouping_column(self, df):
        trip_tags = {trip.tag_name for trip in LOCATIONS if 'trip' in trip.tag_name.lower()}

        def _which_trip_tag(tags):
            inter = trip_tags.intersection(set(tags)) - {'Trip'}
            if len(inter) == 0:
                return 'London'
            elif len(inter) == 1:
                return inter.pop()
            else:
                raise ValueError(f"More than one trip tags for this row: {tags}")

        df[self.col_name] = df['tags'].apply(_which_trip_tag)

        return df[self.col_name]

