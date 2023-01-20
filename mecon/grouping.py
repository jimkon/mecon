import abc

from pandas.core.groupby.generic import DataFrameGroupBy

from mecon.calendar_utils import week_of_month
from mecon.tagging.tags import LOCATIONS, TRIPS, RESIDENCE


def get_grouper(unit):
    for grouper in [DailyGrouping, WeeklyGrouping, MonthlyGrouping, WorkingMonthGrouping, WeeklyGrouping, YearlyGrouping, TripGrouping]:
        if unit == grouper.col_name:
            return grouper


def which_loc(tags):
    for trip_tagger in TRIPS:
        if trip_tagger.tag_name == 'Trip':
            continue
        if trip_tagger.tag_name in tags:
            return trip_tagger.tag_name
    for res_tagger in RESIDENCE:
        if res_tagger.tag_name in tags:
            return res_tagger.tag_name
    return 'Unknown location'


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
    col_name = 'trip'

    def generate_grouping_column(self, df):
        trip_tags = {trip.tag_name for trip in LOCATIONS if 'trip' in trip.tag_name.lower()}

        def _which_trip_tag(tags):
            inter = trip_tags.intersection(set(tags)) - {'Trip'}
            if len(inter) == 0:
                return 'Not in trip'
            elif len(inter) == 1:
                return inter.pop()
            else:
                raise ValueError(f"More than one trip tags for this row: {tags}")

        df[self.col_name] = df['tags'].apply(_which_trip_tag)

        return df[self.col_name]


class LocationGrouping(DataGrouping):
    col_name = 'location'

    def generate_grouping_column(self, df):
        df[self.col_name] = df['tags'].apply(which_loc)
        return df[self.col_name]


class PlaceGrouping(DataGrouping):
    # Same as location but also include time
    col_name = 'place'

    def generate_grouping_column(self, df):
        df[self.col_name] = df['tags'].apply(which_loc)
        df['place_before'] = df['place'].shift(1)
        df['changes_places'] = (df['place'] != df['place_before']).astype(int)
        df['changes_places_cumsum'] = df['changes_places'].cumsum()
        df['place_label'] = df['place']+df['changes_places_cumsum'].astype(str)

        df['date_str'] = df['date'].apply(lambda d: str(d)[:10])

        df_agg = df.groupby('place_label').agg({'date_str': ['min', 'max']}).reset_index()
        df_agg.columns = ["_".join(pair) for pair in df_agg.columns]
        df_agg['min_max_dates'] = " ("+df_agg['date_str_min']+" to "+df_agg['date_str_max']+")"

        df_merged = df.merge(df_agg, left_on="place_label", right_on="place_label_")
        df_merged['place'] = df_merged['place']+df_merged['min_max_dates']
        return df_merged[self.col_name]  #

