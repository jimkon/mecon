from mecon.tags.tagging import Tag
from mecon.utils import calendar_utils as cu
from mecon.etl import account_statements


MISC_TAG_SET = {
    Tag.from_json('Money In', [{"amount": {"greater": 0}}]),
    Tag.from_json('Zero amount', [{"amount": {"equal": 0}}]),
    Tag.from_json('Money Out', [{"amount": {"less": 0}}]),
    Tag.from_json('All', [{"amount": {"greater_equal": 0}},
                          {"amount": {"less_equal": 0}}]),
}

TIME_TAG_SET = {
    Tag.from_json('Morning', [{"datetime.hour": {"greater_equal": 8, "less": 12}}]),
    Tag.from_json('Afternoon', [{"datetime.hour": {"greater_equal": 12, "less": 20}}]),
    Tag.from_json('Night', [{"datetime.hour": {"greater_equal": 20}}, {"datetime.hour": {"less": 8}}]),
}

CALENDAR_WEEK_TAG_SET = ({
    Tag.from_json(dow.value.title(), [{"datetime.day_of_week": {"equal": dow.value}}])
    for dow in cu.DayOfWeek
} \
    .union({
    Tag.from_json('Weekend', [
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.SATURDAY.value}},
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.SUNDAY.value}}
    ]),
    Tag.from_json('Weekday', [
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.MONDAY.value}},
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.TUESDAY.value}},
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.WEDNESDAY.value}},
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.THURSDAY.value}},
        {"datetime.day_of_week": {"equal": cu.DayOfWeek.FRIDAY.value}},
    ])
}))
# .union({
#     Tag('Daylight', ), # Use location and time to see if it is daytime or night time
#     Tag('Nighttime', )
# }))

LOCATION_TAG_SET = {
    # get event time and gps or event data for locations
}


class DataProviderTagSet(set):
    def __init__(self, dataset, source_names_to_look_for=None):
        all_statements_sources = account_statements.StatementsManager.from_dataset(dataset,
                                                                                   source_names_to_look_for=source_names_to_look_for)
        source_tags = {
            Tag.from_json(f"Source {source.dir_name}", [{'description': {'contains': f"bank:{source.id},"}}])
            for source in all_statements_sources.sources
        }

        super().__init__(source_tags)


class HighLevelDataProviderTagSet(set):
    def __init__(self, dataset, source_names_to_look_for=None):
        all_statements_sources = account_statements.StatementsManager.from_dataset(dataset,
                                                                                   source_names_to_look_for=source_names_to_look_for)

        providers_mapping = {}
        for source in all_statements_sources.sources:
            prov_name = f"{source.original_provider}"
            if source.original_provider in providers_mapping:
                providers_mapping[prov_name].append(source)
            else:
                providers_mapping[prov_name] = [source]

        high_level_sources = {
            Tag.from_json(
                original_prov,
                [
                    {'description': {'contains': f"bank:{source.id},"}}
                    for source in sources
                ]
            )
            for original_prov, sources in providers_mapping.items()
        }

        super().__init__(high_level_sources)


CURRENCY_TAG_SET = {
    Tag.from_json('£', [{"currency": {'equal': 'GBP'}}]),
    Tag.from_json('€', [{"currency": {'equal': 'EUR'}}]),
    Tag.from_json('$', [{"currency": {'equal': 'USD'}}]),
    Tag.from_json('Hungarian Forint', [{"currency": {'equal': 'HUF'}}]), # 0 HUF transactions
    Tag.from_json('Romanian Leu', [{"currency": {'equal': 'RON'}}]),
    Tag.from_json('Swiss Franc', [{"currency": {'equal': 'CHF'}}]),
    # or use a list of all currencies
}

CURRENCY_TRANSFER_TAG_SET = {}  # check if local currency and amount currency is the same

PAYMENT_TYPE_TAG_SET = {
    # online payment
    # transfers
    # tap???
}


def get_additional_tags(dataset, source_names_to_look_for=None):
    all_tags = set()
    all_tags = all_tags.union(MISC_TAG_SET)
    all_tags = all_tags.union(TIME_TAG_SET)
    all_tags = all_tags.union(CALENDAR_WEEK_TAG_SET)
    all_tags = all_tags.union(LOCATION_TAG_SET)
    all_tags = all_tags.union(DataProviderTagSet(dataset))
    all_tags = all_tags.union(HighLevelDataProviderTagSet(dataset))
    all_tags = all_tags.union(CURRENCY_TAG_SET)
    all_tags = all_tags.union(CURRENCY_TRANSFER_TAG_SET)

    return all_tags
