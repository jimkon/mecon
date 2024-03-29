from legacy.calendar_utils import DayOfWeek
from legacy.tagging.json_tags import JsonTag
from legacy.tagging.manual_tag import HardCodedTag


class MonzoTag(HardCodedTag):
    def __init__(self):
        super().__init__('Monzo')

    def condition(self, element):
        return 'Bank:Monzo' in element['description']


class RevolutTag(HardCodedTag):
    def __init__(self):
        super().__init__('Revolut')

    def condition(self, element):
        return 'Bank:Revolut' in element['description']


class HSBCTag(HardCodedTag):
    def __init__(self):
        super().__init__('HSBC')

    def condition(self, element):
        return 'Bank:HSBC' in element['description']


class ITVIncomeTag(HardCodedTag):
    def __init__(self):
        super().__init__('ITV income')

    def condition(self, element):
        return element['amount'] > 2000 and \
            ('Monzo' in element['tags'] and 'Name:ITV' in element['description'] and 'Category:Income' in element[
                'description']) or \
            ('HSBC' in element['tags'] and 'ITV PLC CR' in element['description'])


class DeloitteIncomeTag(HardCodedTag):
    def __init__(self):
        super().__init__('Deloitte income')

    def condition(self, element):
        return element['amount'] > 2000 and \
            ('HSBC' in element['tags'] and 'DELOITTE LLP CR' in element['description'])


class IncomeTag(HardCodedTag):
    def __init__(self):
        super().__init__('Income')

    def condition(self, element):
        return ('Deloitte income' in element['tags']) or ('ITV income' in element['tags'])


class SpotifyTag(HardCodedTag):
    def __init__(self):
        super().__init__('Spotify')

    def condition(self, element):
        return 'Spotify'.lower() in element['description'].lower()


class GiffgaffTag(HardCodedTag):
    def __init__(self):
        super().__init__('Giffgaff')

    def condition(self, element):
        return 'giffgaff' in element['description'].lower()


class CosmoteTag(JsonTag):
    def __init__(self):
        super().__init__('Cosmote', [
            {'description.lower': {'contains': 'cosmote'}},
        ])


class SantaderBikesTag(HardCodedTag):
    def __init__(self):
        super().__init__('Santader Bikes')

    def condition(self, element):
        return 'Tfl Cycle Hire'.lower() in element['description'].lower()


class TFLTag(JsonTag):
    def __init__(self):
        super().__init__('TFL (excluding cycle hire)', {
            'description.lower': {'contains': 'tfl', 'not_contains': 'netflix'},
            'tags': {'not_contains': SantaderBikesTag().tag_name}
        })


class TherapyTag(JsonTag):
    def __init__(self):
        super().__init__('Therapy', {
            'description.lower': {'contains': 'karamanos'},
        })


class SuperMarketTag(JsonTag):
    def __init__(self):
        super().__init__('Super Market', [
            {'description.lower': {'contains': 'sainsbury', 'not_contains': 'cash sainsby'}},
            {'description.lower': {'contains': 'tesco', 'not_contains': 'cash at tesco'}},
            {'description.lower': {'contains': 'aldi'}},
            {'description.lower': {'contains': 'ally'}},
            {'description.lower': {'contains': 'lidl'}},
            {'description.lower': {'contains': 'poundland'}},
            {'description.lower': {'contains': 'co-op'}},
            {'description.lower': {'contains': 'waitrose'}},
            {'description.lower': {'contains': 'morrisons'}},
            {'description.lower': {'contains': 'marks spencer'}},
            {'description.lower': {'contains': 'nisa'}},
        ])


class FoodDeliveryTag(JsonTag):
    def __init__(self):
        super().__init__('Food Delivery', [
            {'description.lower': {'contains': ['just', 'eat']}},
            {'description.lower': {'contains': ['uber', 'eat']}},
            {'description.lower': {'contains': 'efood'}},
            {'description.lower': {'contains': 'e food'}},
        ])


class FlightTicketsTag(JsonTag):
    def __init__(self):
        super().__init__('Flight tickets', [
            {'description.lower': {'contains': 'ryanair'}},
            {'description.lower': {'contains': 'kiwi.com'}},
            {'description.lower': {'contains': 'easyjet'}},
            {'description.lower': {'contains': 'sky express'}},
            {'description.lower': {'contains': 'krhtikes aeroporikes'}},
            {'description.lower': {'contains': 'budgetair'}},
            {'description.lower': {'contains': 'olympic'}},
        ])


class TrainTicketsTag(JsonTag):
    def __init__(self):
        super().__init__('Train tickets', [
            {'description.lower': {'contains': 'trainline'}},
            {'description.lower': {'contains': 'stansted express'}},
        ])


class RentTag(JsonTag):
    def __init__(self):
        super().__init__('Rent', [
            {'description.lower': {'contains': 'joseph'}},
            {'description.upper': {'contains': 'PEACHEY'}},
            {'description.upper': {'contains': 'GRADY ALWYNE'}},
        ])


class HomeBillsTag(JsonTag):
    def __init__(self):
        super().__init__('Home Bills', [
            {'tags': {'contains': 'Rent'}},
            {'description.lower': {'contains': 'thames water'}},
            {'description.lower': {'contains': 'exarchou'}},
            {'description.lower': {'contains': 'sideris'}},
            {'description.lower': {'contains': 'virgin media'}},
            {'description.lower': {'contains': 'bulb energy'}},
        ])


class OtherBillsTag(JsonTag):
    def __init__(self):
        super().__init__('Other Bills', [
            {'tags': {'contains': 'Spotify'}},
            {'tags': {'contains': 'Giffgaff'}},
            {'tags': {'contains': 'Therapy'}},
            {'description.lower': {'contains': 'vision express'}},
        ])


class OnlineOrdersTag(JsonTag):
    def __init__(self):
        super().__init__('Online orders', [
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amznmktplace'}},
            {'description.lower': {'contains': 'ebay'}},
            {'description.lower': {'contains': 'asos'}},
            {'description.lower': {'contains': 'samsung'}},
        ])


class AirbnbTag(JsonTag):
    def __init__(self):
        super().__init__('Airbnb', [
            {'description.lower': {'contains': 'airbnb'}},
        ])


class TooGoodToGoTag(JsonTag):
    def __init__(self):
        super().__init__('Too Good To Go', [
            {'description.lower': {'contains': 'toogoodtog'}},
        ])


class CashTag(JsonTag):
    def __init__(self):
        super().__init__('Cash', [
            {'description.lower': {'contains': 'cash'}},
        ])


class CafeTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Cafe', [
            {'description.lower': {'contains': 'cafe'}},
            {'description.lower': {'contains': 'coffee'}},
            {'description.lower': {'contains': 'briki'}},
        ])


class FoodOutTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Food out', [
            {'description.lower': {'contains': 'nando'}},
            {'description.lower': {'contains': 'kfc'}},
            {'description.lower': {'contains': 'mcdonalds'}},
            {'description.lower': {'contains': 'resta'}},
            {'description.lower': {'contains': 'pizza'}},
            {'description.lower': {'contains': 'burger'}},
            {'description.lower': {'contains': 'grill'}},
        ])


class FoodTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Food', [
            {'tags': {'contains': SuperMarketTag().tag_name}},
            {'tags': {'contains': FoodDeliveryTag().tag_name}},
            {'tags': {'contains': TooGoodToGoTag().tag_name}},
            {'tags': {'contains': FoodOutTag().tag_name}},
        ])


class CommuteTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Commute', [
            {'tags': {'contains': SantaderBikesTag().tag_name}},
            {'tags': {'contains': TFLTag().tag_name}},
            {'description.lower': {'contains': 'uber', 'not_contains': 'eat'}},
        ])


class DrinksTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Drinks', [
            # {'tags': {'contains': SantaderBikesTag().tag_name}},
            # {'tags': {'contains': TFLTag().tag_name}},
        ])


class EntertainmentTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Entertainment', [
            # {'tags': {'contains': SantaderBikesTag().tag_name}},
            # {'tags': {'contains': TFLTag().tag_name}},
        ])


class NightOutTag(JsonTag):  # TODO
    def __init__(self):
        super().__init__('Night out', [
            {'time.str': {'greater_equal': "21:00:00"}, 'tags': {'not_contains': ['Home Bills', 'Income', 'Bank transfer']}},
            {'time.str': {'less': "08:00:00"}, 'tags': {'not_contains': ['Home Bills', 'Income', 'Bank transfer']}},
            # {'tags': {'contains': TFLTag().tag_name}},
        ])


class TapTag(JsonTag):
    def __init__(self):
        super().__init__('Tap', [
            {'tags': {'contains': CommuteTag().tag_name}},
            {'tags': {'contains': FoodTag().tag_name}},
            {'tags': {'contains': DrinksTag().tag_name}},
            {'tags': {'contains': EntertainmentTag().tag_name}},
            {'tags': {'contains': NightOutTag().tag_name}},
        ])


class LondonResTag(JsonTag):
    def __init__(self):
        super().__init__('London residency', {'date.str': {'greater_equal': '2020-01-05', 'less': '2099-01-01'}})


class LondonTripTag(JsonTag):
    def __init__(self):
        super().__init__('London trip', [
            {'date.str': {'greater_equal': '2019-05-10', 'less': '2019-06-13'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2019-09-16', 'less': '2019-09-21'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2019-11-24', 'less': '2019-11-28'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
        ])


class ChaniaResTag(JsonTag):
    def __init__(self):
        super().__init__('Chania residency', {'date.str': {'greater_equal': '2000-01-01', 'less': '2020-01-01'}})


class ChaniaTripTag(JsonTag):
    def __init__(self):
        super().__init__('Chania trip', [
            {'date.str': {'greater_equal': '2020-05-16', 'less': '2020-07-01'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2020-07-11', 'less': '2020-09-05'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-07-02', 'less': '2021-07-11'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-07-22', 'less': '2021-07-09'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-01-01', 'less': '2022-01-08'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-04-07', 'less': '2022-04-19'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-05-25', 'less': '2022-06-04'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-08-04', 'less': '2022-09-08'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
        ])


class AthensTripTag(JsonTag):
    def __init__(self):
        super().__init__('Athens trip', [
            {'date.str': {'greater_equal': '2020-01-01', 'less': '2020-01-04'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2020-07-02', 'less': '2020-07-11'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-07-12', 'less': '2021-07-22'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-08-10', 'less': '2021-08-14'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-12-14', 'less': '2022-01-01'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-01-09', 'less': '2022-01-09'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-06-04', 'less': '2022-06-07'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-09-08', 'less': '2022-10-12'},
             'tags': {'not_contains': ['Home Bills', 'Income']}},
        ])


class BarcelonaTripTag(JsonTag):
    def __init__(self):
        super().__init__('Barcelona trip', [{
            'date.str': {
                'greater_equal': '2022-03-10',  # 10am
                'less_equal': '2022-03-22',  # 10am?
            }, 'tags': {'not_contains': ['Home Bills', 'Income']}}, ])


class BudapestTripTag(JsonTag):
    def __init__(self):
        super().__init__('Budapest trip', [{
            'date.str': {
                'greater_equal': '2022-04-22',  # 18:00
                'less_equal': '2022-04-27',  # 6am?
            }, 'tags': {'not_contains': ['Home Bills', 'Income']}}, ])


class ParisTripTag(JsonTag):
    def __init__(self):
        super().__init__('Paris trip', [{
            'date.str': {
                'greater_equal': '2022-07-07',  # apogeuma
                'less_equal': '2022-07-15',  # prwi
            }, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {
                'description': {
                    'contains': 'To Ifigeneia Giannoukakou-Leontsini'
                }
            },
            {
                'description.lower': {
                    'contains': 'velib metropole'
                }
            },
        ])


class TripsTag(JsonTag):
    def __init__(self):
        super().__init__('Trip', [{
            'tags.str': {
                'contains': 'trip'
            }}])


class BankTransferTag(JsonTag):
    def __init__(self):
        super().__init__('Bank transfer', [
            {'description.lower': {'contains': 'dimitrios kontzedakis'}}
        ])


class MondayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.MONDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.MONDAY.value}})


class TuesdayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.TUESDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.TUESDAY.value}})


class WednesdayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.WEDNESDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.WEDNESDAY.value}})


class ThursdayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.THURSDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.THURSDAY.value}})


class FridayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.FRIDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.FRIDAY.value}})


class SaturdayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.SATURDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.SATURDAY.value}})


class SundayTag(JsonTag):
    def __init__(self):
        super().__init__(DayOfWeek.SUNDAY.value,
                         {'date.dayofweek': {'equals': DayOfWeek.SUNDAY.value}})


class Guest(JsonTag):
    def __init__(self):
        super().__init__('Guest', [{
                'date.str': {  # Golfidis
                    'greater_equal': '2021-10-28',
                    'less_equal': '2021-11-02',
                },
                'tags': {
                    'not_contains': ['Home Bills', 'Income'],
                    'contains': 'Monzo'
                }},
            {
                'date.str': {  # Sofia
                    'greater_equal': '2022-06-14',
                    'less_equal': '2022-06-28',
                },
                'tags': {
                    'not_contains': ['Home Bills', 'Income'],
                    'contains': 'Monzo'
                }},
            {
                'date.str': {  # Mama
                    'greater_equal': '2023-02-02',
                    'less_equal': '2023-02-06',
                },
                'tags': {
                    'not_contains': ['Home Bills', 'Income'],
                    'contains': 'Monzo'
                }}
        ])


BANK_TAGS = [
    MonzoTag(),
    RevolutTag(),
    HSBCTag()
]

INCOME_TAGS = [
    ITVIncomeTag(),
    DeloitteIncomeTag(),
    IncomeTag(),
]

SERVICE_TAGS = [
    SpotifyTag(),
    GiffgaffTag(),
    CosmoteTag(),
    SantaderBikesTag(),
    TFLTag(),
    TherapyTag(),
    SuperMarketTag(),
    FoodDeliveryTag(),
    FlightTicketsTag(),
    TrainTicketsTag(),
    RentTag(),
    HomeBillsTag(),
    OtherBillsTag(),
    OnlineOrdersTag(),
    TooGoodToGoTag(),
    CashTag(),
    AirbnbTag(),
    BankTransferTag(),
    CafeTag(),
    FoodOutTag(),
    FoodTag(),
    CommuteTag(),
    DrinksTag(),
    EntertainmentTag(),
    NightOutTag(),
    TapTag(),
    Guest(),
]

TIME = [
    MondayTag(),
    TuesdayTag(),
    WednesdayTag(),
    ThursdayTag(),
    FridayTag(),
    SaturdayTag(),
    SundayTag(),
]

TRIPS = [
    ChaniaTripTag(),
    AthensTripTag(),
    BarcelonaTripTag(),
    BudapestTripTag(),
    ParisTripTag(),
    TripsTag(),
]

RESIDENCE = [
    LondonResTag(),
    ChaniaResTag(),
]

LOCATIONS = RESIDENCE+TRIPS

ALL_TAGS = BANK_TAGS + INCOME_TAGS + SERVICE_TAGS + TIME + LOCATIONS  # order matters
