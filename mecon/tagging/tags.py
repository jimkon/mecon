from pandas import Timestamp

from mecon.tagging.dict_tag import DictTag
from mecon.tagging.manual_tag import HardCodedTag


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
        return element['amount'] > 2000 and\
                ('Monzo' in element['tags'] and 'Name:ITV' in element['description'] and 'Category:Income' in element['description']) or \
                ('HSBC' in element['tags'] and 'ITV PLC CR' in element['description'])


class DeloitteIncomeTag(HardCodedTag):
    def __init__(self):
        super().__init__('Deloitte income')

    def condition(self, element):
        return element['amount'] > 2000 and\
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


class CosmoteTag(DictTag):
    def __init__(self):
        super().__init__('Cosmote', [
            {'description.lower': {'contains': 'cosmote'}},
        ])


class SantaderBikesTag(HardCodedTag):
    def __init__(self):
        super().__init__('Santader Bikes')

    def condition(self, element):
        return 'Tfl Cycle Hire'.lower() in element['description'].lower()


class TFLTag(DictTag):
    def __init__(self):
        super().__init__('TFL (excluding cycle hire)', {
            'description.lower': {'contains': 'tfl'},
            'tags': {'not_contains': SantaderBikesTag().tag_name}
        })


class TherapyTag(DictTag):
    def __init__(self):
        super().__init__('Therapy', {
            'description.lower': {'contains': 'karamanos'},
        })


class SuperMarketTag(DictTag):
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


class FoodDeliveryTag(DictTag):
    def __init__(self):
        super().__init__('Food Delivery', [
            {'description.lower': {'contains': ['just', 'eat']}},
            {'description.lower': {'contains': ['uber', 'eat']}},
            {'description.lower': {'contains': 'efood'}},
        ])


class FlightTicketsTag(DictTag):
    def __init__(self):
        super().__init__('Flight tickets', [
            {'description.lower': {'contains': 'ryanair'}},
            {'description.lower': {'contains': 'kiwi.com'}},
            {'description.lower': {'contains': 'easyjet'}},
            {'description.lower': {'contains': 'sky express'}},
            {'description.lower': {'contains': 'krhtikes aeroporikes'}},
            {'description.lower': {'contains': 'budgetair'}},
        ])


class TrainTicketsTag(DictTag):
    def __init__(self):
        super().__init__('Train tickets', [
            {'description.lower': {'contains': 'trainline'}},
            {'description.lower': {'contains': 'stansted express'}},
        ])


class RentTag(DictTag):
    def __init__(self):
        super().__init__('Rent', [
            {'description.lower': {'contains': 'joseph'}},
            {'description.upper': {'contains': 'PEACHEY'}},
        ])


class HomeBillsTag(DictTag):
    def __init__(self):
        super().__init__('Home Bills', [
            {'tags': {'contains': 'Rent'}},
            {'description.lower': {'contains': 'thames water'}},
            {'description.lower': {'contains': 'exarchou'}},
            {'description.lower': {'contains': 'sideris'}},
            {'description.lower': {'contains': 'virgin media'}},
        ])


class OtherBillsTag(DictTag):
    def __init__(self):
        super().__init__('Other Bills', [
            {'tags': {'contains': 'Spotify'}},
            {'tags': {'contains': 'Giffgaff'}},
            {'tags': {'contains': 'Therapy'}},
        ])


class OnlineOrdersTag(DictTag):
    def __init__(self):
        super().__init__('Online orders', [
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amznmktplace'}},
            {'description.lower': {'contains': 'ebay'}},
            {'description.lower': {'contains': 'asos'}},
            {'description.lower': {'contains': 'samsung'}},
        ])


class AirbnbTag(DictTag):
    def __init__(self):
        super().__init__('Airbnb', [
            {'description.lower': {'contains': 'airbnb'}},
        ])


class TooGoodToGoTag(DictTag):
    def __init__(self):
        super().__init__('Too Good To Go', [
            {'description.lower': {'contains': 'toogoodtog'}},
        ])


class CashTag(DictTag):
    def __init__(self):
        super().__init__('Cash', [
            {'description.lower': {'contains': 'cash'}},
        ])


class ChaniaTripTag(DictTag):
    def __init__(self):
        super().__init__('Chania trip', [
            {'date.str': {'greater_equal': '2019-04-11', 'less_equal': '2020-01-01'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2020-05-16', 'less_equal': '2020-07-01'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2020-07-11', 'less_equal': '2020-09-05'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-07-02', 'less_equal': '2021-07-11'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2021-07-22', 'less_equal': '2021-07-09'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-01-01', 'less_equal': '2022-01-08'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-04-07', 'less_equal': '2022-04-19'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-05-25', 'less_equal': '2022-06-04'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
            {'date.str': {'greater_equal': '2022-08-04', 'less_equal': '2022-09-08'}, 'tags': {'not_contains': ['Home Bills', 'Income']}},
        ])


class AthensTripTag(DictTag):
    def __init__(self):
        super().__init__('Athens trip', [
            {'date.str': {'greater_equal': '2020-01-01', 'less_equal': '2020-01-04'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2020-07-02', 'less_equal': '2020-07-11'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2021-07-12', 'less_equal': '2021-07-22'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2021-08-10', 'less_equal': '2021-08-14'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2021-12-14', 'less_equal': '2022-01-01'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2022-01-09', 'less_equal': '2022-01-09'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2022-06-04', 'less_equal': '2022-06-07'},
             'tags': {'not_contains': 'Home Bills'}},
            {'date.str': {'greater_equal': '2022-09-08', 'less_equal': '2022-10-12'},
             'tags': {'not_contains': 'Home Bills'}}
        ])


class BarcelonaTripTag(DictTag):
    def __init__(self):
        super().__init__('Barcelona trip', [{
            'date.str': {
                'greater_equal': '2022-03-10', # 10am
                'less_equal': '2022-03-22', # 10am?
            },
            'tags': {
                'not_contains': 'Home Bills'
            }
        }])


class BudapestTripTag(DictTag):
    def __init__(self):
        super().__init__('Budapest trip', [{
            'date.str': {
                'greater_equal': '2022-04-22', # 18:00
                'less_equal': '2022-04-27', # 6am?
            },
            'tags': {
                'not_contains': 'Home Bills'
            }
        }])


class ParisTripTag(DictTag):
    def __init__(self):
        super().__init__('Paris trip', [{
                'date.str': {
                    'greater_equal': '2022-07-07', # apogeuma
                    'less_equal': '2022-07-15', # prwi
                },
                'tags': {
                    'not_contains': ['Home Bills', 'Other Bills']
                }
            },
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


class TripsTag(DictTag):
    def __init__(self):
        super().__init__('Trip', [{
            'tags.str': {
                'contains': 'trip'
            }}])


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
]

TRIPS = [
ChaniaTripTag(),
AthensTripTag(),
BarcelonaTripTag(),
BudapestTripTag(),
ParisTripTag(),
TripsTag()
]

ALL_TAGS = BANK_TAGS + INCOME_TAGS + SERVICE_TAGS + TRIPS  # order matters

