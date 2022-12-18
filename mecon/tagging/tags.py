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
        ])


class FlightTicketsTag(DictTag):
    def __init__(self):
        super().__init__('Flight tickets', [
            {'description.lower': {'contains': 'ryanair'}},
            {'description.lower': {'contains': 'kiwi.com'}},
            {'description.lower': {'contains': 'easyjet.com'}},
            {'description.lower': {'contains': 'sky express'}},
            {'description.lower': {'contains': 'krhtikes aeroporikes'}},
            {'description.lower': {'contains': 'budgetair'}},
        ])


class RentTag(DictTag):
    def __init__(self):
        super().__init__('Rent', [
            {'description.lower': {'contains': 'joseph'}},
            {'description.upper': {'contains': 'PEACHEY'}},
        ])


class HomeBillsTag(DictTag):
    def __init__(self):
        super().__init__('Bills', [
            {'tags': {'contains': 'Rent'}},
            {'description.lower': {'contains': 'thames water'}},
            {'description.lower': {'contains': 'exarchou'}},
            {'description.lower': {'contains': 'sideris'}},
            {'description.lower': {'contains': 'virgin media'}},
        ])


class OnlineOrdersTag(DictTag):
    def __init__(self):
        super().__init__('Online orders', [
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amazon'}},
            {'description.lower': {'contains': 'amznmktplace'}},
            {'description.lower': {'contains': 'ebay'}},
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


class BarchelonaTripTag(DictTag):
    def __init__(self):
        super().__init__('Barcelona trip', [{
            'date.str': {
                'greater_equal': '2022-03-10', # 10am
                'less_equal': '2022-03-22', # 10am?
            }}])


class BudapestTripTag(DictTag):
    def __init__(self):
        super().__init__('Budapest trip', [{
            'date.str': {
                'greater_equal': '2022-04-22', # 18:00
                'less_equal': '2022-04-27', # 6am?
            }}])


class ParisTripTag(DictTag):
    def __init__(self):
        super().__init__('Paris trip', [{
            'date.str': {
                'greater_equal': '2022-07-07', # apogeuma
                'less_equal': '2022-07-15', # prwi
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
SantaderBikesTag(),
TFLTag(),
TherapyTag(),
SuperMarketTag(),
FlightTicketsTag(),
RentTag(),
HomeBillsTag(),
OnlineOrdersTag(),
TooGoodToGoTag(),
CashTag(),
BarchelonaTripTag(),
BudapestTripTag(),
ParisTripTag(),
]

ALL_TAGS = BANK_TAGS + INCOME_TAGS + SERVICE_TAGS

