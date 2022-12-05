from mecon.tagging.dict_tag import DictTag
from mecon.tagging.manual_tag import ManualTag


class MonzoTag(ManualTag):
    tag_value = 'Monzo'

    def condition(self, element):
        return 'Bank:Monzo' in element['description']


class RevolutTag(ManualTag):
    tag_value = 'Revolut'

    def condition(self, element):
        return 'Bank:Revolut' in element['description']


class HSBCTag(ManualTag):
    tag_value = 'HSBC'

    def condition(self, element):
        return 'Bank:HSBC' in element['description']


class ITVIncomeTag(ManualTag):
    tag_value = 'ITV income'

    def condition(self, element):
        return element['amount'] > 2000 and\
                ('Monzo' in element['tags'] and 'Name:ITV' in element['description'] and 'Category:Income' in element['description']) or \
                ('HSBC' in element['tags'] and 'ITV PLC CR' in element['description'])


class DeloitteIncomeTag(ManualTag):
    tag_value = 'Deloitte income'

    def condition(self, element):
        return element['amount'] > 2000 and\
                ('HSBC' in element['tags'] and 'DELOITTE LLP CR' in element['description'])


class IncomeTag(ManualTag):
    tag_value = 'Income'

    def condition(self, element):
        return ('Deloitte income' in element['tags']) or ('ITV income' in element['tags'])


class SpotifyTag(ManualTag):
    tag_value = 'Spotify'

    def condition(self, element):
        return 'Spotify'.lower() in element['description'].lower()


class GiffgaffTag(ManualTag):
    tag_value = 'Giffgaff'

    def condition(self, element):
        return 'giffgaff' in element['description'].lower()


class SantaderBikesTag(ManualTag):
    tag_value = 'SantaderBikes'

    def condition(self, element):
        return 'Tfl Cycle Hire'.lower() in element['description'].lower()


class TFLTag(DictTag):
    def __init__(self):
        super().__init__('TFL (excluding cycle hire)', {
            'description.lower': {'contains': 'tfl'},
            'tags': {'not_contains': SantaderBikesTag.tag_value}
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
        ])