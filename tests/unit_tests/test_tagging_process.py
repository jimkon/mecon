import unittest

import pandas as pd
from pandas import Timestamp

from mecon.data.transactions import Transactions
from mecon.tags.process import RuleExecutionPlanTagging
from mecon.tags.tagging import Tag


class RuleExecutionPlanTaggingTestCase1(unittest.TestCase):
    def setUp(self):
        # [tag for tag in self.tags if tag.name == 'Afternoon'][0].rule.to_json()
        tag_0 = Tag.from_json('Online payments',
                              [{'description.lower': {'contains': 'paypal'}},
                               # {'tags': {'contains': 'Flight tickets'}},
                               {'tags': {'contains': 'Accommodation'}}])
        tag_1 = Tag.from_json('Accommodation',
                              [{'amount.abs': {'greater': 30},
                                'description.lower': {'contains': 'hotel'}},
                               {'tags': {'contains': 'Rent'}},
                               {'tags': {'contains': 'Airbnb'}}])
        tag_21 = Tag.from_json('Rent',
                               [{'description': {'contains': 'landlord'}}])
        tag_22 = Tag.from_json('Airbnb',
                               [{'description.lower': {'contains': 'airbnb'}}])

        self.tags = [tag_0, tag_1, tag_21, tag_22]
        self.rep = RuleExecutionPlanTagging(self.tags)

    def test_create_rule_execution_plan(self):
        df_plan = self.rep.create_rule_execution_plan()
        df_plan['rule'] = df_plan['rule'].apply(str)
        expected_df = pd.DataFrame([
            {'priority': 2.8, 'rule': 'TagApplicator(Online payments)', 'tag': 'Online payments',
             'type': 'TagApplicator'},
            {'priority': 2.4, 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CBF7B90>',
             'tag': 'Online payments', 'type': 'Disjunction'},
            {'priority': 2.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7810>',
             'tag': 'Online payments', 'type': 'Conjunction'},
            {'priority': 2.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7AD0>',
             'tag': 'Online payments', 'type': 'Conjunction'},
            {'priority': 2.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7B50>',
             'tag': 'Online payments', 'type': 'Conjunction'},
            {'priority': 0.0, 'rule': 'lower(description) contains paypal', 'tag': 'Online payments',
             'type': 'Condition'},
            {'priority': 2.1, 'rule': 'tags contains Flight tickets', 'tag': 'Online payments', 'type': 'Condition'},
            {'priority': 2.1, 'rule': 'tags contains Accommodation', 'tag': 'Online payments', 'type': 'Condition'},
            {'priority': 1.8, 'rule': 'TagApplicator(Accommodation)', 'tag': 'Accommodation', 'type': 'TagApplicator'},
            {'priority': 1.4, 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC04210>',
             'tag': 'Accommodation', 'type': 'Disjunction'},
            {'priority': 1.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CBF7E90>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': 1.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04050>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': 1.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04150>',
             'tag': 'Accommodation', 'type': 'Conjunction'},
            {'priority': 0.0, 'rule': 'abs(amount) greater 30', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': 0.0, 'rule': 'lower(description) contains hotel', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': 1.1, 'rule': 'tags contains Rent', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': 1.1, 'rule': 'tags contains Airbnb', 'tag': 'Accommodation', 'type': 'Condition'},
            {'priority': 0.8, 'rule': 'TagApplicator(Rent)', 'tag': 'Rent', 'type': 'TagApplicator'},
            {'priority': 0.4, 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC04450>', 'tag': 'Rent',
             'type': 'Disjunction'},
            {'priority': 0.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04390>', 'tag': 'Rent',
             'type': 'Conjunction'},
            {'priority': 0.0, 'rule': 'description contains landlord', 'tag': 'Rent', 'type': 'Condition'},
            {'priority': 0.8, 'rule': 'TagApplicator(Airbnb)', 'tag': 'Airbnb', 'type': 'TagApplicator'},
            {'priority': 0.4, 'rule': '<mecon.tags.tagging.Disjunction object at 0x000001B06CC046D0>', 'tag': 'Airbnb',
             'type': 'Disjunction'},
            {'priority': 0.2, 'rule': '<mecon.tags.tagging.Conjunction object at 0x000001B06CC04610>', 'tag': 'Airbnb',
             'type': 'Conjunction'},
            {'priority': 0.0, 'rule': 'lower(description) contains airbnb', 'tag': 'Airbnb', 'type': 'Condition'}
        ])
        pd.testing.assert_frame_equal(df_plan[['priority', 'tag', 'type']], expected_df[['priority', 'tag', 'type']])

    def test_split_in_batches(self):
        batches = self.rep.split_in_batches()
        self.assertEqual(len(batches), 12)
        self.assertListEqual(list(batches.keys()), [0.0, 0.2, 0.4, 0.8, 1.1, 1.2, 1.4, 1.8, 2.1, 2.2, 2.4, 2.8])
        self.assertEqual(len(batches[0.0]), 5)
        self.assertEqual(len(batches[0.2]), 2)
        self.assertEqual(len(batches[0.4]), 2)
        self.assertEqual(len(batches[0.8]), 2)
        self.assertEqual(len(batches[1.1]), 2)
        self.assertEqual(len(batches[1.2]), 3)
        self.assertEqual(len(batches[1.4]), 1)
        self.assertEqual(len(batches[1.8]), 1)
        self.assertEqual(len(batches[2.1]), 2)
        self.assertEqual(len(batches[2.2]), 3)
        self.assertEqual(len(batches[2.4]), 1)
        self.assertEqual(len(batches[2.8]), 1)

    def test_tag(self):
        transactions = Transactions(pd.DataFrame([  # the tags col will be reset, just keeping it for reference
            [{'amount': -400, 'amount_cur': -400, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'landlord',
              'id': 'id_1',
              'tags': 'Rent,Accommodation,Online payments'},
             {'amount': 600.0, 'amount_cur': 600.0, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'landlord',
              'id': 'id_2',
              'tags': 'Rent,Accommodation,Online payments'},
             {'amount': -100, 'amount_cur': -100, 'currency': 'GBP',
              'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'Airbnb',
              'id': 'id_4',
              'tags': 'Airbnb,Accommodation,Online payments'},
             {'amount': -80, 'amount_cur': -80, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'Airbnb',
              'id': 'id_5',
              'tags': 'Airbnb,Accommodation,Online payments'},
             {'amount': -70, 'amount_cur': -70, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'hotel',
              'id': 'id_6',
              'tags': 'Accommodation,Online payments'},
             {'amount': -30, 'amount_cur': -30, 'currency': 'GBP', 'datetime': Timestamp('2020-01-01 00:00:00'),
              'description': 'paypal',
              'id': 'id_6',
              'tags': 'Online payments'}]

        ]))
        new_transactions = self.rep.tag(transactions)
        result_df = new_transactions.dataframe()
        expected_df = pd.DataFrame()

        pd.testing.assert_frame_equal(result_df, expected_df)

        if __name__ == '__main__':
            unittest.main()
