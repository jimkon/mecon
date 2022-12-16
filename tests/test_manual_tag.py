import unittest
from unittest.mock import patch, call

import pandas as pd
from mecon.tagging import manual_tag as tag


class TestManualTag(unittest.TestCase):
    @patch.object(tag.HardCodedTag, 'condition', side_effect=[False, True, True, False])
    def test__calc_condition(self, mock_cond):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 10, 100, 1000], 'tags': [[], [], [], []]})

        tag.HardCodedTag.__abstractmethods__ = set()
        tagger = tag.HardCodedTag('test_tag')
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
        self.assertEqual(mock_cond.call_count, 4)
        calls = [call({'a': 1, 'b': 1, 'tags': []}),
                 call({'a': 2, 'b': 10, 'tags': ['test_tag']}),
                 call({'a': 3, 'b': 100, 'tags': ['test_tag']}),
                 call({'a': 4, 'b': 1000, 'tags': []})]
        mock_cond.assert_has_calls(calls)
        self.assertListEqual(test_df['tags'].to_list(), [[], ['test_tag'], ['test_tag'], []])

