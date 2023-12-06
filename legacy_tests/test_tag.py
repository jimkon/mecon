import unittest
from unittest.mock import patch

import pandas as pd
from legacy.tagging import tag


class TestTag(unittest.TestCase):
    @patch.object(tag.Tag, '_calc_condition', return_value=[True, False, True, False])
    def test_tag(self, mock_tag):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4], 'tags': [[], [], [], []]})

        tag.Tag.__abstractmethods__ = set()
        tagger = tag.Tag('test_tag')
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [['test_tag'], [], ['test_tag'], []])

    @patch.object(tag.Tag, '_calc_condition', return_value=[True, True, True, True])
    def test_tag_all(self, mock_tag):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4], 'tags': [[], [], [], []]})

        tag.Tag.__abstractmethods__ = set()
        tagger = tag.Tag('test_tag')
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [['test_tag'], ['test_tag'], ['test_tag'], ['test_tag']])

    @patch.object(tag.Tag, '_calc_condition', return_value=[False, False, False, False])
    def test_tag_none(self, mock_tag):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4], 'tags': [[], [], [], []]})

        tag.Tag.__abstractmethods__ = set()
        tagger = tag.Tag('test_tag')
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [[], [], [], []])

    @patch.object(tag.Tag, '_calc_condition', return_value=[True, False, True, False])
    def test_same_tag_multiple_times(self, mock_tag):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4], 'tags': [[], [], [], []]})

        tag.Tag.__abstractmethods__ = set()
        tagger = tag.Tag('test_tag')
        tagger.tag(test_df)

        self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
        self.assertListEqual(test_df['tags'].to_list(), [['test_tag'], [], ['test_tag'], []])

    def test_same_tag_multiple_tags(self):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4], 'tags': [[], [], [], []]})

        with patch.object(tag.Tag, '_calc_condition', return_value=[True, False, True, False]) as mock_tag:
            tag.Tag.__abstractmethods__ = set()
            tagger = tag.Tag('test_tag')
            tagger.tag(test_df)

            self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
            self.assertListEqual(test_df['tags'].to_list(), [['test_tag'], [], ['test_tag'], []])

        with patch.object(tag.Tag, '_calc_condition', return_value=[False, True, True, False]) as mock_tag:
            tag.Tag.__abstractmethods__ = set()
            tagger = tag.Tag('test_tag_2')
            tagger.tag(test_df)

            self.assertEqual(set(test_df.columns), {'a', 'b', 'tags'})
            self.assertListEqual(test_df['tags'].to_list(), [['test_tag'], ['test_tag_2'], ['test_tag', 'test_tag_2'], []])

    @patch.object(tag.Tag, '_calc_condition')
    def test_add_tag_exception(self, mock_tag):
        test_df = pd.DataFrame(data={'a': [1, 2, 3, 4], 'b': [1, 2, 3, 4]})

        tag.Tag.__abstractmethods__ = set()
        tagger = tag.Tag('test_tag')

        with self.assertRaises(tag.TagsColumnDoesNotExistException):
            tagger.tag(test_df)
