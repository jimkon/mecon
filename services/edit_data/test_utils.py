import unittest
from unittest.mock import Mock, patch, call

import utils


class TestUtils(unittest.TestCase):
    def test_save_tag_changes_one(self):
        self.data_manager = Mock()

        # Mock the get_tag and update_tag methods
        self.data_manager.get_tag.return_value = 'mocked_tag_object'
        self.data_manager.update_tag.return_value = None
        added_tags = {
            'id1': ['tag1']
        }
        removed_tags = {}

        # Run the function under test
        with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
            utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
            mock_add_rule_for_id.assert_called_with('mocked_tag_object', ['id1'])

        # Verify the calls to data_manager
        self.data_manager.get_tag.assert_called_once_with('tag1')
        self.data_manager.update_tag.assert_called_once()

    def test_save_tag_changes_more(self):
        self.data_manager = Mock()

        # Mock the get_tag and update_tag methods
        self.data_manager.get_tag.side_effect = ['mocked_tag_object1', 'mocked_tag_object2']
        self.data_manager.update_tag.return_value = None
        added_tags = {
            'id1': ['tag1'],
            'id2': ['tag2']
        }
        removed_tags = {}

        # Run the function under test
        with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
            utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
            mock_add_rule_for_id.assert_has_calls([call('mocked_tag_object1', ['id1']),
                                                   call('mocked_tag_object2', ['id2'])])

        # Verify the calls to data_manager
        self.data_manager.get_tag.assert_has_calls([call('tag1'), call('tag2')])
        self.assertEqual(self.data_manager.update_tag.call_count, 2)

    def test_save_tag_changes_one_tag_many_ids(self):
        self.data_manager = Mock()

        # Mock the get_tag and update_tag methods
        self.data_manager.get_tag.side_effect = ['mocked_tag_object1']
        self.data_manager.update_tag.return_value = None
        added_tags = {
            'id1': ['tag1'],
            'id2': ['tag1']
        }
        removed_tags = {}

        # Run the function under test
        with patch('mecon.tags.tag_helpers.add_rule_for_id') as mock_add_rule_for_id:
            utils.save_tag_changes(added_tags, removed_tags, self.data_manager)
            mock_add_rule_for_id.assert_has_calls([call('mocked_tag_object1', ['id1', 'id2'])])

        # Verify the calls to data_manager
        self.data_manager.get_tag.assert_has_calls([call('tag1')])
        self.assertEqual(self.data_manager.update_tag.call_count, 1)


if __name__ == '__main__':
    unittest.main()
