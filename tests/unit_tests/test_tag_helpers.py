import unittest

from mecon.tags import tag_helpers, tagging


class TagHelpersTestCase(unittest.TestCase):
    def test_add_rule_for_id_create_from_scratch(self):
        tag = tagging.Tag.from_json('test',
                                    [{'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id1')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_append_in_existing_rule(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id2')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_id_already_exists(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id2,test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, 'test_id1')
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )

    def test_add_rule_for_id_multiple_ids(self):
        tag = tagging.Tag.from_json('test',
                                    [{'id': {'in_csv': 'test_id2,test_id1'}},
                                     {'col1': {'greater': 1}},
                                     {'col1': {'less': -1}}])
        new_tag = tag_helpers.add_rule_for_id(tag, ['test_id1', 'test_id3', 'test_id4'])
        self.assertListEqual(new_tag.rule.to_json(),
                             [{'id': {'in_csv': 'test_id3,test_id4,test_id2,test_id1'}},
                              {'col1': {'greater': 1}},
                              {'col1': {'less': -1}}]
                             )


if __name__ == '__main__':
    unittest.main()
