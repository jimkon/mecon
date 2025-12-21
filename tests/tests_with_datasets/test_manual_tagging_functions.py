from unittest.mock import patch



def test_sanitize_tx_id(shiny_helpers):
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id1') == 'test_id1'
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id_with_-_in_it') == 'test_id_with__hyphen__in_it'
    assert shiny_helpers.manual_tagging.sanitize_tx_id('test_id_with_._in_it') == 'test_id_with__dot__in_it'


def test_desanitize_tx_id(shiny_helpers):
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id1') == 'test_id1'
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id_with__hyphen__in_it') == 'test_id_with_-_in_it'
    assert shiny_helpers.manual_tagging.desanitize_tx_id('test_id_with__dot__in_it') == 'test_id_with_._in_it'


def test_construct_amount_str(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    df = manager.get_transactions().dataframe()
    amount_strs = df.apply(shiny_helpers.manual_tagging.construct_amount_str, axis=1).tolist()
    expected_amount_strs = ['⮝ 3000.5 GBP', '⮝ 250.0 GBP', '⮝ 2500.0 GBP', '⮟ 1200.0 GBP', '⮟ 3.2 GBP', '⮟ 45.2 GBP', '⮟ 45.6 GBP', '⮟ 75.5 GBP', '⮟ 150.0 GBP', '⮝ 12.3 GBP', '⮝ 5.2 GBP']
    assert amount_strs == expected_amount_strs


def test_enhance_transactions_df(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    df = manager.get_transactions().dataframe()
    all_tags = {tag.name for tag in manager.all_tags()}

    with patch("services.edit_data.manual_tagging_app.ui_view_tx_button") as mck_view_tx_btn:
        mck_view_tx_btn.return_value = 'dummy_mck_view_tx_btn'
        df_enhanced = shiny_helpers.manual_tagging.enhance_transactions_df(df, all_tags, input_comps=None)
    assert len(df_enhanced.columns) == 9
    assert df_enhanced.columns.tolist() == ['Tx_ID', 'amount', 'date', 'time', 'week_id', 'n_tags', 'current_tags', 'add_tags', 'view_full']


def test_transform_tag_diffs(shiny_helpers):
    example_tag_diffs = [
        {'selectize_id': 'selectize_tx_id1', 'changes': ('tag1',)},
        {'selectize_id': 'selectize_tx_id2', 'changes': ('tag2',)},
        {'selectize_id': 'selectize_tx_id3', 'changes': ('tag3', 'tag4', 'tag1')}]

    expected_changes_per_tag = {
        'tag1': ['tx_id1', 'tx_id3'],
        'tag2': ['tx_id2'],
        'tag3': ['tx_id3'],
        'tag4': ['tx_id3'],
    }

    changes_per_tag = shiny_helpers.manual_tagging.transform_tag_diffs(example_tag_diffs)
    assert changes_per_tag == expected_changes_per_tag
