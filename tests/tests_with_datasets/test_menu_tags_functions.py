import pandas as pd

def test_build_tags_table(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager
    tags_table = shiny_helpers.menu_tags.build_tags_table(manager)
    assert "Name" in tags_table.columns
    assert any(name == "Commute" for name in tags_table["Name"])


def test_create_tag_from_name(shiny_helpers):
    tag = shiny_helpers.menu_tags.create_tag_from_name("Sample")
    assert tag.name == "Sample"


def test_refresh_tag_reactives(dataset_manager, shiny_helpers):
    _, manager, _ = dataset_manager

    class DummyReactive:
        def __init__(self):
            self.value = None

        def set(self, value):
            self.value = value

    all_tags_reactive = DummyReactive()
    tags_metadata_reactive = DummyReactive()

    shiny_helpers.menu_tags.refresh_tag_reactives(
        manager, all_tags_reactive, tags_metadata_reactive
    )

    assert [tag.name for tag in all_tags_reactive.value] == [
        tag.name for tag in manager.all_tags()
    ]
    pd.testing.assert_frame_equal(
        tags_metadata_reactive.value.reset_index(drop=True),
        manager.get_tags_metadata().reset_index(drop=True),
    )
