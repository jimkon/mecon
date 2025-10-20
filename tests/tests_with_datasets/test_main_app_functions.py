
def test_create_markdown_menu_from_links(dataset_manager, shiny_helpers):
    dataset, _, _ = dataset_manager
    links = dataset.settings.get("links", {})
    markdown = shiny_helpers.main_app.create_markdown_menu_from_links(links)
    assert "### Comparisons" in markdown
    assert "### Reports" in markdown
    assert "### Tagging" in markdown
    assert "[Commute]" in markdown
