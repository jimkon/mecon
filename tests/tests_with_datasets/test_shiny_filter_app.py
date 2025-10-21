import asyncio
import datetime
from types import SimpleNamespace

import pytest
from shiny import App, reactive, ui
from shiny._connection import MockConnection


def _flush_reactive():
    asyncio.run(reactive.flush())


def _trigger_initialisation(context: SimpleNamespace):
    with reactive.isolate():
        current = context.session.input[".clientdata_url_search"]()
        context.session.input[".clientdata_url_search"].set(current + "&init=1")
    _flush_reactive()


@pytest.fixture
def shiny_filter_context(dataset_manager, monkeypatch):
    from shiny._validation import req as shiny_req
    from mecon.app import shiny_app as shiny_module

    if not hasattr(reactive, "req"):
        monkeypatch.setattr(reactive, "req", shiny_req, raising=False)
    if not hasattr(shiny_module.reactive, "req"):
        monkeypatch.setattr(shiny_module.reactive, "req", shiny_req, raising=False)

    _dataset, _manager, _root = dataset_manager
    data_manager = shiny_module.create_data_manager()

    test_app = App(ui.page_fluid(), server=lambda input, output, session: None)
    session = test_app._create_session(MockConnection())

    transactions = data_manager.get_transactions()
    dataset_start, dataset_end = transactions.date_range()

    with reactive.isolate():
        session.input[".clientdata_url_search"] = reactive.Value(
            "?filter_in_tags=Commute&filter_out_tags=&time_unit=month&start_date=2024-03-01&end_date=2024-03-25"
        )
        session.input["transactions_date_range"] = reactive.Value((dataset_start, dataset_end))
        session.input["date_period_input_select"] = reactive.Value("All")
        session.input["time_unit_select"] = reactive.Value("month")
        session.input["filter_in_tags_select"] = reactive.Value([])
        session.input["filter_out_tags_select"] = reactive.Value([])
        session.input["compare_tags_select"] = reactive.Value([])

    def _set_input_value(input_id: str, value):
        with reactive.isolate():
            session.input[input_id].set(value)

    def update_selectize(*, id, choices=None, selected=None, **kwargs):
        _set_input_value(id, selected if selected is not None else [])

    def update_radio_buttons(*, id, selected=None, **kwargs):
        _set_input_value(id, selected)

    def update_date_range(*, id, start=None, end=None, **kwargs):
        _set_input_value(id, (start, end))

    monkeypatch.setattr(shiny_module.ui, "update_selectize", update_selectize)
    monkeypatch.setattr(shiny_module.ui, "update_radio_buttons", update_radio_buttons)
    monkeypatch.setattr(shiny_module.ui, "update_date_range", update_date_range)

    get_filter_params, default_transactions, init_effect, filtered_transactions = shiny_module.filter_funcs_factory(
        session.input,
        session.output,
        session,
        data_manager,
    )

    context = SimpleNamespace(
        app=test_app,
        session=session,
        shiny_module=shiny_module,
        data_manager=data_manager,
        get_filter_params=get_filter_params,
        default_transactions=default_transactions,
        init_effect=init_effect,
        filtered_transactions=filtered_transactions,
    )

    _trigger_initialisation(context)

    return context


def test_url_params_seed_filter_inputs(shiny_filter_context):
    ctx = shiny_filter_context

    with reactive.isolate():
        time_unit = ctx.session.input["time_unit_select"]()
        include_tags = ctx.session.input["filter_in_tags_select"]()
        start_date, end_date = ctx.session.input["transactions_date_range"]()

    assert time_unit == "month"
    assert include_tags == ["Commute"]
    assert start_date == datetime.date(2024, 3, 1)
    assert end_date == datetime.date(2024, 3, 25)


def test_filtered_transactions_respects_tag_filters(shiny_filter_context):
    ctx = shiny_filter_context

    with reactive.isolate():
        trans = ctx.default_transactions()
    df = trans.dataframe()

    assert len(df) == 2
    assert all("Commute" in tags for tags in df["tags"])

    with reactive.isolate():
        ctx.session.input["filter_out_tags_select"].set(["Monzo"])
    _flush_reactive()
    with reactive.isolate():
        filtered = ctx.filtered_transactions()
    filtered_df = filtered.dataframe()

    assert len(filtered_df) == 1
    assert all("Monzo" not in tags for tags in filtered_df["tags"])


def test_filtered_transactions_respects_date_range(shiny_filter_context):
    ctx = shiny_filter_context

    with reactive.isolate():
        ctx.session.input["transactions_date_range"].set(
            (datetime.date(2024, 3, 4), datetime.date(2024, 3, 4))
        )
    _flush_reactive()
    with reactive.isolate():
        filtered = ctx.filtered_transactions()

    filtered_df = filtered.dataframe()
    assert len(filtered_df) == 1
    assert all("2024-03-04" in ts for ts in filtered_df["datetime"].astype(str))
    assert all("Commute" in tags for tags in filtered_df["tags"])
