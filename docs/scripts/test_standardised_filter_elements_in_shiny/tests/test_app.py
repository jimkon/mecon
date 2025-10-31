from __future__ import annotations

import datetime
import shutil
import sys
from pathlib import Path
from typing import Iterable

from playwright.sync_api import Page
from shiny.playwright import controller
from shiny.pytest import create_app_fixture
from shiny.run import ShinyAppProc

from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset


TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import dataset_helper  # noqa: E402  (import after sys.path mutation)


APP_PATH = APP_DIR / "simple_filter_app.py"


app = create_app_fixture(str(APP_PATH))


def _normalize_tags(raw: Iterable[str] | str | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    return tuple(raw)


def _create_data_manager() -> tuple[CachedFileDataManager, Path]:
    dataset_path = dataset_helper.create_manual_check_dataset()
    return CachedFileDataManager(Dataset(dataset_path)), dataset_path


def _expected_filtered_transactions(
    data_manager: CachedFileDataManager,
    *,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    filter_in_tags: tuple[str, ...],
    filter_out_tags: tuple[str, ...],
    time_unit: str,
):
    transactions = data_manager.get_transactions()
    in_date_range = transactions.select_date_range(start_date, end_date)
    filtered_in = in_date_range.containing_tags(filter_in_tags)
    filtered_out = filtered_in.not_containing_tags(
        filter_out_tags, empty_tags_strategy="all_true"
    )
    return filtered_out.group_and_fill_transactions(
        grouping_key=time_unit,
        aggregation_key="sum",
    )


def test_app(page: Page, app: ShinyAppProc):
    page.goto(app.url)

    filter_params_output = controller.OutputText(page, "get_filter_params_text")
    filter_params_text = filter_params_output.get_value()
    filter_params = eval(filter_params_text, {"datetime": datetime}, {})

    filter_in_tags = _normalize_tags(filter_params.get("filter_in_tags"))
    filter_out_tags = _normalize_tags(filter_params.get("filter_out_tags"))

    assert filter_params["time_unit"] == "day"
    assert filter_in_tags == ()
    assert filter_out_tags == ()

    data_manager, dataset_path = _create_data_manager()
    try:
        transactions = data_manager.get_transactions()
        expected_default_df = transactions.dataframe()

        default_table = controller.OutputDataFrame(page, "default_transactions_table")
        default_table.expect_ncol(expected_default_df.shape[1])
        default_table.expect_nrow(expected_default_df.shape[0])
        default_table.expect_column_labels(expected_default_df.columns.tolist())

        if not expected_default_df.empty:
            id_col_index = expected_default_df.columns.get_loc("id")
            default_table.expect_cell_value(
                str(expected_default_df.iloc[0, id_col_index]), row=0, col=id_col_index
            )

        filtered_transactions = _expected_filtered_transactions(
            data_manager,
            start_date=filter_params.get("start_date"),
            end_date=filter_params.get("end_date"),
            filter_in_tags=filter_in_tags,
            filter_out_tags=filter_out_tags,
            time_unit=filter_params.get("time_unit"),
        )
        expected_filtered_df = filtered_transactions.dataframe()

        filtered_table = controller.OutputDataFrame(page, "filtered_transactions_table")
        filtered_table.expect_ncol(expected_filtered_df.shape[1])
        filtered_table.expect_nrow(expected_filtered_df.shape[0])
        filtered_table.expect_column_labels(expected_filtered_df.columns.tolist())

        if not expected_filtered_df.empty:
            id_col_index = expected_filtered_df.columns.get_loc("id")
            filtered_table.expect_cell_value(
                str(expected_filtered_df.iloc[0, id_col_index]), row=0, col=id_col_index
            )
    finally:
        shutil.rmtree(dataset_path.parent, ignore_errors=True)

