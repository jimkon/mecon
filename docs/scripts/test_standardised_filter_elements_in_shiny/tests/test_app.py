from playwright.sync_api import Page
from shiny.playwright import controller
from shiny.pytest import create_app_fixture
from shiny.run import ShinyAppProc

from mecon.data.data_management import CachedFileDataManager
from mecon.etl.dataset import Dataset

dataset = Dataset(r"C:\Users\dimitris\PycharmProjects\mecon\tests\tests_with_datasets\datasets\test_full")
data_manager = CachedFileDataManager(dataset)


app = create_app_fixture("..\simple_filter_app.py")


# pip install pytest-playwright
# playwright install
def test_app(page: Page, app: ShinyAppProc):

    page.goto(app.url)
    # Add test code here
    filter_params = controller.OutputText(page, 'get_filter_params_text')
    filter_params.expect_value("{'start_date': None, 'end_date': datetime.date(2024, 3, 25), 'time_unit': 'month', 'filter_in_tags': (), 'filter_out_tags': ()}")

    expected_default_transactions_df = data_manager.get_transactions().dataframe()
    default_transactions_df = controller.OutputDataFrame(page, 'default_transactions_table')
    default_transactions_df.expect_ncol(expected_default_transactions_df.shape[1])
    default_transactions_df.expect_nrow(expected_default_transactions_df.shape[0])
