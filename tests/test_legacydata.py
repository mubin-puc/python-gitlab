import pytest
from src.generate_legacy_data import UpdateLegacyTaskDetails

@pytest.fixture(scope="module")
def update_task_details():
    return UpdateLegacyTaskDetails('DMA-Legacy')

def test_all_legacy_tasks_closed(update_task_details):
    legacy_all_open_tasks_df = update_task_details.get_legacy_all_open_tasks_list()
    assert legacy_all_open_tasks_df.empty, "There are still open tasks for DMA Legacy Launcher"