import pytest
from src.generate_modernized_data import UpdateK8sTaskDetails  # Import the UpdateK8sTaskDetails class

@pytest.fixture(scope="module")
def update_task_details():
    return UpdateK8sTaskDetails('DMA-K8s')

def test_all_k8s_tasks_closed(update_task_details):
    k8s_all_open_tasks_df = update_task_details.get_k8s_all_open_tasks_list()
    assert k8s_all_open_tasks_df.empty, "There are still open tasks for DMA K8s Launcher"