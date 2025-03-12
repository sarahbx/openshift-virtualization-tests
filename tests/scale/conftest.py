import pytest
from ocp_resources.cdi import CDI
from ocp_resources.data_source import DataSource
from ocp_resources.kubevirt import KubeVirt
from ocp_resources.ssp import SSP

from utilities.hco import ResourceEditorValidateHCOReconcile, wait_for_hco_conditions


class DataSourceNotReadyError(Exception):
    pass


@pytest.fixture(scope="class")
def total_allocatable_pods(schedulable_nodes):
    total_pods = 0
    for node in schedulable_nodes:
        node_status = node.instance.to_dict()["status"]
        value = node_status["allocatable"].get("pods", node_status["capacity"]["pods"])
        total_pods += int(value)

    assert total_pods, "No allocatable pods in cluster"
    return total_pods


@pytest.fixture(scope="class")
def golden_data_source(request, unprivileged_client, golden_images_namespace):
    if not hasattr(request, "param"):
        return

    data_source = DataSource(
        client=unprivileged_client,
        name=request.param["data_source_name"],
        namespace=golden_images_namespace.name,
    )

    not_ready_error_message = None
    for condition in data_source.instance.status.conditions:
        if condition.type == "Ready":
            if condition.status != "True":
                not_ready_error_message = f"Golden DataSource {data_source.name} not ready: {condition}"
            break

    if not_ready_error_message:
        if "win" in data_source.name:
            pytest.skip(not_ready_error_message)
        else:
            raise DataSourceNotReadyError(not_ready_error_message)

    return data_source


@pytest.fixture(scope="class")
def is_condensed_cluster(nodes, masters, workers, schedulable_nodes):
    """
    Condensed cluster consists of cluster with all nodes
    labeled as masters/workers/schedulable
    """
    return (
        {node.name for node in nodes}
        == {node.name for node in masters}
        == {node.name for node in workers}
        == {node.name for node in schedulable_nodes}
    )


@pytest.fixture(scope="module")
def patched_hco_with_high_burst_tuning_policy(admin_client, hco_namespace, hyperconverged_resource_scope_module):
    with ResourceEditorValidateHCOReconcile(
        patches={
            hyperconverged_resource_scope_module: {
                "spec": {
                    "tuningPolicy": "highBurst",
                },
            },
        },
        list_resource_reconcile=[KubeVirt, CDI, SSP],
        wait_for_reconcile_post_update=True,
    ):
        wait_for_hco_conditions(
            admin_client=admin_client,
            hco_namespace=hco_namespace,
        )
        yield
