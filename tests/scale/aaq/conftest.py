import logging
from copy import deepcopy

import pytest
import yaml
from ocp_resources.aaq import AAQ
from ocp_resources.application_aware_cluster_resource_quota import ApplicationAwareClusterResourceQuota
from ocp_resources.application_aware_resource_quota import ApplicationAwareResourceQuota
from ocp_resources.cluster_resource_quota import ClusterResourceQuota
from ocp_resources.hyperconverged import HyperConverged
from ocp_resources.namespace import Namespace
from ocp_resources.project_project_openshift_io import Project
from ocp_resources.project_request import ProjectRequest
from ocp_resources.resource import ResourceEditor
from ocp_resources.resource_quota import ResourceQuota
from ocp_resources.virtual_machine import VirtualMachine
from ocp_scale_utilities.monitoring import MonitorResourceAPIServerRequests
from ocp_scale_utilities.threaded.scale import ThreadedScaleResources
from ocp_scale_utilities.threaded.utils import threaded_wait_deleted_resources

from tests.scale.aaq.constants import ALLOW_AACRQ_PARAM
from tests.scale.threaded_utils.virt import threaded_wait_for_running_vms, threaded_wait_for_scheduled_vms
from tests.scale.utils import capture_func_elapsed
from utilities.constants import EXPECTED_STATUS_CONDITIONS, TIMEOUT_8MIN
from utilities.hco import wait_for_hco_conditions
from utilities.infra import wait_for_consistent_resource_conditions
from utilities.virt import VirtualMachineForTests, fedora_vm_body

LOGGER = logging.getLogger(__name__)

VIRT_HANDLER_API_IDLE_STATE = 0.067


@pytest.fixture(scope="class")
def aaq_dict(request):
    with open(request.param["aaq_yaml_file"]) as file:
        _aaq_dict = yaml.safe_load(file)

    if ALLOW_AACRQ_PARAM in request.param:
        _aaq_dict["spec"]["configuration"]["allowApplicationAwareClusterResourceQuota"] = request.param[
            ALLOW_AACRQ_PARAM
        ]

    return _aaq_dict


@pytest.fixture(scope="class")
def vm_config(request):
    vm_body_yaml_file = request.param["vm_body_yaml_file"]
    if vm_body_yaml_file:
        with open(vm_body_yaml_file) as file:
            vm_body = yaml.safe_load(file)
    else:
        vm_body = fedora_vm_body(name="placeholder")

    return {
        "vm_count": request.param["vm_count"],
        "diskless": request.param["diskless"],
        "body": vm_body,
    }


@pytest.fixture(scope="class")
def request_quota_config(request):
    with open(request.param["rq_fields_yaml_file"]) as file:
        return {"rq_count": request.param["rq_count"], "fields": yaml.safe_load(file)}


@pytest.fixture(scope="class")
def cluster_request_quota_config(request):
    with open(request.param["crq_fields_yaml_file"]) as file:
        return {"crq_count": request.param["crq_count"], "fields": yaml.safe_load(file)}


@pytest.fixture(scope="class")
def scale_projects(request, scale_admin_client, scale_unprivileged_client):
    project_requests = []
    projects = []
    namespaces = []
    scale_aaq_namespace_prefix = "test-scale-aaq"
    project_label = {
        "aaq-test": "test",
    }

    for index in range(request.param["project_count"]):
        name = f"{scale_aaq_namespace_prefix}-{index}"
        project_requests.append(
            ProjectRequest(
                name=name,
                client=scale_unprivileged_client,
                label=deepcopy(project_label),
            ),
        )
        projects.append(
            Project(
                name=name,
                client=scale_unprivileged_client,
            )
        )
        namespaces.append(
            Namespace(
                name=name,
                client=scale_admin_client,
            )
        )

    with ThreadedScaleResources(
        resources=projects, request_resources=project_requests, wait_for_status=Project.Status.ACTIVE
    ):
        yield projects
    threaded_wait_deleted_resources(resources=namespaces)


@pytest.fixture()
def cache_data_parsed(request, vm_config):
    data = {"deploy": {}, "scheduled": {}, "running": {}, "delete": {}, "pass": False, "errors": []}
    if vm_config["diskless"]:
        del data["running"]

    cache_keys = request.param["cache_keys"]
    output_file = request.param.get("output_file")

    for entry in set(data.keys()) ^ {"pass", "errors"}:
        for cache_key in cache_keys:
            data[entry][cache_key] = {}
            for field in ["start", "stop", "elapsed"]:
                if field == "elapsed":
                    try:
                        data[entry][cache_key][field] = data[entry][cache_key]["stop"] - data[entry][cache_key]["start"]
                    except KeyError as exp:
                        LOGGER.error(f"Key error for cache_key: {cache_key} {exp}")
                        raise
                    except TypeError:
                        data["errors"].append(f"Cache key {cache_key} has incomplete data: {data[entry][cache_key]}")
                else:
                    data[entry][cache_key][field] = request.config.cache.get(f"{cache_key}-{entry}-{field}", None)

    # check data
    baseline_key = [key for key in cache_keys if key.endswith("baseline")][0]
    baseline_elapsed = data["scheduled"][baseline_key]["elapsed"]
    for cache_key in set(cache_keys) ^ {baseline_key}:
        key_elapsed = data["scheduled"][cache_key]["elapsed"]
        if key_elapsed > (baseline_elapsed * 1.1):
            data["pass"] = False
            data["errors"].append(
                f"Fail: Scheduled {cache_key} elapsed time was {key_elapsed} "
                f"which was >10% over baseline elapsed: {baseline_elapsed}"
            )

    if not data["errors"]:
        data["pass"] = True
    data["errors"] = "\n".join(data["errors"])

    if output_file:
        with open(output_file, "ab") as file:
            yaml.dump(data, file, explicit_start=True, encoding="utf-8")

    return data


@pytest.fixture()
def created_aaq_resource(admin_client, aaq_dict):
    with AAQ(
        name=aaq_dict["metadata"]["name"],
        client=admin_client,
        kind_dict=deepcopy(aaq_dict),
        delete_timeout=TIMEOUT_8MIN,
    ) as aaq:
        wait_for_consistent_resource_conditions(
            dynamic_client=admin_client,
            namespace=None,
            resource_kind=AAQ,
            expected_conditions=EXPECTED_STATUS_CONDITIONS[AAQ],
            consecutive_checks_count=3,
        )
        yield aaq


@pytest.fixture()
def created_aaq_resource_via_hco(aaq_dict, admin_client, hco_namespace, hyperconverged_resource_scope_class):
    consecutive_checks_count = 3

    with ResourceEditor(
        patches={
            hyperconverged_resource_scope_class: {
                "spec": {
                    "featureGates": {
                        "enableApplicationAwareQuota": True,
                    },
                    "applicationAwareConfig": deepcopy(aaq_dict["spec"]["configuration"]),
                },
            },
        },
    ):
        AAQ(
            name=f"aaq-{hyperconverged_resource_scope_class.name}",
            client=admin_client,
        ).wait()
        wait_for_hco_conditions(
            admin_client=admin_client,
            hco_namespace=hco_namespace,
            consecutive_checks_count=consecutive_checks_count,
            list_dependent_crs_to_check=set(EXPECTED_STATUS_CONDITIONS.keys()) ^ {HyperConverged},
        )
        yield
    wait_for_hco_conditions(
        admin_client=admin_client,
        hco_namespace=hco_namespace,
        consecutive_checks_count=consecutive_checks_count,
        list_dependent_crs_to_check=set(EXPECTED_STATUS_CONDITIONS.keys()) ^ {AAQ, HyperConverged},
    )


@pytest.fixture()
def scale_vms(request, vm_config, scale_unprivileged_client, virt_handler_pods, prometheus, scale_projects):
    vms = []
    for project in scale_projects:
        for index in range(vm_config["vm_count"]):
            vms.append(
                VirtualMachineForTests(
                    name=f"vm-{project.name}-{index}",
                    namespace=project.name,
                    client=scale_unprivileged_client,
                    generate_unique_name=False,
                    body=deepcopy(vm_config["body"]),
                    diskless_vm=vm_config["diskless"],
                    run_strategy=VirtualMachine.RunStrategy.ALWAYS,
                )
            )

    virtual_machine_resource_idle = VIRT_HANDLER_API_IDLE_STATE * len(virt_handler_pods)
    monitor_api_requests = MonitorResourceAPIServerRequests(
        prometheus=prometheus,
        resource_class=VirtualMachine,
        idle_requests_value=virtual_machine_resource_idle,
    )

    cache_key = request.param["cache_key"]
    monitor_api_requests.wait_for_idle()
    with ThreadedScaleResources(resources=vms, pytest_cache=request.config.cache, cache_key_prefix=cache_key):
        capture_func_elapsed(
            cache=request.config.cache,
            cache_key_prefix=f"{cache_key}-scheduled",
            func=threaded_wait_for_scheduled_vms,
            vms=vms,
        )
        if not vm_config["diskless"]:
            capture_func_elapsed(
                cache=request.config.cache,
                cache_key_prefix=f"{cache_key}-running",
                func=threaded_wait_for_running_vms,
                vms=vms,
            )
        monitor_api_requests.wait_for_idle()
        yield vms
    monitor_api_requests.wait_for_idle()


@pytest.fixture()
def scale_rq(scale_admin_client, request_quota_config, scale_projects):
    resources = [
        ResourceQuota(
            name=f"rq-{index}",
            namespace=project.name,
            client=scale_admin_client,
            **request_quota_config["fields"],
        )
        for index in range(request_quota_config["rq_count"])
        for project in scale_projects
    ]

    with ThreadedScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_aarq(scale_admin_client, request_quota_config, scale_projects):
    resources = [
        ApplicationAwareResourceQuota(
            name=f"aarq-{index}",
            namespace=project.name,
            client=scale_admin_client,
            **request_quota_config["fields"],
        )
        for index in range(request_quota_config["rq_count"])
        for project in scale_projects
    ]

    with ThreadedScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_crq(scale_admin_client, cluster_request_quota_config):
    resources = [
        ClusterResourceQuota(
            name=f"crq-{index}",
            client=scale_admin_client,
            **cluster_request_quota_config["fields"],
        )
        for index in range(cluster_request_quota_config["crq_count"])
    ]

    with ThreadedScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_aacrq(scale_admin_client, cluster_request_quota_config):
    resources = [
        ApplicationAwareClusterResourceQuota(
            name=f"aacrq-{index}",
            client=scale_admin_client,
            **cluster_request_quota_config["fields"],
        )
        for index in range(cluster_request_quota_config["crq_count"])
    ]

    with ThreadedScaleResources(resources=resources):
        yield resources
