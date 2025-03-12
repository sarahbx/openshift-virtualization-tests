from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

import pytest
import yaml
from ocp_resources.aaq import AAQ
from ocp_resources.application_aware_cluster_resource_quota import ApplicationAwareClusterResourceQuota
from ocp_resources.application_aware_resource_quota import ApplicationAwareResourceQuota
from ocp_resources.exceptions import MissingRequiredArgumentError
from ocp_resources.hyperconverged import HyperConverged
from ocp_resources.pod import Pod
from ocp_resources.project_request import Project, ProjectRequest
from ocp_resources.resource import Resource, ResourceEditor
from ocp_resources.resource_quota import ResourceQuota
from ocp_resources.virtual_machine import VirtualMachine

from tests.scale.aaq.constants import ALLOW_AACRQ_PARAM
from tests.scale.async_utils.scale import AsyncScaleResources
from tests.scale.async_utils.virt import async_wait_for_running_vms, async_wait_for_scheduled_vms
from tests.scale.utils import MonitorResourceAPIServerRequests, capture_func_elapsed
from utilities.constants import EXPECTED_STATUS_CONDITIONS, TIMEOUT_8MIN, VIRT_HANDLER
from utilities.hco import wait_for_hco_conditions
from utilities.infra import get_pods, wait_for_consistent_resource_conditions
from utilities.virt import VirtualMachineForTests, fedora_vm_body

LOGGER = logging.getLogger(__name__)

VIRT_HANDLER_API_IDLE_STATE = 0.067


class ClusterResourceQuota(Resource):
    """
        ClusterResourceQuota mirrors ResourceQuota at a cluster scope.  This object is easily convertible to
    synthetic ResourceQuota object to allow quota evaluation re-use.
    Compatibility level 1: Stable within a major release for a
    minimum of 12 months or 3 minor releases (whichever is longer).
    """

    api_group: str = "quota.openshift.io"

    def __init__(
        self,
        quota: dict[str, Any] | None = None,
        selector: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            quota (dict[str, Any]): Quota defines the desired quota
            selector (dict[str, Any]): Selector is the selector used to match projects. It should only select
              active projects on the scale of dozens (though it can select many
              more less active projects).  These projects will contend on object
              creation through this resource.
        """
        super().__init__(**kwargs)

        self.quota = quota
        self.selector = selector

    def to_dict(self) -> None:
        super().to_dict()

        if not self.kind_dict and not self.yaml_file:
            if self.quota is None:
                raise MissingRequiredArgumentError(argument="self.quota")

            if self.selector is None:
                raise MissingRequiredArgumentError(argument="self.selector")

            self.res["spec"] = {}
            _spec = self.res["spec"]

            _spec["quota"] = self.quota
            _spec["selector"] = self.selector


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

    return {"vm_count": request.param["vm_count"], "body": vm_body}


@pytest.fixture(scope="class")
def request_quota_config(request):
    with open(request.param["rq_fields_yaml_file"]) as file:
        return {"rq_count": request.param["rq_count"], "fields": yaml.safe_load(file)}


@pytest.fixture(scope="class")
def cluster_request_quota_config(request):
    with open(request.param["crq_fields_yaml_file"]) as file:
        return {"crq_count": request.param["crq_count"], "fields": yaml.safe_load(file)}


@pytest.fixture(scope="class")
def scale_projects(request, unprivileged_client):
    project_requests = []
    projects = []
    scale_aaq_namespace_prefix = "test-scale-aaq"

    for index in range(request.param["project_count"]):
        name = f"{scale_aaq_namespace_prefix}-{index}"
        project_requests.append(
            ProjectRequest(
                name=name,
                client=unprivileged_client,
            ),
        )
        projects.append(
            Project(
                name=name,
                client=unprivileged_client,
            )
        )

    with AsyncScaleResources(
        resources=projects, request_resources=project_requests, wait_for_status=Project.Status.ACTIVE
    ):
        yield projects


@pytest.fixture()
def cache_data_parsed(request):
    data = {"deploy": {}, "scheduled": {}, "running": {}, "delete": {}}
    for entry in data.keys():
        for cache_key in request.param["cache_keys"]:
            data[entry][cache_key] = {}
            for field in ["start", "stop", "elapsed"]:
                if field == "elapsed":
                    try:
                        data[entry][cache_key][field] = data[entry][cache_key]["stop"] - data[entry][cache_key]["start"]
                    except KeyError as exp:
                        LOGGER.error(f"Key error for cache_key: {cache_key} {exp}")
                        raise
                else:
                    data[entry][cache_key][field] = request.config.cache.get(f"{cache_key}-{entry}-{field}", None)
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
def scale_vms(request, vm_config, admin_client, unprivileged_client, hco_namespace, prometheus, scale_projects):
    vms = []
    for project in scale_projects:
        for index in range(vm_config["vm_count"]):
            vms.append(
                VirtualMachineForTests(
                    name=f"vm-{project.name}-{index}",
                    namespace=project.name,
                    client=unprivileged_client,
                    generate_unique_name=False,
                    body=deepcopy(vm_config["body"]),
                    diskless_vm=True,
                    run_strategy=VirtualMachine.RunStrategy.ALWAYS,
                )
            )

    virt_handler_pods = get_pods(
        dyn_client=admin_client,
        namespace=hco_namespace,
        label=f"{Pod.ApiGroup.KUBEVIRT_IO}={VIRT_HANDLER}",
    )
    virtual_machine_resource_idle = VIRT_HANDLER_API_IDLE_STATE * len(virt_handler_pods)
    monitor_api_requests = MonitorResourceAPIServerRequests(
        prometheus=prometheus,
        resource_class=VirtualMachine,
        idle_requests_value=virtual_machine_resource_idle,
    )

    cache_key = request.param["cache_key"]
    monitor_api_requests.wait_for_idle()
    with AsyncScaleResources(resources=vms, pytest_cache=request.config.cache, cache_key_prefix=cache_key):
        capture_func_elapsed(
            cache=request.config.cache,
            cache_key_prefix=f"{cache_key}-scheduled",
            func=async_wait_for_scheduled_vms,
            vms=vms,
        )
        capture_func_elapsed(
            cache=request.config.cache,
            cache_key_prefix=f"{cache_key}-running",
            func=async_wait_for_running_vms,
            vms=vms,
        )
        monitor_api_requests.wait_for_idle()
        yield vms
    monitor_api_requests.wait_for_idle()


@pytest.fixture()
def scale_rq(admin_client, request_quota_config, scale_projects):
    resources = [
        ResourceQuota(
            name=f"rq-{index}",
            namespace=project.name,
            client=admin_client,
            **request_quota_config["fields"],
        )
        for index in range(request_quota_config["rq_count"])
        for project in scale_projects
    ]

    with AsyncScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_aarq(admin_client, request_quota_config, scale_projects):
    resources = [
        ApplicationAwareResourceQuota(
            name=f"aarq-{index}",
            namespace=project.name,
            client=admin_client,
            **request_quota_config["fields"],
        )
        for index in range(request_quota_config["rq_count"])
        for project in scale_projects
    ]

    with AsyncScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_crq(admin_client, cluster_request_quota_config):
    resources = [
        ClusterResourceQuota(
            name=f"crq-{index}",
            client=admin_client,
            **cluster_request_quota_config["fields"],
        )
        for index in range(cluster_request_quota_config["crq_count"])
    ]

    with AsyncScaleResources(resources=resources):
        yield resources


@pytest.fixture()
def scale_aacrq(admin_client, cluster_request_quota_config):
    resources = [
        ApplicationAwareClusterResourceQuota(
            name=f"aacrq-{index}",
            client=admin_client,
            **cluster_request_quota_config["fields"],
        )
        for index in range(cluster_request_quota_config["crq_count"])
    ]

    with AsyncScaleResources(resources=resources):
        yield resources
