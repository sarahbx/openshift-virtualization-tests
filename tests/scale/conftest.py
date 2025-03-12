from __future__ import annotations

import logging
import math
import os
import resource
from subprocess import check_output
from typing import Any

import kubernetes
import pytest
import yaml
from kubernetes.dynamic import DynamicClient
from ocp_resources.cdi import CDI
from ocp_resources.kubevirt import KubeVirt
from ocp_resources.pod import Pod
from ocp_resources.resource import Resource

# from ocp_resources.resource import get_client
from ocp_resources.ssp import SSP
from urllib3.exceptions import MaxRetryError

from utilities.constants import TIMEOUT_20MIN, TIMEOUT_30SEC, UNPRIVILEGED_PASSWORD, UNPRIVILEGED_USER, VIRT_HANDLER
from utilities.hco import ResourceEditorValidateHCOReconcile, wait_for_hco_conditions
from utilities.infra import get_pods, login_with_user_password
from utilities.operator import wait_for_mcp_updated_condition_true

LOGGER = logging.getLogger(__name__)


class KubeletConfig(Resource):
    """
        KubeletConfig describes a customized Kubelet configuration.

    Compatibility level 1: Stable within a major release
    for a minimum of 12 months or 3 minor releases (whichever is longer).
    """

    api_group: str = Resource.ApiGroup.MACHINECONFIGURATION_OPENSHIFT_IO

    def __init__(
        self,
        auto_sizing_reserved: bool | None = None,
        kubelet_config: Any | None = None,
        log_level: int | None = None,
        machine_config_pool_selector: dict[str, Any] | None = None,
        tls_security_profile: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        r"""
        Args:
            auto_sizing_reserved (bool): No field description from API

            kubelet_config (Any): kubeletConfig fields are defined in kubernetes upstream. Please refer
              to the types defined in the version/commit used by OpenShift of
              the upstream kubernetes. It's important to note that, since the
              fields of the kubelet configuration are directly fetched from
              upstream the validation of those values is handled directly by the
              kubelet. Please refer to the upstream version of the relevant
              kubernetes for the valid values of these fields. Invalid values of
              the kubelet configuration fields may render cluster nodes
              unusable.

            log_level (int): No field description from API

            machine_config_pool_selector (dict[str, Any]): MachineConfigPoolSelector
              selects which pools the KubeletConfig shoud
              apply to. A nil selector will result in no pools being selected.

            tls_security_profile (dict[str, Any]): If unset, the default is based on the
              apiservers.config.openshift.io/cluster resource. Note that only
              Old and Intermediate profiles are currently supported, and the
              maximum available minTLSVersion is VersionTLS12.

        """
        super().__init__(**kwargs)

        self.auto_sizing_reserved = auto_sizing_reserved
        self.kubelet_config = kubelet_config
        self.log_level = log_level
        self.machine_config_pool_selector = machine_config_pool_selector
        self.tls_security_profile = tls_security_profile

    def to_dict(self) -> None:
        super().to_dict()

        if not self.kind_dict and not self.yaml_file:
            self.res["spec"] = {}
            _spec = self.res["spec"]

            if self.auto_sizing_reserved is not None:
                _spec["autoSizingReserved"] = self.auto_sizing_reserved

            if self.kubelet_config is not None:
                _spec["kubeletConfig"] = self.kubelet_config

            if self.log_level is not None:
                _spec["logLevel"] = self.log_level

            if self.machine_config_pool_selector is not None:
                _spec["machineConfigPoolSelector"] = self.machine_config_pool_selector

            if self.tls_security_profile is not None:
                _spec["tlsSecurityProfile"] = self.tls_security_profile

    # End of generated code


def get_client(
    config_file: str = "",
    config_dict: dict[str, Any] | None = None,
    context: str = "",
    **kwargs: Any,
) -> DynamicClient:
    """
    Get a kubernetes client.


    This function is a replica of `ocp_utilities.infra.get_client` which cannot be imported as ocp_utilities imports
    from ocp_resources.

    Pass either config_file or config_dict.
    If none of them are passed, client will be created from default OS kubeconfig
    (environment variable or .kube folder).

    Args:
        config_file (str): path to a kubeconfig file.
        config_dict (dict): dict with kubeconfig configuration.
        context (str): name of the context to use.

    Returns:
        DynamicClient: a kubernetes client.
    """
    # Ref: https://github.com/kubernetes-client/python/blob/v26.1.0/kubernetes/base/config/kube_config.py
    if config_dict:
        return kubernetes.dynamic.DynamicClient(
            client=kubernetes.config.new_client_from_config_dict(
                config_dict=config_dict, context=context or None, **kwargs
            )
        )
    client_configuration = kwargs.get("client_configuration", kubernetes.client.Configuration())
    try:
        # Ref: https://github.com/kubernetes-client/python/blob/v26.1.0/kubernetes/base/config/__init__.py
        LOGGER.info("Trying to get client via new_client_from_config")

        # kubernetes.config.kube_config.load_kube_config sets KUBE_CONFIG_DEFAULT_LOCATION during module import.
        # If `KUBECONFIG` environment variable is set via code, the `KUBE_CONFIG_DEFAULT_LOCATION` will be None since
        # is populated during import which comes before setting the variable in code.
        config_file = config_file or os.environ.get("KUBECONFIG", "~/.kube/config")

        if os.environ.get("OPENSHIFT_PYTHON_WRAPPER_CLIENT_USE_PROXY"):
            proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
            if not proxy:
                raise ValueError(
                    "Proxy configuration is enabled but neither "
                    "HTTPS_PROXY nor HTTP_PROXY environment variables are set."
                )
            if client_configuration.proxy and client_configuration.proxy != proxy:
                raise ValueError(
                    f"Conflicting proxy settings: client_configuration.proxy={client_configuration.proxy}, "
                    f"but the environment variable 'HTTPS_PROXY/HTTP_PROXY' defines proxy as {proxy}."
                )
            client_configuration.proxy = proxy

        kwargs["client_configuration"] = client_configuration

        return kubernetes.dynamic.DynamicClient(
            client=kubernetes.config.new_client_from_config(
                config_file=config_file,
                context=context or None,
                **kwargs,
            )
        )
    except MaxRetryError:
        # Ref: https://github.com/kubernetes-client/python/blob/v26.1.0/kubernetes/base/config/incluster_config.py
        LOGGER.info("Trying to get client via incluster_config")
        return kubernetes.dynamic.DynamicClient(
            client=kubernetes.config.incluster_config.load_incluster_config(
                client_configuration=client_configuration,
                try_refresh_token=kwargs.get("try_refresh_token", True),
            )
        )


@pytest.fixture(scope="module")
def scale_client_configuration(request):
    client_configuration = kubernetes.client.Configuration()
    client_configuration.connection_pool_maxsize = request.param["connection_pool_maxsize"]
    return client_configuration


@pytest.fixture(scope="module")
def scale_admin_client(scale_client_configuration):
    yield get_client(client_configuration=scale_client_configuration)


@pytest.fixture(scope="module")
def scale_unprivileged_client(
    scale_client_configuration, skip_unprivileged_client, exported_kubeconfig, admin_client, unprivileged_client
):
    if skip_unprivileged_client:
        yield
    else:
        current_user = check_output("oc whoami", shell=True).decode().strip()  # Get current admin account
        if login_with_user_password(
            api_address=admin_client.configuration.host,
            user=UNPRIVILEGED_USER,
            password=UNPRIVILEGED_PASSWORD,
        ):  # Login to unprivileged account
            with open(exported_kubeconfig, "r") as fd:
                kubeconfig_content = yaml.safe_load(fd)
            unprivileged_context = kubeconfig_content["current-context"]
            login_with_user_password(
                api_address=admin_client.configuration.host,
                user=current_user.strip(),
            )  # Get back to admin account

            yield get_client(
                client_configuration=scale_client_configuration,
                config_file=exported_kubeconfig,
                context=unprivileged_context,
            )
        else:
            yield


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


@pytest.fixture(scope="module")
def increased_open_file_limit(request):
    nofile_hard_limit = request.param["nofile_hard_limit"]
    original_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (nofile_hard_limit, nofile_hard_limit))
    yield
    resource.setrlimit(resource.RLIMIT_NOFILE, original_limits)


@pytest.fixture(scope="module")
def virt_handler_pods(admin_client, hco_namespace):
    return get_pods(
        dyn_client=admin_client,
        namespace=hco_namespace,
        label=f"{Pod.ApiGroup.KUBEVIRT_IO}={VIRT_HANDLER}",
    )


@pytest.fixture(scope="module")
def virt_handler_nodes(virt_handler_pods):
    return [pod.node for pod in virt_handler_pods]


@pytest.fixture(scope="module")
def existing_pod_count(admin_client):
    return len(list(Pod.get(dyn_client=admin_client)))


@pytest.fixture(scope="module")
def calculated_max_vms_per_virt_node(request, existing_pod_count, virt_handler_nodes):
    total_pod_count = existing_pod_count + request.param["total_vm_count"]
    max_pods = total_pod_count / (len(virt_handler_nodes) - 1)

    total_cores = 0
    existing_max_pods_per_node = {}
    for node in virt_handler_nodes:
        node_capacity = node.instance.status.capacity
        total_cores += int(node_capacity.cpu)
        existing_max_pods_per_node[node.name] = int(node_capacity.pods)

    pods_per_core = (total_pod_count / total_cores) * 2

    return dict(
        max_pods=math.ceil(max_pods),
        pods_per_core=math.ceil(pods_per_core),
        min_existing_max_pods_per_node=min(existing_max_pods_per_node.values()),
    )


@pytest.fixture(scope="module")
def scale_max_vms_per_virt_node(request, calculated_max_vms_per_virt_node, workers, machine_config_pools):
    max_pods = calculated_max_vms_per_virt_node["max_pods"]
    pods_per_core = calculated_max_vms_per_virt_node["pods_per_core"]
    min_existing_max_pods_per_node = calculated_max_vms_per_virt_node["min_existing_max_pods_per_node"]

    if max_pods > min_existing_max_pods_per_node:
        # Allow early abort to prevent rolling MCP when not desired.
        # Making this change to the cluster can be time intensive.
        assert request.param["scale"], (
            f"Aborting early, scaling max pods is not desired programatically. "
            f"Requred values to set: {calculated_max_vms_per_virt_node}"
        )
        with KubeletConfig(
            name="test-set-worker-max-pods",
            kubelet_config={
                "maxPods": max_pods,
                "podsPerCore": pods_per_core,
            },
            machine_config_pool_selector={
                "matchLabels": {
                    "pools.operator.machineconfiguration.openshift.io/worker": "",
                }
            },
        ):
            wait_for_mcp_updated_condition_true(
                machine_config_pools_list=machine_config_pools,
                timeout=TIMEOUT_20MIN * len(workers),
                sleep=TIMEOUT_30SEC,
            )
            yield
        wait_for_mcp_updated_condition_true(
            machine_config_pools_list=machine_config_pools,
            timeout=TIMEOUT_20MIN * len(workers),
            sleep=TIMEOUT_30SEC,
        )
    else:
        LOGGER.info(
            "Skipping scaling max pods per worker node, "
            f"current existing {min_existing_max_pods_per_node} is greater than {max_pods}"
        )
        yield
