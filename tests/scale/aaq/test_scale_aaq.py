import logging
import os

import pytest
import yaml

from tests.scale.aaq.constants import ALLOW_AACRQ_PARAM

LOGGER = logging.getLogger(__name__)

YAMLS_DIR = os.path.join(os.path.dirname(__file__), "yamls")

PROJECT_COUNT = 1
AAQ_YAML_FILE = f"{YAMLS_DIR}/aaq-test.yaml"

RQ_COUNT_PER_PROJECT = 1
RQ_FIELDS_YAML_FILE = f"{YAMLS_DIR}/rq-config.yaml"

CRQ_COUNT = 1
CRQ_FIELDS_YAML_FILE = f"{YAMLS_DIR}/crq-config.yaml"

VM_COUNT_PER_PROJECT = 2000
VM_BODY_YAML_FILE = f"{YAMLS_DIR}/vm-body-cirros-containerdisk.yaml"
DISKLESS_VM = True


OUTPUT_YAML_FILE = "/tmp/test_scale_aaq_data.yaml"


TOTAL_VM_COUNT = PROJECT_COUNT * VM_COUNT_PER_PROJECT

CONNECTION_POOL_MAXSIZE = max(
    TOTAL_VM_COUNT,
    PROJECT_COUNT * RQ_COUNT_PER_PROJECT,
    CRQ_COUNT,
)

pytestmark = [
    pytest.mark.scale,
    pytest.mark.usefixtures(
        "increased_open_file_limit",
        "patched_hco_with_high_burst_tuning_policy",
        "scale_client_configuration",
        "scale_admin_client",
        "scale_unprivileged_client",
        "calculated_max_vms_per_virt_node",
        "scale_max_vms_per_virt_node",
    ),
    pytest.mark.parametrize(
        "increased_open_file_limit,"
        "scale_client_configuration,"
        "calculated_max_vms_per_virt_node,"
        "scale_max_vms_per_virt_node",
        [
            pytest.param(
                {"nofile_hard_limit": 2**19},
                {"connection_pool_maxsize": CONNECTION_POOL_MAXSIZE},
                {"total_vm_count": TOTAL_VM_COUNT},
                {"scale": False},
            ),
        ],
        indirect=True,
    ),
]


@pytest.mark.usefixtures("aaq_dict", "request_quota_config", "vm_config", "scale_projects")
@pytest.mark.parametrize(
    "aaq_dict,request_quota_config,vm_config,scale_projects",
    [
        pytest.param(
            {"aaq_yaml_file": AAQ_YAML_FILE},
            {"rq_count": RQ_COUNT_PER_PROJECT, "rq_fields_yaml_file": RQ_FIELDS_YAML_FILE},
            {"vm_count": VM_COUNT_PER_PROJECT, "vm_body_yaml_file": VM_BODY_YAML_FILE, "diskless": DISKLESS_VM},
            {"project_count": PROJECT_COUNT},
        ),
    ],
    indirect=True,
)
class TestScaleRQandAARQ:
    @pytest.mark.polarion("CNV-0000")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_baseline"})], indirect=True)
    def test_scale_baseline(self, scale_vms): ...

    @pytest.mark.polarion("CNV-0001")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_rq"})], indirect=True)
    def test_scale_rq(self, scale_rq, scale_vms): ...

    @pytest.mark.polarion("CNV-0010")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_aaq_aarq"})], indirect=True)
    def test_scale_aaq_aarq(self, created_aaq_resource, scale_aarq, scale_vms): ...

    @pytest.mark.polarion("CNV-0011")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_hco_aarq"})], indirect=True)
    def test_scale_hco_aarq(self, created_aaq_resource_via_hco, scale_aarq, scale_vms): ...

    @pytest.mark.polarion("CNV-0100")
    @pytest.mark.last
    @pytest.mark.parametrize(
        "cache_data_parsed",
        [
            pytest.param({
                "cache_keys": ["test_scale_baseline", "test_scale_rq", "test_scale_aaq_aarq", "test_scale_hco_aarq"],
                "output_file": OUTPUT_YAML_FILE,
            })
        ],
        indirect=True,
    )
    def test_rq_variance(self, cache_data_parsed):
        LOGGER.info(f"test_rq_variance:\n{yaml.dump(cache_data_parsed)}")
        assert cache_data_parsed["pass"], cache_data_parsed["errors"]


@pytest.mark.usefixtures("aaq_dict", "cluster_request_quota_config", "vm_config", "scale_projects")
@pytest.mark.parametrize(
    "aaq_dict,cluster_request_quota_config,vm_config,scale_projects",
    [
        pytest.param(
            {"aaq_yaml_file": AAQ_YAML_FILE, ALLOW_AACRQ_PARAM: True},
            {"crq_count": CRQ_COUNT, "crq_fields_yaml_file": CRQ_FIELDS_YAML_FILE},
            {"vm_count": VM_COUNT_PER_PROJECT, "vm_body_yaml_file": VM_BODY_YAML_FILE, "diskless": DISKLESS_VM},
            {"project_count": PROJECT_COUNT},
        ),
    ],
    indirect=True,
)
class TestScaleCRQandAACRQ:
    @pytest.mark.polarion("CNV-0101")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_crq"})], indirect=True)
    def test_scale_crq(self, scale_crq, scale_vms): ...

    @pytest.mark.polarion("CNV-0110")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_aaq_aacrq"})], indirect=True)
    def test_scale_aaq_aacrq(self, created_aaq_resource, scale_aacrq, scale_vms): ...

    @pytest.mark.polarion("CNV-0111")
    @pytest.mark.parametrize("scale_vms", [pytest.param({"cache_key": "test_scale_hco_aacrq"})], indirect=True)
    def test_scale_hco_aacrq(self, created_aaq_resource_via_hco, scale_aacrq, scale_vms): ...

    @pytest.mark.polarion("CNV-1000")
    @pytest.mark.last
    @pytest.mark.parametrize(
        "cache_data_parsed",
        [
            pytest.param({
                "cache_keys": ["test_scale_baseline", "test_scale_crq", "test_scale_aaq_aacrq", "test_scale_hco_aacrq"],
                "output_file": OUTPUT_YAML_FILE,
            })
        ],
        indirect=True,
    )
    def test_crq_variance(self, cache_data_parsed):
        LOGGER.info(f"test_crq_variance:\n{yaml.dump(cache_data_parsed)}")
        assert cache_data_parsed["pass"], cache_data_parsed["errors"]
