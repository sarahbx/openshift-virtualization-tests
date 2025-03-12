import logging
import os

import pytest
import yaml

from tests.scale.aaq.constants import ALLOW_AACRQ_PARAM

LOGGER = logging.getLogger(__name__)

YAMLS_DIR = os.path.join(os.path.dirname(__file__), "yamls")

pytestmark = [
    pytest.mark.scale,
    pytest.mark.usefixtures("patched_hco_with_high_burst_tuning_policy"),
]


VM_COUNT = 10


@pytest.mark.usefixtures("aaq_dict", "request_quota_config", "vm_config", "scale_projects")
@pytest.mark.parametrize(
    "aaq_dict,request_quota_config,vm_config,scale_projects",
    [
        pytest.param(
            {"aaq_yaml_file": f"{YAMLS_DIR}/aaq-test.yaml"},
            {"rq_count": 1, "rq_fields_yaml_file": f"{YAMLS_DIR}/rq-config.yaml"},
            {"vm_count": VM_COUNT, "vm_body_yaml_file": ""},
            {"project_count": 1},
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
                "cache_keys": ["test_scale_baseline", "test_scale_rq", "test_scale_aaq_aarq", "test_scale_hco_aarq"]
            })
        ],
        indirect=True,
    )
    def test_rq_variance(self, cache_data_parsed):
        LOGGER.info(f"test_rq_variance:\n{yaml.dump(cache_data_parsed)}")


@pytest.mark.usefixtures("aaq_dict", "cluster_request_quota_config", "vm_config", "scale_projects")
@pytest.mark.parametrize(
    "aaq_dict,cluster_request_quota_config,vm_config,scale_projects",
    [
        pytest.param(
            {"aaq_yaml_file": f"{YAMLS_DIR}/aaq-test.yaml", ALLOW_AACRQ_PARAM: True},
            {"crq_count": 1, "crq_fields_yaml_file": f"{YAMLS_DIR}/crq-config.yaml"},
            {"vm_count": VM_COUNT, "vm_body_yaml_file": ""},
            {"project_count": 1},
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
                "cache_keys": ["test_scale_baseline", "test_scale_crq", "test_scale_aaq_aacrq", "test_scale_hco_aacrq"]
            })
        ],
        indirect=True,
    )
    def test_crq_variance(self, cache_data_parsed):
        LOGGER.info(f"test_crq_variance:\n{yaml.dump(cache_data_parsed)}")
