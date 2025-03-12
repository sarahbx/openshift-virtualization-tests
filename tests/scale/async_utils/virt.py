from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ocp_resources.virtual_machine import VirtualMachine
from timeout_sampler import TimeoutExpiredError, TimeoutSampler

from utilities.constants import TIMEOUT_4MIN
from utilities.virt import (
    wait_for_cloud_init_complete,
    wait_for_running_vm,
    wait_for_ssh_connectivity,
    wait_for_vm_interfaces,
)

LOGGER = logging.getLogger("asyncio").getChild(__name__)


def async_wait_for_accessible_vms(vms: list[VirtualMachine]) -> list[Any]:
    def _wait_for_accessible_vm(_vm: VirtualMachine) -> None:
        wait_for_vm_interfaces(vmi=_vm.vmi)
        wait_for_ssh_connectivity(vm=_vm)

    with ThreadPoolExecutor(max_workers=len(vms)) as executor:
        return list(executor.map(_wait_for_accessible_vm, vms))


def async_wait_for_running_vms(
    vms: list[VirtualMachine],
    wait_for_interfaces: bool = False,
    check_ssh_connectivity: bool = False,
    wait_for_cloud_init: bool = False,
) -> list[Any]:
    """
    Asynchronously wait for running VMs

    Args:
        vms (list): List of VirtualMachines
        wait_for_cloud_init (bool, optional): Wait for VM cloud-init completion

    Returns:
        dict: Data related to the running of the async function
    """

    def _wait_running_vm(_vm: VirtualMachine) -> None:
        try:
            wait_for_running_vm(
                vm=_vm, wait_for_interfaces=wait_for_interfaces, check_ssh_connectivity=check_ssh_connectivity
            )
        except TimeoutExpiredError:
            LOGGER.error(f"VM: {_vm.name} Status: {_vm.instance.status}")
            raise

        if wait_for_cloud_init:
            wait_for_cloud_init_complete(vm=_vm)

    with ThreadPoolExecutor(max_workers=len(vms)) as executor:
        return list(executor.map(_wait_running_vm, vms))


def async_wait_for_scheduled_vms(vms: list[VirtualMachine]) -> list[Any]:
    """
    Asynchronously wait for scheduled VMs

    Args:
        vms (list): List of VirtualMachines

    Returns:
        dict: Data related to the running of the async function
    """

    def _wait_for_scheduled_vm(_vm: VirtualMachine) -> None:
        def _get_virt_launcher_instance():
            virt_launcher_pod = _vm.vmi.virt_launcher_pod
            if virt_launcher_pod:
                return virt_launcher_pod.exists

        sampler = TimeoutSampler(
            wait_timeout=TIMEOUT_4MIN,
            sleep=1,
            func=_get_virt_launcher_instance,
        )
        try:
            sample = None
            for sample in sampler:
                if sample and sample.spec.nodeName:
                    break
        except TimeoutExpiredError:
            LOGGER.error(f"VM: {_vm.name} Status: {_vm.instance.status} virt-launcher: {sample}")

            raise

    with ThreadPoolExecutor(max_workers=len(vms)) as executor:
        return list(executor.map(_wait_for_scheduled_vm, vms))
