# async_utils
* https://docs.python.org/3/library/concurrent.futures.html
* https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor


## Usage
```
from ocp_resources.virtual_machine import VirtualMachine
from tests.scale.async_utils.utils import (
    async_deploy_resources,
    async_delete_resources,
    async_wait_deleted_resources,
    async_wait_for_running_vms,
    async_wait_for_scheduled_vms,
)
from tests.scale.async_utils.virt import async_wait_for_running_vms
from tests.scale.utils import AsyncScaleResources

# Create iterable of VirtualMachine / VirtualMachineForTest python objects to deploy
# Be sure to use deepcopy() when passing dicts to objects to avoid collisions
vms = [VirtualMachine(..., body=deepcopy(body))]

# Option A:

async_deploy_resources(resources=vms)
async_wait_for_running_vms(vms=vms)
yield vms
async_delete_resources(resources=vms)
async_wait_deleted_resources(resources=vms)

# Option B:

with AsyncScaleResources(resources=vms, wait_for_status=VirtualMachine.Status.RUNNING):
    yield vms

# Option C (Wait specifically for scheduling):

with AsyncScaleResources(resources=vms):
    async_wait_for_scheduled_vms(vms=vms)
    async_wait_for_running_vms(vms=vms)
    yield vms
```
