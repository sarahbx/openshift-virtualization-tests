from __future__ import annotations

import signal
import threading
import time
from contextlib import ExitStack, contextmanager
from typing import Any, Optional

import pytest
from ocp_resources.resource import Resource

from tests.scale.async_utils.utils import (
    async_delete_resources,
    async_deploy_requested_resources,
    async_deploy_resources,
    async_wait_deleted_resources,
    async_wait_for_resources_status,
)


def _resource_enter(cls: Resource) -> Any:
    if threading.current_thread().native_id == threading.main_thread().native_id:
        signal.signal(signal.SIGINT, cls._sigint_handler)
    return cls.deploy(wait=cls.wait_for_resource)


Resource.__enter__ = _resource_enter


class AsyncScaleResources(ExitStack):
    def __init__(
        self,
        resources: list[Resource],
        request_resources: Optional[list[Resource]] = None,
        pytest_cache: Optional[pytest.Cache] = None,
        cache_key_prefix: Optional[str] = None,
        wait_for_status: Optional[bool] = False,
    ):
        """
        Args:
            resources (list): List of Resource objects to be managed
        """
        super().__init__()
        self.resources = resources
        self.request_resources = request_resources
        self.pytest_cache = pytest_cache
        self.cache_key_prefix = cache_key_prefix
        self.wait_for_status = wait_for_status

    @contextmanager
    def _cleanup_on_error(self, stack_exit):
        with ExitStack() as stack:
            stack.push(exit=stack_exit)
            yield
            stack.pop_all()

    def __enter__(self) -> AsyncScaleResources:
        with self._cleanup_on_error(stack_exit=super().__exit__):
            start_time = time.time()
            if self.request_resources:
                async_deploy_requested_resources(
                    resources=self.resources, request_resources=self.request_resources, exit_stack=self
                )
            else:
                async_deploy_resources(resources=self.resources, exit_stack=self)

            if self.wait_for_status:
                async_wait_for_resources_status(resources=self.resources, status=self.wait_for_status)

            stop_time = time.time()
            if self.pytest_cache and self.cache_key_prefix:
                self.pytest_cache.set(f"{self.cache_key_prefix}-deploy-start", start_time)
                self.pytest_cache.set(f"{self.cache_key_prefix}-deploy-stop", stop_time)
                self.pytest_cache.set(f"{self.cache_key_prefix}-deploy-elapsed", stop_time - start_time)
        return self

    def __exit__(self: AsyncScaleResources, *exc_arguments: Any) -> Any:
        """
        Delete all resources, mark the start and end fields.
        Deletion when exiting context manager will unwind ExitStack,
        including any sleeps between batches.
        Wait for resources to be deleted in reverse order of creation.
        """
        with self._cleanup_on_error(stack_exit=super().__exit__):
            start_time = time.time()
            async_delete_resources(resources=self.resources)
            async_wait_deleted_resources(resources=self.resources)
            stop_time = time.time()
            if self.pytest_cache and self.cache_key_prefix:
                self.pytest_cache.set(f"{self.cache_key_prefix}-delete-start", start_time)
                self.pytest_cache.set(f"{self.cache_key_prefix}-delete-stop", stop_time)
                self.pytest_cache.set(f"{self.cache_key_prefix}-delete-elapsed", stop_time - start_time)
