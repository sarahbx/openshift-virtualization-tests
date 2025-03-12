from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pytest
from ocp_resources.resource import Resource
from ocp_utilities.monitoring import Prometheus
from timeout_sampler import TimeoutExpiredError, TimeoutSampler

from utilities.constants import TIMEOUT_5MIN, TIMEOUT_30SEC

LOGGER = logging.getLogger(__name__)


class MonitorResourceAPIServerRequests:
    def __init__(
        self,
        prometheus: Prometheus,
        resource_class: type[Resource],
        idle_requests_value: float,
        time_duration_seconds: int = TIMEOUT_5MIN,
    ):
        """
        Monitor API Server Requests for a particular Resource

        Args:
            prometheus (Prometheus): Prometheus object from ocp_utilities
            resource_class (Resource): Resource class to monitor
            time_duration_seconds (int, optional): Time duration to use with prometheus query
            idle_requests_value (float, optional): Minimum value indicating an 'idle' state
        """
        self.prometheus = prometheus
        self.resource_class = resource_class
        self.time_duration_seconds = time_duration_seconds
        self.idle_requests_value = idle_requests_value

        self.apiserver_requests_query = (
            "sum by (resource) (rate(apiserver_request_total{"
            f'group="{self.resource_class.api_group}",'
            f'resource="{self.resource_class.kind.lower()}s"'
            f"}}[{self.time_duration_seconds}s]))"
        )

    def _initial_wait(self) -> None:
        """
        The initial state is unknown, and will be unknown, it cannot be guaranteed.
        The code calling this could be the first code that upsets the cluster, or not.
        Wait for the cluster to either stay silent or become noisy.
        """
        initial_silence_count = 0
        initial_noise_count = 0
        sampler = TimeoutSampler(
            wait_timeout=TIMEOUT_30SEC,
            sleep=5,
            func=self.prometheus.query_sampler,
            query=self.apiserver_requests_query,
        )
        try:
            for sample in sampler:
                if sample:
                    value = float(sample[0]["value"][1])
                    if value < self.idle_requests_value:
                        initial_noise_count = 0
                        initial_silence_count += 1
                    else:
                        initial_silence_count = 0
                        initial_noise_count += 1

                    if initial_silence_count > 5 or initial_noise_count > 2:
                        break
        except TimeoutExpiredError:
            pass

    def wait_for_idle(self) -> None:
        """
        Wait for 'idle' cluster state based on provided Resource
        """
        self._initial_wait()

        sampler = TimeoutSampler(
            wait_timeout=self.time_duration_seconds * 2,
            sleep=5,
            func=self.prometheus.query_sampler,
            query=self.apiserver_requests_query,
        )
        sample = None
        try:
            for sample in sampler:
                if sample and float(sample[0]["value"][1]) < self.idle_requests_value:
                    return
        except TimeoutExpiredError:
            LOGGER.error(
                f"Metric value: {sample} of {self.apiserver_requests_query!r} "
                f"is not below minimum: {self.idle_requests_value}"
            )
            raise


def capture_func_elapsed(cache: pytest.Cache, cache_key_prefix: str, func: Callable, **kwargs: Any) -> Any:
    """
    Capture the start/stop/elapsed of arbitrary functions
    """
    start_time = time.time()
    return_value = func(**kwargs)
    stop_time = time.time()
    cache.set(f"{cache_key_prefix}-start", start_time)
    cache.set(f"{cache_key_prefix}-stop", stop_time)
    cache.set(f"{cache_key_prefix}-elapsed", stop_time - start_time)
    return return_value
