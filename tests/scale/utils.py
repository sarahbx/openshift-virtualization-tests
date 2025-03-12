from __future__ import annotations

import time
from typing import Any, Callable

import pytest


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
