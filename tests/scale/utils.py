from __future__ import annotations

import time
from typing import Any, Callable

import pytest
import yaml


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


def get_user_kubeconfig_context(kubeconfig_filename, username):
    with open(kubeconfig_filename, "r") as file:
        kubeconfig_content = yaml.safe_load(file)

    all_contexts = kubeconfig_content["contexts"]
    current_context = kubeconfig_content["current-context"]
    current_cluster = [entry["context"]["cluster"] for entry in all_contexts if entry["name"] == current_context][0]

    user_context = None
    for entry in all_contexts:
        context = entry["context"]
        if context["cluster"] == current_cluster and context["user"] == f"{username}/{current_cluster}":
            user_context = entry["name"]
            break

    assert user_context, "No context found for user"
    return user_context
