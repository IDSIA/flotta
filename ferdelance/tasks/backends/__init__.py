"""Backends are the possible workers that can be used for asynchronous tasks."""
from .backend import Backend


def get_jobs_backend() -> Backend:
    return Backend()
