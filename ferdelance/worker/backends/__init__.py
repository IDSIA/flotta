"""Backends are the possible workers that can be used for asynchronous tasks.
A backend 

"""

from ferdelance.config import conf

from .backend import Backend
from .local import LocalBackend
from .remote import RemoteBackend

import logging

LOGGER = logging.getLogger(__name__)


def get_jobs_backend() -> Backend:
    if conf.STANDALONE:
        return LocalBackend()
    else:
        return RemoteBackend()
