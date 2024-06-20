from flotta.core.distributions.core import Distribution
from flotta.schemas.components import Component


class DirectToNext(Distribution):
    next: Component

    def bind_locks(self, job_ids0: list[int], job_ids1: list[int]) -> list[list[int]]:
        # TODO: this is a special case solved by the operative part
        return []
