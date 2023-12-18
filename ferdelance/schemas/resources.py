from pydantic import BaseModel


class ResourceIdentifier(BaseModel):
    """If resource_id is set, then use it to get the required resource. Otherwise all of
    artifact_id, producer_id, and iteration must be set to get the correct resource from
    the node.
    """

    resource_id: str | None = None
    artifact_id: str | None = None
    producer_id: str | None = None
    iteration: int | None = None
