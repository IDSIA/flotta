from pydantic import BaseModel


class UpdateData(BaseModel):
    """Basic update response from the server with the next action to do."""

    action: str


class UpdateToken(UpdateData):
    """The client has a new token to use."""

    token: str

    def __str__(self) -> str:
        return super().__str__()


class UpdateClientApp(UpdateData):
    """Data for the client on the new app to download."""

    checksum: str
    name: str
    version: str

    def __str__(self) -> str:
        return f"{super().__str__()}, name={self.name}, version={self.version}"


class UpdateExecute(UpdateData):
    """Task that the client has to execute next."""

    job_id: str


class UpdateNothing(UpdateData):
    """Nothing else to do."""

    pass


class DownloadApp(BaseModel):
    """Details from the client to the app to download"""

    name: str
    version: str
