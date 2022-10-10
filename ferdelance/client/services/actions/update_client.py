from ferdelance_shared.schemas import UpdateClientApp

from ...config import Config
from ..routes import RouteService
from .action import Action


class UpdateClientAction(Action):

    def __init__(self, config: Config, data: UpdateClientApp) -> None:
        self.routes_service: RouteService = RouteService(config)
        self.data = data

    def validate_input(self):
        if not isinstance(self.routes_service, RouteService):
            raise ValueError(f"config parameter must be of type Config")
        if not isinstance(self.data, UpdateClientApp):
            raise ValueError(f"data parameter must be of type UpdateToken")

    def execute(self) -> None:
        self.routes_service.get_new_client(self.data)
