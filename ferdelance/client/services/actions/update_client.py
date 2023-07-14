from ferdelance.client.config import Config
from ferdelance.client.services.actions.action import Action
from ferdelance.client.services.routes import RouteService
from ferdelance.schemas.updates import UpdateClientApp


class UpdateClientAction(Action):
    def __init__(self, config: Config, data: UpdateClientApp) -> None:
        self.routes_service: RouteService = RouteService(config)
        self.data = data

    def execute(self) -> None:
        self.routes_service.get_new_client(self.data)
