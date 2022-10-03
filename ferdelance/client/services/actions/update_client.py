from ferdelance.client.services.routes import RouteService
from ferdelance_shared.schemas import UpdateClientApp


class UpdateClientAction:
    def __init__(self, routes_service: RouteService, data: UpdateClientApp) -> None:
        self.routes_service = routes_service
        self.data = data
        self._validate_input(self.config, self.data)

    def _validate_input(config, data):
        if not isinstance(config, RouteService):
            raise ValueError(f"config parameter must be of type Config")
        elif not isinstance(data, UpdateClientApp):
            raise ValueError(f"data parameter must be of type UpateToken")
    
    def execute(self, ) -> None:
        self.routes_service.get_new_client(self.data)