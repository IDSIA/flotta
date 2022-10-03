from dataclasses import dataclass

from ferdelance.client.services.actions import Action


@dataclass
class ClientActionController:
    """For the moment just execute the actions, in the future may be useful to have a separate controller class"""
    def execute(self, action: Action) -> None:
        action.execute()