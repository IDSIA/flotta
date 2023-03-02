from ferdelance.client.services.actions.action import Action


class ClientActionController:
    """For the moment just execute the actions, in the future may be useful to have a separate controller class"""

    def execute(self, action: Action) -> None:
        action.validate_input()
        action.execute()
