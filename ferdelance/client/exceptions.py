class ClientExitStatus(Exception):
    def __init__(self, exit_code: int, *args: object) -> None:
        super().__init__(*args)
        self.exit_code = exit_code


class RelaunchClient(ClientExitStatus):
    """Exit code = 0"""

    def __init__(self, *args: object) -> None:
        super().__init__(0, *args)


class UpdateClient(ClientExitStatus):
    """Exit code = 1"""

    def __init__(self, *args: object) -> None:
        super().__init__(1, *args)


class ErrorClient(ClientExitStatus):
    """Exit code = 2"""

    def __init__(self, *args: object) -> None:
        super().__init__(2, *args)
