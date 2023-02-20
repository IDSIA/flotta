from dataclasses import dataclass
from typing import Any, Type


@dataclass
class CLIArgument:
    """Argument that will be linked to a specific command"""

    dash_string: str
    var_name: str
    var_type: Type
    default: Any = None
    help: str | None = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CLIArgument):
            return False

        return self.dash_string == other.dash_string

    def __hash__(self):
        return hash(self.dash_string)
