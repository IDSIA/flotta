import pytest

from ferdelance.server.cli.artifacts import artifacts_cli_suite
from ferdelance.server.cli.base import CLICommandSuite, CLIParser


class TestCLIBase:

    def setup_class(self):
        """Create empty CLI"""
        self.parser: CLIParser = CLIParser(
            prog="Ferdelance Admin CLI",
            description="Command Line Interface to administrate the server",
        )
        self.suite: CLICommandSuite = artifacts_cli_suite

        

    @pytest.mark.asyncio
    async def test_empty_cli(self):
        with pytest.raises(ValueError) as e:
            self.parser.parse_args(["clients", "ls"])
        assert "No command suite found for entity" in str(e)
        assert self.parser.selected_suite is None

    @pytest.mark.asyncio
    async def test_incompatible_command_cli(self):
        self.parser.add_command_suite(self.suite)
        with pytest.raises(ValueError) as e:
            self.parser.parse_args(["artifacts", "non_existent_command"])
        assert "does not support command" in str(e)
        assert self.parser.selected_command is None

    @pytest.mark.asyncio
    async def test_suite_command_found_cli(self):
        self.parser.add_command_suite(self.suite)
        self.parser.parse_args(["artifacts", "ls"])
        assert self.parser.selected_suite is not None
        assert self.parser.selected_command is not None

    
    
            