import pytest

from ferdelance.server.cli.artifacts import artifacts_cli_suite
from ferdelance.server.cli.base import CLICommandSuite, CLIParser


@pytest.fixture
def parser() -> CLIParser:
    """An empty CLIParser"""
    return CLIParser(
        prog="Ferdelance Admin CLI",
        description="Command Line Interface to administrate the server",
    )


@pytest.fixture
def suite() -> CLICommandSuite:
    """An arbitrary command suite"""
    return artifacts_cli_suite


@pytest.mark.asyncio
async def test_empty_cli(parser, suite):
    with pytest.raises(ValueError) as e:
        parser.parse_args(["clients", "ls"])
    assert "No command suite found for entity" in str(e)
    assert parser.selected_suite is None
    parser.add_command_suite(suite)
    with pytest.raises(ValueError) as e:
        parser.parse_args(["clients", "ls"])
    assert "No command suite found for entity" in str(e)
    assert parser.selected_suite is None


@pytest.mark.asyncio
async def test_incompatible_command_cli(parser, suite):
    parser.add_command_suite(suite)
    with pytest.raises(ValueError) as e:
        parser.parse_args(["artifacts", "non_existent_command"])
    assert "does not support command" in str(e)
    assert parser.selected_command is None


@pytest.mark.asyncio
async def test_suite_command_found_cli(parser, suite):
    parser.add_command_suite(suite)
    parser.parse_args(["artifacts", "ls"])
    assert parser.selected_suite is not None
    assert parser.selected_command is not None
