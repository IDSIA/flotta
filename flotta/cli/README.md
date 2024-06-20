# flotta CLI

The flotta Command Line Interface (flotta CLI) is a tool that enables the user to interact with the local flotta Server/Client instance.

## Quick Setup

Commands fired from the CLI will load some configurations from a .env file:

```yaml
DB_USER=flotta
DB_PASS=flotta
DB_SCHEMA=flotta
DB_HOST=./sqlite.db
DB_DIALECT=sqlite
```

## Usage

```bash
$ python -m flotta.cli <entity> <command> [params list]
```

### Supported entities:

| Entity | Description |
| --- | --- |
| Artifacts | Artifacts submitted by workbenchs |
| Clients | Clients connected to the server |
| Models | Models generated either by the server (aggregated) or by the single clients |
| Jobs | Task assigned to clients |

### Supported commands:

Every entity support 

| Command | Description | Parameters |
| --- | --- | --- |
| ls | Show a complete or filtered list of items |  |
| descr | Show a single item in detail | —<entity>-id |

## Examples

List clients connected to the server:

```bash
$ python -m flotta.cli clients ls
```

List artifacts submitted by workbenchs:

```bash
$ python -m flotta.cli artifacts ls
```

Show specific client description by client ID:

```bash
$ python -m flotta.cli clients descr --client-id <client-id-string>
```