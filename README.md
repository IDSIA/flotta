# Ferdelance, a Federated Learning framework


## What is Ferdelance?

_Ferdelance_ is a **distributed framework** intended to be used both as a workbench to develop new distributed algorithm within a Federated Learning (FL) based environment, and perform distributed statistical analysis on private data.

Federated Learning is a Machine Learning (ML) approach that allows for training models across decentralized devices or servers while keeping the data localized, increasing the privacy of data holders.
Instead of collecting data from various sources and centralizing it in one location for training, federated learning enables model training directly on the devices where the data resides.
In FL the training of models is distributed across a series of data holders (client nodes) that have direct and exclusive access to their data.
The particularity of this approach is that the training data never leave these nodes, while only aggregated data, such as model parameters, are exchanged to build an aggregated model.

The current implementation support both a centralized setup, where model's parameters are sent from the client nodes to an aggregation server, and distributed setup, where a model is shared across multiple nodes and multiple model aggregation can happen on different nodes.

The intent of this framework is to develop a solution that enable researcher to develop and test new ML models in a FL context without interacting directly with the data.
The framework wraps a familiar set of Python packages and libraries, such as Scikit-Learn and Pandas.
This allows researchers to quickly setup data extraction pipeline, following the _Extract-Transform-Load_ paradigm, and build models or analyze data.

The main component of the framework is the **node**: a [FastAPI](https://fastapi.tiangolo.com/) based application capable of manage, schedule, and execute jobs in a [Ray](https://www.ray.io/) worker.

The implementation of the distributed network of node have been inspired by the [Apache Spark](https://spark.apache.org/) framework; to interact with the framework, the researchers can use a **workbench** context to create and submit jobs to the distributed network through a node.


## Why this name?

* _Fer-De-Lance_ means "Spearhead" in French.
* It is also a kind of snake of the genus *Bothrops*. 
* Asclepius, the greek divinity of the medicine, had a staff entwined with snakes, which is the symbol of medicine today.
* Many letters in _Federated Learning_ can also be found in _Fer-De-Lance_.
* Python _is_ a snake!

---

## Use the framework

The framework is available as a Python 3.10 package
Configuring a node can be done through environment variables or a YAML configuration file.


### Workbench

The _workbench_ is not a standalone application but a library that need to be imported.
It is used to communicate with a node and submit Artifacts, that encapsulate instructions for the job scheduling and execution.

Installation is straightforward.

```bash
pip install ferdelance[workbench]
```

Once installed, just create a context object and obtain a project handler with a token.
A project is a collection of data sources.
The token is created by the node network administrator and it is unique for each project.

Following an example of how to use the workbench library to connect to a server.

```python
from ferdelance.workbench import Context, Project
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.schemas.models import FederatedRandomForestClassifier, ParametersRandomForestClassifier, StrategyRandomForestClassifier

server_url = "http://localhost:1456"
project_token = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

# create the context
ctx = Context(server_url)

# load a project
project = ctx.project(project_token)

# an aggregated view on data
ds = project.data  

# print all available features
for feature in ds.features:
    print(feature)

# create a query starting from the project's data
q = ds.extract()

# add an execution plan for the data
q = q.add_plan(
    TrainTestSplit(
        label="variety",
        test_percentage=0.5,
    )
)

# ad a Federated model
q = q.add_model(
    FederatedRandomForestClassifier(
        strategy=StrategyRandomForestClassifier.MERGE,
        parameters=ParametersRandomForestClassifier(n_estimators=10),
    )
)

# submit new artifact
a = ctx.submit(project, q)
```

> **Note:** More examples are available in the [`examples`](./examples/) and in the [`tests`](./tests/) folders.

### Server

The _aggregation server_ is the central node of the framework.
All workbenches send their payload, called artifacts, to the aggregation server; while all the clients query the server for the next job to run.

The installation of the server is simple:

```bash
pip install ferdelance
```

The server is composed by a web API that runs and spawns [Ray](https://ray.io) aggregation tasks.
The server also uses a database to keep track of every stored object.

The easiest way to deploy a server is using **Docker Compose**.

The file [docker-compose.server.yaml](./docker-compose.server.yaml) contains a definition of all services required for the server's stack.

This stack includes:
- a Python repository used to update clients and workbenches,
- a [PostgreSQL](https://www.postgresql.org/) database,
- the server service.

> **Note:** The services that need to be exposed to the world (server and repository) are labeled to be used with [Traefik proxy](https://traefik.io/traefik/) (highly recommended to use).

The compose stack requires some environment variables that can be collected in a `.env` file as follows:

```bash
DOMAIN=<external url>

DATABASE_USER=ferdelance
DATABASE_PASS=<something good>
DATABASE_SCHEMA=ferdelance

SERVER_MAIN_PASSWORD=<something strong>
```

> **Note:** `SERVER_MAIN_PASSWORD` defines the main secret key of the server.
> This key is used to encrypt sensible information and data in the database.

> ⚠ Losing this key will cause data loss in the server application!

Once the stack is up and running, thanks to Traefik support, the server will be reachable at `ferdelance.${DOMAIN}` while the repository at `fdl-repo.${DOMAIN}`.


### Client

The client application is an executable library that is written exclusively in Python:

```bash
pip install ferdelance
```

Once installed it can be run by specifying a YAML configuration file:

```bash
python -m ferdelance.client -c ./config.yaml
```

The minimal content of the configuration file is the definition of the server url to use and at least one datasource.
The datasource must have a name and be associated with one or more project thought the `token` field. 

```yaml
workdir: ./storage               # OPTIONAL: local path of the working directory

server:
    host: http://localhost:1456/ # url of remote server

client:
    heartbeat: 10.0              # OPTIONAL: float, waiting seconds for server updates

datasources:
  - name: iris                   # name of the source
    kind: file                   # how the datasource is stored (only 'file')
    type: csv                    # file format supported (only 'csv' or 'tsv')
    path: /data/iris.csv         # path to the file to use
    token: 
    - 58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94
```

Like th server, a Docker Compose is also available for the client through the [docker-compose.client.yaml](./docker-compose.client.yaml) file.
To work correctly, there must exists a valid `./config/config.yaml` file and the data sources files must be stored in the `./data/` folder (and correctly defined in the `config.yaml` file).

It is also required to specify through environment variables or with a `.env` file the server url using the `FDL_SERVER` variable name.

---

## Development

The Ferdelance framework is open for contributions and offer a quick development environment.

It is usefull to use a local Python virtual environment, like [virtualenv](https://docs.python.org/3/library/venv.html) or [conda](https://docs.conda.io/), during the development of the library.

The repository contains a `Makefile` that can be used to quickly create an environment and install the framework in development mode.

> **Note:** Make sure that the `make` command is available.

To install the library in development mode, use the following command:

```bash
pip install -e ".[dev]" 
```

To test the changes in development mode there are a number of possibilities:

- standalone mode,
- unit tests using `pytest`,
- integration tests using Docker,
- full development using Docker.

---

## Testing

For testing purposes it is useful to install the test version of the framework:

```bash
pip install ferdelance[test]
```

> **Note:** The development version already include the test part

### Standalone mode

One of the simplest way to test changes to the framework is through the so called `standalone mode`.
In this mode, the framework is executed as a standalone application. 

```bash
python -m ferdelance.standalone -c ./config.yaml
```

The server's variables can be set as environment variables, while the client config file can be set through the `-c` or `--config` arguments.

Instead, to develop for the whole infrastructure, the Docker Compose is the right way.

### Unit tests

To test single part of code, such as transformers, models, or estimators, it is possible to write test files with the [`pytest`](https://docs.pytest.org/) library.

A simple test case can be setup as follow:

```python
from ferdelance.server.api import api

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.utils import connect
import pytest

@pytest.mark.asyncio
async def test_workbench_read_home(session: AsyncSession):
  with TestClient(api) as server:
    args = await connect(server, session)
        wb_exc = args.wb_exc

        res = server.get(
            "/workbench",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200
```

The fixture to connect to the test db (which is an [`SQLite`](https://www.sqlite.org/) database) through the `session` object are defined in the `/tests/conftests.py` file.

Other utility (component connection, clients operations, ...) methods are defined in the `/tests/utils.py` file.

> **Note:** Remember that the framework uses [`FastAPI`](https://fastapi.tiangolo.com/) in full asynchronous mode: the test functions need to be defined as `async` and decorated with `@pytest.mark.asyncio` to work with the fixtures.


### Integration tests

Integration tests are done using a special [Docker Compose](./tests/integration/docker-compose.2clients.yaml) file.
This file defines a framework with two clients and the [California Housing Pricing dataset](https://inria.github.io/scikit-learn-mooc/python_scripts/datasets_california_housing.html).
This dataset has been split between the two clients.

Integration tests are written as a script and simulates what a researcher could writhe through the workbench interface.
Although a little bit primitive (and not so fast to setup and teardown), it is an effective way to test the workflow of the framework.

All integration tests should be placed in the `tests/integration/tests` folder.
These test should be named with the format `test_NNN.<name>.py`, where `NNN` is an incremental number padded with zeros.

To execute the integration tests, simply run the following command from inside the integration folder:

```bash
make start
```

> **Note:** A practical Makefile is included in the `tests/integration` folder with useful commands to start, stop, clear, and reload the Docker compose stack.

---
