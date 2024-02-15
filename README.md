# Ferdelance, a Federated Learning framework


## What is Ferdelance?

_Ferdelance_ is a **distributed framework** intended to be used both as a workbench to develop new distributed algorithm within a Federated Learning (FL) based environment, and perform distributed statistical analysis on private data.

Federated Learning is a Machine Learning (ML) approach that allows for training models across decentralized devices or servers while keeping the data localized, increasing the privacy of data holders.
Instead of collecting data from various sources and centralizing it in one location for training, federated learning enables model training directly on the devices where the data resides.
In FL the training of models is distributed across a series of data holders (client nodes) that have direct and exclusive access to their data.
The particularity of this approach is that the training data never leave these nodes, while only aggregated data, such as model parameters, are exchanged to build an aggregated model.

The current implementation support both a centralized setup, where model's parameters are sent from the client nodes to an aggregation node, and distributed setup, where a model is shared across multiple nodes and multiple model aggregation can happen on different nodes.

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

The framework is available as a Python 3.10 package.
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

Following an example of how to use the workbench library to connect to a node.

```python
from ferdelance.core.distributions import Collect
from ferdelance.core.model_operations import Aggregation, Train, TrainTest
from ferdelance.core.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier
from ferdelance.core.steps import Finalize, Parallel
from ferdelance.core.transformers import FederatedSplitter
from ferdelance.workbench import Context, Artifact

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
q = q.add(q["feature"] < 2)

# create a Federated model
model = FederatedRandomForestClassifier(
    n_estimators=10,
    strategy=StrategyRandomForestClassifier.MERGE,
)

label = "label"

# describe how to distribute the work and how to train teh model
steps = [
    Parallel(
        TrainTest(
            query=project.extract().add(
                FederatedSplitter(
                    random_state=42,
                    test_percentage=0.2,
                    label=label,
                )
            ),
            trainer=Train(model=model),
            model=model,
        ),
        Collect(),
    ),
    Finalize(
        Aggregation(model=model),
    ),
]

# submit artifact
artifact: Artifact = ctx.submit(project, steps)
```

> **Note:** More examples are available in the [`examples`](./examples/) and in the [`tests`](./tests/) folders.

### Node deployment

The _aggregation node_ is a node reachable from all nodes in the network and the central node of the framework.
All workbenches send their payload, called Artifacts, to the aggregation node; while all the clients query the same node for the next job to run.
This allows the clients to have more control on the access and an additional layer of protection: a client node is not reachable from the internet and it is the client that contact the known reference node and initiate the execution process.

The installation of a node is simple:

```bash
pip install ferdelance
```

Once installed it can be run by specifying a YAML configuration file:

```bash
python -m ferdelance -c ./config.yaml
```

The node is composed by a web API written with [FastAPI](https://fastapi.tiangolo.com/) that runs and spawns [Ray](https://ray.io) tasks.
The node also uses a database to keep track of every stored object.

The easiest way to deploy a node is using **Docker Compose**.

The file [docker-compose.integration.yaml](./tests/integration/docker-compose.integration.yaml) contains a definition of all services required to create a stack that simulates a central server node and some client nodes.

Once one node is up and running, with default parameters the node will be reachable at `http://server:1456/`.


### Node configuration

The minimal content of the configuration file is the definition of the server url to use and at least one datasource.
The datasource must have a name and be associated with one or more project thought the `token` field. 

```yaml
workdir: ./storage                  # OPTIONAL: local path of the working directory

mode: node                          # one of: node, client, standalone

node:
  name: FerdelanceNode
  healthcheck: 3600.0               # wait in seconds for check self status
  heartbeat: 10.0                   # wait in seconds for clients to fetch updates
  allow_resource_download: true     # if false, nobody can download resources from this node

  protocol: http                    # external protocol (http or https)
  interface: 0.0.0.0                # interface to use (0.0.0.0 for node, "localhost" for clients)
  url: ""                           # external url that the node will be reachable at
  port: 1456                        # external port to use to reach the APIs

  token_projects_initial:           # initial projects available at node start
    - name: my_beautiful_project    # name of the project
      token: 58981bcbab...          # unique token assigned to the project

join:
  first: true                       # if true, this is the first node in the distributed network
  url: ""                           # when a node is note the first, set the url for the join node

datasources:                        # list of available datasources
  - name: iris                      # name of the source
    kind: file                      # how the datasource is stored (only 'file')
    type: csv                       # file format supported (only 'csv' or 'tsv')
    path: /data/iris.csv            # path to the file to use
    token:                          # list of project token that can access this datasource
    - 58981bcbab7...                

database:
  username: ""                      # username used to access the database
  password: ""                      # password used to access the database
  scheme: ferdelance                # specify the name of the database schema to use
  memory: false                     # when set to true, a SQLite in-memory database will be used
  dialect: sqlite                   # current accepted dialects are: SQLite and Postgresql
  host: ./sqlite.db                 # local path for local file (QSLite) or url for remote database
  port: ""                          # port to use to connect to a remote database
```

> **Note:** It is also possible to specify environment variables in the configuration file using the syntax `${ENVIRONMENT_VARIABLE_NAME}` inside the fields of parameters.
This is specially useful when setting parameters, such as domains or password, through a Docker compose file.

For the first node of the distributed network, the `join.first` parameter must always be set to `true`.
In the network it must always be a first node with this configuration.
In all the other cases, both for `client` and `node` mode, the configuration need to specify the `join.url` parameter to a valid url of an existing node.
Only urls of nodes in `node` mode can be used in this parameter.

Database configuration is completely optional.
Every node needs a database to work properly.
Minimal setup is to use an SQLite in-memory database by setting `database.memory: true`.
If not database is configured, then the in-memory database will be used.
Other supported database are:
* SQLite file database,

```yaml
database:
  scheme: ferdelance
  dialect: sqlite
  host: ./sqlite.db
  memory: false
```

* Postgresql remote database.

```yaml
database:
  username: "${DATABASE_USER}"
  password: "${DATABASE_PASSWORD}"
  scheme: ferdelance
  dialect: postgresql
  host: remote_url
  port: 5432
  memory: false
```

---

## Development

The Ferdelance framework is open for contributions and offer a quick development environment.

It is useful to use a local Python virtual environment, like [virtualenv](https://docs.python.org/3/library/venv.html) or [conda](https://docs.conda.io/), during the development of the library.

The repository contains a `Makefile` that can be used to quickly create an environment and install the framework in development mode.

> **Note:** Make sure that the `make` command is available on the test machine.

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
In this mode, the framework is executed as a standalone application: this is just a node scheduling jobs for itself with an hardcoded base configuration.

```bash
python -m ferdelance.standalone
```


### Integration tests

Integration tests are the perfect entrypoint for start deploying and use the framework.
These tests simulates a real deployment, although on the same machine, with a dataset split and shared across multiple nodes.

The execution requires a special [Docker Compose](./tests/integration/docker-compose.integration.yaml) that will produce a stack with:

* repository with the packed wheel of the library
* a postgres database
* a node acting as an aggregation server
* 2 nodes in client mode
* 2 nodes in default mode (*not used yet*)
* a workbench service

The two client nodes and the two default nodes include the [California Housing Pricing dataset](https://inria.github.io/scikit-learn-mooc/python_scripts/datasets_california_housing.html).
This dataset has been split in three: two parts for the nodes, one part for the evaluation in the workbench.
These datasets are saved in CSV format in the [data](./tests/integration/data) folder.

Configuration of single nodes are stored in the [conf](./tests/integration/conf) folder in YAML format.

Integration tests are written as scripts and simulates what an user could write through the workbench interface.
Although a little bit primitive (and not so fast to setup and teardown), it is an effective way to test the workflow of the framework.

All integration tests should be placed in the `tests/integration/tests` folder.
These test should be named following the convention `test_NNN.<name>.py`, where `NNN` is an incremental number padded with zeros and `<name>` is just a reference.

To execute the integration tests, simply run the following command from inside the integration folder:

```bash
make start
```

> **Note:** The Makefile included in `tests/integration` folder has other useful commands to start, stop, clear, and reload the Docker compose stack and also dump and clean the internal logs.


### Unit tests

To test single part of code, such as transformers, models, or estimators, it is advised to write test files using the [`pytest`](https://docs.pytest.org/) library.

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

The fixture to connect to the test db (which for tests is an [`SQLite`](https://www.sqlite.org/) database) through the `session` object are defined in the [`conftest.py`](`./tests/conftests.py`) file.

Other utility (component connection, clients operations, ...) methods are defined in the `/tests/utils.py` file.

> **Note:** Remember that the APIs defined in the framework use [`FastAPI`](https://fastapi.tiangolo.com/) in full _asynchronous_ mode: the test functions need to be defined as `async` and decorated with `@pytest.mark.asyncio` to work with the fixtures.

> **Note:** Code executed by the Ray's workers are _synchronous_ and these workers are designed to never access a database. Only the asynchronous APIs can access the database.

---
