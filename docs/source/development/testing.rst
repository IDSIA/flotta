==============================
Testing
==============================

For testing purposes it is useful to install the test version of the framework::

   pip install flotta[test]


.. Note:
   The development version already include the test part


Integration tests
==============================

Integration tests are the perfect entrypoint for start deploying and use the framework.
These tests simulates a real deployment, although on the same machine, with a dataset split and shared across multiple nodes.

The execution requires a special `Docker Compose <https://github.com/IDSIA/flotta/blob/main/tests/integration/docker-compose.integration.yaml>`_ that will produce a stack with:

- repository with the packed wheel of the library
- a postgres database
- a node acting as an aggregation server
- 2 nodes in client mode
- 2 nodes in default mode (*not used yet*)
- a workbench service

The two client nodes and the two default nodes include the `California Housing Pricing dataset <https://inria.github.io/scikit-learn-mooc/python_scripts/datasets_california_housing.html>`_.
This dataset has been split in three: two parts for the nodes, one part for the evaluation in the workbench.
These datasets are saved in CSV format in the `data <https://github.com/IDSIA/flotta/tree/main/tests/integration/data>`_ folder.

Configuration of single nodes are stored in the `conf <https://github.com/IDSIA/flotta/tree/main/tests/integration/conf>`_ folder in YAML format.

Integration tests are written as scripts and simulates what an user could write through the workbench interface.
Although a little bit primitive (and not so fast to setup and teardown), it is an effective way to test the workflow of the framework.

All integration tests should be placed in the ``tests/integration/tests`` folder.
These test should be named following the convention ``test_NNN.<name>.py``, where ``NNN`` is an incremental number padded with zeros and ``<name>`` is just a reference.

To execute the integration tests, simply run the following command from inside the integration folder::

   make start


.. Note:
   The Makefile included in ``tests/integration`` folder has other useful commands to start, stop, clear, and reload the Docker compose stack and also dump and clean the internal logs.


Unit tests
==============================

To test single part of code, such as transformers, models, or estimators, it is advised to write test files using the `pytest <https://docs.pytest.org/>`_ library.

A simple test case can be setup as follow::

  from flotta.server.api import api

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


The fixture to connect to the test db (which for tests is an `SQLite <https://www.sqlite.org/>`_ database) through the `session` object are defined in the `conftest.py <https://github.com/IDSIA/flotta/blob/main/tests/conftest.py>`_ file.

Other utility (component connection, clients operations, ...) methods are defined in the ``tests/utils.py`` file.

.. Note:
   Remember that the APIs defined in the framework use `FastAPI <https://fastapi.tiangolo.com/>`_ in full _asynchronous_ mode: the test functions need to be defined as ``async`` and decorated with ``@pytest.mark.asyncio`` to work with the fixtures.

.. Note:
   Code executed by the Ray's workers are *synchronous* and these workers are designed to never access a database. Only the asynchronous APIs can access the database.
