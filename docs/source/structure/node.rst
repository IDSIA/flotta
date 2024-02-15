==============================
Node
==============================

The *aggregation node* is a node reachable from all other nodes in the network and the central node of the framework.
All workbenches send their payload, called Artifacts, to the aggregation node; while all the clients query the same node for the next job to run.
This allows the clients to have more control on the access and an additional layer of protection: a client node is not reachable from the internet and it is the client that contact the known reference node and initiate the execution process.

The node is composed by a web API written with `FastAPI <https://fastapi.tiangolo.com/>`_ that runs and spawns `Ray <https://ray.io/>`_ tasks.
The node also uses a database to keep track of every stored object.

The easiest way to deploy a node is using **Docker Compose**.

The file `docker-compose.integration.yaml <https://github.com/IDSIA/Ferdelance/blob/main/tests/integration/docker-compose.integration.yaml>`_ contains a definition of all services required to create a stack that simulates a central server node and some client nodes.


Installation
==============================

The installation of a node is simple::

  pip install ferdelance

Once installed it can be run by specifying a YAML configuration file::

  python -m ferdelance -c ./config.yaml

Once one node is up and running, with default parameters the node will be reachable at `http://server:1456/`.


Node configuration
==============================

The minimal content of the configuration file is the definition of the server url to use and at least one datasource.
The datasource must have a name and be associated with one or more project thought the ``token`` field. ::

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

.. Note::
   It is also possible to specify environment variables in the configuration file using the syntax ``${ENVIRONMENT_VARIABLE_NAME}`` inside the fields of parameters.
   This is specially useful when setting parameters, such as domains or password, through a Docker compose file.

For the first node of the distributed network, the ``join.first`` parameter must always be set to ``true``.
In the network it must always be a first node with this configuration.
In all the other cases, both for ``client`` and ``node`` mode, the configuration need to specify the ``join.url`` parameter to a valid url of an existing node.
Only urls of nodes in ``node`` mode can be used in this parameter.


Database configuration
==============================

Database configuration is completely optional.
Every node needs a database to work properly.
Minimal setup is to use an SQLite in-memory database by setting ``database.memory: true``.
If not database is configured, then the in-memory database will be used.
Other supported database are:

- SQLite file database::

    database:
      scheme: ferdelance
      dialect: sqlite
      host: ./sqlite.db
      memory: false

- Postgresql remote database::

    database:
      username: "${DATABASE_USER}"
      password: "${DATABASE_PASSWORD}"
      scheme: ferdelance
      dialect: postgresql
      host: remote_url
      port: 5432
      memory: false


