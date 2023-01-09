## Federated Learning (Server)


# Fer-De-Lance, a Federated Learning framework

## Why this name?

* _Fer-De-Lance_ means "Spearhead" in Franch.
* It is also a kind of snake of the genus *Bothrops*. 
* Asclepius, the greek divinity of the medicine, had a staff enwined with snakes, which is the symbol of medicine today.
* Many letters in _Federated Learning_ can also be found in _Fer-De-Lance_.
* Python _is_ a snake!


## Development server


### With the make command

Make sure that the `make` command is available.

The `Makefile` is used to group commands commonly used during the development of the application.

To start from zero, create a new virtual environment:

```bash
make venv-create
```

In case you need to delete and create from scratch the environment, use this command:

```bash
make venv-recreate
```

To install the server in development mode (pip editable mode), with the  dependencies also for the tests, use this command:

```bash
make venv-dev-install
```


### Classic development

Craete a virtual env:

```bash
python -m venv SpearHeadServerEnv
```

Install the submodule [federated-learning-shared](https://gitlab-core.supsi.ch/dti-idsia/spearhead/federated-learning-shared), since it is a dependency:

```bash
pip install federated-learning-shared/
```

For testing purposes, install the server in editable mode using pip:

```bash
pip install -e ".[test]"
```


## Environment variables

* `DATABASE_URL` specifies the url to the database.\
  For a PostgreSQL database, the connection string could be something like

```
postgresql://${DATABASE_USER}:${DATABASE_PASS}@${DATABASE_HOST}/${DATABASE_SCHEMA}
```

* `SERVER_MAIN_PASSWORD` defines the main secret key of the server.\
  This key is used to encrypt sensible information and data to the database.\
  Losing this key will cause data loss in the application!
# federated-learning-client

Client application for the Federated Learning Framework.

# Development client

The docker-compose file requires a `.env` file with the following information in it:

```
SERVER=<url of the server to use>
```

If useful, it is possible to create a `config.yaml` file and use it in the docker-compose file by mounting the following volume:

````
volumes:
  - <path to local config.yaml>:/spearhead/config.yaml
```

The content of the `config.yaml` file is the following:

```
ferdelance:
  client:
    server: <url of remote server>
    workdir: <local path - don't use it in docker>
    heartbeat: <float, in seconds>

  datasource:
    - name: <name of the source>
      kind: file
      type: <'csv' or 'tsv'> 
      path: <path to the file to use (in docker, remember to mount volume to this path!)>

    - name: <name of the source>
      kind: db
      type: <'sqlite' or 'postgres'>
      conn: <connection string to use>
```
