# federated-learning-client

Client application for the Federated Learning Framework.

# Development

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