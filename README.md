## Federated Learning (Server)

# Fer-De-Lance, a Federated Learning framework

Why the name?
* _Fer-De-Lance_ means "Spearhead" in Franch.
* It is also a kind of snake of the genus *Bothrops*. 
* Asclepius, the greek divinity of the medicine, had a staff enwined with snakes, which is the symbol of medicine today.
* Many letters in _Federated Learning_ can also be found in _Fer-De-Lance_.
* Python _is_ a snake!

## Development

Craete a virtual env:

```bash
python -m venv SpearHeadServerEnv
```

For testing purposes, install the server in editable mode using pip:
```bash
pip install -e .
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
