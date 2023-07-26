1. Launch a local ray cluster in head mode from command line with all required environment variables

```bash
DB_DIALECT="sqlite" DB_HOST="./sqlite.db" ray start --head
```

In the above example, we are forcing Ferdelance to use a SQLite database.

2. Deploy server APIs.

```bash
python -m ferdelance.server
```

The command wil return since we are deploying a new job, with the server APIs, on the local cluster.

Check `ferdelance.log` for log output.

```bash
tail -f ferdelance.log
```

3. Launch client application.

```bash
python -f ferdelance.client
```

This application currently will not return.

4. Stop the ray cluster.

```bash
ray stop
```
