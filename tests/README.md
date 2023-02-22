# Run integration tests

Run all test from repository root directory.

Start the application in standalone mode using the makefile:

```bash
make standalone
```

In case you need to clean up the folders and start fresh use the `make clean` command.

Run the tests using python. An example with `submit_artifact.py`:

```bash
python tests/integration/submit_artifact.py
```
