==============================
Setup
==============================

The Ferdelance framework is open for contributions and offer a quick development environment.

It is useful to use a local Python virtual environment, like `virtualenv <https://docs.python.org/3/library/venv.html>`_ or `conda <https://docs.conda.io/>`_, during the development of the library.

The repository contains a ``Makefile`` that can be used to quickly create an environment and install the framework in development mode.

.. Note:
   Make sure that the `make` command is available on the test machine.

To install the library in development mode, use the following command::

  pip install -e ".[dev]" 


To test the changes in development mode there are a number of possibilities:

- standalone mode,
- unit tests using ``pytest``,
- integration tests using Docker,
- full development using Docker.
