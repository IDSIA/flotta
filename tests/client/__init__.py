from ..db import database_create, database_delete


def setup_module(module):
    database_create()


def teardown_module(module):
    database_delete()
