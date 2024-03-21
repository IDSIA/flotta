==============================
Entities
==============================

Anything that can be exchanged through the API of a node and compose an artifact is an *entity*.
``Entity`` is the main class that defines the hierarchy of objects that can be used in an artifact.
This class defines a ``class_registry`` that is used by a node to convert JSON objects to Python objects, and vice versa, using `Pydantic <https://docs.pydantic.dev/1.10/>`_'s automatic marshalling mechanism.
