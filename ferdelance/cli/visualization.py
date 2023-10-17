"""Define how to visualize resources on the CLI"""
from typing import TypeVar

import pandas as pd
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def show_one(resource: BaseModel) -> None:
    """Show one resource of one element on the command line

    Args:
        resource (BaseModel): Single object (View) to print
    """
    print(pd.Series(resource.dict()))


def show_many(resource: list[T]) -> None:
    """Show one resource of many elements on the command line

    Args:
        resource (List[BaseModel]): List of objects to print
    """
    print(pd.DataFrame([r.dict() for r in resource]))


def show_string(s: str) -> None:
    print(s)
