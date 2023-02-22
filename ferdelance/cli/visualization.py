"""Define how to visualize results on the CLI"""
from typing import TypeVar

import pandas as pd
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def show_one(result: BaseModel) -> None:
    """Show one result of one element on the command line

    Args:
        result (BaseModel): Single object (View) to print
    """
    print(pd.Series(result.dict()))


def show_many(result: list[T]) -> None:
    """Show one result of many elements on the command line

    Args:
        result (List[BaseModel]): List of objects to print
    """
    print(pd.DataFrame([r.dict() for r in result]))


def show_string(s: str) -> None:
    print(s)
