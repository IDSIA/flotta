from typing import List


async def list_projects() -> List:
    print("Listing projects...")
    raise NotImplementedError()


async def create_project(name: str) -> str:
    print("Creating project ", name)
    raise NotImplementedError()
