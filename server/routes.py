from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session

import uvicorn

from server.middleware import AccessMiddleware

from database import SessionLocal, crud, startup


api = FastAPI()

api.add_middleware(AccessMiddleware)


def get_db():
    """This is a generator for obtain the session to the database through SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.on_event('startup')
async def populate_database():
    """All operations marked as ``on_event('startup')`` are executed when the APi are runned.
    """
    try:
        db = SessionLocal()
        startup.init_content(db)
    finally:
        db.close()


@api.get("/")
def root(db: Session = Depends(get_db)):
    """This is the endpoint for the home page."""
    return 'Hi! 😀'


if __name__ == '__main__':
    uvicorn.run(api)
