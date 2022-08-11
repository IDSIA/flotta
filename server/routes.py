from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session

import uvicorn
from server import crypto

from server.middleware import AccessMiddleware

from database import SessionLocal, crud, startup


api = FastAPI()

api.add_middleware(AccessMiddleware)


def get_db():
    """This is a generator to get a session to the database through SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.on_event('startup')
async def populate_database():
    """All operations marked as ``on_event('startup')`` are executed when the API are started."""
    try:
        db = SessionLocal()
        startup.init_content(db)
        crypto.generate_keys(db)
    finally:
        db.close()


@api.get("/")
async def root(db: Session=Depends(get_db)):
    """This is the endpoint for the home page."""
    return 'Hi! 😀'


@api.post("/client/join")
async def client_join(db: Session=Depends(get_db)):
    """API for new client joining."""

    return


@api.get("/client/update")
async def client_update(db: Session=Depends(get_db)):
    """API used by the client to get the updates. Updates can be one of the following:
    - new server public key
    - new algorithm package
    - new client package
    """

    return


if __name__ == '__main__':
    uvicorn.run(api)
