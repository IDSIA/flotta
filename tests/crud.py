from ferdelance.database.tables import (
    Artifact,
    Client,
    ClientDataSource,
    ClientEvent,
    ClientFeature,
    ClientToken,
    Job,
    Model,
)

from sqlalchemy.orm import Session
from sqlalchemy import select, delete


def get_client_by_id(session: Session, client_id: str) -> Client | None:
    return session.execute(
        select(Client)
        .where(Client.client_id == client_id)
    ).scalar_one_or_none()


def get_token_by_id(session: Session, client_id: str) -> ClientToken | None:
    return session.execute(
        select(ClientToken)
        .where(
            ClientToken.client_id == client_id,
            ClientToken.valid == True
        )
    ).scalar_one_or_none()


def get_client_events(session: Session, client_id: str) -> list[str]:
    return session.scalars(
        select(ClientEvent.event)
        .where(ClientEvent.client_id == client_id)
    ).all()


def delete_client(session: Session, client_id: str) -> None:
    session.execute(
        delete(ClientEvent).where(ClientEvent.client_id == client_id)
    )
    session.execute(
        delete(ClientToken).where(ClientToken.client_id == client_id)
    )
    session.execute(
        delete(Client).where(Client.client_id == client_id)
    )
    session.commit()


def delete_datasource(session: Session, datasource_id: str | None = None, client_id: str | None = None) -> None:
    if datasource_id is None and client_id is not None:
        datasource_id = session.scalar(select(ClientDataSource.datasource_id).where(ClientDataSource.client_id == client_id))

    session.execute(
        delete(ClientFeature).where(ClientFeature.datasource_id == datasource_id)
    )
    session.execute(
        delete(ClientDataSource).where(ClientDataSource.datasource_id == datasource_id)
    )
    session.commit()


def delete_job(session: Session, client_id: str) -> None:
    session.execute(delete(Job).where(Job.client_id == Job.client_id))
    session.commit()


def delete_artifact(session: Session, artifact_id: str) -> None:
    session.execute(delete(Model).where(Model.artifact_id == artifact_id))
    session.execute(delete(Artifact).where(Artifact.artifact_id == artifact_id))
    session.commit()
