from ferdelance.database.tables import (
    Artifact,
    Component,
    DataSource,
    Event,
    Feature,
    Token,
    Job,
    Model,
    Token,
)

from sqlalchemy.orm import Session
from sqlalchemy import select, delete


def get_client_by_id(session: Session, client_id: str) -> Component | None:
    return session.execute(select(Component).where(Component.component_id == client_id)).scalar_one_or_none()


def get_user_by_id(session: Session, user_id: str) -> Component | None:
    return session.execute(select(Component).where(Component.component_id == user_id)).scalar_one_or_none()


def get_token_by_id(session: Session, client_id: str) -> Token | None:
    return session.execute(
        select(Token).where(Token.component_id == client_id, Token.valid == True)
    ).scalar_one_or_none()


def get_client_events(session: Session, client_id: str) -> list[str]:
    return list(session.scalars(select(Event.event).where(Event.component_id == client_id)).all())


def delete_client(session: Session, client_id: str) -> None:
    session.execute(delete(Event).where(Event.component_id == client_id))
    session.execute(delete(Token).where(Token.component_id == client_id))
    session.execute(delete(Component).where(Component.component_id == client_id))
    session.commit()


def delete_user(session: Session, user_id: str) -> None:
    session.execute(delete(Token).where(Token.component_id == user_id))
    session.execute(delete(Component).where(Component.component_id == user_id))


def delete_datasource(session: Session, datasource_id: str | None = None, client_id: str | None = None) -> None:
    if datasource_id is None and client_id is not None:
        datasource_id = session.scalar(select(DataSource.datasource_id).where(DataSource.component_id == client_id))

    session.execute(delete(Feature).where(Feature.datasource_id == datasource_id))
    session.execute(delete(DataSource).where(DataSource.datasource_id == datasource_id))
    session.commit()


def delete_job(session: Session, client_id: str) -> None:
    session.execute(delete(Job).where(Job.component_id == client_id))
    session.commit()


def delete_artifact(session: Session, artifact_id: str) -> None:
    session.execute(delete(Model).where(Model.artifact_id == artifact_id))
    session.execute(delete(Artifact).where(Artifact.artifact_id == artifact_id))
    session.commit()
