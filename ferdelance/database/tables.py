from __future__ import annotations

from sqlalchemy import ForeignKey, String, DateTime, Integer, Float, Table, Column
from sqlalchemy.sql.functions import now
from sqlalchemy.orm import relationship, mapped_column, Mapped

from datetime import datetime

from . import Base


class Setting(Base):
    """Key-value store for settings, parameters, and arguments."""

    __tablename__ = "settings"
    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(String)


class ComponentType(Base):
    """Table to store component types. Current valid types are SERVER, CLIENT, WORKER, WORKBENCH."""

    __tablename__ = "component_types"
    type: Mapped[str] = mapped_column(primary_key=True)


class Component(Base):
    """Table used to keep track of current components."""

    __tablename__ = "components"

    component_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())

    type_name: Mapped[str] = mapped_column(ForeignKey("component_types.type"))
    type = relationship("ComponentType")

    active: Mapped[bool] = mapped_column(default=True)
    left: Mapped[bool] = mapped_column(default=False)

    # this is b64+utf8 encoded bytes
    public_key: Mapped[str] = mapped_column(nullable=False)

    # --- ds-client only part ---
    blacklisted: Mapped[bool] = mapped_column(default=False)
    version: Mapped[str | None] = mapped_column(String, nullable=True)
    # platform.system()
    machine_system: Mapped[str | None] = mapped_column(String, nullable=True)
    # from getmac import get_mac_address; get_mac_address()
    machine_mac_address: Mapped[str | None] = mapped_column(String, nullable=True)
    # uuid.getnode()
    machine_node: Mapped[str | None] = mapped_column(String, nullable=True)
    # client ip address
    ip_address: Mapped[str | None] = mapped_column(String, nullable=True)


class Token(Base):
    """Table that collects all used access tokens for the components.
    If an invalid token is reused, a client could be blacklisted (or updated).
    """

    __tablename__ = "tokens"

    token_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    expiration_time: Mapped[float] = mapped_column(nullable=True)
    token: Mapped[str] = mapped_column(nullable=False, index=True, unique=True)
    valid: Mapped[bool] = mapped_column(default=True)

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.component_id"))
    component = relationship("Component")


class Event(Base):
    """Table that collects all the event from the components."""

    __tablename__ = "events"

    event_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    event: Mapped[str] = mapped_column(nullable=False)

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.component_id"))
    component = relationship("Component")


class Application(Base):
    """Table that keeps track of the available client app version."""

    __tablename__ = "applications"

    app_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    version: Mapped[str] = mapped_column(String)
    active: Mapped[bool] = mapped_column(default=False)
    path: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(String)
    checksum: Mapped[str] = mapped_column(String)


class Artifact(Base):
    """Table that keep tracks of the artifacts available for the components.
    An Artifact is the code that will be run in a client, based on the assigned task.

    Check 'status.py' in shared lib for details on the possible status.
    """

    __tablename__ = "artifacts"

    artifact_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    path: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)


class Job(Base):
    """Table that keep track of which artifact has been submitted and the state of the request.

    A task is equal to an artifact and is composed by a filter query, a model to train, and an aggregation strategy.

    Check 'status.py' in shared lib for details on the possible status.
    """

    __tablename__ = "jobs"

    job_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)

    artifact_id: Mapped[str] = mapped_column(String(36), ForeignKey("artifacts.artifact_id"))
    artifact = relationship("Artifact")

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.component_id"))
    component = relationship("Component")

    status: Mapped[str] = mapped_column(nullable=True)

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    execution_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    termination_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Model(Base):
    """Table that keep track of all the model created and stored on the server."""

    __tablename__ = "models"

    model_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    path: Mapped[str] = mapped_column(String)
    aggregated: Mapped[bool] = mapped_column(default=False)

    # TODO: one model per artifact or one artifact can have multiple models
    artifact_id: Mapped[str] = mapped_column(String(36), ForeignKey("artifacts.artifact_id"))
    artifact = relationship("Artifact")

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.component_id"))
    component = relationship("Component")


class Feature(Base):
    """Table that collects all metadata sent by the client."""

    __tablename__ = "features"

    feature_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)

    name: Mapped[str] = mapped_column(String)
    dtype: Mapped[str | None] = mapped_column(String, nullable=True)

    v_mean: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_std: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_p25: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_p50: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_p75: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    v_miss: Mapped[float | None] = mapped_column(Float, nullable=True)

    n_cats: Mapped[int | None]  # number of categorical values

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    update_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    removed: Mapped[bool] = mapped_column(default=False)

    datasource_id: Mapped[str] = mapped_column(ForeignKey("datasources.datasource_id"))
    datasource: Mapped[DataSource] = relationship(back_populates="features")


project_datasource = Table(
    "project_datasource",
    Base.metadata,
    Column("project_id", ForeignKey("projects.project_id")),
    Column("datasource_id", ForeignKey("datasources.datasource_id")),
)


class Project(Base):
    """Table that collect all projects stored in the system."""

    __tablename__ = "projects"

    project_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String)

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())

    token: Mapped[str] = mapped_column(unique=True)

    valid: Mapped[bool] = mapped_column(default=True)
    active: Mapped[bool] = mapped_column(default=True)

    datasources: Mapped[list["DataSource"]] = relationship(secondary=project_datasource, back_populates="projects")


class DataSource(Base):
    """Table that collects the data source available on each client."""

    __tablename__ = "datasources"

    datasource_id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    datasource_hash: Mapped[str] = mapped_column(String(64))

    name: Mapped[str] = mapped_column(String)

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    update_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    removed: Mapped[bool] = mapped_column(default=False)

    n_records: Mapped[int | None] = mapped_column(Integer, nullable=True)
    n_features: Mapped[int | None] = mapped_column(Integer, nullable=True)

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.component_id"))
    component = relationship("Component")

    projects: Mapped[list[Project]] = relationship(secondary=project_datasource, back_populates="datasources")
    features: Mapped[list[Feature]] = relationship(back_populates="datasource")
