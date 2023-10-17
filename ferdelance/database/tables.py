from __future__ import annotations

from sqlalchemy import (
    ForeignKey,
    String,
    DateTime,
    Integer,
    Table,
    Column,
    Boolean,
)
from sqlalchemy.sql.functions import now
from sqlalchemy.orm import relationship, mapped_column, Mapped

from datetime import datetime

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


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
    """Table used to keep track of components in the network."""

    __tablename__ = "components"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())

    name: Mapped[str] = mapped_column(nullable=True)
    is_self: Mapped[bool] = mapped_column(default=False)
    is_join: Mapped[bool] = mapped_column(default=False)

    type_name: Mapped[str] = mapped_column(ForeignKey("component_types.type"))
    type = relationship("ComponentType")

    blacklisted: Mapped[bool] = mapped_column(default=False)
    active: Mapped[bool] = mapped_column(default=True)
    left: Mapped[bool] = mapped_column(default=False)

    # this is b64+utf8 encoded bytes
    public_key: Mapped[str] = mapped_column(nullable=False)

    # fdl component's version
    version: Mapped[str | None] = mapped_column(String)

    # node component ip addresss (for indirect communication)
    ip_address: Mapped[str | None] = mapped_column(String)
    # node component complete url (for direct communication)
    url: Mapped[str | None] = mapped_column(String)


class Event(Base):
    """Table that collects all the event on the components."""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    event: Mapped[str] = mapped_column(nullable=False)

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.id"))
    component = relationship("Component")


class Artifact(Base):
    """Table that keep tracks of the artifacts available for the components.
    An Artifact is the code that will be run in a client, based on the assigned task.

    Check 'status.py' in shared lib for details on the possible status.
    """

    __tablename__ = "artifacts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    path: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)

    # Zero-based index, same as relative Job.iteration
    iteration: Mapped[int] = mapped_column(default=0)

    is_model: Mapped[bool] = mapped_column(default=False)
    is_estimation: Mapped[bool] = mapped_column(default=False)


class Job(Base):
    """Table that keeps track of which artifact has been submitted and the state of the request.

    A task is equal to an artifact and is composed by a filter query, a model to train, and an aggregation strategy.

    Check 'status.py' in shared lib for details on the possible status.
    """

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)

    artifact_id: Mapped[str] = mapped_column(String(36), ForeignKey("artifacts.id"))
    artifact = relationship("Artifact")

    # True if the job trains a new model
    is_model: Mapped[bool] = mapped_column(default=False)
    # True if the job fit a new estimation
    is_estimation: Mapped[bool] = mapped_column(default=False)
    # True if the job is an aggregation of models or estimations
    is_aggregation: Mapped[bool] = mapped_column(default=False)
    # Zero-based counter for iterations
    iteration: Mapped[int] = mapped_column(default=0)

    # This counter keeps track of how many other jobs this job is waiting for, when 0 it can be set to scheduled
    lock_counter: Mapped[int] = mapped_column(default=0)
    work_type: Mapped[str] = mapped_column(String())

    # Id of the component executing the job
    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.id"))
    component = relationship("Component")

    # Last known status of the job
    status: Mapped[str] = mapped_column(nullable=True)

    # When the job has been created in waiting state
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    # When the job has been scheduled
    scheduling_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    # When the job started
    execution_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # When the job terminated
    termination_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class JobLock(Base):
    """Jobs are related to each other through a Directed Acyclic Graph (DAG). Each time a job is completed
    successfully, it can unlock other jobs. This table keeps track of which jobs are unlocked.

    When the job designed by `job_id` is completed without error, the entries in this table will be updated
    to have their `locked` state to be False.
    """

    __tablename__ = "job_locks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    artifact_id: Mapped[str] = mapped_column(String(36), ForeignKey("artifacts.id"))
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"))
    next_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"))
    locked: Mapped[bool] = mapped_column(Boolean, default=True)

    artifat = relationship("Artifact", foreign_keys=[artifact_id])
    job = relationship("Job", foreign_keys=[job_id])
    next_job = relationship("Job", foreign_keys=[next_id])


class Resource(Base):
    """Table that keep track of all the resources produced by each job and stored on the server."""

    __tablename__ = "resources"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    path: Mapped[str] = mapped_column(String)

    # TODO: if both is no, then it is a plain resource?
    is_model: Mapped[bool] = mapped_column(default=False)
    is_estimation: Mapped[bool] = mapped_column(default=False)
    is_aggregation: Mapped[bool] = mapped_column(default=False)
    is_error: Mapped[bool] = mapped_column(default=False)

    iteration: Mapped[int] = mapped_column(default=0)

    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"))
    job = relationship("Job")

    # TODO: one model per artifact or one artifact can have multiple models?
    artifact_id: Mapped[str] = mapped_column(String(36), ForeignKey("artifacts.id"))
    artifact = relationship("Artifact")

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.id"))
    component = relationship("Component")


project_datasource = Table(
    "project_datasource",
    Base.metadata,
    Column("project_id", ForeignKey("projects.id")),
    Column("datasource_id", ForeignKey("datasources.id")),
)


class Project(Base):
    """Table that collect all projects stored in the system."""

    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String)

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())

    token: Mapped[str] = mapped_column(unique=True)

    valid: Mapped[bool] = mapped_column(default=True)
    active: Mapped[bool] = mapped_column(default=True)

    datasources: Mapped[list["DataSource"]] = relationship(secondary=project_datasource, back_populates="projects")


class DataSource(Base):
    """Table that collects the data source available on each client."""

    __tablename__ = "datasources"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)
    hash: Mapped[str] = mapped_column(String(64))

    name: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String)

    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    update_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=now())
    removed: Mapped[bool] = mapped_column(default=False)

    n_records: Mapped[int | None] = mapped_column(Integer, nullable=True)
    n_features: Mapped[int | None] = mapped_column(Integer, nullable=True)

    component_id: Mapped[str] = mapped_column(String(36), ForeignKey("components.id"))
    component = relationship("Component")

    projects: Mapped[list[Project]] = relationship(secondary=project_datasource, back_populates="datasources")
