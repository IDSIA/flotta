from sqlalchemy import Column, ForeignKey, String, Float, DateTime, Integer, Boolean, Date
from sqlalchemy.sql.functions import now
from sqlalchemy.orm import relationship

from . import Base


class Setting(Base):
    """Key-value store for settings, parameters, and arguments."""
    __tablename__ = 'settings'
    key = Column(String, primary_key=True, index=True)
    value = Column(String)


class Client(Base):
    """Table used to keep track of current clients."""
    __tablename__ = 'clients'
    client_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())

    version = Column(String)
    # this is b64+utf8 encoded bytes
    public_key = Column(String)

    # platform.system()
    machine_system = Column(String, nullable=False)
    # from getmac import get_mac_address; get_mac_address()
    machine_mac_address = Column(String, nullable=False, unique=True)
    # uuid.getnode()
    machine_node = Column(String, nullable=False)

    # valid values: SERVER, CLIENT, WORKER, WORKBENCH
    type = Column(String, nullable=False)

    active = Column(Boolean, default=True)
    blacklisted = Column(Boolean, default=False)
    left = Column(Boolean, default=False)
    ip_address = Column(String, nullable=False)


class ClientToken(Base):
    """Table that collects all used tokens for the clients.
    If an invalid token is reused, the client could be blacklisted (or updated).
    """
    __tablename__ = 'client_tokens'

    token_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    expiration_time = Column(Float, nullable=True)
    token = Column(String, nullable=False, index=True, unique=True)
    valid = Column(Boolean, default=True)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')


class ClientEvent(Base):
    """Table that collects all the event from the clients."""
    __tablename__ = 'client_events'

    event_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    event_time = Column(DateTime(timezone=True), server_default=now())
    event = Column(String, nullable=False)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')


class ClientApp(Base):
    """Table that keeps track of the available client app version."""
    __tablename__ = 'client_apps'

    app_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    version = Column(String,)
    active = Column(Boolean, default=False)
    path = Column(String, nullable=False)
    name = Column(String)
    description = Column(String)
    checksum = Column(String)


class Artifact(Base):
    """Table that keep tracks of the artifacts available for the clients.
    An Artifact is the code that will be run in a client, based on the assigned task.

    Check 'status.py' in shared lib for details on the possible status.
    """
    __tablename__ = 'artifacts'

    artifact_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    path = Column(String, nullable=False)
    status = Column(String)


class Job(Base):
    """Table that keep track of which artifact has been submitted and the state of the request.

    A task is equal to an artifact and is composed by a filter query, a model to train, and an aggregation strategy.

    Check 'status.py' in shared lib for details on the possible status.
    """
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True, index=True)

    artifact_id = Column(String, ForeignKey('artifacts.artifact_id'))
    artifact = relationship('Artifact')

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')

    status = Column(String, nullable=True)

    creation_time = Column(DateTime(timezone=True), server_default=now())
    execution_time = Column(DateTime(timezone=True))
    termination_time = Column(DateTime(timezone=True))


class Model(Base):
    """Table that keep track of all the model created and stored on the server."""
    __tablename__ = 'models'

    model_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    path = Column(String, nullable=False)
    aggregated = Column(Boolean, nullable=False, default=False)

    # TODO: one model per artifact or one artifact can have multiple models
    artifact_id = Column(String, ForeignKey('artifacts.artifact_id'))
    artifact = relationship('Artifact')

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')


class ClientDataSource(Base):
    """Table that collects the data source available on each client."""
    __tablename__ = 'client_datasources'

    datasource_id = Column(String, primary_key=True, index=True)

    name = Column(String, nullable=False)

    creation_time = Column(DateTime(timezone=True), server_default=now())
    update_time = Column(DateTime(timezone=True), server_default=now())
    removed = Column(Boolean, nullable=False, default=False)

    n_records = Column(Integer)
    n_features = Column(Integer)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')


class ClientFeature(Base):
    """Table that collects all metadata sent by the client."""
    __tablename__ = 'client_features'

    feature_id = Column(String, primary_key=True, index=True)

    name = Column(String, nullable=False)
    dtype = Column(String)

    v_mean = Column(Float)
    v_std = Column(Float)
    v_min = Column(Float)
    v_p25 = Column(Float)
    v_p50 = Column(Float)
    v_p75 = Column(Float)
    v_max = Column(Float)
    v_miss = Column(Float)

    creation_time = Column(DateTime(timezone=True), server_default=now())
    update_time = Column(DateTime(timezone=True), server_default=now())
    removed = Column(Boolean, nullable=False, default=False)

    datasource_name = Column(String, nullable=False)

    datasource_id = Column(String, ForeignKey('client_datasources.datasource_id'))
    datasource = relationship('ClientDataSource')
