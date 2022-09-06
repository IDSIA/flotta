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
    # hash of above values
    # token = Column(String, nullable=False, index=True, unique=True)

    active = Column(Boolean, default=True)
    blacklisted = Column(Boolean, default=False)
    left = Column(Boolean, default=False)
    ip_address = Column(String, nullable=False)


class ClientToken(Base):
    """Table that collect all used tokens for the clients.
    If an invalid token is reused, the client could be blacklisted (or updated).
    """
    __tablename__ = 'client_tokens'

    token_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    expiration_time = Column(Integer, nullable=True)
    token = Column(String, nullable=False, index=True, unique=True)
    valid = Column(Boolean, default=True)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')


class ClientEvent(Base):
    """Table that collect all the event from the clients."""
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
    An Artifact is the code that will be runned in a client, based on the assigned task.
    """
    __tablename__ = 'artifacts'

    artifact_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    version = Column(String, nullable=False)
    path = Column(String, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)


class Task(Base):
    """Table that keep track of which artifact has been deployed to which client and the state of the request."""
    __tablename__ = 'tasks'

    task_id = Column(String, primary_key=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    start_time = Column(DateTime(timezone=True))
    stop_time = Column(DateTime(timezone=True))

    status = Column(String, nullable=True)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')

    artifact_id = Column(String, ForeignKey('artifacts.artifact_id'))
    artifact = relationship('Artifact')


class Model(Base):
    """Table that keep track of all the model created and stored on the server."""
    __tablename__ = 'models'

    model_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())
    path = Column(String, nullable=False)


class ClientTaskEvent(Base):
    """Table that collect all the events that a client produced during a task."""
    __tablename__ = 'client_task_events'

    event_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    event_start = Column(DateTime(timezone=True), server_default=now())
    event_update = Column(DateTime(timezone=True), server_default=now())
    event = Column(String, nullable=False)
    status = Column(String, nullable=False)

    client_id = Column(String, ForeignKey('clients.client_id'))
    client = relationship('Client')

    task_id = Column(String, ForeignKey('tasks.task_id'))
    task = relationship('Task')
