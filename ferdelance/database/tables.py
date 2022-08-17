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
    client_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    creation_time = Column(DateTime(timezone=True), server_default=now())

    version = Column(String)
    # this is b64+utf8 encoded bytes
    public_key = Column(String)

    # empty: new machine, filled old machine -> created by the server
    uuid = Column(String, nullable=True)

    # platform.system()
    machine_system = Column(String, nullable=False)
    # from getmac import get_mac_address; get_mac_address()
    machine_mac_address = Column(String, nullable=False, unique=True)
    # uuid.getnode()
    machine_node = Column(String, nullable=False)
    # hash of above values
    token = Column(String, nullable=False, unique=True)

    blacklisted = Column(Boolean, default=False)
    ip_address = Column(String, nullable=False)
