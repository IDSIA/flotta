from datetime import datetime
from ipaddress import ip_address
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
    creation_time = Column(DateTime(timezone=True), default_value=now())

    version = Column(String)
    public_key = Column(String)

    uuid = Column(String, nullable=True)  # empty: new machine, filled old machine -> created by the server

    machine_system = Column(String, nullable=False)       # platform.system()
    machine_mac_address = Column(String, nullable=False, unique=True)  # from getmac import get_mac_address; get_mac_address()
    machine_node = Column(String, nullable=False)         # uuid.getnode()
    token = Column(String, nullable=False, unique=True)                # hash of above values

    blacklisted = Column(Boolean, default=False)
    ip_address = Column(String, nullable=False)
