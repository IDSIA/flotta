from sqlalchemy import Column, ForeignKey, String, Float, DateTime, Integer, Boolean, Date
from sqlalchemy.sql.functions import now
from sqlalchemy.orm import relationship

from . import Base


class Client(Base):
    """Table used to keep track of current clients."""
    __tablename__ = 'clients'
    client_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    version = Column(String)
