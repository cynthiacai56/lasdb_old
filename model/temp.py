from sqlalchemy import Column, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from model.storage import Meta

Base = declarative_base()

class TempRange(Base):
    __tablename__ = 'temp_range'
    __table_args__ = {'prefixes': ['TEMPORARY']}
    #__table_args__ = {'schema': 'temp'} # Set the schema to indicate it's temporary

    start = Column(Integer, primary_key=True)
    end = Column(Integer)

'''
class TempOverlap(Base):
    __tablename__ = 'temp_overlap'
    __table_args__ = {'prefixes': ['TEMPORARY']}
    #__table_args__ = {'schema': 'temp'} # Set the schema to indicate it's temporary

    head = Column(Integer, primary_key=True)

class TempPointMorton(Base):
    __tablename__ = 'temp_point'
    __table_args__ = {'prefixes': ['TEMPORARY']}

    sfc_tail = Column(Float, primary_key=True)
    z = Column(Float)
    classification = Column(Integer)
    import_meta = relationship(Meta)

class TempPoint(Base):
    __tablename__ = 'temp_point'
    __table_args__ = {'prefixes': ['TEMPORARY']}

    meta_id = Column(Integer, ForeignKey(Meta.id))

    x = Column(Float, primary_key=True)
    y = Column(Float)
    z = Column(Float)
    classification = Column(Integer)
'''