from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, ForeignKey,Integer, Float, String, ARRAY # LargeBinary
from sqlalchemy import MetaData, Table # for temporary table
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

Base = declarative_base()
metadata = Base.metadata

class Meta(Base):
    __tablename__ = 'pc_metadata'

    name = Column(String)
    crs = Column(String)
    point_count = Column(Integer)
    head_length = Column(Integer)
    tail_length = Column(Integer)
    bbox = Column(ARRAY(Float))

class PointRecord(Base):
    __tablename__ = 'pc_record'

    sfc_head = Column(Integer, primary_key=True, index=True)
    sfc_tail = Column(ARRAY(Integer), nullable=False)
    z = Column(ARRAY(Float))
