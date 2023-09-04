from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, ForeignKey,Integer, Float, String, ARRAY # LargeBinary
from sqlalchemy import MetaData, Table # for temporary table
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
#from geoalchemy2 import Geometry #bbox = Column(Geometry("Polygon"))

Base = declarative_base()
metadata = Base.metadata

class Meta(Base):
    __tablename__ = 'pc_metadata_002201m'
    #__table_args__ = {"schema": "lasdb"}
    id = Column(Integer, primary_key=True)#, autoincrement=True)
    version = Column(Float)
    source_file = Column(String)
    number_of_points = Column(Integer)
    head_length = Column(Integer)
    tail_length = Column(Integer)
    scales = Column(ARRAY(Float))
    offsets = Column(ARRAY(Float))
    bbox = Column(ARRAY(Float))
    children = relationship("PointRecord", back_populates="parent")

class PointRecord(Base):
    __tablename__ = 'pc_record_002201m'
    #__table_args__ = {"schema": "lasdb"}

    # Columns
    meta_id = Column(Integer, ForeignKey(Meta.id))
    sfc_head = Column(Integer, primary_key=True, index=True)
    sfc_tail = Column(ARRAY(Integer), nullable=False)
    z = Column(ARRAY(Float))
    classification = Column(ARRAY(Integer))
    parent = relationship('Meta', back_populates='children')
