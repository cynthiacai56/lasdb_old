from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, ForeignKey,Integer, Float, LargeBinary, String, ARRAY#, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
#from geoalchemy2 import Geometry #bbox = Column(Geometry("Polygon"))

Base = declarative_base()

class Meta(Base):
    __tablename__ = 'pc_metadata'
    __table_args__ = {"schema": "lasdb"}
    id = Column(Integer, primary_key=True)
    version = Column(Float)
    source_file = Column(String)
    number_of_points = Column(Integer)
    transform = Column(ARRAY(Float))
    bbox = Column(ARRAY(Float))
    #transform = Column(JSONB(none_as_null=True)) # scales, offsets
    #bbox = Column(JSONB(none_as_null=True)) # xyz_min, xyz_max
    #started_at = Column(TIMESTAMP, default=func.now())
    #finished_at = Column(TIMESTAMP)


class PointRecord(Base):
    __tablename__ = 'pc_record'
    __table_args__ = {"schema": "lasdb"}

    # Columns
    meta_id = Column(Integer, ForeignKey(Meta.id))
    sfc_head = Column(LargeBinary, primary_key=True, index=True)
    sfc_tail = Column(ARRAY(LargeBinary), nullable = False)
    Z = Column(ARRAY(Float))
    classification = Column(ARRAY(Integer))
    import_meta = relationship(Meta)