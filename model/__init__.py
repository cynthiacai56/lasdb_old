from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, ARRAY
# from sqlalchemy.orm import relationship

Base = declarative_base()

class PointCloudTable(Base):
    __tablename__ = 'point_cloud'

    # Columns
    sfc_head = Column(Integer, primary_key=True, index=True)
    sfc_tail = Column(ARRAY(Integer), nullable = False)
    Z = Column(ARRAY(Float))
    intensity = Column(ARRAY(Integer))
    return_number = Column(ARRAY(Integer))
    number_of_returns = Column(ARRAY(Integer))
    scan_direction_flag= Column(ARRAY(Integer))
    edge_of_flight_line = Column(ARRAY(Integer))
    classification = Column(ARRAY(Integer))
    synthetic = Column(ARRAY(Integer))
    key_point = Column(ARRAY(Integer))
    withheld = Column(ARRAY(Integer))
    scan_angle_rank = Column(ARRAY(Integer))
    user_data = Column(ARRAY(Integer))
    point_source_id = Column(ARRAY(Integer))
    gps_time = Column(ARRAY(Integer))


