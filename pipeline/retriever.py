import numpy as np
from sqlalchemy import create_engine, DDL
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pcsfc.decoder import DecodeMorton2D
from pcsfc.range_search import morton_range
from pcsfc.geometry_query import bbox_filter, circle_filter, polygon_filter
from model.storage import Meta, PointRecord
from model.temp import TempRange#, TempOverlap, TempPoint


class GeometryFilter:
    MODE_BBOX = 'mode_bbox'
    MODE_CIRCLE = 'mode_circle'
    MODE_POLYGON = 'mode_polygon'
    def __init__(self, mode, constr, head_len, tail_len, db_url):
        self.mode = mode
        self.constr = constr
        self.head_len = head_len
        self.tail_len = tail_len
        self.db_url = db_url

        self.bbox = self.get_bbox()

    def query(self):
        # meta filter: to be continued
        # Bbox filter
        head_rgs, head_ols = morton_range(self.bbox, 0, self.head_len, self.tail_len)

        if len(head_rgs) > 0:
            range_pts = self.get_groups_with_containment(head_rgs)
        else:
            range_pts = [0]
        if len(head_ols) > 0:
            overlap_pts = self.get_groups_with_overlaps(head_ols)
        else:
            overlap_pts = []

        print('range points', len(range_pts))
        print('overlapping points', len(overlap_pts))

        # Geometry filter
        points = range_pts + overlap_pts if range_pts is not None and overlap_pts is not None else []

        if self.mode == self.MODE_BBOX:
            return points
        elif self.mode == self.MODE_CIRCLE:
            return circle_filter(points, self.constr[0], self.constr[1])
        elif self.mode == self.MODE_POLYGON:
            return polygon_filter(points, self.constr)

    def get_bbox(self):
        if self.mode == self.MODE_BBOX: # [x_min, x_max, y_min, y_max]
            bbox = self.constr
        elif self.mode == self.MODE_CIRCLE: # [[center_x, center_y], radius]
            x_min, x_max = self.constr[0][0] - self.constr[1], self.constr[0][0] + self.constr[1]
            y_min, y_max = self.constr[0][1] - self.constr[1], self.constr[0][1] + self.constr[1]
            bbox = [x_min, x_max, y_min, y_max]
        elif self.mode == self.MODE_POLYGON: # a set of point coordinates
            x = [pt[0] for pt in self.constr[0]]
            y = [pt[1] for pt in self.constr[0]]
            x_min, x_max = min(x), max(x)
            y_min, y_max = min(y), max(y)
            bbox = [x_min, x_max, y_min, y_max]
        else:
            print("Invalid mode")
        return bbox


    def get_groups_with_containment(self, head_list):
        # Connect to the database and create a session
        engine = create_engine(self.db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Second-filter: head
        # For all sfc_heads in the range list, extract all records and decode the points
        # However, the third-filter should be applied to the decoded points

        # Create temporary range table
        TempRange.__table__.create(engine, checkfirst=True)
        for item in head_list:
            # Insert queried ranges into the range table
            temp_range_row = TempRange(start=item[0], end=item[1])
            session.add(temp_range_row)

        # Query the ranges in the pc_record table
        res_rg = session.query(PointRecord).join(TempRange,
                                                     PointRecord.sfc_head.between(TempRange.start, TempRange.end)).all()

        # Unpack and convert the records to points
        query_pts = []
        for record in res_rg:
            # Unpack the tails
            for i in range(len(record.sfc_tail)):  # Each point
                # Decode
                sfc_key = record.sfc_head << self.tail_len | record.sfc_tail[i]
                x, y = DecodeMorton2D(sfc_key)
                query_pts.append([x, y, record.z[i], record.classification[i]])

        return query_pts


    def get_groups_with_overlaps(self, head_ols):
        engine = create_engine(self.db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        # 对于overlap table里得到的head，继续对tail执行range_search()
        # 但是，有的要进行二次验证，针对具体的geometry
        res_ol = session.query(PointRecord).filter(PointRecord.sfc_head.in_(head_ols)).all()

        # Unpack and convert the records to points
        # pc_record: meta_id, sfc_head, sfc_tail, Z, classification
        # temp_point: X, Y, Z, classification
        query_pts = []
        for record in res_ol:  # Each group
            # only ranges have results, just extract them out; overlaps should be empty
            # Check which tails of this head in within the ranges
            tail_rgs, tail_ols = morton_range(self.bbox, record.sfc_head, self.tail_len, 0)
            # Unpack the tails
            for i in range(len(record.sfc_tail)):  # Each point
                # Check if the tail in within the ranges
                check_in_range = any(start <= record.sfc_tail[i] <= end for start, end in tail_rgs)
                if check_in_range == 1:
                    # Decode
                    sfc_key = record.sfc_head << self.tail_len | record.sfc_tail[i]
                    x, y = DecodeMorton2D(sfc_key)
                    query_pts.append([x, y, record.z[i], record.classification[i]])

        session.commit()
        session.close()

        return query_pts

