import numpy as np
from sqlalchemy import create_engine, DDL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from pcsfc.decoder import DecodeMorton2D
from pcsfc.range_search import morton_range
from pcsfc.geometry_query import bbox_filter, circle_filter, polygon_filter
from model.storage import Meta, PointRecord
from model.temp import TempRange, TempOverlap, TempPoint




nbits = 64
x_min, x_max, y_min, y_max = 80000000, 80000050, 443750000, 443800000
tail_length = 12
engine_key = 'postgresql://postgres:050694@localhost:5432/lasdb_1_70'


def head_filter(x_min, x_max, y_min, y_max, tail_length, engine_key):

    # Step 1: First-filter search range
    ranges, overlaps = morton_range(nbits, x_min, y_min, x_max, y_max, tail_length)
    # Step 2: Create Range Table
    # Step 3: Join Range Table and the Data Table
    engine = create_engine()

    Session = sessionmaker(bind=engine)
    session = Session()


    if len(ranges) > 0:
        ranges_shift = [key >> tail_length for key in ranges]
        TempRange.__table__.create(engine, checkfirst=True)
        for item in ranges_shift:
            temp_range_row = TempRange(start=item[0], end=item[1])
            session.add(temp_range_row)
        query_res_1 = session.query(PointRecord).join(TempRange,
                                                      PointRecord.head_sfc.between(TempRange.start, TempRange.end)).all()

    if len(overlaps) > 0:
        overlaps_shift = [key >> tail_length for key in overlaps]
        TempOverlap.__table__.create(engine, checkfirst=True)
        for item in overlaps_shift:  # overlaps_shift:
            temp_overlap_row = TempOverlap(head=item)
            session.add(temp_overlap_row)
        # session.commit()
        # first_100_records = session.query(TempOverlap).limit(20).all()

        # for record in first_100_records:
        # print(record.head)
        query_res_2 = session.query(PointRecord).join(TempOverlap, PointRecord.sfc_head == TempOverlap.head).all()

    # Step 4: Decode
    # pc_record: meta_id, sfc_head, sfc_tail, Z, classification
    # temp_point: X, Y, Z, classification
    query_points_list = []
    for record in query_res_2:  # Each group
        sfc_head = record.sfc_head
        sfc_tail_list = record.sfc_tail
        Z_list = record.Z
        classification_list = record.classification

        for i in range(len(sfc_tail_list)):  # Each point
            sfc_tail = sfc_tail_list[i]
            Z = Z_list[i]
            classification_code = classification_list[i]

            # Decode
            sfc_key = sfc_head << tail_length | sfc_tail
            X, Y = DecodeMorton2D(sfc_key)
            query_points_list.append([X, Y, Z, classification_code])

    session.commit()
    session.close()

    query_points = np.array(query_points_list)
    return query_points # a list of points

# Step 5: Second-filter geometry
# In other functions


def bbox_search(args):
    # Load parameters
    x_min, x_max, y_min, y_max = args.x[0], args.x[1], args.y[0], args.y[1]
    tail_length = args.t
    engine_key = args.k

    # First filter and decode
    pts1 = head_filter(x_min, x_max, y_min, y_max, tail_length, engine_key)

    # Second filter
    pts2 = bbox_filter(x_min, x_max, y_min, y_max, pts1)

    # Display?
    # Export to txt, las | Show in the database
    # a view is the result set of a stored query, which can be queried in the same manner as a persistent database collection object.
    # as a result set, it is a virtual table computed or collated dynamically from data in the database when access to that view is requested


    engine = create_engine(engine_key)

    TempPoint.__table__.create(engine, checkfirst=True)

    # Import data to temp_point table
    for pt in pts2:
        temp_pt_record = TempPoint(X=pt[0], Y=pt[1], Z=pt[2], classification=pt[3])
        session = Session()
        session.add(temp_pt_record)
        session.commit()
        session.close()

    # Create view
    view = DDL("CREATE VIEW query_point_view AS SELECT X, Y, Z, classification FROM temp_points")
    engine.execute(view)

    return pts2

def circle_search(args):
    # Load parameters
    center, radius = args.p, args.r
    x_min, x_max = center[0] - radius, center[0] + radius
    y_min, y_max = center[1] - radius, center[1] + radius
    tail_length = args.t
    engine_key = args.k

    # First filter and decode
    pts1 = head_filter(x_min, x_max, y_min, y_max, tail_length, engine_key)

    # Second filter
    pts2 = circle_filter(center, radius, pts1)

    # Display?
    # Export to txt, las | Show in the database

    return pts2

def polygon_search(args):
    poly_pts = args.p
    x_min, x_max = min(poly_pts, key=lambda coord: coord[0])[0], max(poly_pts, key=lambda coord: coord[0])[0]
    y_min, y_max = min(poly_pts, key=lambda coord: coord[1])[1], max(poly_pts, key=lambda coord: coord[1])[1]
    tail_length = args.t
    engine_key = args.k

    # First filter and decode
    pts1 = head_filter(x_min, x_max, y_min, y_max, tail_length, engine_key)

    # Second filter
    pts2 = polygon_filter(poly_pts, pts1)

    return pts2

def nn_search(args):
    return 0

def cla_search(args):
    return 0