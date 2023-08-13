from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, Table
from model.storage import Meta, PointRecord
from pcsfc.range_search import morton_range



def query_with_range(engine_key, start_value, end_value):
    # 2. Create a connection to the PostgreSQL database using SQLAlchemy
    engine = create_engine(engine_key)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query the database for records within the specified range
    query = session.query(PointRecord).filter(PointRecord.sfc_head.between(start_value, end_value))

    # Fetch all matching records
    results = query.all()

    # Process the results
    results = [[res.sfc_head] for res in results] #(result.id, result.your_column)

    return results


def query_with_bbox(engine_key, search_list):
    # 2. Create a connection to the PostgreSQL database using SQLAlchemy
    engine = create_engine(engine_key)
    Session = sessionmaker(bind=engine)
    session = Session()


    # Query the database for records where the value of 'your_column' is in the search list
    query = session.query(PointRecord).filter(PointRecord.sfc_head.in_(search_list))
    # Query the database for records within the specified range
    query = session.query(PointRecord).filter(PointRecord.sfc_head.between(start_value, end_value))

    # Fetch all matching records
    results = query.all()

    # Process the results
    results = [[res.sfc_head] for res in results] #(result.id, result.your_column)

    return results

'''
head_length = 46
tail_length = 12
x_min, y_min, x_max, y_max = 80000000, 443750000, 80003000,443753000
print('x_query_range:', x_min, x_max)
print('y_query_range:', y_min, y_max)
search_list = [43669030, 43669097, 43669192, 43669219]
contains, overlaps = morton_range(head_length, x_min, y_min, x_max, y_max, tail_length=tail_length)

#result = query_with_range('postgresql://postgres:050694@localhost:5432/lasdb_1_70', contains)
print('Contains', len(contains))
print(contains)
print('Overlaps', len(overlaps))
print(overlaps)
'''
