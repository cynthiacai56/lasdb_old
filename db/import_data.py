from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from model.storage import Base, Meta, PointRecord


def import_data_connection(engine_key, meta_dict, pc_groups):
    # 2. Create a connection to the PostgreSQL database using SQLAlchemy
    engine = create_engine(engine_key)
    if not database_exists(engine.url):
        create_database(engine.url)

    Session = sessionmaker(bind=engine)
    session = Session()

    # 3.Create tables in the database
    Base.metadata.create_all(engine)  # , schema='lasdb')

    # 4. Import data to pc_metadata table
    pc_meta_table = Meta(id=meta_dict['meta_id'],
                         version=meta_dict['version'],
                         source_file=meta_dict['source_file'],
                         number_of_points=meta_dict['number_of_points'],
                         head_length=meta_dict['head_length'],
                         tail_length=meta_dict['tail_length'],
                         scales=meta_dict['scales'],
                         offsets=meta_dict['offsets'],
                         transform=meta_dict['transform'],
                         bbox=meta_dict['bbox'])
    session.add(pc_meta_table)
    session.commit()
    session.close()

    # Import data to pc_record table
    for pt in pc_groups:
        pc_record = PointRecord(meta_id = meta_dict['meta_id'], sfc_head=pt[0], sfc_tail=pt[1], Z=pt[2], classification=pt[3])
        session = Session()
        session.add(pc_record)
        session.commit()
        session.close()