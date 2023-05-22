from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Base, Meta, PointRecord


def import_data(engine_key, meta_dict, groups):
    # 2. Create a connection to the PostgreSQL database using SQLAlchemy
    engine = create_engine(engine_key)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 3.Create tables in the database
    Base.metadata.create_all(engine)  # , schema='lasdb')

    # 4. Import data to pc_metadata table
    pc_meta_table = Meta(id=1, version=meta_dict['version'],
                         source_file=meta_dict['source_file'],
                         number_of_points=meta_dict['number_of_points'],
                         transform=meta_dict['transform'],
                         bbox=meta_dict['bbox'])
    session.add(pc_meta_table)

    # Import data to pc_record table
    pc_record_table = [PointRecord(meta_id=1,
                                    sfc_head=row[0],
                                    sfc_tail=row[1],
                                    Z=row[2],
                                    classification=row[3]) for row in groups]
    session.add_all(pc_record_table)

    # 5. Commit the changes to the database
    try:
        # Attempt to commit changes to the database
        session.commit()
    except Exception as e:
        # Roll back the session and close it
        session.rollback()
        session.close()
        # Create a new session and try again
        session = Session()
        session.add_all(pc_record_table)
        session.commit()