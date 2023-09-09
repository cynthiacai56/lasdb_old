import numpy as np
import pandas as pd
import laspy
from tqdm import tqdm

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from model.storage import Base, Meta, PointRecord

from pcsfc.encoder import EncodeMorton2D, compute_split_length, split_bin, make_groups


class PointGroupProcessor:
    def __init__(self, meta_id, path, file, ratio):
        self.input = path + '/' + file

        self.metas = self.get_metadata(meta_id, file, ratio)
        if self.metas['number_of_points'] <= 50000000:
            self.pc_groups = self.process_points(self.metas['tail_length'])
        else:
            self.pc_groups = self.process_points_chunk(self.metas['tail_length'])

    def get_metadata(self, meta_id, file, ratio):
        with laspy.open(self.input) as f:
            # Determine tail length
            head_len, tail_len = compute_split_length(int(f.header.x_max), int(f.header.y_max), ratio)

            # Load meta_dict
            meta_dict = {'meta_id': meta_id,
                         'version': float(str(f.header.version)),
                         'source_file': file,
                         'number_of_points': f.header.point_count,
                         'head_length': head_len,
                         'tail_length': tail_len,
                         'scales': [scale for scale in f.header.scales],
                         'offsets': [offset for offset in f.header.offsets],
                         'bbox': [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max,
                                  f.header.z_min, f.header.z_max]
                         }
        print('Metadata: ', meta_dict)
        return meta_dict

    def process_points(self, tail_len):
        las = laspy.read(self.input)
        points = np.vstack((las.x, las.y, las.z, las.classification)).transpose()
        encoded_pts = []
        for pt in tqdm(points):
            mkey = EncodeMorton2D(int(pt[0]), int(pt[1]))
            head, tail = split_bin(mkey, tail_len)
            encoded_pts.append([head, tail, float(pt[2]), int(pt[3])])
        
        pc_groups = make_groups(encoded_pts)
        print(len(pc_groups))
        return pc_groups
            
    def process_points_chunk(self, tail_len):
        encoded_pts = []
        with laspy.open(self.input) as f:
            for points in f.chunk_iterator(50000000):
                encoded_pts_per_iter = []
                pts = np.vstack((points.x, points.y, points.z, points.classification)).transpose()
                print('chunk interator is processing.')
                for pt in tqdm(range(len(pts))):
                    mkey = EncodeMorton2D(int(pt[0]), int(pt[1]))
                    head, tail = split_bin(mkey, tail_len)
                    encoded_pts_per_iter.append([head, tail, float(pt[2]), int(pt[3])])
                encoded_pts = encoded_pts + encoded_pts_per_iter

        pc_groups = make_groups(encoded_pts)
        print(len(pc_groups))

        return pc_groups

    def import_to_database(self, db_url):
        #import_data_connection(db_url, self.metas, self.pc_groups)

        # 1. Create a connection to the PostgreSQL database using SQLAlchemy
        engine = create_engine(db_url)
        if not database_exists(engine.url):
            create_database(engine.url)

        Session = sessionmaker(bind=engine)
        session = Session()

        # 2.Create tables in the database
        Base.metadata.create_all(engine)  # , schema='lasdb')

        # 3. Import data to pc_metadata table
        pc_meta_table = Meta(id=self.metas['meta_id'],
                             version=self.metas['version'],
                             source_file=self.metas['source_file'],
                             number_of_points=self.metas['number_of_points'],
                             head_length=self.metas['head_length'],
                             tail_length=self.metas['tail_length'],
                             scales=self.metas['scales'],
                             offsets=self.metas['offsets'],
                             bbox=self.metas['bbox'])
        session.add(pc_meta_table)
        session.commit()
        session.close()

        # 4. Import data to pc_record table
        for pt in self.pc_groups:
            pc_record = PointRecord(meta_id=self.metas['meta_id'], sfc_head=pt[0], sfc_tail=pt[1], z=pt[2], classification=pt[3])
            session = Session()
            session.add(pc_record)
            session.commit()
            session.close()
