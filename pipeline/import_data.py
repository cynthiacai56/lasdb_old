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
        points_per_iter = 50000000
        self.input = path + '/' + file

        self.metas = self.get_metadata(meta_id, file, ratio)
        self.pc_groups = self.process_points(self.metas['tail_length'], points_per_iter)

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

    def process_points(self, tail_len, points_per_iter):
        # Read and encode the data
        with laspy.open(self.input) as f:
            split_keys = []
            if f.header.point_count < points_per_iter:
                las = laspy.read(self.input)
                for i in range(f.header.point_count):
                    mkey = EncodeMorton2D(int(las.x[i]), int(las.y[i]))
                    head, tail = split_bin(mkey, tail_len)
                    split_keys.append([head, tail, float(las.z[i]), int(las.classification[i])])
            else:
                for pts in f.chunk_iterator(points_per_iter):
                    split_keys_per_iter = []
                    i = 0
                    for i in range(len(pts)):
                        i = i+1
                        mkey = EncodeMorton2D(int(pts.x[i]), int(pts.y[i]))
                        head, tail = split_bin(mkey, tail_len)
                        split_keys_per_iter.append([head, tail, float(pts.z[i]), int(pts.classification[i])])
                        print(i)
                    split_keys = split_keys + split_keys_per_iter

        pc_groups = make_groups(split_keys)
        print(len(pc_groups)
              
        '''
        # To print the sfc groups
        pc_groups_df = pd.DataFrame(pc_groups, columns=['sfc_head', 'sfc_tail', 'Z', 'classification'])
        if len(pc_groups) > 100:
            pc_groups_df.to_csv('pc_groups.csv', index=False)
        print(pc_groups_df)
        '''

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
