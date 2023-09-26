import os
from pathlib import Path
import time
import numpy as np
import pandas as pd
import laspy
from tqdm import tqdm

from pcsfc.encoder import compute_split_length, process_point, make_groups
from db import PgDatabase


def get_file_names(directory_path):
    directory_path = Path(directory_path)
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory.")
        return []

    file_names = [file_path.name for file_path in directory_path.glob('*') if file_path.is_file()]
    return file_names


def multi_importer(args):
    p, r, n = args.p, args.r, args.n
    files = get_file_names(p)
    dbname, user, password = args.db, args.user, args.key
    host, port = 'localhost', 5432

    #db = PgDatabase(dbname, user, password, host, port)
    #db.connect()
    #db.create_table()
    #db.disconnect()
    
    #interation = min(len(files), n)
    intertaion = 133
    for i in range(0, interation):
        f = files[i]
        print(i+20, f)
        importer = PointGroupProcessor(i+20, p, f, r)
        importer.connect_db(dbname, user, password)


def single_importer(args):
    # Load parameters
    path, file, ratio = args.p, args.f, args.r
    dbname, user, password = args.db, args.user, args.key
    host, port = 'localhost', 5432

    # Load metadata; Read, encode and group the points
    importer = PointGroupProcessor(1, path, file, ratio)
    db = PgDatabase(dbname, user, password, host, port)
    db.connect()
    db.create_table()
    db.disconnect()
    importer.connect_db(dbname, user, password)


class PointGroupProcessor:
    def __init__(self, meta_id, path, file, ratio):
        self.input = path + '/' + file

        self.meta = self.get_metadata(meta_id, file, ratio)

        if self.meta[3] <= 50000000:
            encoded_pts = self.process_points(self.meta[5])
        else:
            encoded_pts = self.process_points_chunk(self.meta[5])

        self.write_csv(encoded_pts, meta_id)

    def get_metadata(self, meta_id, file, ratio):
        with laspy.open(self.input) as f:
            head_len, tail_len = compute_split_length(int(f.header.x_max), int(f.header.y_max), ratio)

            meta = (meta_id, float(str(f.header.version)), file, f.header.point_count,
                    head_len, tail_len, [1, 1, 1], [0, 0, 0],
                    [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max,
                     f.header.z_min, f.header.z_max]
                    )

        print('Metadata: ', meta)
        return meta

    def process_points(self, tail_len):
        las = laspy.read(self.input)
        points = np.vstack((las.x, las.y, las.z)).transpose()
        encoded_pts = [process_point(pt, tail_len) for pt in points]
        return encoded_pts

    def process_points_chunk(self, tail_len):
        encoded_pts = []
        with laspy.open(self.input) as f:
            for points in f.chunk_iterator(50000000):
                pts = np.vstack((points.x, points.y, points.z)).transpose()
                encoded_pts = encoded_pts + [process_point(pt, tail_len) for pt in pts]

        return encoded_pts

    def write_csv(self, encoded_pts, meta_id):
        pc_groups = make_groups(encoded_pts)
        print("The number of groups:", len(pc_groups))

        df = pd.DataFrame(pc_groups, columns=['sfc_head','sfc_tail','z'])
        df['sfc_tail'] = df['sfc_tail'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df['z'] = df['z'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df.insert(0, 'meta_id', meta_id)
        df.to_csv("pc_record.csv", index=False, mode='w')

    def connect_db(self, dbname, user, password, host="localhost", port=5432):
        db = PgDatabase(dbname, user, password, host, port)
        db.connect()

        insert_meta_sql = "INSERT INTO pc_metadata_2201m VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s);"
        #db.create_table()
        db.execute_query(insert_meta_sql, self.meta)
        db.execute_copy("pc_record.csv")

        db.disconnect()
