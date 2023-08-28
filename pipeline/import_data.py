import os
from pathlib import Path

import numpy as np
import pandas as pd
import laspy

from tqdm import tqdm

from db.import_data import import_data_connection
from pcsfc.encoder import EncodeMorton2D, compute_split_length, split_bin, make_groups

import time

def get_file_names_in_directory(directory_path):
    directory_path = Path(directory_path)
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory.")
        return []

    file_names = [file_path.name for file_path in directory_path.glob('*') if file_path.is_file()]
    return file_names


def multi_importer(args):
    p, r, n = args.p, args.r, args.n
    files = get_file_names_in_directory(p)

    user, password, host, db = args.user, args.key, args.host, args.db
    db_url = 'postgresql://' + user + ':' + password + '@'+ host + '/' + db
    # database url: dialect+driver://username:password@host:port/database
    # example: 'postgresql://cynthia:123456@localhost:5432/lasdb'
    points_per_iter = 50000000

    for i in range(min(len(files), n)):
        f = files[i]
        importer = PointGroupProcessor(i, p, f, r)
        importer.import_to_database(db_url)


def single_importer(args):
    # Load parameters
    path, file, ratio = args.p, args.f, args.r
    user, password, host, db = args.user, args.key, args.host, args.db
    db_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + db

    # Load metadata; Read, encode and group the points
    start_time = time.time()
    importer = PointGroupProcessor(1, path, file, ratio)
    encode_time = time.time()

    # Import the point groups into the database
    importer.import_to_database(db_url)
    import_time = time.time()

    # Print run times
    print('Encoding time: ', encode_time - start_time)
    print('Importing time: ', import_time - encode_time)
    print('Total time:', import_time - start_time)

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

        return meta_dict

    def process_points(self, tail_len, points_per_iter):
        # Read and encode the data
        with laspy.open(self.input) as f:
            split_keys = []
            if f.header.point_count < points_per_iter:
                las = laspy.read(self.input)
                for i in tqdm(range(f.header.point_count)):
                    mkey = EncodeMorton2D(int(las.x[i]), int(las.y[i]))
                    head, tail = split_bin(mkey, tail_len)
                    split_keys.append([head, tail, float(las.z[i]), int(las.classification[i])])
            else:
                for pts in f.chunk_iterator(points_per_iter):
                    split_keys_per_iter = []
                    for i in tqdm(range(points_per_iter)):
                        mkey = EncodeMorton2D(int(pts.x[i]), int(pts.y[i]))
                        head, tail = split_bin(mkey, tail_len)
                        split_keys_per_iter.append([head, tail, float(pts.z[i]), int(pts.classification[i])])

                    split_keys = split_keys + split_keys_per_iter

        pc_groups = make_groups(split_keys)

        # To print the sfc groups
        pc_groups_df = pd.DataFrame(pc_groups, columns=['sfc_head', 'sfc_tail', 'Z', 'classification'])
        print(pc_groups_df)

        return pc_groups

    def import_to_database(self, db_url):
        import_data_connection(db_url, self.metas, self.pc_groups)

