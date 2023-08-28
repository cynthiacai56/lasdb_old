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
    username, password, hostname, dbname = args.user, args.key, args.host, args.db

    files = get_file_names_in_directory(p)
    db_url = 'postgresql://'+ username + ':' + password + '@'+ hostname + '/' + dbname
    # database url: dialect+driver://username:password@host:port/database
    # example: 'postgresql://cynthia:123456@localhost:5432/lasdb'

    iteration = min(len(files), n)
    for i in range(iteration):
        f = files[i]
        one_file_importer(p, f, r, db_url)


def single_importer(args):
    username, password, hostname, dbname = args.user, args.key, args.host, args.db
    db_url = 'postgresql://' + username + ':' + password + '@' + hostname + '/' + dbname
    one_file_importer(1, args.p, args.f, args.r, db_url)


def one_file_importer(meta_id, input_path, input_filename, ratio, engine_key):
    # 1 Preprocess the data
    start_time = time.time

    input = input_path + input_filename
    with laspy.open(input) as f:
        # 1.1 pc_metadata
        meta_dict = {'meta_id': meta_id,
                     'version': float(str(f.header.version)),
                     'source_file': input_filename,
                     'number_of_points': f.header.point_count,
                     'head_length': 0,
                     'tail_length': 0,
                     'scales': [scale for scale in f.header.scales],
                     'offsets': [offset for offset in f.header.offsets],
                     'bbox': [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max,
                              f.header.z_min, f.header.z_max]
                     }
        print(meta_dict)

        # 1.2 pc_record
        # Determine tail length
        head_length, tail_length = compute_split_length(int(f.header.x_max), int(f.header.y_max), ratio)
        meta_dict['head_length'] = head_length
        meta_dict['tail_length'] = tail_length


        points_per_iter = 50000000
        split_keys = []

        if f.header.point_count < points_per_iter:
            las = laspy.read(input)
            for i in range(len(las.x)):
                mkey = EncodeMorton2D(int(las.x[i]), int(las.y[i]))
                head, tail = split_bin(mkey, tail_length)
                split_keys.append([head, tail, las.z[i], las.classification[i]])
        else:
            for pts in f.chunk_iterator(points_per_iter):
                split_keys_per_iter = []
                for i in tqdm(range(points_per_iter)):
                    mkey = EncodeMorton2D(int(pts.x[i]), int(pts.y[i]))
                    head, tail = split_bin(mkey, tail_length)
                    split_keys_per_iter.append([head, tail, pts.z[i], pts.classification[i]])

                split_keys = split_keys + split_keys_per_iter
                print('.')

    encode_time = time.time()

    ### Improvement: Use the 'Group By' function in PostgreSQL
    pc_record = make_groups(split_keys)

    pc_record_df = pd.DataFrame(pc_record, columns=['sfc_head', 'sfc_tail', 'Z', 'classification'])

    print(pc_record_df)
    encode_time = time.time
    print('encoding time: ', encode_time - start_time)

    # 2 Connect to the database and commit change
    import_data_connection(engine_key, meta_dict, pc_record)
    import_time = time.time
    print('importing time: ', import_time-encode_time)
