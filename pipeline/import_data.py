import os
from pathlib import Path

import numpy as np
import pandas as pd
import laspy

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
    p, r, u, n = args.p, args.r, args.u, args.n
    files = get_file_names_in_directory(p)

    iteration = min(len(files), n)
    for i in range(iteration):
        f = files[i]
        one_file_importer(p, f, r, u)


def single_importer(args):
    one_file_importer(1, args.p, args.f, args.r, args.u)


def one_file_importer(meta_id, input_path, input_filename, ratio, engine_key):
    # 1 Preprocess the data
    las = laspy.read(input_path + input_filename)

    # 1.1 pc_metadata
    trans = [scale for scale in las.header.scales] + [offset for offset in las.header.offsets]
    meta_dict = {'meta_id': meta_id,
                 'version': float(str(las.header.version)),
                 'source_file': input_filename,
                 'number_of_points': las.header.point_count,
                 'head_length': 0,
                 'tail_length': 0,
                 'transform': trans,
                 'bbox': [las.header.x_min, las.header.x_max, las.header.y_min, las.header.y_max, las.header.z_min,
                          las.header.z_max]
                 }

    # 1.2 pc_record
    # Determine tail length
    mkey_max = EncodeMorton2D(int(las.header.x_max), int(las.header.y_max))
    tail_length = compute_split_length(mkey_max, ratio)
    meta_dict['tail_length'] = tail_length

    start_time = time.time
    try:
        points = np.vstack((las.x, las.y, las.z, las.classification)).transpose()
        split_keys = []
        for pt in points:
            mkey = EncodeMorton2D(int(pt[0]), int(pt[1]))
            head, tail = split_bin(mkey, tail_length)
            split_keys.append([head, tail, pt[2], pt[3]])

    except Exception as e:
        points_per_iter = 10000000
        split_keys = []
        with laspy.open(input_path + input_filename) as file:
            for points in file.chunk_iterator(points_per_iter):
                # Load the coordinates and the attributes
                x, y, z, classification = points.x, points.y, points.z, points.classification
                one_pts = np.vstack((x, y, z, classification)).transpose()

                # Encode the points and split the sfc keys
                one_split_keys = []
                for pt in one_pts:
                    mkey = EncodeMorton2D(int(pt[0]), int(pt[1]))
                    head, tail = split_bin(mkey, tail_length)
                    one_split_keys.append([head, tail, pt[2], pt[3]])

                split_keys = split_keys + one_split_keys

    encode_time = time.time()

    ### Improvement: Use the 'Group By' function in PostgreSQL
    pc_record = make_groups(split_keys)

    pc_record_df = pd.DataFrame(pc_record, columns=['sfc_head', 'sfc_tail', 'Z', 'classification'])

    meta_dict['head_length'] = len(bin(pc_record_df['sfc_head'].max()))-2

    print(pc_record_df)
    encode_time = time.time

    # 2 Connect to the database and commit change
    import_data_connection(engine_key, meta_dict, pc_record)
    import_time = time.time
    print('encoding time: ', encode_time-start_time)
    print('importing time: ', import_time-encode_time)