import os
from pathlib import Path

import numpy as np
import pandas as pd
import laspy

from db.import_data import import_data_connection
from pcsfc.encoder import EncodeMorton2D, compute_split_length, split_string, make_groups


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

    try:
        points = np.vstack((las.x, las.y, las.z, las.classification)).transpose()
    except Exception as e:
        points_per_iter = 10000000
        chunks = []
        with laspy.open(input_path + input_filename) as file:
            for points in file.chunk_iterator(points_per_iter):
                x, y, z, classification = points.x.copy(), points.y.copy(), points.z.copy(), points.classification.copy()
                one_chunk = np.vstack((x, y, z, classification)).transpose()
                chunks = chunks + one_chunk
        points = np.array(chunks)
        print(len(points), len(points[0]))

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
    # Encoded all points and split the morton keys
    mkey_list = [EncodeMorton2D(int(pt[0]), int(pt[1])) for pt in points]
    mkey_max = max(mkey_list)
    tail_length = compute_split_length(mkey_max, ratio)

    encoded_list = []
    for i in range(len(points)):
        head, tail = split_string(mkey_list[i], tail_length)
        encoded_list.append([head, tail, points[i][2], points[i][3]])


    pc_record = make_groups(encoded_list)
    # print('Data is encoded.')

    pc_record_df = pd.DataFrame(pc_record, columns=['sfc_head', 'sfc_tail', 'Z', 'classification'])

    meta_dict['tail_length'] = tail_length
    meta_dict['head_length'] = len(bin(pc_record_df['sfc_head'].max()))-2

    print(len(bin(pc_record[0][0])), len(bin(pc_record[0][1][0])))
    print(pc_record_df)


    # 2 Connect to the database and commit change
    import_data_connection(engine_key, meta_dict, pc_record)
    #print('Data inserted to the database.')