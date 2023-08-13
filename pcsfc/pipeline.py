import numpy as np
import pandas as pd
import laspy
from pcsfc.encoder import EncodeMorton2D, split_string, make_groups


def preprocess_las(input_path, input_filename, split_length):
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

    # pc_metadata
    trans = [scale for scale in las.header.scales] + [offset for offset in las.header.offsets]
    meta_dict = {'version': float(str(las.header.version)),
                 'source_file': input_filename,
                 'number_of_points': len(points),
                 'transform': trans,
                 'bbox': [las.header.x_min, las.header.x_max, las.header.y_min, las.header.y_max, las.header.z_min,
                          las.header.z_max]
                 }

    # pc_record
    # Encoded all points and split the morton keys
    encoded_list = []
    for pt in points:
        x, y = int(pt[0]), int(pt[1])
        mkey = EncodeMorton2D(int(x), int(y))
        head, tail = split_string(mkey, ratio = split_length)#split_length=split_length)
        encoded_list.append([head, tail, pt[2], pt[3]])

    #print('Data is encoded.')

    groups = make_groups(encoded_list)
    #print('Data is grouped.')

    return meta_dict, groups