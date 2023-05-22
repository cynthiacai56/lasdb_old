import numpy as np
import pandas as pd
import laspy
from pcsfc.encoder import EncodeMorton2D, split_string, make_groups


def preprocess_las(input_path, input_filename, ratio):
    las = laspy.read(input_path + input_filename)
    points = np.vstack((las.x, las.y, las.z, las.classification)).transpose()

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
        mkey = EncodeMorton2D(x, y)
        head, tail = split_string(mkey, ratio=0.5)
        encoded_list.append([head, tail, pt[2], pt[3]])

    print('Data is encoded.')

    groups = make_groups(encoded_list)
    print('Data is grouped.')

    return meta_dict, groups