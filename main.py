import numpy as np
import pandas as pd
from pcsfc.pipeline import preprocess_las
from db import import_data
import time


def main():
    start_time = time.time()

    # 0. Parameters
    input_path = 'E:\\Geomatics\\GEO2020 Thesis\\code\\data\\'
    input_filename = 'jv_500m.las'
    ratio = 0.6
    engine_key = 'postgresql://postgres:050694@localhost:5432/lasdb3'

    # 1. Preprocess the data
    meta_dict, groups = preprocess_las(input_path, input_filename, ratio)
    print('Data is ready.')
    print(meta_dict)
    print(pd.DataFrame(groups, columns=['sfc_head', 'sfc_tail', 'Z', 'classification']))

    # Connect to the database and commit change
    import_data(engine_key, meta_dict, groups)

    end_time = time.time()
    runtime = end_time - start_time
    print("Runtime:", runtime, "seconds")
    print('Data inserted to the database.')


if __name__ == "__main__":
    main()