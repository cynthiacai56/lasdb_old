import numpy as np
import pandas as pd
from pcsfc.encoder import PointCloudLoader, PointCloudEncoder

def get_pc_block(filepath, split_portion):
    # Load data
    pc_lo = PointCloudLoader(filepath)

    # Encode and group data
    pc_en = PointCloudEncoder(pc_lo.coord, pc_lo.df_attr, split_portion)

    return pc_en.df_block