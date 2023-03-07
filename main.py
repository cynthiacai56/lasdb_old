import pandas as pd

from pcsfc.pipeline import get_pc_block

from sqlalchemy import create_engine
from model import PointCloudTable
from sqlalchemy.orm import sessionmaker

def main():
    # obtain point cloud blocks
    filepath = "E:\Geomatics\GEO2020 Thesis\code\data\subset_yue.las"
    split_portion = 2
    df_block = get_pc_block(filepath, split_portion)
    # df_block.to_csv('pc_block.csv',index=False)

    # Connect to database
    engine = create_engine('postgresql://postgres:050694@localhost:5432/geo2020')

    # Create the table
    # PointCloudTable.metadata.create_all(engine)

    # Insert data
    df_block.to_sql('point_cloud', engine)


if __name__ == "__main__":
    main()
