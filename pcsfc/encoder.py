import numpy as np
import pandas as pd
import laspy


class PointCloudLoader:
    def __init__(self, filepath):
        # Read data
        las = laspy.read(filepath)

        # Colume names
        cols = []
        for dimension in las.point_format.dimension_names:
            if dimension != 'X' and dimension != 'Y':
                cols.append(dimension)

                # Organizing attributes
        self.coord = np.vstack((las.X, las.Y))

        # Non-organizing attributes
        data = np.vstack((las.Z, las.intensity, las.return_number, las.number_of_returns, las.scan_direction_flag,
                          las.edge_of_flight_line, las.classification, las.synthetic, las.key_point, las.withheld,
                          las.scan_angle_rank, las.user_data, las.point_source_id, las.gps_time)).transpose()
        self.df_attr = pd.DataFrame(data, columns=cols)


class PointCloudEncoder:
    def __init__(self, coord, df_attr, split_portion):
        # Encode SFC keys and split them in half
        self.df_sfc = self.morton_encode(coord, split_portion)
        self.df_points = self.df_sfc.join(df_attr)

        # Group points based on SFC head
        self.df_block = self.make_group()

    def count_bits(self, n, base):
        count = 0
        while (n):
            count += 1
            n >>= base  # right shift operator; it requires a bitwise representation of object as first operand
        return count

    def decimal_to_binary(self, n, mbits):
        numA = bin(n).replace("0b", "")
        nbin = [int(x) for x in str(numA)]
        if len(nbin) < mbits:
            diff = mbits - len(nbin)
            dests = [0] * diff
            return dests + nbin
        else:
            return nbin

    # Key issue: where to split?
    def morton_encode(self, coord, split_portion):
        # Step 1: Determine dimensions and bits
        ndims = len(coord)
        max_coord = []
        for i in range(ndims):
            max_coord.append(max(coord[i]))
        mbits = self.count_bits(max(max_coord), 1)
        # #coord = np.vstack((las.X, las.Y, las.Z)).transpose()
        # df_attri = pd.DataFrame(data = attri.transpose(), columns=list(las.point_format.dimension_names))

        # Step 2: decimal to binary
        df_sfc = pd.DataFrame(columns=['sfc_head', 'sfc_tail'])
        for i in range(len(coord[0])):
            x, y = coord[0][i], coord[1][i]
            x_bin, y_bin = self.decimal_to_binary(x, mbits), self.decimal_to_binary(y, mbits)

            # Step 3: Combine chunks, split to head and tile
            my_head, my_tail = str(), str()
            for j in range(mbits // split_portion):
                my_head = my_head + str(x_bin[j]) + str(y_bin[j])
            for j in range(mbits // split_portion, mbits):
                my_tail = my_tail + str(x_bin[j]) + str(y_bin[j])

            df_sfc.loc[len(df_sfc)] = [my_head, my_tail]

        return df_sfc

    def make_group(self):
        # Group points using GroupBy of DataFrame
        grouped = self.df_points.groupby('sfc_head')

        # Make an empty DataFrame to store
        df_block = pd.DataFrame(columns=list(self.df_points.columns))

        for name, group in grouped:
            group = group.drop(['sfc_head'], axis=1)

            # Extract the attributes of each group
            my_attr = [name]
            for i in range(len(group.columns)):
                my_attr.append(group[group.columns[i]].values.tolist())

            df_block.loc[len(df_block)] = my_attr
        return df_block