from pcsfc.decoder import DecodeMorton2D
#from lasdb.model.storage import TempRange, metadata, temp_point_table


def morton_range(x_min, y_min, x_max, y_max, head_length, tail_length):
    # Initialize
    nbits = head_length + tail_length
    base_units = [0, 1, 2, 3]  # Each slice has 4 sub-slice
    fronts = [base_unit << (nbits - 2) for base_unit in base_units]
    # for front in fronts:
    # print(bin(front))
    ranges = []

    # Iterate through all possible Morton code slices, moving two bits at a time
    for i in range(2, head_length, 2):  # i is the length of the head_sfc
        full_one_end = (1 << (nbits - i)) - 1  # nbits个1

        overlaps = []
        for slice_min in fronts:
            slice_max = slice_min + full_one_end

            xs_min, ys_min = DecodeMorton2D(slice_min)
            xs_max, ys_max = DecodeMorton2D(slice_max)
            # print('           x:', xs_min, xs_max, 'y:', ys_min, ys_max)

            # Fully containment
            if xs_min >= x_min and xs_max <= x_max and ys_min >= y_min and ys_max <= y_max:
                ranges.append((slice_min, slice_max))
                print(slice_min, slice_max, 'fully contained')
            # No containment
            elif xs_max < x_min or xs_min > x_max or ys_max < y_min or ys_min > y_max:
                print(slice_min, slice_max, 'no containment')
                pass
            # Overlap
            else:
                print(slice_min, slice_max, 'overlaps')
                new_units = [unit << (nbits - i - 2) for unit in base_units]
                for new_unit in new_units:
                    new_slice_min = slice_min | new_unit
                    overlaps.append(new_slice_min)

        fronts = overlaps
        if len(fronts) == 0:
            break

    overlaps_shift = [key >> tail_length for key in overlaps]

    return ranges, overlaps

