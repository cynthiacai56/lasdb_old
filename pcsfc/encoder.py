import numpy as np
import pandas as pd
import laspy
from itertools import groupby


def Expand2D(n):
    """
    Encodes the 64 bit morton code for a 31 bit number in the 2D space using
    a divide and conquer approach for separating the bits.
    1 bit is not used because the integers are not unsigned

    Args:
        n (int): a 2D dimension

    Returns:
        int: 64 bit morton code in 2D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
    """
    if n < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")

    b = n & 0x7fffffff
    b = (b ^ (b << 16)) & 0x0000ffff0000ffff
    b = (b ^ (b << 8)) & 0x00ff00ff00ff00ff
    b = (b ^ (b << 4)) & 0x0f0f0f0f0f0f0f0f
    b = (b ^ (b << 2)) & 0x3333333333333333
    b = (b ^ (b << 1)) & 0x5555555555555555
    return b


def EncodeMorton2D(x, y):
    """
    Calculates the 2D morton code from the x, y dimensions

    Args:
        x (int): the x dimension
        y (int): the y dimension

    Returns:
        int: 64 bit morton code in 2D

    """
    return bin(Expand2D(x) + (Expand2D(y) << 1))


def split_string(my_string, ratio=0.5):
    my_string = str(my_string)
    if my_string [:2] =='0b':
        my_string = my_string[2:len(my_string)]
    length = len(my_string)
    split_index = int(length * ratio)
    head_str, tail_str = my_string[:split_index], my_string[split_index:]
    head_bin = bytes(int(head_str[i:i+8], 2) for i in range(0, len(head_str), 8))
    tail_bin = bytes(int(tail_str[i:i+8], 2) for i in range(0, len(tail_str), 8))

    return head_bin, tail_bin


def make_groups(lst):
    # group the list by the first element of each sublist
    sorted_list = sorted(lst, key=lambda x: x[0])
    groups = groupby(sorted_list, lambda x: x[0])

    # print the groups
    zz = []
    for key, group in groups:
        group = list(group)

        yy = [key]
        for j in range(1, len(group[0])):
            xx = [group[i][j] for i in range(len(group))]
            yy.append(xx)
        zz.append(yy)

    return zz