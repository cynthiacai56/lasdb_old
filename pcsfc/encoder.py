import numpy as np
from itertools import groupby

###############################################################################
######################      Morton conversion in 2D      ######################
###############################################################################

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
    return Expand2D(x) + (Expand2D(y) << 1)


###############################################################################
######################      Morton conversion in 3D      ######################
######################       21 bits per dimension       ######################
###############################################################################


def Expand3D_21bit(x):
    """
    Encodes the 64 bit morton code for a 21 bit number in the 3D space using
    a divide and conquer approach for separating the bits.

    Args:
        x (int): the requested 3D dimension

    Returns:
        int: 64 bit morton code in 3D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers

    """

    if x < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    x = (x ^ (x << 32)) & 0x7fff00000000ffff
    x = (x ^ (x << 16)) & 0x00ff0000ff0000ff
    x = (x ^ (x << 8)) & 0x700f00f00f00f00f
    x = (x ^ (x << 4)) & 0x30c30c30c30c30c3
    x = (x ^ (x << 2)) & 0x1249249249249249
    return x


def Compact3D_21bit(x):
    """
    Decodes the 64 bit morton code into a 21 bit number in the 3D space  using
    a divide and conquer approach for separating the bits.

    Args:
        x (int): a 64 bit morton code

    Returns:
        int: a dimension in 3D space

    Raises:
        Exception: ERROR: Morton code is always positive
    """

    if x < 0:
        raise Exception("""ERROR: Morton code is always positive""")
    x &= 0x1249249249249249
    x = (x ^ (x >> 2)) & 0x30c30c30c30c30c3
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f
    x = (x ^ (x >> 8)) & 0x00ff0000ff0000ff
    x = (x ^ (x >> 16)) & 0x7fff00000000ffff
    x = (x ^ (x >> 32)) & 0x00000000ffffffff
    return x


def EncodeMorton3D_21bit(x, y, z):
    """
    Calculates the 3D morton code from the x, y, z dimensions

    Args:
        x (int): the x dimension
        y (int): the y dimension
        z (int): the z dimension

    Returns:
        int: 64 bit morton code in 3D

    """
    return Expand3D_21bit(x) + (Expand3D_21bit(y) << 1) + (Expand3D_21bit(z) << 2)


def DecodeMorton3DX_21bit(mortonCode):
    """
    Calculates the x coordinate from a 64 bit morton code

    Args:
        mortonCode (int): the 64 bit morton code

    Returns:
        int: 21 bit x coordinate in 3D

    """
    return Compact3D_21bit(mortonCode)


def DecodeMorton3DY_21bit(mortonCode):
    """
    Calculates the y coordinate from a 64 bit morton code

    Args:
        mortonCode (int): the 64 bit morton code

    Returns:
        int: 21 bit y coordinate in 3D

    """
    return Compact3D_21bit(mortonCode >> 1)


def DecodeMorton3DZ_21bit(mortonCode):
    """
    Calculates the z coordinate from a 64 bit morton code

    Args:
        mortonCode (int): the 64 bit morton code

    Returns:
        int: 21 bit z coordinate in 3D

    """
    return Compact3D_21bit(mortonCode >> 2)


###############################################################################
######################      Morton conversion in 3D      ######################
######################       31 bits per dimension       ######################
###############################################################################

'''
def Expand3D(x):
    """
    Encodes the 93 bit morton code for a 31 bit number in the 3D space using
    a divide and conquer approach for separating the bits.


    Args:
        x (int): the requested 3D dimension

    Returns:
        int: 93 bit morton code in 3D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers

    """

    if x < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    x &= 0x7fffffffL
    x = (x ^ x << 32) & 0x7fff00000000ffffL
    x = (x ^ x << 16) & 0x7f0000ff0000ff0000ffL
    x = (x ^ x << 8) & 0x700f00f00f00f00f00f00fL
    x = (x ^ x << 4) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ x << 2) & 0x49249249249249249249249L
    return x


def EncodeMorton3D(x, y, z):
    """
    Calculates the 3D morton code from the x, y, z dimensions

    Args:
        x (int): the x dimension of 31 bits
        y (int): the y dimension of 31 bits
        z (int): the z dimension of 31 bits

    Returns:
        int: 93 bit morton code in 3D

    """
    return Expand3D(x) + (Expand3D(y) << 1) + (Expand3D(z) << 2)


def Compact3D(x):
    """
    Decodes the 93 bit morton code into a 31 bit number in the 3D space using
    a divide and conquer approach for separating the bits.

    Args:
        x (int): a 93 bit morton code

    Returns:
        int: a dimension in 3D space

    Raises:
        Exception: ERROR: Morton code is always positive
    """

    if x < 0:
        raise Exception("""ERROR: Morton code is always positive""")

    x &= 0x49249249249249249249249L
    x = (x ^ (x >> 2)) & 0x430c30c30c30c30c30c30c3L
    x = (x ^ (x >> 4)) & 0x700f00f00f00f00f00f00fL
    x = (x ^ (x >> 8)) & 0x7f0000ff0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x7fff00000000ffffL
    x = (x ^ (x >> 32)) & 0x7fffffffL
    return x


def DecodeMorton3DX(mortonCode):
    """
    Calculates the x coordinate from a 93 bit morton code

    Args:
        mortonCode (int): the 93 bit morton code

    Returns:
        int: 31 bit x coordinate in 3D

    """
    return Compact3D(mortonCode)


def DecodeMorton3DY(mortonCode):
    """
    Calculates the y coordinate from a 93 bit morton code

    Args:
        mortonCode (int): the 93 bit morton code

    Returns:
        int: 31 bit y coordinate in 3D

    """
    return Compact3D(mortonCode >> 1)


def DecodeMorton3DZ(mortonCode):
    """
    Calculates the z coordinate from a 93 bit morton code

    Args:
        mortonCode (int): the 93 bit morton code

    Returns:
        int: 31 bit z coordinate in 3D

    """
    return Compact3D(mortonCode >> 2)
'''

###############################################################################
######################         Morton conversion         ######################
######################            Small tools            ######################
###############################################################################


def compute_split_length(mkey, ratio):
    length = len(bin(mkey)) - 2
    head_length = int(length * ratio)
    if head_length % 2 != 0:
        split_index = head_length - 1
    tail_length = length - head_length
    return tail_length


def split_bin(num, tail_length):
    head = num >> tail_length
    tail = num - (head << tail_length)

    return head, tail


def split_string(my_string, tail_length):
    if my_string[:2] =='0b':
        my_string = my_string[2:len(my_string)] # remove 0b

    split_index = len(my_string) - tail_length

    head_str = my_string[:split_index]
    tail_str = my_string[split_index:]
    head_decimal = int(head_str, 2)
    tail_decimal = int(tail_str, 2)

    return head_decimal, tail_decimal


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