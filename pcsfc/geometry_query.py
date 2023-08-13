import numpy as np
from scipy.spatial import KDTree
from shapely.geometry import Polygon, Point

'''
Second-filter
There are four geometrical searches:

1. Bounding box
2. Circle
3. Polygon
4. Nearest neighbour
'''

def bbox_filter(x_min, x_max, y_min, y_max, points):
    points_within_bbox = points[
    (points[:, 0] >= x_min) &
    (points[:, 0] <= x_max) &
    (points[:, 1] >= y_min) &
    (points[:, 1] <= y_max)]

    return points_within_bbox


def circle_filter(center, radius, points):
    pts_2d = points[:, :2]
    tree = KDTree(pts_2d)
    indices = tree.query_ball_point(center, radius)
    points_within_circle = [points[i] for i in indices]
    return points_within_circle


def polygon_filter(polygon_vertices, points):
    polygon = Polygon(polygon_vertices)
    points_within_polygon = []

    for point in points:
        x, y = point[0], point[1]
        point_obj = Point(x, y)

        if polygon.contains(point_obj):
            points_within_polygon.append(point)

    return points_within_polygon