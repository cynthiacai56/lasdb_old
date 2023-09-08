import numpy as np
from scipy.spatial import KDTree
from shapely.geometry import Polygon, Point

def bbox_filter(points, x_min, x_max, y_min, y_max):
    points_within_bbox = points[
    (points[:, 0] >= x_min) &
    (points[:, 0] <= x_max) &
    (points[:, 1] >= y_min) &
    (points[:, 1] <= y_max)]

    return points_within_bbox


def circle_filter(points, center, radius):
    pts_2d = np.array(points)[:, :2]
    tree = KDTree(pts_2d)
    indices = tree.query_ball_point(center, radius)
    points_within_circle = [points[i] for i in indices]
    return points_within_circle


def polygon_filter(points, polygon_vertices, hole_vertices=[]):
    polygon = Polygon(polygon_vertices)
    if len(hole_vertices) >= 3:
        hole_polygon = Polygon(hole_vertices)
        polygon = polygon.difference(hole_polygon)
        
    points_within_polygon = []
    for pt in points:
        point_obj = Point(pt[0], pt[1])
        if polygon.contains(point_obj):
            points_within_polygon.append(pt)

    return points_within_polygon
