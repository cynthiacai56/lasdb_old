import numpy as np
import pandas as pd
from pipeline.retriever import GeometryFilter
import time
import argparse

def bbox_search(args):
    start_time = time.time()
    constr = [args.x[0], args.x[1], args.y[0], args.y[1]] # x_min, x_max, y_min, y_max
    tail_len = args.t
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    bbox_filter = GeometryFilter(GeometryFilter.MODE_BBOX, constr, head_len, tail_len, db_url)
    results = bbox_filter.query()

    print(len(results))
    print(results)

    end_time = time.time()
    print('The run time:', end_time-start_time)

def circle_search(args):
    start_time = time.time()
    constr = [args.p, args.r] # [center_pt, radius]
    tail_len = args.t
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    circle_filter = GeometryFilter(GeometryFilter.MODE_CIRCLE, constr, head_len, tail_len, db_url)
    results = circle_filter.query()

    print(len(results))
    print(results)

    end_time = time.time()
    print('The run time:', end_time-start_time)

def polygon_search(args):
    start_time = time.time()
    constr = args.p  # a set of points
    tail_len = args.t
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    polygon_filter = GeometryFilter(GeometryFilter.MODE_POLYGON, constr, head_len, tail_len, db_url)
    results = polygon_filter.query()

    print(len(results))
    print(results)

    end_time = time.time()
    print('The run time:', end_time-start_time)

def test():
    start_time = time.time()

    tail_len = 12
    db_url = 'postgresql://postgres:050694@localhost:5432/lasdb_500m_70'
    head_len = 26

    '''
    spatial_extent = [80000,80500,443750,444250]
    constr_bbox = [80010, 80310, 443760, 443960]
    geofilter = GeometryFilter(GeometryFilter.MODE_BBOX, constr_bbox, head_len, tail_len, db_url)

    constr_circle = [[80250, 444000], 150]
    geofilter = GeometryFilter(GeometryFilter.MODE_CIRCLE, constr_circle, head_len, tail_len, db_url)
    '''

    constr_polygon = [[80100, 444000], [80250, 444500], [80300, 444000], [80250, 44350]]
    geofilter = GeometryFilter(GeometryFilter.MODE_POLYGON, constr_polygon, head_len, tail_len, db_url)
    results = geofilter.query()
    print(len(results))

    for i in range(10):
        print(results[i])

    end_time = time.time()
    print('The run time:', end_time-start_time)


def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: bbox
    parser_bbox = subparsers.add_parser("bbox", help="query-with-bounding-box mode help")
    parser_bbox.add_argument("-x", default=[0, 50], help='the boundary of x value of the selected points, e.g. [0, 50]')
    parser_bbox.add_argument("-y", default=[0, 50], help='the boundary of y value of the selected points, e.g. [0, 50]')
    parser_bbox.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_bbox.add_argument("-user", default='cynthia', help='database username')
    parser_bbox.add_argument("-key", default='123456', help='database password')
    parser_bbox.add_argument("-host", default='localhost', help='database host')
    parser_bbox.add_argument('-db', default='cynthia', help='database name')
    parser_bbox.set_defaults(func=bbox_search)

    # Mode 2: circle
    parser_circle = subparsers.add_parser("circle", help="query-with-circle mode help")
    parser_circle.add_argument("-p", default=[0, 0], help='the minimum x value of the selected points, e.g. [0, 0]')
    parser_circle.add_argument("-r", default=50, help='the maximum x value of the selected points, e.g. 50')
    parser_circle.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_circle.add_argument("-user", default='cynthia', help='database username')
    parser_circle.add_argument("-key", default='123456', help='database password')
    parser_circle.add_argument("-host", default='localhost', help='database host')
    parser_circle.add_argument('-db', default='cynthia', help='database name')
    parser_circle.set_defaults(func=circle_search)

    # Mode 3: Polygon
    parser_polygon = subparsers.add_parser("polygon", help="query-with-polygon mode help")
    parser_polygon.add_argument("-p", default=[[0, 0], [50, 0], [0, 50]], help='the set of points of the polygon boundary, e.g. [[0, 0], [50, 0], [0, 50]]')
    parser_polygon.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_polygon.add_argument("-user", default='cynthia', help='database username')
    parser_polygon.add_argument("-key", default='123456', help='database password')
    parser_polygon.add_argument("-host", default='localhost', help='database host')
    parser_polygon.add_argument('-db', default='cynthia', help='database name')
    parser_polygon.set_defaults(func=polygon_search)

    '''
    # Mode 4: Nearest Neighbour
    parser_nn = subparsers.add_parser("nn", help="nearest-neighbour mode help")
    parser_nn.add_argument("-p", default=[0,0], help="the points that we will search for its neighbour")
    parser_nn.add_argument("-n", default=1, help="the number of selected points")
    parser_nn.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_nn.add_argument("-user", default='cynthia', help='database username')
    parser_nn.add_argument("-key", default='123456', help='database password')
    parser_nn.add_argument("-host", default='localhost', help='database host')
    parser_nn.add_argument('-db', default='cynthia', help='database name')
    parser_nn.set_defaults(func=nn_search)
    '''

    args = parser.parse_args()
    if hasattr(args, "func"):
        start_time = time.time()
        args.func(args)
        run_time = time.time() - start_time()
        print("The running time:", run_time)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()