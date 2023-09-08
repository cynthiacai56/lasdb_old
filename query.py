import numpy as np
import pandas as pd
from pipeline.retriever import GeometryFilter
import time
import argparse

def bbox_search(args):
    start_time = time.time()
    constr = [int(args.xmin), int(args.xmax), int(args.ymin), int(args.ymax)] # x_min, x_max, y_min, y_max
    tail_len = int(args.t)
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    bbox_filter = GeometryFilter(GeometryFilter.MODE_BBOX, constr, head_len, tail_len, db_url)
    results = bbox_filter.query()

    print('The results:', results)
    print('The number of results:', len(results))

    end_time = time.time()
    print('The run time:', end_time-start_time)

def circle_search(args):
    start_time = time.time()
    constr = [[int(args.x), int(args.y)], int(args.r)] # [center_pt, radius]
    tail_len = args.t
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    circle_filter = GeometryFilter(GeometryFilter.MODE_CIRCLE, constr, head_len, tail_len, db_url)
    results = circle_filter.query()

    print('The results:', results)
    print('The number of results:', len(results))

    end_time = time.time()
    print('The run time:', end_time-start_time)

def polygon_search(args):
    start_time = time.time()
    tail_len = args.t
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    head_len = 26

    # Split the input string into individual lists using a delimiter (e.g., ';')
    input_string = args.p
    outer_polygon = []
    for inner in input_string.split(';'):
        pt = [int(item) for item in inner.split(',')]
        outer_polygon.append(pt)
        
    if args.hole != 'null':
        input_string = args.p
        hole = []
        for inner in input_string.split(';'):
            pt = [int(item) for item in inner.split(',')]
            hole.append(pt)
    else:
        hole = []
        
    polygon = [outer_polygon, hole]
    print(polygon)
    
    polygon_filter = GeometryFilter(GeometryFilter.MODE_POLYGON, polygon, head_len, tail_len, db_url)
    results = polygon_filter.query()

    print('The results:', results)
    print('The number of results:', len(results))

    end_time = time.time()
    print('The run time:', end_time-start_time)
    
def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: bbox
    parser_bbox = subparsers.add_parser("bbox", help="query-with-bounding-box mode help")
    parser_bbox.add_argument("-xmin", default=85000, type=int, help='the minimum x value of the selected points')
    parser_bbox.add_argument("-xmax", default=86000, type=int, help='the maximum x value of the selected points')
    parser_bbox.add_argument("-ymin", default=446250, type=int, help='the minimum y value of the selected points')
    parser_bbox.add_argument("-ymax", default=447000, type=int, help='the maximum y value of the selected points')
    parser_bbox.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_bbox.add_argument("-user", default='cynthia', help='database username')
    parser_bbox.add_argument("-key", default='123456', help='database password')
    parser_bbox.add_argument("-host", default='localhost', help='database host')
    parser_bbox.add_argument('-db', default='cynthia', help='database name')
    parser_bbox.set_defaults(func=bbox_search)

    # Mode 2: circle
    parser_circle = subparsers.add_parser("circle", help="query-with-circle mode help")
    parser_circle.add_argument("-x", default=85500, type=int, help='the x coordinate of the center point')
    parser_circle.add_argument("-y", default=445700, type=int, help='the y coordinate of the center point')
    parser_circle.add_argument("-r", default=50, type=int, help='the radius of the circle, e.g. 50')
    parser_circle.add_argument("-t", default=12, type=int, help='tail length of the sfc key, e.g. 12')
    parser_circle.add_argument("-user", default='cynthia', help='database username')
    parser_circle.add_argument("-key", default='123456', help='database password')
    parser_circle.add_argument("-host", default='localhost', help='database host')
    parser_circle.add_argument('-db', default='cynthia', help='database name')
    parser_circle.set_defaults(func=circle_search)

    # Mode 3: Polygon
    parser_polygon = subparsers.add_parser("polygon", help="query-with-polygon mode help")
    parser_polygon.add_argument("-p", default=[[0, 0], [50, 0], [0, 50]], help='the set of points of the polygon boundary, e.g. [[0, 0], [50, 0], [0, 50]]')
    parser_polygon.add_argument("-hole", default='null', help='the vertices of holes in the polygon'
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
        run_time = time.time() - start_time
        print("The total running time:", run_time)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
