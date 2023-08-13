import numpy as np
import pandas as pd
from pipeline.retriever import bbox_search, circle_search, polygon_search, nn_search, cla_search
import time
import argparse
#from IPython.display import display


def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: bbox
    parser_bbox = subparsers.add_parser("bbox", help="query-with-bounding-box mode help")
    parser_bbox.add_argument("-x", default=[0, 50], help='the boundary of x value of the selected points, e.g. [0, 50]')
    parser_bbox.add_argument("-y", default=[0, 50], help='the boundary of y value of the selected points, e.g. [0, 50]')
    parser_bbox.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_bbox.add_argument("-u", default='postgresql://cynthia: dV337bUP!@pakhuis.tudelft.nl:5432/cynthia', help="database url")
    parser_bbox.set_defaults(func=bbox_search)

    # Mode 2: circle
    parser_circle = subparsers.add_parser("circle", help="query-with-circle mode help")
    parser_circle.add_argument("-p", default=[0,0], help='the minimum x value of the selected points, e.g. [0, 0]')
    parser_circle.add_argument("-r", default=50, help='the maximum x value of the selected points, e.g. 50')
    parser_circle.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_circle.add_argument("-u", default='postgresql://cynthia: dV337bUP!@pakhuis.tudelft.nl:5432/cynthia', help="database url")
    parser_circle.set_defaults(func=circle_search)

    # Mode 3: Polygon
    parser_polygon = subparsers.add_parser("polygon", help="query-with-polygon mode help")
    parser_polygon.add_argument("-p", default=[[0, 0], [50, 0], [0, 50]], help='the set of points of the polygon boundary, e.g. [[0, 0], [50, 0], [0, 50]]')
    parser_polygon.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_polygon.add_argument("-u", default='postgresql://cynthia: dV337bUP!@pakhuis.tudelft.nl:5432/cynthia', help="database url")
    parser_polygon.set_defaults(func=polygon_search)

    # Mode 4: Nearest Neighbour
    parser_nn = subparsers.add_parser("nn", help="nearest-neighbour mode help")
    parser_nn.add_argument("-p", default=[0,0], help="the points that we will search for its neighbour")
    parser_nn.add_argument("-n", default=1, help="the number of selected points")
    parser_nn.add_argument("-t", default=12, help='tail length of the sfc key, e.g. 12')
    parser_nn.add_argument("-u", default='postgresql://cynthia: dV337bUP!@pakhuis.tudelft.nl:5432/cynthia', help="database url")
    parser_nn.set_defaults(func=nn_search)

    # Mode 5: Classification
    parser_cla = subparsers.add_parser("cla", help="query-with-classification-code mode help")
    parser_cla.add_argument("-c", default=0, help="the classification code")
    parser_cla.add_argument("-u", default='postgresql://cynthia: dV337bUP!@cynthia:5432/cynthia', help="database url")
    parser_cla.set_defaults(func=cla_search)

    args = parser.parse_args()
    if hasattr(args, "func"):
        start_time = time.time
        args.func(args)
        run_time = time.time - start_time
        print("The running time:", run_time)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()