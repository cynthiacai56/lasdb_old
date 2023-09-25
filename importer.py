import argparse
import time
from pipeline.import_data import single_importer, multi_importer
from pipeline.import_data import PointGroupProcessor


def main():
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: single-file
    parser_single = subparsers.add_parser("single", help="single-file mode help")
    parser_single.add_argument("-p", default="/work/tmp/cynthia/bench_000210m", help="input path")
    parser_single.add_argument("-f", default="ahn_bench00210.las",  help="input file")
    parser_single.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_single.add_argument("-user", default='cynthia', help='database username')
    parser_single.add_argument("-key", default='123456', help='database password')
    parser_single.add_argument("-host", default='localhost', help='database host')
    parser_single.add_argument('-db', default='cynthia', help='database name')
    parser_single.set_defaults(func=single_importer)

    # Mode 2: multiple-file (a directory)
    parser_multi = subparsers.add_parser("multiple", help="single-file mode help")
    parser_multi.add_argument("-p", default="/work/tmp/cynthia/bench_002201m", help="input path")
    parser_multi.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_multi.add_argument("-user", default='cynthia', help='database username')
    parser_multi.add_argument("-key", default='123456', help='database password')
    parser_multi.add_argument("-host", default='localhost', help='database host')
    parser_multi.add_argument('-db', default='cynthia', help='database name')
    parser_multi.add_argument("-n", default=20, help="the number of imported files")
    parser_multi.set_defaults(func=multi_importer)

    args = parser.parse_args()
    if hasattr(args, "func"):
        start_time = time.time()
        args.func(args)
        run_time = time.time() - start_time
        print("The total running time:", run_time)
        '''
        runtime_formatted = time.strftime("%H:%M:%S", time.gmtime(runtime_seconds))
        csv_filename = args.f + "runtime_log.csv"
        with open(csv_filename, mode="w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([args.f])
            csv_writer.writerow([runtime_formatted])
        '''
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
