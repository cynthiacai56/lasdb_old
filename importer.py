import os
from pathlib import Path
import time
import argparse
from pipeline.import_data import PointGroupProcessor


def get_file_names_in_directory(directory_path):
    directory_path = Path(directory_path)
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory.")
        return []

    file_names = [file_path.name for file_path in directory_path.glob('*') if file_path.is_file()]
    return file_names


def multi_importer(args):
    p, r, n = args.p, args.r, args.n
    files = get_file_names_in_directory(p)
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db
    # database url: dialect+driver://username:password@host:port/database
    # example: 'postgresql://cynthia:123456@localhost:5432/lasdb'
    points_per_iter = 50000000

    for i in range(min(len(files), n)):
        f = files[i]
        importer = PointGroupProcessor(i, p, f, r)
        importer.import_to_database(db_url)


def single_importer(args):
    # Load parameters
    start_time = time.time()
    path, file, ratio = args.p, args.f, args.r
    db_url = 'postgresql://' + args.user + ':' + args.key + '@' + args.host + '/' + args.db

    # Load metadata; Read, encode and group the points
    importer = PointGroupProcessor(1, path, file, ratio)
    encode_time = time.time()

    # Import the point groups into the database
    importer.import_to_database(db_url)
    import_time = time.time()

    # Print run times
    print('Encoding time: ', encode_time - start_time)
    print('Importing time: ', import_time - encode_time)
    print('Total time:', import_time - start_time)


def main():
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: single-file
    parser_single = subparsers.add_parser("single", help="single-file mode help")
    parser_single.add_argument("-p", default="/work/tmp/cynthia", help="input path")
    parser_single.add_argument("-f", default="C_68BN2.LAZ",  help="input file")
    parser_single.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_single.add_argument("-user", default='cynthia', help='database username')
    parser_single.add_argument("-key", default='123456', help='database password')
    parser_single.add_argument("-host", default='localhost', help='database host')
    parser_single.add_argument('-db', default='cynthia', help='database name')
    #parser_single.add_argument("-s", default='lasdb',help="schema")
    parser_single.set_defaults(func=single_importer)

    # Mode 2: multiple-file (a directory)
    parser_multi = subparsers.add_parser("multiple", help="single-file mode help")
    parser_multi.add_argument("-p", default="/work/tmp/cynthia", help="input path")
    parser_multi.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_multi.add_argument("-user", default='cynthia', help='database username')
    parser_multi.add_argument("-key", default='123456', help='database password')
    parser_multi.add_argument("-host", default='localhost', help='database host')
    parser_multi.add_argument('-db', default='cynthia', help='database name')
    parser_multi.add_argument("-n", default=10, help="the number of imported files")
    parser_multi.set_defaults(func=multi_importer)

    args = parser.parse_args()
    if hasattr(args, "func"):
        start_time = time.time()
        args.func(args)
        run_time = time.time() - start_time
        print("The total running time:", run_time)
    else:
        parser.print_help()


def test():
    input_path = 'E:\\Geomatics\\GEO2020 Thesis\\data'
    input_filename = 'C_37EN1_500m.las'
    ratio = 0.7
    meta_id = 1
    points_per_iter = 50000000

    db_url = 'postgresql://postgres:050694@localhost:5432/lasdb_500_70'

    #start_time = time.time()
    importer = PointGroupProcessor(meta_id, input_path, input_filename, ratio)
    importer.import_to_database(db_url)
    #end_time = time.time()
    #print('Run time:', end_time-start_time)

if __name__ == "__main__":
    main()