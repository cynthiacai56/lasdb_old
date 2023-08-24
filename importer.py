import time
import argparse
from pipeline.import_data import single_importer, multi_importer, one_file_importer


def main():
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: single-file
    parser_single = subparsers.add_parser("single", help="single-file mode help")
    parser_single.add_argument("-p", default="/work/tmp/cynthia/", help="input path")
    parser_single.add_argument("-f", default="C_68BN2.LAZ",  help="input file")
    parser_single.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_single.add_argument("-u", default='postgresql://cynthia:dV337bUP!@localhost/cynthia', help="database url")
    # database url: dialect+driver://username:password@host:port/database
    #parser_single.add_argument("-k", default='postgresql://postgres:050694@localhost:5432/lasdb', help="the key to creat engine")
    #parser_single.add_argument("-s", default='lasdb',help="schema")
    parser_single.set_defaults(func=single_importer)

    # Mode 2: multiple-file (a directory)
    parser_multi = subparsers.add_parser("multiple", help="single-file mode help")
    parser_multi.add_argument("-p", default="/work/tmp/cynthia/", help="input path")
    parser_multi.add_argument("-r", default=0.7, help="the ratio to split the sfc key")
    parser_multi.add_argument("-u", default='postgresql://cynthia:dV337bUP!@localhost/cynthia', help="database url")
    parser_multi.add_argument("-n", default=20, help="the number of imported files")
    #parser_single.add_argument("-k", default='postgresql://postgres:050694@localhost:5432/lasdb', help="the key to creat engine")
    parser_multi.set_defaults(func=multi_importer)


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