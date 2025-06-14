#! python
from argparse import ArgumentParser

from mm_cataloguer.associate_ensemble import main


def associate():
    parser = ArgumentParser(description="Associate an ensemble to datafiles")
    parser.add_argument("-d", "--dsn", required=True, help="DSN for metadata database")
    parser.add_argument(
        "-n",
        "--ensemble-name",
        dest="ensemble_name",
        required=True,
        help="Name of ensemble",
    )
    parser.add_argument(
        "-v",
        "--ensemble-ver",
        dest="ensemble_ver",
        type=float,
        required=True,
        help="Version of ensemble",
    )
    parser.add_argument(
        "-V",
        "--variables",
        dest="var_names",
        help="Comma-separated list of names of variables to associate "
        "(unspecified: all variables in file)",
    )
    parser.add_argument(
        "-r",
        "--regex-filepaths",
        dest="regex_filepaths",
        action="store_true",
        default=False,
        help="Interpret filepaths as regular expressions. Associate to"
        "variables of files matching any of those regular "
        "expressions.",
    )
    parser.add_argument("filepaths", nargs="+", help="Files to process")
    args = parser.parse_args()

    if args.var_names:
        var_names = args.var_names.split(",")
    else:
        var_names = None

    main(
        args.dsn,
        args.ensemble_name,
        args.ensemble_ver,
        args.regex_filepaths,
        args.filepaths,
        var_names,
    )
