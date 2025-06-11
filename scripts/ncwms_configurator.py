#! python

import sys
import logging
from argparse import ArgumentParser

from ncwms_configurator import create, update


def configurator():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--dsn",
        help="""PostgreSQL connection string of form:\n
    \tdialect+driver://username:password@host:port/database\n
Examples:\n
    \tpostgresql://scott:tiger@localhost/mydatabase\n
    \tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n
    \tpostgresql+pg8000://scott:tiger@localhost/mydatabase\n""",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        default=None,
        help='Output file path. To overwrite an existing file use the "--overwrite" option',
    )
    parser.add_argument(
        "-e",
        "--ensemble",
        required=True,
        help="Ensemble to use for updating/creating the output file",
    )
    parser.add_argument(
        "-v",
        "--version",
        type=int,
        default=2,
        choices=[1, 2],
        help="Version of ncWMS to target configuration to",
    )
    subparsers = parser.add_subparsers(title="Operation type")

    # Parser for creating a new config file
    create_parser = subparsers.add_parser(
        "create", help="Create a new ncWMS config file"
    )
    create_parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrites any file that may be present and output file path",
    )
    create_parser.set_defaults(func=create)

    # Parser for updating an existing config file
    update_parser = subparsers.add_parser(
        "update",
        help="Updates an existing config by adding entries which do not exist and updating those that do",
    )
    update_parser.set_defaults(func=update)

    args = parser.parse_args()
    args.func(args)
