#! python
"""
List contents of a modelmeta database (by default db3/pcic_meta).

Output types are:

Either:

- filepaths: list filepaths from matching records
  - with or without names of associated ensembles
- dirpaths: list unique directory paths (to depth specified)

Either:

- results: (default, no option) list results
- count: print count only of results that would be listed

Outputs can be filtered by one or more of the following criteria:

- multi-variable (t/f/none)
- multi-year mean (t/f/none)
- multi-year mean, with concatenated time axes (t/f/none)
"""


from argparse import ArgumentParser

from mm_cataloguer.list import \
    strtobool, \
    list_filepaths, list_dirpaths

def list():
    main_parser = ArgumentParser(
        description='Tools for listing and summarizing contents of a '
                    'modelmeta database.'
    )
    main_parser.add_argument(
        '-d', '--dsn',
        default='postgresql://httpd_meta@db3.pcic.uvic.ca/pcic_meta',
        help="Source database DSN from which to read"
    )
    main_parser.add_argument(
        '-q', '--print-queries', action='store_true',
        help='Print SQL of queries generated'
    )
    # Selection criteria
    main_parser.add_argument(
        '-e', '--ensemble',
        help='Filter on association to ensemble'
    )
    main_parser.add_argument(
        '--mv', '--multi-variable', dest='multi_variable', type=strtobool,
        help='Filter on whether file contains more than 1 variable'
    )
    main_parser.add_argument(
        '--mym', '--multi-year-mean', dest='multi_year_mean', type=strtobool,
        help='Filter on whether file contains multi-year means'
    )
    main_parser.add_argument(
        '--mym-c', '--mym-concatenated', dest='mym_concatenated',
        type=strtobool,
        help='Filter on whether file contains multi-year means with '
             'concatenated time axes'
    )
    # Display type
    main_parser.add_argument(
        '-c', '--count', action='store_true',
        help='Display count only of records'
    )

    subparsers = main_parser.add_subparsers()

    filepaths_parser = subparsers.add_parser(
        'filepaths', aliases=['f', 'fp'],
        help='Display full filepaths of files'
    )
    filepaths_parser.add_argument(
        '-E', '--list-ensembles', dest='list_ensembles', action='store_true',
        help='Display associated ensembles for each file'
    )
    filepaths_parser.set_defaults(action=list_filepaths)

    dirpaths_parser = subparsers.add_parser(
        'dirpaths', aliases=['d', 'dp'],
        help='Display (unique) directory paths of files'
    )
    dirpaths_parser.add_argument(
        'depth', type=int, nargs='?', default=99,
        help='Cutoff depth of directory paths to display'
    )
    dirpaths_parser.set_defaults(action=list_dirpaths)

    args = main_parser.parse_args()
    args.action(args)
