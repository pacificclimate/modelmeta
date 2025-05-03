#! python
"""
Generate a CSV file from the contents of a modelmeta database.

Output to a fixed filename, 'modelmeta.csv'.
"""
from argparse import ArgumentParser

from mm_cataloguer.list_csv import main

def list():
    # if __name__ == '__main__':
    parser = ArgumentParser(
        description='List contents of a modelmeta database into a CSV file. '
                    'Filename is fixed: modelmeta.csv'
    )
    parser.add_argument(
        '-d', '--dsn',
        default='postgresql://httpd_meta@db3.pcic.uvic.ca/pcic_meta',
        help="Source database DSN from which to read"
    )
    args = parser.parse_args()
    main(args.dsn)