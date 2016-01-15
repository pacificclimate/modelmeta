import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine

log = logging.getLogger(__name__)

def get_session(dsn):
    engine = create_engine(args.dsn)
    Session = sessionmaker(bind=engine)
    return = Session()


def create(args):
    log.info("Using dsn: {}".format(args.dsn))
    log.info('Writing to file: {}'.format(args.outfile))

    session = get_session(args.dsn)


def update(args):
    log.info("Using dsn: {}".format(args.dsn))
    log.info('Updating file: {}'.format(args.outfile))

    session = get_session(args.dsn)


if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = ArgumentParser()
    parser.add_argument('-d', '--dsn',
        help='Destination database DSN to which to write',
        default='postgresql://httpd_meta@atlas.pcic/pcic_meta')
    parser.add_argument('-o', '--outfile',
        help='Output file path. To overwrite an existing file use the "--overwrite" option')
    parser.add_argument('-e', '--ensemble', required=True,
        help='Ensemble to use for updating/creating the output file')
    subparsers = parser.add_subparsers(title='Operation type')

    ## Parser for creating a new config file
    create_parser = subparsers.add_parser('create',
        help='Create a new ncWMS config file')
    create_parser.add_argument('--overwrite', action='store_true', default=False,
        help='Overwrites any file that may be present and output file path')
    create_parser.set_defaults(func=create)

    ## Parser for updating an existing config file
    update_parser = subparsers.add_parser("update",
        help="Updates an existing config by adding entries which do not exist and updating those that do")
    update_parser.set_defaults(func=update)

    args = parser.parse_args()
    args.func(args)