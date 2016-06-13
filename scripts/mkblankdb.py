import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--dsn", help="Destination database DSN to which to write")
    parser.add_argument("-v", "--version", type=int, choices=[1, 2], help="Schema version the database is using")
    parser.set_defaults(
        dsn='sqlite:///modelmeta.sqlite3',
        version=2
    )
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    if args.version == 1:
        from modelmeta.v1 import Base
    elif args.version == 2:
        from modelmeta.v2 import Base
    else:
        raise Exception('Schema version must be 1 or 2, not {}'.format(args.version))

    logger.info("Using dsn: {}".format(args.dsn))
    engine = create_engine(args.dsn)
    Base.metadata.create_all(bind=engine)
    sys.exit(0)
