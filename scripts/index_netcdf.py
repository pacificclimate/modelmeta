#! python
from argparse import ArgumentParser

from mm_cataloguer.index_netcdf import index_netcdf_files


def index():
    parser = ArgumentParser(
        description="Index PCIC metadata standard compliant NetCDF files "
        "into modelmeta database"
    )
    parser.add_argument("-d", "--dsn", help="DSN for metadata database")
    parser.add_argument("filenames", nargs="+", help="Files to process")
    args = parser.parse_args()
    index_netcdf_files(args.filenames, args.dsn)
