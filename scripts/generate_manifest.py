#! python
from argparse import ArgumentParser
import sys
from dateutil.parser import parse

from mm_cataloguer.generate_manifest import generate_manifest

def generate():
    parser = ArgumentParser(
        description='Generate manifest of files requested from database')
    parser.add_argument("-c", "--connection_string", help="DSN for modelmeta database")
    parser.add_argument("-e", "--ensembles", nargs="+", default=["all"], 
                        help="Ensembles from which files should be listed")
    parser.add_argument("-s", "--since", help="Date after which files should be listed. "
                                              "Date is parsed using dateutil.parser.parse")
    parser.add_argument("-o", "--outfile", default=sys.stdout, 
                        help="Path to file that should contain output")
    args = parser.parse_args()
    since = parse(args.since)
    generate_manifest(args.connection_string, args.ensembles, since, args.outfile)
