import sys
import os
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modelmeta import DataFile, DataFileVariable, Ensemble, EnsembleDataFileVariables

from lxml import etree

log = logging.getLogger(__name__)

DEFAULT_CONTACT_CHILDREN = {
    "name": "",
    "organization": "",
    "telephone": "",
    "email": ""
}

DEFAULT_SERVER_CHILDREN = {
    "title": "My ncWMS server",
    "allowFeatureInfo": "True",
    "maxImageWidth": "1024",
    "maxImageHeight": "1024",
    "abstract": "",
    "keywords": "",
    "url": "",
    "adminpassword": "ncWMS",
    "allowglobalcapabilities": "true"
}

DEFAULT_CACHE_CHILDREN = {
    "elementLifetimeMinutes": "1440",
    "maxNumItemsInMemory": "200",
    "enableDiskStore": "true",
    "maxNumItemsOnDisk": "2000"
}

DEFAULT_CACHE_ATTS = {
    "enabled": "true"
}

REQUIRED_VARIABLE_ATTS = ["id", "title", "colorScaleRange"]

DEFAULT_VARIABLE_ATTS = {
    "palette": "rainbow",
    "scaling": "linear",
    "numColorBands": "250",
    "disabled": "false"
}

REQUIRED_DATASET_ATTS = ["id", "location", "title"]

DEFAULT_DATASET_ATTS = {
    "queryable": "true",
    "dataReaderClass": "",
    "copyrightStatement": "",
    "moreInfo": "",
    "disabled": "false",
    "updateInterval": "-1"
}
def get_element(element_name, atts={}, **kwargs):
    '''
    Generates a general xml element with provided name, attributes (dictionary), and basic children with text
    '''

    children = {}
    default_atts = {}

    if element_name == "contact":
        children = DEFAULT_CONTACT_CHILDREN

    elif element_name == "server":
        children = DEFAULT_SERVER_CHILDREN

    elif element_name == "cache":
        children = DEFAULT_CACHE_CHILDREN
        default_atts = DEFAULT_CACHE_ATTS

    elif element_name == "variable":
        required_atts = REQUIRED_VARIABLE_ATTS
        if not all(map(lambda x: x in atts, required_atts)):
            raise Exception("Required attributes to create a 'variable' are not present")
        default_atts = DEFAULT_VARIABLE_ATTS

    elif element_name == "dataset":
        required_atts = REQUIRED_DATASET_ATTS
        if not all(map(lambda x: x in atts, required_atts)):
            raise Exception("Required attributes to create a 'dataset' are not present")
        default_atts = DEFAULT_DATASET_ATTS

    root = etree.Element(element_name)

    # Add children
    children.update(kwargs)
    for k, v in children.items():
        etree.SubElement(root, k).text = v

    # Assign attributes
    default_atts.update(atts)
    for k, v in default_atts.items():
        root.set(k, v)

    return root

class Config:
    '''
    The main class which represents a ncWMS config file

    Arguments:
        datasets
        threddsCatalog
        contact
        server
        cache
        dynamicServices
    '''

    def __init__(self,
                 datasets = None,
                 threddsCatalog = None,
                 contact = None,
                 server = None,
                 cache = None,
                 dynamicServices = None):

        self.root = etree.Element("config")

        self.contact = contact if contact else get_element("contact")
        self.server = server if server else get_element("server")
        self.cache = cache if cache else get_element("cache")

        self.datasets = datasets if datasets else etree.Element("datasets")
        self.threddsCatalog = threddsCatalog if threddsCatalog else etree.Element("threddsCatalog")
        self.dynamicServices = dynamicServices if dynamicServices else etree.Element("dynamicServices")

        map(self.root.append, [self.datasets, self.threddsCatalog, self.contact, self.server, self.cache, self.dynamicServices])

    def __str__(self):
        return '<Root ncWMS config object>'

    def xml(self, pretty=True):
        return etree.tostring(self.root, pretty_print=pretty)

    def add_dataset(self, dataset):
        self.datasets.append(dataset)


def get_session(dsn):
    engine = create_engine(args.dsn)
    Session = sessionmaker(bind=engine)
    return Session()


def create(args):
    log.info("Using dsn: {}".format(args.dsn))
    log.info('Writing to file: {}'.format(args.outfile))

    sesh = get_session(args.dsn)
    q = sesh.query(DataFileVariable)\
                .join(EnsembleDataFileVariables, Ensemble)\
                .filter(Ensemble.name == args.ensemble)

    results = [(
        dfv.file.unique_id,
        dfv.file.filename,
        dfv.netcdf_variable_name,
        dfv.range_min,
        dfv.range_max,
        dfv.variable_alias.standard_name
    ) for dfv in q.all()]

    rv = {}

    for unique_id, filename, var_name, range_min, range_max, variable_standard_name in results:
        if unique_id not in rv:
            rv[unique_id] = {
                'filename': filename,
                'variables': [{
                    'id': var_name,
                    'title': variable_standard_name,
                    'colorScaleRange': '{} {}'.format(range_min, range_max)
                }]
            }
        else:
            rv[unique_id]['variables'].append({
                'id': var_name,
                'title': variable_standard_name,
                'colorScaleRange': '{} {}'.format(range_min, range_max)
            })

    # Create base config object
    config = Config()

    # Iterate through db results, adding to config as required
    for k, v in rv.items():
        k.replace('+', '-')
        dataset = get_element('dataset', {
                                    "id":k,
                                    "location": v['filename'],
                                    "title": k})
        variables = [get_element('variable', {
                                    "id": var_['id'],
                                    "title": var_['title'],
                                    "colorScaleRange":  var_['colorScaleRange']
                                }) for var_ in v['variables']]
        map(dataset.append, variables)
        config.add_dataset(dataset)

    # If we aren't saving, print to stdout and exit
    if not args.outfile:
        print(config.xml())
        sys.exit(0)

    # Check if the output filepath exists
    if os.path.exists(args.outfile) and not args.overwrite:
        raise Exception('File {} already exists, remove it or use --overwrite before continuing'.format(args.outfile))

    # Write output to file
    with open(args.outfile, 'w') as f:
            f.write(config.xml())

def update(args):
    raise NotImplemented

if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = ArgumentParser()
    parser.add_argument('-d', '--dsn',
        help='Destination database DSN from which to read',
        default='postgresql://httpd_meta@atlas.pcic/pcic_meta')
    parser.add_argument('-o', '--outfile', default=None,
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
