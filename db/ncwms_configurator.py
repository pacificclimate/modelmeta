import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lxml import etree

log = logging.getLogger(__name__)


class Config(object):
    pass

class Dataset(object):
    pass

class Variable(object):
    pass

class Datasets(object):
    pass

class Contact(object):
    def __str__(self):
        return '''
<contact>
    <name> </name>
    <organization> </organization>
    <telephone> </telephone>
    <email> </email>
</contact>
   '''
class Server(object):
    def __init__(self):
        pass
    def __str__(self):
        return '''
<server>
    <title>My ncWMS server</title>
    <allowFeatureInfo>true</allowFeatureInfo>
    <maxImageWidth>1024</maxImageWidth>
    <maxImageHeight>1024</maxImageHeight>
    <abstract> </abstract>
    <keywords> </keywords>
    <url> </url>
    <adminpassword>password</adminpassword>
    <allowglobalcapabilities>true</allowglobalcapabilities>
</server>
'''

class Cache(object):
    def __str__(self):
        return '''
<cache enabled="true">
    <elementLifetimeMinutes>1440</elementLifetimeMinutes>
    <maxNumItemsInMemory>200</maxNumItemsInMemory>
    <enableDiskStore>true</enableDiskStore>
    <maxNumItemsOnDisk>2000</maxNumItemsOnDisk>
</cache>
'''

## From https://gist.github.com/reimund/5435343/
def dict2xml(d, root_node=None):
    wrap          =     False if None == root_node or isinstance(d, list) else True
    root          = 'objects' if None == root_node else root_node
    root_singular = root[:-1] if 's' == root[-1] and None == root_node else root
    xml           = ''
    children      = []

    if isinstance(d, dict):
        for key, value in dict.items(d):
            if isinstance(value, dict):
                children.append(dict2xml(value, key))
            elif isinstance(value, list):
                children.append(dict2xml(value, key))
            else:
                xml = xml + ' ' + key + '="' + str(value) + '"'
    else:
        for value in d:
            children.append(dict2xml(value, root_singular))

    end_tag = '>' if 0 < len(children) else '/>'

    if wrap or isinstance(d, dict):
        xml = '<' + root + xml + end_tag

    if 0 < len(children):
        for child in children:
            xml = xml + child

        if wrap or isinstance(d, dict):
            xml = xml + '</' + root + '>'
        
    return xml

def get_session(dsn):
    engine = create_engine(args.dsn)
    Session = sessionmaker(bind=engine)
    return Session()


def create(args):
    log.info("Using dsn: {}".format(args.dsn))
    log.info('Writing to file: {}'.format(args.outfile))

    session = get_session(args.dsn)

    config = {
        'datasets': {
            'dataset': {}
        },
        'threddsCatalog': {},
        'contact': {
            'name': '',
            'organization': '',
            'telephone': '',
            'email': ''
        },
        'server': {
            'title': 'My ncWMS server',
            'allowFeatureInfo': 'true',
            'maxImageWidth': 1024,
            'maxImageHeight': 1024,
            'abstract': '',
            'keywords': '',
            'url': '',
            'adminpassword': 'password',
            'allowglobalcapabilities': 'true'
        },
        'cache': {
            'elementLifetimeMinutes': 1440,
            'maxNumItemsInMemory': 200,
            'enableDiskStore': 'true',
            'maxNumItemsOnDisk': 2000
        }
    }

    print(dict2xml(config, 'config'))

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