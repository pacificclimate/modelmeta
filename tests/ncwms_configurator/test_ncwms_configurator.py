'''A simple end-to-end test for the ncwms_configurator functionality. '''

import pytest
from pkg_resources import resource_filename
from argparse import Namespace
from filecmp import cmp
import os

from nchelpers import CFDataset
from mm_cataloguer.index_netcdf import index_cf_file
from mm_cataloguer.associate_ensemble import associate_ensemble_to_data_file
from ncwms_configurator import create

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from modelmeta import create_test_database

# This test does not work properly with data added to an ensemble in the database.
# The test database seems to be reset between when the data is added and the
# configuration file is output. Need to figure out how to make the fixtures work for
# this application.
# At present, tests can only catch errors so severe they prevent the script from 
# operating at all.

@pytest.mark.parametrize('include_datasets', [
                            False, 
                            pytest.param(True, marks=pytest.mark.skip),
                            ])
def test_ncwms_configurator_datasets(include_datasets, test_dsn_fs, test_engine_fs, ensemble1):
    # first make a test database with an ensemble (and sometimes a dataset) in it
    create_test_database(test_engine_fs)
    engine = create_engine(test_dsn_fs)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add_all([ensemble1])
    if include_datasets:
        print("including datasets")
        dataset = CFDataset(resource_filename("modelmeta", "data/tiny_gcm.nc"))
        datafile = index_cf_file(session, dataset)
        associate_ensemble_to_data_file(session, ensemble1, datafile, ["tasmax"])
    session.close()
        
    #fake up arguments for the script
    configurator_args = Namespace()
    configurator_args.dsn = test_dsn_fs
    configurator_args.ensemble = ensemble1.name
    configurator_args.outfile = 'outfile.xml'
    configurator_args.version = 2
    configurator_args.overwrite = True
    
    create(configurator_args)
    
    expected_outfile = "tests/ncwms_configurator/{}.xml".format("data" if include_datasets else "nodata")
    assert(cmp("outfile.xml", expected_outfile))
    os.remove("outfile.xml")