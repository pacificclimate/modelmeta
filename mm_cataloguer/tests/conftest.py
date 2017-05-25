import sys
import os
# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))

from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
from nchelpers import CFDataset

from modelmeta import create_test_database

from mock_helper import Mock


@pytest.fixture
def test_session():
    f = resource_filename('modelmeta', 'data/mddb-v2.sqlite')
    engine = create_engine('sqlite:///{0}'.format(f))
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture
def blank_test_session():
    engine = create_engine('sqlite:///')
    create_test_database(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture
def mock_cf():
    return Mock(
        metadata=Mock(
            project="CMIP5",
            run="r1e3v3",
            model="CGCM3",
            institution="CCCMA",
            emissions="SRESA2"
        )
    )


@pytest.fixture
def tiny_gcm():
    return CFDataset(resource_filename('modelmeta', 'data/tiny_gcm.nc'))
