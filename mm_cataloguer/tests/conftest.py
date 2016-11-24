from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
from netCDF4 import Dataset

from modelmeta import create_test_database


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
def tiny_gcm():
    return Dataset(resource_filename('modelmeta', 'data/tiny_gcm.nc'))
