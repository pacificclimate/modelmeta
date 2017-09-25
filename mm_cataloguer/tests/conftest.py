import sys
import os
from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
from nchelpers import CFDataset

from modelmeta import create_test_database
from modelmeta import Ensemble

# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
from mock_helper import Fake


@pytest.fixture
def test_dsn():
    f = resource_filename('modelmeta', 'data/mddb-v2.sqlite')
    dsn = 'sqlite:///{0}'.format(f)
    return dsn


@pytest.fixture
def test_engine(test_dsn):
    engine = create_engine(test_dsn)
    return engine


@pytest.fixture
def test_session_factory(test_engine):
    Session = sessionmaker(bind=test_engine)
    return Session


@pytest.fixture
def test_session(test_session_factory):
    return test_session_factory()


@pytest.fixture
def blank_test_session():
    engine = create_engine('sqlite:///')
    create_test_database(engine)
    session = sessionmaker(bind=engine)
    return session()


def make_ensemble(id):
    return Ensemble(
        changes='wonder what this is for',
        description='Ensemble {}'.format(id),
        name='ensemble{}'.format(id),
        version=float(id)
    )


@pytest.fixture
def ensemble1():
    return make_ensemble(1)


@pytest.fixture
def ensemble2():
    return make_ensemble(2)


@pytest.fixture
def test_session_with_ensembles(blank_test_session, ensemble1, ensemble2):
    blank_test_session.add_all([ensemble1, ensemble2])
    yield blank_test_session


@pytest.fixture
def tiny_gcm():
    return CFDataset(resource_filename('modelmeta', 'data/tiny_gcm.nc'))


# We parametrize this fixture so that every test that uses it is run for all
# params. This can be overridden on specific tests by using
# `@pytest.mark.parametrize` with arg `indirect=['tiny_dataset']`;
# see `test_get_level_set_info` for an example.
# TODO: Parametrize over more tiny datasets.
@pytest.fixture(params='''
    gcm
    downscaled
    hydromodel_gcm
    gcm_climo_monthly
    gcm_climo_seasonal
    gcm_climo_yearly
'''.split())
def tiny_dataset(request):
    """Return a 'tiny' test dataset, based on request param.
    This fixture is used to parametrize over test data files.
    This fixture must be invoked with indirection.

    request.param: (str) selects the test file to be returned
    returns: (nchelpers.CFDataset) test file as a CFDataset object
    """
    filename = 'data/tiny_{}.nc'.format(request.param)
    return CFDataset(resource_filename('modelmeta', filename))
