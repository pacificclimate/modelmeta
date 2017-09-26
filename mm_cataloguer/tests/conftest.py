import sys
import os
from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
from nchelpers import CFDataset
import testing.postgresql

from modelmeta import create_test_database
from modelmeta import Ensemble
from mm_cataloguer.index_netcdf import find_update_or_insert_cf_file

# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
from mock_helper import Fake


# Predefined objects

def make_ensemble(id):
    return Ensemble(
        changes='wonder what this is for',
        description='Ensemble {}'.format(id),
        name='ensemble{}'.format(id),
        version=float(id)
    )


@pytest.fixture(scope='function')
def ensemble1():
    return make_ensemble(1)


@pytest.fixture(scope='function')
def ensemble2():
    return make_ensemble(2)


# Session-scoped databases, engines, session factories, and derived sessions
# Use these databases and these sessions in preference to reduce per-test 
# overhead. Sessions roll back any database actions on teardown.

@pytest.fixture(scope='session')
def test_dsn():
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope='session')
def test_engine(test_dsn):
    engine = create_engine(test_dsn)
    yield engine
    engine.dispose()


@pytest.fixture(scope='session')
def test_session_factory(test_engine):
    Session = sessionmaker(bind=test_engine)
    yield Session


@pytest.fixture
def test_session_with_empty_db(test_dsn):
    engine = create_engine(test_dsn)
    create_test_database(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def test_session_with_ensembles(
        test_session_with_empty_db, ensemble1, ensemble2
):
    test_session_with_empty_db.add_all([ensemble1, ensemble2])
    yield test_session_with_empty_db


# TODO: Necessary?
@pytest.fixture
def test_session_with_ensembles_and_data_files(test_session_with_ensembles):
    session = test_session_with_ensembles
    test_files = [
        'data/tiny_gcm.nc',
        'data/tiny_downscaled.nc',
        'data/tiny_hydromodel_gcm.nc',
        'data/tiny_gcm_climo_monthly.nc',
    ]
    filenames = [resource_filename('modelmeta', f) for f in test_files]
    for filename in filenames:
        with CFDataset(filename) as cf:
            find_update_or_insert_cf_file(session, cf)
    yield session


# Function-scoped databases
# Use these databases when testing functions that take a database or session 
# factory argument rather than a session. Because these databases are scoped 
# only per test, tests are isolated, but are much slower than using 
# (automatically rolled back) sessions based on session-scoped databases.
# Suffix ``_fs`` stands for "function scope".


@pytest.fixture(scope='function')
def test_dsn_fs():
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope='function')
def test_engine_fs(test_dsn_fs):
    engine = create_engine(test_dsn_fs)
    yield engine
    engine.dispose()


@pytest.fixture(scope='function')
def test_session_factory_fs(test_engine_fs):
    Session = sessionmaker(bind=test_engine_fs)
    yield Session


@pytest.fixture
def test_session_with_empty_db_fs(test_dsn_fs):
    engine = create_engine(test_dsn_fs)
    create_test_database(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


# Dataset fixtures

# TODO: Is this in use?
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
