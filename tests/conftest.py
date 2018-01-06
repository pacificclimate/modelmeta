"""
Test configuration.

Since the target database is a Postgresql database, we test against that.
Testing against a SQLite database failed to catch some errors, in some cases
quite surprisingly. And it turns out that testing against a Postgresql database
is not appreciably slower than against SQLite.

``mm_cataloguer`` functions, and thus the tests, are divided into 3 types:

- functions that take an instantiated database session
- functions that take a database session factory
- functions that take a database dsn

The first type (instantiated sessions) are best tested against a database
with session (all tests) scope. Individual sessions that roll back changes on
teardown make test isolation simple and efficient.

The second two types (session factory, dsn) are best tested against databases
with function (per-test) scope. This is slower, but it isolates tests.

There are thus two cascades of fixtures for databases, engines, session
factories, and sessions, one starting with databases with session scope,
the other with databases with function scope.
"""
import sys
import os
from pkg_resources import resource_filename

import pytest
import testing.postgresql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema

from nchelpers import CFDataset

from modelmeta import create_test_database
from modelmeta import Ensemble

# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))


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


def init_database(engine):
    engine.execute("create extension postgis")


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
    init_database(engine)
    create_test_database(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope='session')
def test_session_factory(test_engine):
    Session = sessionmaker(bind=test_engine)
    yield Session


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
    init_database(engine)
    create_test_database(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope='function')
def test_session_factory_fs(test_engine_fs):
    Session = sessionmaker(bind=test_engine_fs)
    yield Session


# Function-scoped test session based on session-scoped database, engine, and
# session factory fixtures.
# These sessions are fast to create, and achieve test isolation by rolling back
# their actions on teardown.

@pytest.fixture(scope='function')
def test_session_with_empty_db(test_session_factory):
    session = test_session_factory()
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope='function')
def test_session_with_ensembles(
        test_session_with_empty_db, ensemble1, ensemble2
):
    test_session_with_empty_db.add_all([ensemble1, ensemble2])
    yield test_session_with_empty_db


# Parametrized fixtures

# We parametrize this fixture so that every test that uses it is run for all
# params. This can be overridden on specific tests by using
# ``@pytest.mark.parametrize`` with arg ``indirect=['tiny_dataset']``;
# see ``test_get_level_set_info`` for an example.
# TODO: Parametrize over more tiny datasets.
@pytest.fixture(params='''
    gcm
    downscaled
    hydromodel_gcm
    gcm_climo_monthly
    gcm_climo_seasonal
    gcm_climo_yearly
    gridded_obs
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


@pytest.fixture(params=[False, True])
def insert(request):
    return request.param
