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
import datetime

import pytest
import testing.postgresql

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema

from nchelpers import CFDataset

from modelmeta import create_test_database
from modelmeta import \
    ClimatologicalTime, \
    DataFile, \
    DataFileVariable, \
    DataFileVariableDSGTimeSeries, \
    DataFileVariableDSGTimeSeriesXStation, \
    DataFileVariableGridded, \
    DataFileVariable, \
    DataFileVariablesQcFlag, \
    Emission, \
    Ensemble, \
    EnsembleDataFileVariables, \
    Grid, \
    Level, \
    LevelSet, \
    Model, \
    QcFlag, \
    Run, \
    Station, \
    Time, \
    TimeSet, \
    Variable, \
    VariableAlias, \
    YCellBound, \
    SpatialRefSys

# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))


# Predefined objects

# DataFile

def make_data_file(i, run=None, timeset=None):
    return DataFile(
        id=i,
        filename='data_file_{}'.format(i),
        first_1mib_md5sum='first_1mib_md5sum',
        unique_id='unique_id_{}'.format(i),
        x_dim_name='lon',
        y_dim_name='lat',
        t_dim_name='time',
        index_time=datetime.datetime.now(),
        run=run,
        timeset=timeset,
    )


@pytest.fixture(scope='function')
def data_file_1():
    return make_data_file(1)


# Grid

def make_grid(i):
    return Grid(
        name='grid_{}'.format(i),
        xc_count=10,
        xc_grid_step=0.1,
        xc_origin=0,
        xc_units='units',
        yc_count=10,
        yc_grid_step=0.1,
        yc_origin=0,
        yc_units='units',
        evenly_spaced_y=True,
    )

@pytest.fixture(scope='function')
def grid_1():
    return make_grid(1)


# VariableAlias

def make_variable_alias(i):
    return VariableAlias(
        long_name='long_name_{}'.format(i),
        standard_name='standard_name_{}'.format(i),
        units='units_{}'.format(i),
    )


@pytest.fixture(scope='function')
def variable_alias_1():
    return make_variable_alias(1)


@pytest.fixture(scope='function')
def variable_alias_2():
    return make_variable_alias(2)


# LevelSet

@pytest.fixture(scope='function')
def level_set_1():
    return LevelSet(
        level_units='units'
    )


# DataFileVariableGridded

def make_dfv_gridded(i, file=None, variable_alias=None, level_set=None, grid=None):
    return DataFileVariableGridded(
        derivation_method='derivation_method_{}'.format(i),
        variable_cell_methods='variable_cell_methods_{}'.format(i),
        netcdf_variable_name='var_{}'.format(i),
        disabled=False,
        range_min=0,
        range_max=100,
        file=file,
        variable_alias=variable_alias,
        level_set=level_set,
        grid=grid,
    )


@pytest.fixture(scope='function')
def dfv_gridded_1(data_file_1, variable_alias_1, level_set_1, grid_1):
    return make_dfv_gridded(
        1, file=data_file_1, variable_alias=variable_alias_1,
        level_set=level_set_1, grid=grid_1)


# DataFileVariableDSGTimeSeries

def make_test_dfv_dsg_time_series(i, file=None, variable_alias=None):
    return DataFileVariableDSGTimeSeries(
        id=i,
        derivation_method='derivation_method_{}'.format(i),
        variable_cell_methods='variable_cell_methods_{}'.format(i),
        netcdf_variable_name='var_{}'.format(i),
        disabled=False,
        range_min=0,
        range_max=100,
        file=file,
        variable_alias=variable_alias,
    )


@pytest.fixture(scope='function')
def dfv_dsg_time_series_1(data_file_1, variable_alias_1):
    return make_test_dfv_dsg_time_series(
        1, file=data_file_1, variable_alias=variable_alias_1)


@pytest.fixture(scope='function')
def dfv_dsg_time_series_2(data_file_1, variable_alias_2):
    return make_test_dfv_dsg_time_series(
        2, file=data_file_1, variable_alias=variable_alias_2)


# Station

def make_station(i):
    return Station(
        x=float(i),
        x_units='m',
        y=float(i),
        y_units='m',
        name='STN{}'.format(i),
        long_name='station {}'.format(i),
    )


@pytest.fixture(scope='function')
def station_1():
    return make_station(1)


@pytest.fixture(scope='function')
def station_2():
    return make_station(2)


# Ensemble

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


# Database initialization

def init_database(engine):
	with engine.connect() as connection:
		with connection.begin():
			connection.execute(text("create extension postgis"))


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


# Function-scoped test session based on SESSION-scoped database, engine, and
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


# Function-scoped test session based on FUNCTION-scoped database, engine, and
# session factory fixtures.
# These sessions are SLOW to create, and achieve test isolation using a new
# database each time.

@pytest.fixture(scope='function')
def test_session_with_empty_db_fs(test_session_factory_fs):
    session = test_session_factory_fs()
    yield session
    session.close()


# Database for migration testing

@pytest.fixture(scope='module')
def db_uri(test_dsn):
    yield test_dsn


# Parametrized fixtures

def open_tiny_dataset(abbrev):
    filename = 'data/tiny_{}.nc'.format(abbrev)
    return CFDataset(resource_filename('modelmeta', filename))


# We parametrize these fixture so that every test that uses it is run for all
# params. This can be overridden on specific tests by using
# ``@pytest.mark.parametrize`` with arg ``indirect=['<fixture name>']``;
# see ``test_get_level_set_info`` for an example.

# TODO: Parametrize over more gridded datasets.
gridded_dataset_names = '''
    gcm
    downscaled
    hydromodel_gcm
    gcm_climo_monthly
    gcm_climo_seasonal
    gcm_climo_yearly
    gridded_obs
'''.split()

# TODO: Parametrize over more dsg datasets.
dsg_dataset_names = '''
    streamflow
'''.split()

any_dataset_names = gridded_dataset_names + dsg_dataset_names


@pytest.fixture(params=gridded_dataset_names)
def tiny_gridded_dataset(request):
    """Return a 'tiny' test gridded dataset, based on request param.
    This fixture is used to parametrize over test data files.
    This fixture must be invoked with indirection.

    request.param: (str) selects the test file to be returned
    returns: (nchelpers.CFDataset) test file as a CFDataset object
    """
    dst = open_tiny_dataset(request.param)
    yield dst
    dst.close()


@pytest.fixture(params=dsg_dataset_names)
def tiny_dsg_dataset(request):
    """Return a 'tiny' test discrete sampling geometry dataset, based on
    request param.
    This fixture is used to parametrize over test data files.
    This fixture must be invoked with indirection.

    request.param: (str) selects the test file to be returned
    returns: (nchelpers.CFDataset) test file as a CFDataset object
    """
    dst = open_tiny_dataset(request.param)
    yield dst
    dst.close()


@pytest.fixture(params=any_dataset_names)
def tiny_any_dataset(request):
    """Return a 'tiny' test discrete sampling geometry dataset, based on
    request param.
    This fixture is used to parametrize over test data files.
    This fixture must be invoked with indirection.

    request.param: (str) selects the test file to be returned
    returns: (nchelpers.CFDataset) test file as a CFDataset object
    """
    dst = open_tiny_dataset(request.param)
    yield dst
    dst.close()


@pytest.fixture(params=[False, True])
def insert(request):
    return request.param
