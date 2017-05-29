import itertools

import pytest

from mm_cataloguer.index_netcdf import \
    insert_model, find_model, find_or_insert_model, \
    insert_emission, find_emission, find_or_insert_emission, \
    insert_run, find_run, find_or_insert_run, \
    insert_timeset, find_timeset, find_or_insert_timeset, \
    get_grid_info, get_var_bounds_and_values, \
    insert_grid, find_grid, find_or_insert_grid, \
    to_datetime


# Helper functions for defining tests

def conditional(f, false_value=None):
    """Return a function that, dependent on an additional boolean keyword parameter `invoke`,
     either invokes and returns the value of the argument function 
     or else returns the value of the argument `false_value`.

     Argument function is invoked with positional arguments passed to returned function.
     """
    def wrapper(*args, invoke=True):
        if not invoke:
            return false_value
        return f(*args)
    return wrapper


def check_find(find_thing, cond_insert_thing, session, cf, *args, **kwargs):
    """"Test a find operation
    Handles cases that thing to be found was previously inserted or not, controlled by keyword param `invoke`.
    """
    thing_inserted = cond_insert_thing(session, cf, *args, **kwargs)
    thing_found = find_thing(session, cf, *args)
    assert thing_inserted == thing_found  # Both are None when not inserted and not found


def check_find_or_insert(find_or_insert_thing, cond_insert_thing, session, cf, *args, **kwargs):
    """"Test a find-or-insert operation
    Handles cases that thing to be found was previously inserted or not, controlled by keyword param `invoke`.
    """
    thing_inserted = cond_insert_thing(session, cf, *args, **kwargs)
    thing_found_or_inserted = find_or_insert_thing(session, cf, *args)
    if kwargs['invoke']:
        assert thing_found_or_inserted == thing_inserted
    else:
        assert thing_found_or_inserted


# TODO: DataFile


# Run

def insert_run_plus(blank_test_session, mock_cf):
    """Insert a run plus associated emission and model objects.
    Return run, model, and emission inserted.
    """
    emission = insert_emission(blank_test_session, mock_cf)
    model = insert_model(blank_test_session, mock_cf)
    run = insert_run(blank_test_session, mock_cf, model, emission)
    return run, model, emission


def insert_run_plus_prime(*args):
    """Same as above, but just return the run."""
    return insert_run_plus(*args)[0]


cond_insert_run_plus = conditional(insert_run_plus, false_value=(None, None, None))
cond_insert_run_plus_prime = conditional(insert_run_plus_prime)


def test_insert_run(blank_test_session, mock_cf):
    run, model, emission = insert_run_plus(blank_test_session, mock_cf)
    assert run.name == mock_cf.metadata.run
    assert run.project == mock_cf.metadata.project
    assert run.model == model
    assert run.emission == emission


@pytest.mark.parametrize('insert', [False, True])
def test_find_run(blank_test_session, mock_cf, insert):
    check_find(find_run, cond_insert_run_plus_prime, blank_test_session, mock_cf, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_run(blank_test_session, mock_cf, insert):
    check_find_or_insert(find_or_insert_run, cond_insert_run_plus_prime, blank_test_session, mock_cf,
                         invoke=insert)


# Model

cond_insert_model = conditional(insert_model)


def test_insert_model(blank_test_session, mock_cf):
    model = insert_model(blank_test_session, mock_cf)
    assert model.short_name == mock_cf.metadata.model
    assert model.organization == mock_cf.metadata.institution
    assert model.type == 'GCM'


@pytest.mark.parametrize('insert', [False, True])
def test_find_model(blank_test_session, mock_cf, insert):
    check_find(find_model, cond_insert_model, blank_test_session, mock_cf, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_model(blank_test_session, mock_cf, insert):
    check_find_or_insert(find_or_insert_model, cond_insert_model, blank_test_session, mock_cf,
                         invoke=insert)


# Emission

cond_insert_emission = conditional(insert_emission)


def test_insert_emission(blank_test_session, mock_cf):
    emission = insert_emission(blank_test_session, mock_cf)
    assert emission.short_name == mock_cf.metadata.emissions


@pytest.mark.parametrize('insert', [False, True])
def test_find_emission(blank_test_session, mock_cf, insert):
    check_find(find_emission, cond_insert_emission, blank_test_session, mock_cf, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_emission(blank_test_session, mock_cf, insert):
    check_find_or_insert(find_or_insert_emission, cond_insert_emission, blank_test_session, mock_cf,
                         invoke=insert)


# TODO: DataFileVariable


# TODO: VariableAlias


# TODO: LevelSet


# Grid, YCellBound
# TODO: Add tests for YCellBound

cond_insert_grid = conditional(insert_grid)


def test_insert_grid(blank_test_session, mock_cf):
    grid = insert_grid(blank_test_session, mock_cf, mock_cf.dependent_varnames[0])
    assert grid


@pytest.mark.parametrize('insert', [False, True])
def test_find_grid(blank_test_session, mock_cf, insert):
    check_find(find_grid, cond_insert_grid, blank_test_session, mock_cf, mock_cf.dependent_varnames[0],
               invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_grid(blank_test_session, mock_cf, insert):
    check_find_or_insert(find_or_insert_grid, cond_insert_grid, blank_test_session, mock_cf,
                         mock_cf.dependent_varnames[0], invoke=insert)



# Timeset
# TODO: Add tests for Time, ClimatologicalTime

cond_insert_timeset = conditional(insert_timeset)


def test_insert_timeset(blank_test_session, mock_cf):
    timeset = insert_timeset(blank_test_session, mock_cf)
    start_date, end_date = to_datetime(mock_cf.time_range_as_dates)
    assert timeset.calendar == mock_cf.time_var.calendar
    assert timeset.start_date == start_date
    assert timeset.end_date == end_date
    assert timeset.multi_year_mean == mock_cf.is_multi_year_mean
    assert timeset.num_times == mock_cf.time_var.size
    assert timeset.time_resolution == mock_cf.time_resolution
    # TODO: Test associated Time objects


@pytest.mark.parametrize('insert', [False, True])
def test_find_timeset(blank_test_session, mock_cf, insert):
    check_find(find_timeset, cond_insert_timeset, blank_test_session, mock_cf, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_timeset(blank_test_session, mock_cf, insert):
    check_find_or_insert(find_or_insert_timeset, cond_insert_timeset, blank_test_session, mock_cf,
                         invoke=insert)


# Helper functions

def test_get_grid_info(mock_cf):
    info = get_grid_info(mock_cf, mock_cf.dependent_varnames[0])
    assert set(info.keys()) == \
           set('xc_var yc_var xc_values yc_values xc_grid_step yc_grid_step evenly_spaced_y'.split())
    assert info['xc_var'] == mock_cf.variables['lon']
    assert info['yc_var'] == mock_cf.variables['lat']


def test_get_var_bounds_and_values(mock_cf):
    bvs = get_var_bounds_and_values(mock_cf, 'lat')
    lat = mock_cf.variables['lat']
    for i, (lower, value, upper) in enumerate(bvs):
        assert lower < value < upper
        assert value == lat[i]


