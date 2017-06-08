"""Test indexing functions against 'tiny' data files:

    tiny_gcm: unprocessed GCM output
    tiny_downscaled: downscaled GCM output (TODO)
    tiny_hydromodel_obs: Interpolated observation-forced hydrological model output (TODO)
    tiny_hydromodel_gcm: GCM-forced hydrological model output

The data in these files is very limited spatially and temporally (though valid) in order to reduce their size,
and their global metadata is standard.

All tests are parameterized over these files, which requires a little trickiness with fixtures.
pytest doesn't directly support parametrizing over fixtures (which here delivers the test input file)
To get around that, we use indirect fixtures, which are passed a parameter that they use to determine
what tiny data file to return.

The tiny_dataset fixture comes pre-parametrized, so every test that uses it is automatically run for every
specified dataset. Only in special cases do we have to specify (override) that parametrization.
See fixture definition for more details.
"""

# TODO: Tests against more types of files (as above)

import datetime

import pytest

from modelmeta import Level
from nchelpers.date_utils import to_datetime

from mm_cataloguer.index_netcdf import \
    find_update_or_insert_cf_file, \
    find_data_file_by_unique_id_and_hash, insert_data_file, delete_data_file, \
    insert_run, find_run, find_or_insert_run, \
    insert_model, find_model, find_or_insert_model, \
    insert_emission, find_emission, find_or_insert_emission, \
    insert_data_file_variable, find_data_file_variable, find_or_insert_data_file_variable, \
    insert_variable_alias, find_variable_alias, find_or_insert_variable_alias, \
    insert_level_set, find_level_set, find_or_insert_level_set, \
    insert_grid, find_grid, find_or_insert_grid, \
    insert_timeset, find_timeset, find_or_insert_timeset, \
    get_grid_info, get_level_set_info


# Helper functions for defining tests

def conditional(f, false_value=None):
    """Return a function that, dependent on an additional boolean keyword parameter `invoke`,
     either invokes and returns the value of the argument function 
     or else returns the value of the argument `false_value`.

     Argument function is invoked with positional arguments passed to returned function.
     """
    def cond_f(*args, invoke=True):
        if not invoke:
            return false_value
        return f(*args)
    return cond_f


def check_properties(obj, **properties):
    """Check that object has expected values for all properties (attributes) specified in dict."""
    if not properties:
        assert obj is None
    else:
        for key, value in properties.items():
            assert getattr(obj, key) == value


def check_insert(insert_thing, *args, **properties):
    """Test an insert operation."""
    thing_inserted = insert_thing(*args)
    check_properties(thing_inserted, **properties)
    return thing_inserted


def check_find(find_thing, cond_insert_thing, *args, **kwargs):
    """"Test a find operation.
    Handles cases that thing to be found was previously inserted or not, controlled by keyword param `invoke`.
    """
    thing_inserted = cond_insert_thing(*args, **kwargs)
    thing_found = find_thing(*args)
    assert thing_inserted == thing_found  # Both are None when not inserted and not found


def check_find_or_insert(find_or_insert_thing, cond_insert_thing, *args, expect_insert=True, **kwargs):
    """"Test a find-or-insert operation.
    Handles cases that thing to be found was previously inserted or not, controlled by keyword param `invoke`.
    Parameter `expect_insert` allows for cases where the insert operation correctly does not insert and instead
    returns None.
    """
    thing_inserted = cond_insert_thing(*args, **kwargs)
    thing_found_or_inserted = find_or_insert_thing(*args)
    if kwargs['invoke']:
        assert thing_found_or_inserted == thing_inserted
    elif expect_insert:
        assert thing_found_or_inserted
    else:
        assert not thing_found_or_inserted


def freeze_now(monkeypatch, *args):
    """Freeze datetime.datetime.now()
    This would be more elegant as a fixture or decorator, but it would be a lot more work.
    """
    fake_now = datetime.datetime(*args)

    class fake_datetime(datetime.datetime):
        @classmethod
        def now(cls):
            return fake_now

    monkeypatch.setattr(datetime, 'datetime', fake_datetime)
    return fake_now


# Indexing functions

def test_find_update_or_insert_cf_file__new(blank_test_session, tiny_dataset):
    """Test inserting a new (not already indexed) NetCDF file into the database."""
    data_file = find_update_or_insert_cf_file(blank_test_session, tiny_dataset)
    check_properties(data_file,
                     filename=tiny_dataset.filepath(),
                     first_1mib_md5sum=tiny_dataset.first_MiB_md5sum,
                     unique_id=tiny_dataset.unique_id,
                     )


def test_find_update_or_insert_cf_file__duplicate(blank_test_session, tiny_dataset):
    """Test inserting a duplicate (already indexed, unchanged) NetCDF file into the database."""
    data_file1 = find_update_or_insert_cf_file(blank_test_session, tiny_dataset)
    data_file2 = find_update_or_insert_cf_file(blank_test_session, tiny_dataset)
    assert data_file1 == data_file2


# DataFile

cond_insert_data_file = conditional(insert_data_file)


def test_insert_data_file(monkeypatch, blank_test_session, tiny_dataset):
    # Have to use a datetime with no hours, min, sec because apparently SQLite loses precision
    fake_now = freeze_now(monkeypatch, 2000, 1, 2)
    dim_names = tiny_dataset.axes_dim()
    data_file = check_insert(
        insert_data_file, blank_test_session, tiny_dataset,
        filename=tiny_dataset.filepath(),
        first_1mib_md5sum=tiny_dataset.first_MiB_md5sum,
        unique_id=tiny_dataset.unique_id,
        index_time=fake_now,
        x_dim_name=dim_names.get('X', None),
        y_dim_name=dim_names.get('Y', None),
        z_dim_name=dim_names.get('Z', None),
        t_dim_name=dim_names.get('T', None)
    )
    assert data_file.run == find_run(blank_test_session, tiny_dataset)
    assert data_file.timeset == find_timeset(blank_test_session, tiny_dataset)


@pytest.mark.parametrize('insert', [False, True])
def test_find_data_file(blank_test_session, tiny_dataset, insert):
    data_file = cond_insert_data_file(blank_test_session, tiny_dataset, invoke=insert)
    id_match, hash_match = find_data_file_by_unique_id_and_hash(blank_test_session, tiny_dataset)
    if insert:
        assert id_match == data_file
        assert hash_match == data_file
    else:
        assert not id_match
        assert not hash_match


def test_delete_data_file(blank_test_session, tiny_dataset):
    data_file = insert_data_file(blank_test_session, tiny_dataset)
    delete_data_file(blank_test_session, data_file)
    assert find_data_file_by_unique_id_and_hash(blank_test_session, tiny_dataset) == (None, None)


# Run

def insert_run_plus(blank_test_session, tiny_dataset):
    """Insert a run plus associated emission and model objects.
    Return run, model, and emission inserted.
    """
    emission = insert_emission(blank_test_session, tiny_dataset)
    model = insert_model(blank_test_session, tiny_dataset)
    run = insert_run(blank_test_session, tiny_dataset, model, emission)
    return run, model, emission


def insert_run_plus_prime(*args):
    """Same as above, but just return the run."""
    return insert_run_plus(*args)[0]


cond_insert_run_plus = conditional(insert_run_plus, false_value=(None, None, None))
cond_insert_run_plus_prime = conditional(insert_run_plus_prime)


def test_insert_run(blank_test_session, tiny_dataset):
    run, model, emission = insert_run_plus(blank_test_session, tiny_dataset)
    check_properties(run,
        name=tiny_dataset.metadata.run,
        project=tiny_dataset.metadata.project,
        model=model,
        emission=emission,
    )


@pytest.mark.parametrize('insert', [False, True])
def test_find_run(blank_test_session, tiny_dataset, insert):
    check_find(find_run, cond_insert_run_plus_prime, blank_test_session, tiny_dataset, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_run(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_run, cond_insert_run_plus_prime, blank_test_session, tiny_dataset,
                         invoke=insert)


# Model

cond_insert_model = conditional(insert_model)


def test_insert_model(blank_test_session, tiny_dataset):
    check_insert(
        insert_model, blank_test_session, tiny_dataset,
        short_name=tiny_dataset.metadata.model,
        organization=tiny_dataset.metadata.institution,
        type=tiny_dataset.model_type,
    )


@pytest.mark.parametrize('insert', [False, True])
def test_find_model(blank_test_session, tiny_dataset, insert):
    check_find(find_model, cond_insert_model, blank_test_session, tiny_dataset, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_model(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_model, cond_insert_model, blank_test_session, tiny_dataset,
                         invoke=insert)


# Emission

cond_insert_emission = conditional(insert_emission)


def test_insert_emission(blank_test_session, tiny_dataset):
    check_insert(insert_emission, blank_test_session, tiny_dataset, short_name=tiny_dataset.metadata.emissions)


@pytest.mark.parametrize('insert', [False, True])
def test_find_emission(blank_test_session, tiny_dataset, insert):
    check_find(find_emission, cond_insert_emission, blank_test_session, tiny_dataset, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_emission(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_emission, cond_insert_emission, blank_test_session, tiny_dataset,
                         invoke=insert)


# DataFileVariable

def insert_data_file_variable_plus(blank_test_session, tiny_dataset, var_name, data_file):
    """Insert a DataFileVariable plus associated VariableAlias, LevelSet, and Grid objects.
    Return DataFileVariable inserted.
    """
    variable_alias = insert_variable_alias(blank_test_session, tiny_dataset, var_name)
    level_set = insert_level_set(blank_test_session, tiny_dataset, var_name)
    grid = insert_grid(blank_test_session, tiny_dataset, var_name)
    data_file_variable = insert_data_file_variable(blank_test_session, tiny_dataset, var_name,
                                                   data_file, variable_alias, level_set, grid)
    return data_file_variable


cond_insert_data_file_variable_plus = conditional(insert_data_file_variable_plus)


def test_insert_data_file_variable(blank_test_session, tiny_dataset):
    var_name = tiny_dataset.dependent_varnames[0]
    variable = tiny_dataset.variables[var_name]
    range_min, range_max = tiny_dataset.var_range(var_name)
    data_file = insert_data_file(blank_test_session, tiny_dataset)
    dfv = check_insert(
        insert_data_file_variable_plus, blank_test_session, tiny_dataset, var_name, data_file,
        file=data_file,
        netcdf_variable_name=var_name,
        variable_cell_methods=variable.cell_methods,
        range_min=range_min,
        range_max=range_max,
        disabled=False,
    )
    assert dfv.variable_alias == find_variable_alias(blank_test_session, tiny_dataset, var_name)
    assert dfv.level_set == find_level_set(blank_test_session, tiny_dataset, var_name)
    assert dfv.grid == find_grid(blank_test_session, tiny_dataset, var_name)


@pytest.mark.parametrize('insert', [False, True])
def test_find_data_file_variable(blank_test_session, tiny_dataset, insert):
    var_name = tiny_dataset.dependent_varnames[0]
    data_file = insert_data_file(blank_test_session, tiny_dataset)
    check_find(find_data_file_variable, cond_insert_data_file_variable_plus, blank_test_session, tiny_dataset, var_name,
               data_file, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_data_file_variable(blank_test_session, tiny_dataset, insert):
    var_name = tiny_dataset.dependent_varnames[0]
    data_file = insert_data_file(blank_test_session, tiny_dataset)
    check_find_or_insert(find_or_insert_data_file_variable, cond_insert_data_file_variable_plus, blank_test_session,
                         tiny_dataset, var_name, data_file, invoke=insert)


# VariableAlias

cond_insert_variable_alias = conditional(insert_variable_alias)


def test_insert_variable_alias(blank_test_session, tiny_dataset):
    var_name = tiny_dataset.dependent_varnames[0]
    variable = tiny_dataset.variables[var_name]
    check_insert(
        insert_variable_alias, blank_test_session, tiny_dataset, var_name,
        long_name=variable.long_name,
        standard_name=variable.standard_name,
        units=variable.units,
    )


@pytest.mark.parametrize('insert', [False, True])
def test_find_variable_alias(blank_test_session, tiny_dataset, insert):
    check_find(find_variable_alias, cond_insert_variable_alias, blank_test_session, tiny_dataset,
               tiny_dataset.dependent_varnames[0], invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_variable_alias(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_variable_alias, cond_insert_variable_alias, blank_test_session, tiny_dataset,
                         tiny_dataset.dependent_varnames[0], invoke=insert)


# LevelSet, Level

cond_insert_level_set = conditional(insert_level_set)


@pytest.mark.parametrize('tiny_dataset, var_name, level_axis_var_name', [
    ('gcm', 'tasmax', None),
    ('hydromodel_gcm', 'SWE_BAND', 'depth'),
], indirect=['tiny_dataset'])
def test_insert_level_set(blank_test_session, tiny_dataset, var_name, level_axis_var_name):
    variable = tiny_dataset.variables[var_name]
    if level_axis_var_name:
        level_axis_var = tiny_dataset.variables[level_axis_var_name]
        assert level_axis_var_name in variable.dimensions
        level_set = check_insert(
            insert_level_set, blank_test_session, tiny_dataset, var_name,
            level_units=level_axis_var.units
        )
        levels = blank_test_session.query(Level).filter(Level.level_set == level_set).all()
        assert level_set.levels == levels
        assert list(level.vertical_level for level in levels) == \
               list(vertical_level for vertical_level in level_axis_var[:])
    else:
        check_insert(insert_level_set, blank_test_session, tiny_dataset, var_name)


@pytest.mark.parametrize('insert', [False, True])
def test_find_level_set(blank_test_session, tiny_dataset, insert):
    check_find(find_level_set, cond_insert_level_set, blank_test_session, tiny_dataset,
               tiny_dataset.dependent_varnames[0], invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_level_set(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_level_set, cond_insert_level_set, blank_test_session, tiny_dataset,
                         tiny_dataset.dependent_varnames[0], invoke=insert, expect_insert=False)


# Grid, YCellBound

cond_insert_grid = conditional(insert_grid)


def test_insert_grid(blank_test_session, tiny_dataset):
    var_name = tiny_dataset.dependent_varnames[0]
    info = get_grid_info(tiny_dataset, var_name)
    grid = check_insert(
        insert_grid, blank_test_session, tiny_dataset, var_name,
        xc_origin=info['xc_values'][0],
        yc_origin=info['yc_values'][0],
        xc_grid_step=info['xc_grid_step'],
        yc_grid_step=info['yc_grid_step'],
        xc_count=len(info['xc_values']),
        yc_count=len(info['yc_values']),
        evenly_spaced_y=info['evenly_spaced_y'],
        xc_units=info['xc_var'].units,
        yc_units=info['yc_var'].units,
    )
    if grid.evenly_spaced_y:
        assert len(grid.y_cell_bounds) == 0
    else:
        assert len(grid.y_cell_bounds) == len(info['yc_var'][:])


@pytest.mark.parametrize('insert', [False, True])
def test_find_grid(blank_test_session, tiny_dataset, insert):
    check_find(find_grid, cond_insert_grid, blank_test_session, tiny_dataset, tiny_dataset.dependent_varnames[0],
               invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_grid(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_grid, cond_insert_grid, blank_test_session, tiny_dataset,
                         tiny_dataset.dependent_varnames[0], invoke=insert)


# Timeset

cond_insert_timeset = conditional(insert_timeset)


def test_insert_timeset(blank_test_session, tiny_dataset):
    start_date, end_date = to_datetime(tiny_dataset.time_range_as_dates)
    timeset = check_insert(
        insert_timeset, blank_test_session, tiny_dataset,
        calendar=tiny_dataset.time_var.calendar,
        start_date=start_date,
        end_date=end_date,
        multi_year_mean=tiny_dataset.is_multi_year_mean,
        num_times=tiny_dataset.time_var.size,
        time_resolution=tiny_dataset.time_resolution,
    )
    assert len(timeset.times) == len(tiny_dataset.time_var[:])
    if tiny_dataset.is_multi_year_mean:
        climatology_bounds = tiny_dataset.variables[tiny_dataset.climatology_bounds_var_name][:]
        assert len(timeset.climatological_times) == len(climatology_bounds)


@pytest.mark.parametrize('insert', [False, True])
def test_find_timeset(blank_test_session, tiny_dataset, insert):
    check_find(find_timeset, cond_insert_timeset, blank_test_session, tiny_dataset, invoke=insert)


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_timeset(blank_test_session, tiny_dataset, insert):
    check_find_or_insert(find_or_insert_timeset, cond_insert_timeset, blank_test_session, tiny_dataset,
                         invoke=insert)


# Helper functions

def test_get_grid_info(tiny_dataset):
    info = get_grid_info(tiny_dataset, tiny_dataset.dependent_varnames[0])
    assert set(info.keys()) == \
           set('xc_var yc_var xc_values yc_values xc_grid_step yc_grid_step evenly_spaced_y'.split())
    assert info['xc_var'] == tiny_dataset.variables['lon']
    assert info['yc_var'] == tiny_dataset.variables['lat']


@pytest.mark.parametrize('tiny_dataset, var_name, level_axis_var_name', [
    ('gcm', 'tasmax', None),
    ('hydromodel_gcm', 'SWE_BAND', 'depth'),
], indirect=['tiny_dataset'])
def test_get_level_set_info(tiny_dataset, var_name, level_axis_var_name):
    info = get_level_set_info(tiny_dataset, var_name)
    if level_axis_var_name:
        assert set(info.keys()) == \
               set('level_axis_var vertical_levels'.split())
        assert info['level_axis_var'] == tiny_dataset.variables[level_axis_var_name]
    else:
        assert info is None
