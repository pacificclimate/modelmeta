"""Test indexing functions against 'tiny' data files:

    tiny_gcm: unprocessed GCM output
    tiny_downscaled: downscaled GCM output (TODO)
    tiny_hydromodel_obs: Interpolated observation-forced hydrological model
        output (TODO)
    tiny_hydromodel_gcm: GCM-forced hydrological model output

The data in these files is very limited spatially and temporally (though valid)
in order to reduce their size, and their global metadata is standard.

All tests are parameterized over these files, which requires a little
trickiness with fixtures. pytest doesn't directly support parametrizing over
fixtures (which here delivers the test input file) To get around that, we use
indirect fixtures, which are passed a parameter that they use to determine
what tiny data file to return.

The tiny_gridded_dataset fixture comes pre-parametrized, so every test that uses it
is automatically run for every specified dataset. Only in special cases do we
have to specify (override) that parametrization. See fixture definition for
more details.
"""

# TODO: Tests against more types of files (as above)

import os
import datetime

import pytest
from netCDF4 import date2num, num2date, chartostring

from dateutil.relativedelta import relativedelta

from sqlalchemy import func, text

import pycrs

from modelmeta import create_test_database
from modelmeta import Level, DataFile, SpatialRefSys, Station
from nchelpers.date_utils import to_datetime

from mm_cataloguer.index_netcdf import (
    index_netcdf_file,
    index_netcdf_files,
    find_update_or_insert_cf_file,
    index_cf_file,
    find_data_file_by_id_hash_filename,
    insert_data_file,
    delete_data_file,
    insert_run,
    find_run,
    find_or_insert_run,
    insert_model,
    find_model,
    find_or_insert_model,
    insert_emission,
    find_emission,
    find_or_insert_emission,
    insert_data_file_variable_gridded,
    insert_data_file_variable_dsg_time_series,
    find_data_file_variable,
    find_or_insert_data_file_variable,
    insert_variable_alias,
    find_variable_alias,
    find_or_insert_variable_alias,
    insert_level_set,
    find_level_set,
    find_or_insert_level_set,
    insert_spatial_ref_sys,
    find_spatial_ref_sys,
    find_or_insert_spatial_ref_sys,
    insert_grid,
    find_grid,
    find_or_insert_grid,
    insert_station,
    find_station,
    find_or_insert_station,
    find_or_insert_stations,
    associate_stations_to_data_file_variable_dsg_time_series,
    insert_timeset,
    find_timeset,
    find_or_insert_timeset,
    get_grid_info,
    get_level_set_info,
    seconds_since_epoch,
    usable_name,
    wkt,
)
from tests.test_helpers import resource_filename


from mock_helper import Mock


# Helper functions for defining tests


def conditional(f, false_value=None):
    """Return a function that, dependent on an additional boolean keyword
    parameter ``invoke``, either invokes and returns the value of the argument
    function or else returns the value of the argument ``false_value``.

     Argument function is invoked with positional arguments passed to returned
     function.
    """

    def cond_f(*args, **kwargs):
        invoke = kwargs.get("invoke", True)
        if not invoke:
            return false_value
        return f(*args)

    return cond_f


def check_properties(obj, **properties):
    """Check that object has expected values for all properties (attributes) s
    pecified in dict."""
    if not properties:
        assert obj is None
    else:
        for key, value in properties.items():
            assert getattr(obj, key) == value, "attribute: {}".format(key)


def check_insert(*args, **properties):
    """Test an insert operation."""
    insert_thing, args = args[0], args[1:]
    thing_inserted = insert_thing(*args)
    check_properties(thing_inserted, **properties)
    return thing_inserted


def check_find(*args, **kwargs):
    """ "Test a find operation.
    Handles cases that thing to be found was previously inserted or not,
    controlled by keyword param ``invoke``.
    """
    find_thing, cond_insert_thing = args[0:2]
    args = args[2:]
    thing_inserted = cond_insert_thing(*args, **kwargs)
    thing_found = find_thing(*args)
    # Both are None when not inserted and not found
    assert thing_inserted == thing_found


def check_find_or_insert(*args, **kwargs):
    """ "Test a find-or-insert operation.

    Handles cases that thing to be found was previously inserted or not,
    controlled by keyword param ``invoke``. Parameter ``expect_insert`` allows
    for cases where the insert operation correctly does not insert and instead
    returns None.
    """
    find_or_insert_thing, cond_insert_thing = args[0:2]
    args = args[2:]
    thing_inserted = cond_insert_thing(*args, **kwargs)
    thing_found_or_inserted = find_or_insert_thing(*args)
    if kwargs.get("invoke", True):
        assert thing_found_or_inserted == thing_inserted
    elif kwargs.get("expect_insert", True):
        assert thing_found_or_inserted
    else:
        assert not thing_found_or_inserted
    return thing_found_or_inserted


def freeze_utcnow(*args):
    """Freeze datetime.datetime.now()

    This would be more elegant as a fixture or decorator, but it would be a
    lot more work.
    """
    monkeypatch = args[0]
    fake_now = datetime.datetime(*args[1:])
    print("FAKE NOW", fake_now)

    class fake_datetime(datetime.datetime):
        @classmethod
        def utcnow(cls):
            return fake_now

        def now(cls):
            return fake_now

    monkeypatch.setattr(datetime, "datetime", fake_datetime)
    return fake_now


level_set_parametrization = (
    "tiny_gridded_dataset, var_name, level_axis_var_name",
    [
        ("gcm", "tasmax", None),
        ("downscaled", "tasmax", None),
        ("hydromodel_gcm", "SWE_BAND", "depth"),
        ("gcm_climo_monthly", "tasmax", None),
    ],
)


# Test schema setup


def print_query_results(session, query, title=None):
    print()
    if title:
        print(title)
        print("-" * len(title))
    result = session.execute(text(query))
    for row in result:
        print(row)


def test_schemas(test_session_with_empty_db):
    print_query_results(
        test_session_with_empty_db,
        """
        SHOW search_path
    """,
        title="search_path",
    )
    print_query_results(
        test_session_with_empty_db,
        """
        select nspname
        from pg_catalog.pg_namespace
    """,
        title="Schemas",
    )
    print_query_results(
        test_session_with_empty_db,
        """
        select *
        from information_schema.tables
        where table_schema not in ('pg_catalog', 'information_schema')
    """,
        title="Tables",
    )


def test_spatial_ref_sys_orm(test_session_with_empty_db):
    q = test_session_with_empty_db.query(SpatialRefSys).limit(10)
    for r in q.all():
        print(r.id, r.proj4text)


# Test helper functions


def test_get_grid_info(tiny_gridded_dataset):
    info = get_grid_info(
        tiny_gridded_dataset, tiny_gridded_dataset.dependent_varnames()[0]
    )
    assert set(info.keys()) == set(
        "xc_var yc_var xc_values yc_values "
        "xc_grid_step yc_grid_step evenly_spaced_y".split()
    )
    assert info["xc_var"] == tiny_gridded_dataset.variables["lon"]
    assert info["yc_var"] == tiny_gridded_dataset.variables["lat"]


# Note: Overriding default parametrization of tiny_gridded_dataset in these tests.
@pytest.mark.parametrize(*level_set_parametrization, indirect=["tiny_gridded_dataset"])
def test_get_level_set_info(tiny_gridded_dataset, var_name, level_axis_var_name):
    info = get_level_set_info(tiny_gridded_dataset, var_name)
    if level_axis_var_name:
        assert set(info.keys()) == set("level_axis_var vertical_levels".split())
        assert (
            info["level_axis_var"]
            == tiny_gridded_dataset.variables[level_axis_var_name]
        )
    else:
        assert info is None


# Model

cond_insert_model = conditional(insert_model)


def test_insert_model(test_session_with_empty_db, tiny_gridded_dataset):
    check_insert(
        insert_model,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        short_name=tiny_gridded_dataset.metadata.model,
        organization=tiny_gridded_dataset.metadata.institution,
        type=tiny_gridded_dataset.model_type,
    )


def test_find_model(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_model,
        cond_insert_model,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


def test_find_or_insert_model(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find_or_insert(
        find_or_insert_model,
        cond_insert_model,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


# Emission

cond_insert_emission = conditional(insert_emission)


def test_insert_emission(test_session_with_empty_db, tiny_gridded_dataset):
    check_insert(
        insert_emission,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        short_name=tiny_gridded_dataset.metadata.emissions,
    )


def test_find_emission(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_emission,
        cond_insert_emission,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


def test_find_or_insert_emission(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    check_find_or_insert(
        find_or_insert_emission,
        cond_insert_emission,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


# Run


def insert_run_plus(test_session_with_empty_db, tiny_gridded_dataset):
    """Insert a run plus associated emission and model objects.
    Return run, model, and emission inserted.
    """
    emission = insert_emission(test_session_with_empty_db, tiny_gridded_dataset)
    model = insert_model(test_session_with_empty_db, tiny_gridded_dataset)
    run = insert_run(test_session_with_empty_db, tiny_gridded_dataset, model, emission)
    return run, model, emission


def insert_run_plus_prime(*args):
    """Same as above, but just return the run."""
    return insert_run_plus(*args)[0]


cond_insert_run_plus = conditional(insert_run_plus, false_value=(None, None, None))
cond_insert_run_plus_prime = conditional(insert_run_plus_prime)


def test_insert_run(test_session_with_empty_db, tiny_gridded_dataset):
    run, model, emission = insert_run_plus(
        test_session_with_empty_db, tiny_gridded_dataset
    )
    check_properties(
        run,
        name=tiny_gridded_dataset.metadata.run,
        project=tiny_gridded_dataset.metadata.project,
        model=model,
        emission=emission,
    )


def test_find_run(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_run,
        cond_insert_run_plus_prime,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


def test_find_or_insert_run(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find_or_insert(
        find_or_insert_run,
        cond_insert_run_plus_prime,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


# VariableAlias

cond_insert_variable_alias = conditional(insert_variable_alias)


def test_insert_variable_alias(test_session_with_empty_db, tiny_gridded_dataset):
    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    variable = tiny_gridded_dataset.variables[var_name]
    check_insert(
        insert_variable_alias,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        long_name=variable.long_name,
        standard_name=usable_name(variable),
        units=variable.units,
    )


def test_find_variable_alias(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_variable_alias,
        cond_insert_variable_alias,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )


def test_find_or_insert_variable_alias(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    check_find_or_insert(
        find_or_insert_variable_alias,
        cond_insert_variable_alias,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )


# LevelSet, Level

cond_insert_level_set = conditional(insert_level_set)


# Note: Overriding default parametrization of tiny_gridded_dataset in these tests.


@pytest.mark.parametrize(*level_set_parametrization, indirect=["tiny_gridded_dataset"])
def test_insert_level_set(
    test_session_with_empty_db, tiny_gridded_dataset, var_name, level_axis_var_name
):
    variable = tiny_gridded_dataset.variables[var_name]
    if level_axis_var_name:
        level_axis_var = tiny_gridded_dataset.variables[level_axis_var_name]
        assert level_axis_var_name in variable.dimensions
        level_set = check_insert(
            insert_level_set,
            test_session_with_empty_db,
            tiny_gridded_dataset,
            var_name,
            level_units=level_axis_var.units,
        )
        levels = (
            test_session_with_empty_db.query(Level)
            .filter(Level.level_set == level_set)
            .all()
        )
        assert level_set.levels == levels
        assert list(level.vertical_level for level in levels) == list(
            vertical_level for vertical_level in level_axis_var[:]
        )
    else:
        check_insert(
            insert_level_set, test_session_with_empty_db, tiny_gridded_dataset, var_name
        )


@pytest.mark.parametrize(*level_set_parametrization, indirect=["tiny_gridded_dataset"])
def test_find_level_set(
    test_session_with_empty_db,
    tiny_gridded_dataset,
    var_name,
    level_axis_var_name,
    insert,
):
    check_find(
        find_level_set,
        cond_insert_level_set,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        invoke=insert,
    )


@pytest.mark.parametrize(*level_set_parametrization, indirect=["tiny_gridded_dataset"])
def test_find_or_insert_level_set(
    test_session_with_empty_db,
    tiny_gridded_dataset,
    var_name,
    level_axis_var_name,
    insert,
):
    check_find_or_insert(
        find_or_insert_level_set,
        cond_insert_level_set,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        invoke=insert,
        expect_insert=bool(level_axis_var_name),
    )


# SpatialRefSys

cond_insert_spatial_ref_sys = conditional(insert_spatial_ref_sys)


def test_insert_spatial_ref_sys(test_session_with_empty_db, tiny_gridded_dataset):
    sesh = test_session_with_empty_db
    q = sesh.query(func.max(SpatialRefSys.id).label("max_srid"))
    prev_max_srid = q.scalar()

    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    proj4_string = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    srs = insert_spatial_ref_sys(sesh, tiny_gridded_dataset, var_name)

    # Check that we did in fact insert a new SRS, and its id is according to
    # spec.
    new_max_srid = q.scalar()
    assert new_max_srid >= 990000
    assert new_max_srid > prev_max_srid

    check_properties(
        srs,
        auth_name="PCIC",
        auth_srid=new_max_srid,
        proj4text=proj4_string,
        srtext=wkt(proj4_string),
    )

    sesh.close()


def test_find_spatial_ref_sys(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_spatial_ref_sys,
        cond_insert_spatial_ref_sys,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )


def test_find_or_insert_spatial_ref_sys(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    sesh = test_session_with_empty_db
    q = sesh.query(func.max(SpatialRefSys.id).label("max_srid"))
    prev_max_srid = q.scalar()

    check_find_or_insert(
        find_or_insert_spatial_ref_sys,
        cond_insert_spatial_ref_sys,
        sesh,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )

    if insert:
        new_max_srid = q.scalar()
        assert new_max_srid >= 990000
        assert new_max_srid > prev_max_srid


# Grid, YCellBound


def insert_grid_plus(sesh, cf, var_name):
    """Insert a grid plus associated spatial ref sys object.
    Return grid and spatial ref sys inserted.
    """
    srs = insert_spatial_ref_sys(sesh, cf, var_name)
    grid = insert_grid(sesh, cf, var_name, srs)
    return grid, srs


def insert_grid_plus_prime(*args):
    """Same as above, but just return the grid."""
    return insert_grid_plus(*args)[0]


cond_insert_grid_plus = conditional(insert_grid_plus, false_value=(None, None))
cond_insert_grid_plus_prime = conditional(insert_grid_plus_prime)


def test_insert_grid(test_session_with_empty_db, tiny_gridded_dataset):
    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    info = get_grid_info(tiny_gridded_dataset, var_name)
    grid, srs = insert_grid_plus(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    check_properties(
        grid,
        xc_origin=info["xc_values"][0],
        yc_origin=info["yc_values"][0],
        xc_grid_step=info["xc_grid_step"],
        yc_grid_step=info["yc_grid_step"],
        xc_count=len(info["xc_values"]),
        yc_count=len(info["yc_values"]),
        evenly_spaced_y=info["evenly_spaced_y"],
        xc_units=info["xc_var"].units,
        yc_units=info["yc_var"].units,
    )
    assert grid.srid == srs.id
    if grid.evenly_spaced_y:
        assert len(grid.y_cell_bounds) == 0
    else:
        assert len(grid.y_cell_bounds) == len(info["yc_var"][:])


def test_find_grid(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_grid,
        cond_insert_grid_plus_prime,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )


def test_find_or_insert_grid(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find_or_insert(
        find_or_insert_grid,
        cond_insert_grid_plus_prime,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        tiny_gridded_dataset.dependent_varnames()[0],
        invoke=insert,
    )


# Station

cond_insert_station = conditional(insert_station)


def test_insert_station(test_session_with_empty_db, tiny_dsg_dataset):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    name = tiny_dsg_dataset.id_instance_var(var_name)
    lat = tiny_dsg_dataset.spatial_instance_var(var_name, "X")
    lon = tiny_dsg_dataset.spatial_instance_var(var_name, "Y")
    i = 0
    assert instance_dim.size > 0
    check_insert(
        insert_station,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        i,
        name,
        lon,
        lat,
        name=str(chartostring(name[i])),
        x=lon[i],
        x_units=lon.units,
        y=lat[i],
        y_units=lat.units,
    )


def test_find_station(test_session_with_empty_db, tiny_dsg_dataset, insert):
    assert tiny_dsg_dataset.sampling_geometry == "dsg.timeSeries"
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    name = tiny_dsg_dataset.id_instance_var(var_name)
    lat = tiny_dsg_dataset.spatial_instance_var(var_name, "X")
    lon = tiny_dsg_dataset.spatial_instance_var(var_name, "Y")
    i = 0
    assert instance_dim.size > 0
    check_find(
        find_station,
        cond_insert_station,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        i,
        name,
        lon,
        lat,
        invoke=insert,
    )


def test_find_or_insert_station(test_session_with_empty_db, tiny_dsg_dataset, insert):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    name = tiny_dsg_dataset.id_instance_var(var_name)
    lat = tiny_dsg_dataset.spatial_instance_var(var_name, "X")
    lon = tiny_dsg_dataset.spatial_instance_var(var_name, "Y")
    i = 0
    assert instance_dim.size > 0
    check_find_or_insert(
        find_or_insert_station,
        cond_insert_station,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        i,
        name,
        lon,
        lat,
        invoke=insert,
    )


def test_find_or_insert_stations(test_session_with_empty_db, tiny_dsg_dataset):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    stations = find_or_insert_stations(
        test_session_with_empty_db, tiny_dsg_dataset, var_name
    )

    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    name = tiny_dsg_dataset.id_instance_var(var_name)
    lat = tiny_dsg_dataset.spatial_instance_var(var_name, "X")
    lon = tiny_dsg_dataset.spatial_instance_var(var_name, "Y")
    assert len(stations) == instance_dim.size
    for i, station in enumerate(stations):
        check_properties(
            station,
            name=str(chartostring(name[i])),
            x=lon[i],
            x_units=lon.units,
            y=lat[i],
            y_units=lat.units,
        )


# DataFileVariableDSGTimeSeriesXStation


def test_associate_stations_to_data_file_variable_dsg_time_series(
    test_session_with_empty_db, tiny_dsg_dataset, dfv_dsg_time_series_1
):
    sesh = test_session_with_empty_db
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    variable = tiny_dsg_dataset.variables[var_name]
    sesh.add(dfv_dsg_time_series_1)
    associations = associate_stations_to_data_file_variable_dsg_time_series(
        sesh, tiny_dsg_dataset, var_name, dfv_dsg_time_series_1
    )
    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    assert instance_dim.size > 0
    assert len(associations) == instance_dim.size


# DataFileVariableDSGTimeSeries


def insert_data_file_variable_dsg_plus(
    test_session_with_empty_db, tiny_dsg_dataset, var_name, data_file
):
    """Insert a ``DataFileVariableDSGTimeSeries`` plus associated
    ``VariableAlias`` object.
    Return ``DataFileVariableDSGTimeSeries`` inserted.
    """
    variable_alias = insert_variable_alias(
        test_session_with_empty_db, tiny_dsg_dataset, var_name
    )
    data_file_variable = insert_data_file_variable_dsg_time_series(
        test_session_with_empty_db,
        tiny_dsg_dataset,
        var_name,
        data_file,
        variable_alias,
    )
    associate_stations_to_data_file_variable_dsg_time_series(
        test_session_with_empty_db, tiny_dsg_dataset, var_name, data_file_variable
    )
    return data_file_variable


cond_insert_data_file_variable_dsg_plus = conditional(
    insert_data_file_variable_dsg_plus
)


@pytest.mark.slow
def test_insert_data_file_variable_dsg_time_series(
    test_session_with_empty_db, tiny_dsg_dataset
):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    variable = tiny_dsg_dataset.variables[var_name]
    range_min, range_max = tiny_dsg_dataset.var_range(var_name)
    data_file = insert_data_file(test_session_with_empty_db, tiny_dsg_dataset)
    dfv = check_insert(
        insert_data_file_variable_dsg_plus,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        var_name,
        data_file,
        netcdf_variable_name=var_name,
        variable_cell_methods=getattr(variable, "cell_methods", None),
        range_min=range_min,
        range_max=range_max,
        disabled=False,
    )
    assert dfv.variable_alias == find_variable_alias(
        test_session_with_empty_db, tiny_dsg_dataset, var_name
    )


@pytest.mark.slow
def test_find_data_file_variable_dsg(
    test_session_with_empty_db, tiny_dsg_dataset, insert
):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    data_file = insert_data_file(test_session_with_empty_db, tiny_dsg_dataset)
    check_find(
        find_data_file_variable,
        cond_insert_data_file_variable_dsg_plus,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        var_name,
        data_file,
        invoke=insert,
    )


def test_find_or_insert_data_file_variable_dsg(
    test_session_with_empty_db, tiny_dsg_dataset, insert, data_file_1
):
    var_name = tiny_dsg_dataset.dependent_varnames()[0]
    variable = tiny_dsg_dataset.variables[var_name]
    # data_file = insert_data_file(test_session_with_empty_db, tiny_dsg_dataset)
    dfv = check_find_or_insert(
        find_or_insert_data_file_variable,
        cond_insert_data_file_variable_dsg_plus,
        test_session_with_empty_db,
        tiny_dsg_dataset,
        var_name,
        data_file_1,
        invoke=insert,
    )
    stations = test_session_with_empty_db.query(Station).all()
    instance_dim = tiny_dsg_dataset.instance_dim(var_name)
    assert len(stations) == instance_dim.size
    assert set(dfv.stations) == set(stations)


# DataFileVariableGridded


def insert_data_file_variable_gridded_plus(
    test_session_with_empty_db, tiny_gridded_dataset, var_name, data_file
):
    """Insert a ``DataFileVariableGridded`` plus associated ``VariableAlias``,
    ``LevelSet``, and ``Grid`` objects.
    Return ``DataFileVariableGridded`` inserted.
    """
    variable_alias = insert_variable_alias(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    level_set = insert_level_set(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    grid = insert_grid_plus_prime(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    data_file_variable = insert_data_file_variable_gridded(
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        data_file,
        variable_alias,
        level_set,
        grid,
    )
    return data_file_variable


cond_insert_data_file_variable_gridded_plus = conditional(
    insert_data_file_variable_gridded_plus
)


@pytest.mark.slow
def test_insert_data_file_variable_gridded(
    test_session_with_empty_db, tiny_gridded_dataset
):
    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    variable = tiny_gridded_dataset.variables[var_name]
    range_min, range_max = tiny_gridded_dataset.var_range(var_name)
    data_file = insert_data_file(test_session_with_empty_db, tiny_gridded_dataset)
    dfv = check_insert(
        insert_data_file_variable_gridded_plus,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        data_file,
        file=data_file,
        netcdf_variable_name=var_name,
        variable_cell_methods=variable.cell_methods,
        range_min=range_min,
        range_max=range_max,
        disabled=False,
    )
    assert dfv.variable_alias == find_variable_alias(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    assert dfv.level_set == find_level_set(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )
    assert dfv.grid == find_grid(
        test_session_with_empty_db, tiny_gridded_dataset, var_name
    )


@pytest.mark.slow
def test_find_data_file_variable_gridded(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    data_file = insert_data_file(test_session_with_empty_db, tiny_gridded_dataset)
    check_find(
        find_data_file_variable,
        cond_insert_data_file_variable_gridded_plus,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        data_file,
        invoke=insert,
    )


@pytest.mark.slow
def test_find_or_insert_data_file_variable_gridded(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    var_name = tiny_gridded_dataset.dependent_varnames()[0]
    data_file = insert_data_file(test_session_with_empty_db, tiny_gridded_dataset)
    check_find_or_insert(
        find_or_insert_data_file_variable,
        cond_insert_data_file_variable_gridded_plus,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        var_name,
        data_file,
        invoke=insert,
    )


# Timeset

cond_insert_timeset = conditional(insert_timeset)


def test_insert_timeset(test_session_with_empty_db, tiny_gridded_dataset):
    timeset = check_insert(
        insert_timeset,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        calendar=tiny_gridded_dataset.time_var.calendar,
        multi_year_mean=tiny_gridded_dataset.is_multi_year_mean,
        num_times=tiny_gridded_dataset.time_var.size,
        time_resolution=tiny_gridded_dataset.time_resolution,
    )
    assert len(timeset.times) == len(tiny_gridded_dataset.time_var[:])
    if tiny_gridded_dataset.is_multi_year_mean:
        climatology_bounds = tiny_gridded_dataset.variables[
            tiny_gridded_dataset.climatology_bounds_var_name
        ][:, :]
        units = tiny_gridded_dataset.time_var.units
        calendar = tiny_gridded_dataset.time_var.calendar
        time_starts = [
            date2num(ct.time_start, units, calendar)
            for ct in timeset.climatological_times
        ]
        time_ends = [
            date2num(ct.time_end, units, calendar)
            for ct in timeset.climatological_times
        ]
        assert time_starts == [cb[0] for cb in climatology_bounds]
        assert time_ends == [cb[1] for cb in climatology_bounds]
        assert timeset.start_date, timeset.end_date == to_datetime(
            num2date(tiny_gridded_dataset.nominal_time_span)
        )
        if tiny_gridded_dataset.time_resolution == "seasonal":

            def wrap(month):
                return (month - 1) % 12 + 1

            assert timeset.start_date.month == wrap(
                timeset.climatological_times[0].time_start.month + 1
            )
            assert timeset.end_date.month == wrap(
                (
                    timeset.climatological_times[-1].time_end - relativedelta(seconds=1)
                ).month
                + 1
            )
        else:
            assert (
                timeset.start_date.month
                == timeset.climatological_times[0].time_start.month
            )
            assert (
                timeset.end_date.month
                == (
                    timeset.climatological_times[-1].time_end - relativedelta(seconds=1)
                ).month
            )
    else:
        assert len(timeset.climatological_times) == 0
        assert timeset.start_date, timeset.end_date == to_datetime(
            tiny_gridded_dataset.time_range_as_dates
        )


@pytest.mark.slow
def test_find_timeset(test_session_with_empty_db, tiny_gridded_dataset, insert):
    check_find(
        find_timeset,
        cond_insert_timeset,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


@pytest.mark.slow
def test_find_or_insert_timeset(
    test_session_with_empty_db, tiny_gridded_dataset, insert
):
    check_find_or_insert(
        find_or_insert_timeset,
        cond_insert_timeset,
        test_session_with_empty_db,
        tiny_gridded_dataset,
        invoke=insert,
    )


# DataFile

cond_insert_data_file = conditional(insert_data_file)


def test_insert_data_file(monkeypatch, test_session_with_empty_db, tiny_any_dataset):
    # Have to use a datetime with no hours, min, sec because apparently
    # SQLite loses precision
    fake_now = freeze_utcnow(
        monkeypatch,
        datetime.datetime.now(datetime.timezone.utc).year,
        datetime.datetime.now(datetime.timezone.utc).month,
        datetime.datetime.now(datetime.timezone.utc).day,
    )
    dim_names = tiny_any_dataset.axes_dim()
    data_file = check_insert(
        insert_data_file,
        test_session_with_empty_db,
        tiny_any_dataset,
        filename=tiny_any_dataset.filepath(),
        first_1mib_md5sum=tiny_any_dataset.first_MiB_md5sum,
        unique_id=tiny_any_dataset.unique_id,
        index_time=fake_now,
        x_dim_name=dim_names.get("X", None),
        y_dim_name=dim_names.get("Y", None),
        z_dim_name=dim_names.get("Z", None),
        t_dim_name=dim_names.get("T", None),
    )
    assert data_file.run == find_run(test_session_with_empty_db, tiny_any_dataset)
    assert data_file.timeset == find_timeset(
        test_session_with_empty_db, tiny_any_dataset
    )


def test_find_data_file(test_session_with_empty_db, tiny_any_dataset, insert):
    data_file = cond_insert_data_file(
        test_session_with_empty_db, tiny_any_dataset, invoke=insert
    )
    id_match, hash_match, filename_match = find_data_file_by_id_hash_filename(
        test_session_with_empty_db, tiny_any_dataset
    )
    if insert:
        assert id_match == data_file
        assert hash_match == data_file
        assert filename_match == data_file
    else:
        assert not id_match
        assert not hash_match
        assert not filename_match


@pytest.mark.slow
def test_delete_data_file(test_session_with_empty_db, tiny_any_dataset):
    data_file = insert_data_file(test_session_with_empty_db, tiny_any_dataset)
    delete_data_file(test_session_with_empty_db, data_file)
    assert find_data_file_by_id_hash_filename(
        test_session_with_empty_db, tiny_any_dataset
    ) == (None, None, None)


# Root functions

# TODO: Test for multiple entries for same file

far_future = seconds_since_epoch(datetime.datetime(2100, 1, 1))


@pytest.mark.slow
@pytest.mark.parametrize(
    "dataset_mocks, os_path_mocks, same_data_file",
    [
        # new file: no match on id, hash, or filename
        (
            {
                "unique_id": "foo",
                "first_MiB_md5sum": "foo",
                "filepath": lambda **kwargs: "foo",
            },
            {},
            False,
        ),
        # same file
        ({}, {}, True),
        # symlinked and unmodified file
        (
            {
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: True,  # old file still exists
                "realpath": lambda fp: "bar",  # links to another file
                "getmtime": lambda fp: 0,  # don't care (prevent exception)
            },
            True,
        ),
        # symlinked and modified file (hash changed)
        (
            {
                "first_MiB_md5sum": "foo",  # different hash
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: True,  # old file still exists
                "realpath": lambda fp: "bar",  # links to another file
                "getmtime": lambda fp: 0,  # don't care (prevent exception)
            },
            True,
        ),
        # symlinked and modified file (mod time changed)
        (
            {
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: True,  # old file still exists
                "realpath": lambda fp: "bar",  # links to another file
                "getmtime": lambda fp: far_future,  # much later
            },
            True,
        ),
        # copy of file
        (
            {
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: True,  # old file still exists
                "realpath": lambda fp: fp,  # is the same file
                "getmtime": lambda fp: 0,  # don't care (prevent exception)
            },
            True,
        ),
        # moved file
        (
            {
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: False,  # old file gone
                "realpath": lambda fp: "foo",  # resolve to same file name
                "getmtime": lambda fp: 0,  # don't care (prevent exception)
            },
            True,
        ),
        # indexed under different id
        ({"unique_id": "foo"}, {}, True),  # different unique id
        # modified file (hash changed)
        ({"first_MiB_md5sum": "foo"}, {}, False),  # different hash
        # modified file (modification time changed)
        ({}, {"getmtime": lambda fp: far_future}, False),  # much later
        # moved and modified file (hash changed)
        (
            {
                "first_MiB_md5sum": "foo",  # different hash
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: False,  # old file gone
                "getmtime": lambda fp: 0,  # don't care (prevent exception)
            },
            False,
        ),
        # moved and modified file (modification time changed)
        (
            {
                "filepath": lambda **kwargs: "foo",  # different filepath
            },
            {
                "isfile": lambda fp: False,  # old file gone
                "getmtime": lambda fp: far_future,  # much later
            },
            False,
        ),
    ],
)
def test_find_update_or_insert_cf_file__dup(
    monkeypatch,
    test_session_with_empty_db,
    tiny_any_dataset,
    dataset_mocks,
    os_path_mocks,
    same_data_file,
):
    """Test cases where the data file to be inserted is a variant of an
    existing data file.

    Variations are specified by the arguments ``dataset_mocks`` and `
    `os_path_mocks``.
    ``dataset_mocks`` specifies how attributes of the dataset (``CFDataset``)
    should be mocked for the test.
    ``os_path_mocks`` specifies how attributes of ``os.path`` (which is called
    on the dataset filename) should be changed for the test.
    """
    # Index original file
    data_file1 = index_cf_file(test_session_with_empty_db, tiny_any_dataset)
    assert data_file1

    # Mock specified differences into tiny_gridded_dataset
    other_tiny_gridded_dataset = Mock(tiny_any_dataset, **dataset_mocks)

    # Mock specified differences into os.path
    for attr, value in os_path_mocks.items():
        monkeypatch.setattr(os.path, attr, value)

    # Index mocked duplicate file
    data_file2 = find_update_or_insert_cf_file(
        test_session_with_empty_db, other_tiny_gridded_dataset
    )

    # Set up checks for second indexing
    def mock_value(key):
        thing = dataset_mocks.get(key, None)
        if callable(thing):
            return thing()
        else:
            return thing

    dataset_to_data_file_attr = {
        "filepath": "filename",
        "unique_id": "unique_id",
        "first_MiB_md5sum": "first_1mib_md5sum",
    }
    properties = {
        dataset_to_data_file_attr[key]: mock_value(key) for key in dataset_mocks
    }

    # Check second indexing
    assert (data_file1 == data_file2) == same_data_file
    if not same_data_file and properties:
        check_properties(data_file2, **properties)


@pytest.mark.slow
@pytest.mark.parametrize(
    "rel_filepath",
    [
        "data/tiny_gcm.nc",
        "data/tiny_downscaled.nc",
        "data/tiny_hydromodel_gcm.nc",
        "data/tiny_gcm_climo_monthly.nc",
        "data/tiny_gcm_climo_seasonal.nc",
        "data/tiny_gcm_climo_yearly.nc",
    ],
)
def test_index_netcdf_file(test_engine_fs, test_session_factory_fs, rel_filepath):
    # Set up test database
    create_test_database(test_engine_fs)

    # Index file
    filepath = resource_filename("modelmeta", rel_filepath)
    data_file_id = index_netcdf_file(filepath, test_session_factory_fs)

    # Check results
    assert data_file_id is not None
    session = test_session_factory_fs()
    data_file = session.query(DataFile).filter(DataFile.id == data_file_id).one()
    assert data_file.filename == str(filepath)
    session.close()


@pytest.mark.slow
@pytest.mark.parametrize(
    "rel_filepath",
    [
        "data/bad_tiny_gcm.nc",
    ],
)
def test_index_netcdf_file_with_error(
    test_engine_fs, test_session_factory_fs, rel_filepath
):
    # Set up test database
    create_test_database(test_engine_fs)

    # Index file
    filepath = resource_filename("modelmeta", rel_filepath)
    data_file_id = index_netcdf_file(filepath, test_session_factory_fs)

    # Check results
    assert data_file_id is None
    session = test_session_factory_fs()
    data_file = session.query(DataFile).filter(DataFile.filename == filepath).first()
    assert data_file is None
    session.close()


@pytest.mark.slow
def test_index_netcdf_files(test_dsn_fs, test_engine_fs):
    # Set up test database
    create_test_database(test_engine_fs)

    # Index files
    test_files = [
        "data/tiny_gcm.nc",
        "data/tiny_downscaled.nc",
        "data/tiny_hydromodel_gcm.nc",
        "data/tiny_gcm_climo_monthly.nc",
        "data/tiny_gcm_climo_seasonal.nc",
        "data/tiny_gcm_climo_yearly.nc",
        "data/tiny_streamflow.nc",
    ]
    filenames = [resource_filename("modelmeta", f) for f in test_files]
    data_file_ids = index_netcdf_files(filenames, test_dsn_fs)

    # Check results
    assert all(data_file_ids)
