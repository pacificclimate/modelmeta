# coding=utf-8

"""Functions for adding NetCDF files to the modelmeta database.

The root function is `index_netcdf_file`, which causes a NetCDF file to be
added or updated in the modelmeta database.

`index_netcdf_file` uses a set of database manipulation functions to handle
finding or inserting the objects (records) in the database necessary to
represent the NetCDF file.

Objects in modelmeta database are related to each other as follows. Indentation
can be read "has"; the cardinality of this association is indicated after
the object name. (E.g., Model (1) means a Run has 1 Model associated to it.)

::

    DataFile
        Run (1)
            Model (1)
            Emission (1)
        DataFileVariable (*)
            VariableAlias (1)
                Variable (*)
            LevelSet (1)
                Level
            Grid (1)
                YCellBound (1)
            DataFileVar_QCFlag (*)
                QCFlag (1)
            DataFileVar_Ensemble (*)
                Ensemble (1)
        Timeset (1)
            Time (*)
            ClimatologicalTime (*)

Database manipulation functions are organized (ordered) according to the list
above.

Database manipulation functions have the following typical names and
signatures::

    find_<item>(session, cf_dataset, ...): Item
        -- Find and return an Item representing the Item part of the NetCDF
           file. If none exists return None.
    insert_<item>(session, cf_dataset, ...) -> Item
        -- Insert and return an Item representing the Item part of the NetCDF
           file.
    find_or_insert_<item>(session, cf_dataset, ...) -> Item
        -- Find or insert and return an Item representing the Item part of the
           NetCDF file.

where

    ``Item`` is one of the object types above (e.g., ``DataFile``)
    ``<item>`` is a snake-case representation of the object type (``data_file``)
    ``session`` is a SQLAlchemy database session used to access the database
    ``cf_dataset`` is an ``nchelpers.CFDataset`` object representing the file
        to be indexed

Ideally, all such functions are dependent only on the Session and CFDataset
parameters. In particular, no additional parameters should have to be passed
in that characterize the CFDataset. All necessary information should be
derived from methods/properties of CFDataset, adhering to the principle that
all our NetCDF files are fully self-describing.
"""

import os
import traceback
import logging
import datetime
import functools

from netCDF4 import num2date
import numpy as np
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset
from nchelpers.date_utils import to_datetime
from modelmeta import Model, Run, Emission, DataFile, TimeSet, Time, \
    ClimatologicalTime, DataFileVariable, VariableAlias, \
    LevelSet, Level, Grid, YCellBound, EnsembleDataFileVariables
from mm_cataloguer import psycopg2_adapters


# Set up logging

formatter = logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Register psycopg adapters for numpy types
psycopg2_adapters.register()

# Miscellaneous constants

filepath_converter = 'realpath'


# Decorators

def memoize(obj):
    """Memoize a callable object with only positional args, and where those
    args are hashable. This is simple and sufficient for its application in
    this code. It works in all versions of Python >= 2.7, which is not true
    for many of the more featureful modules (e.g., `functools.lru_cache`).

    Adapted from http://book.pythontips.com/en/latest/function_caching.html
    """
    memo = {}

    @functools.wraps(obj)
    def memoized(*args):
        if args in memo:
            return memo[args]
        else:
            value = obj(*args)
            memo[args] = value
            return value

    return memoized


# Helper functions

def is_regular_series(values, relative_tolerance=1e-6):
    """Return True iff the given series of values is regular, i.e., has equal
    steps between values, within a relative tolerance."""
    diffs = np.diff(values)
    return abs(np.max(diffs) / np.min(diffs) - 1) < relative_tolerance


def mean_step_size(values):
    """Return mean of differences between successive elements of values list"""
    return np.mean(np.diff(values))


def seconds_since_epoch(t):
    """Convert a datetime to the number of seconds since the Unix epoch."""
    return (t-datetime.datetime(1970, 1, 1)).total_seconds()


@memoize
def get_level_set_info(cf, var_name):
    """Return a dict containing information characterizing the level set
    (Z axis values) associated with a specified dependent variable, or
    None if there is no associated Z axis we can identify.

    This information is expensive to compute, and typically requested 2 or
    more times in quick succession, so it is cached.

    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of NetCDF dependent variable
        (variable with associated level set)
    :return (dict)
    """
    variable = cf.variables[var_name]
    # Find the level dimension if it exists
    level_axis_dim_name = cf.axes_dim(variable.dimensions).get('Z', None)

    if not level_axis_dim_name:
        return None
    level_axis_var = cf.variables[level_axis_dim_name]

    # Find LevelSet corresponding to the level axis
    vertical_levels = level_axis_var[:]
    return {
        'level_axis_var': level_axis_var,
        'vertical_levels': vertical_levels,
    }


@memoize
def get_grid_info(cf, var_name):
    """Get information defining the Grid record corresponding to the spatial
    dimensions of a variable in a NetCDF file.

    This information is expensive to compute, and typically requested 2 or
    more times in quick succession, so it is cached.

    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :return (dict)
    """
    axes_to_dim_names = cf.axes_dim(cf.variables[var_name].dimensions)

    if not all(axis in axes_to_dim_names for axis in 'XY'):
        return None

    if 'S' in axes_to_dim_names:
        dim_names = cf.reduced_dims(var_name)
    else:
        dim_names = axes_to_dim_names

    xc_var, yc_var = (cf.variables[dim_names[axis]] for axis in 'XY')
    xc_values, yc_values = (var[:] for var in [xc_var, yc_var])

    return {
        'xc_var': xc_var,
        'yc_var': yc_var,
        'xc_values': xc_values,
        'yc_values': yc_values,
        'xc_grid_step': mean_step_size(xc_values),
        'yc_grid_step': mean_step_size(yc_values),
        'evenly_spaced_y': is_regular_series(yc_values),
    }


# Model

def find_model(sesh, cf):
    """Find existing ``Model`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing ``Model`` record or None
    """
    query = sesh.query(Model).filter(Model.short_name == cf.metadata.model)
    return query.first()


def insert_model(sesh, cf):
    """Insert new ``Model`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: new ``Model`` record
    """
    model = Model(
        short_name=cf.metadata.model,
        type=cf.model_type,
        organization=cf.metadata.institution
    )
    sesh.add(model)
    return model


def find_or_insert_model(sesh, cf):
    """Find existing or insert new ``Model`` record corresponding to a
    NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new ``Model`` record
    """
    model = find_model(sesh, cf)
    if model:
        return model
    return insert_model(sesh, cf)


# Emission

def find_emission(sesh, cf):
    """Find existing ``Emission`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing ``Emission`` record or None
    """
    q = (
        sesh.query(Emission)
            .filter(Emission.short_name == cf.metadata.emissions)
    )
    return q.first()


def insert_emission(sesh, cf):
    """Insert new ``Emission`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: new ``Emission`` record
    """
    emission = Emission(short_name=cf.metadata.emissions)
    sesh.add(emission)
    return emission


def find_or_insert_emission(sesh, cf):
    """Find existing or insert new ``Emission`` record corresponding to a
    NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new ``Emission`` record
    """
    emission = find_emission(sesh, cf)
    if emission:
        return emission
    return insert_emission(sesh, cf)


# Run

def find_run(sesh, cf):
    """Find existing ``Run`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing ``Run`` record or None
    """
    q = sesh.query(Run).join(Model).join(Emission) \
        .filter(Model.short_name == cf.metadata.model) \
        .filter(Emission.short_name == cf.metadata.emissions) \
        .filter(Run.name == cf.metadata.run)
    return q.first()


def insert_run(sesh, cf, model, emission):
    """Insert new ``Run`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param model: (Model) Model record corresponding to NetCDF file
    :param emission: (Emission) Emission record corresponding to NetCDF file
    :return: new ``Run`` record
    """
    run = Run(
        name=cf.metadata.run,
        project=cf.metadata.project,
        model=model,
        emission=emission
    )
    sesh.add(run)
    return run


def find_or_insert_run(sesh, cf):
    """Find existing or insert new ``Run`` record corresponding to a NetCDF
    file.
    Find or insert required ``Model`` and ``Emission`` records as necessary.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new ``Run`` record
    """
    run = find_run(sesh, cf)
    if run:
        return run

    # No matching ``Run``: Insert new ``Run`` and find or insert accompanying
    # ``Model`` and ``Emission`` records.
    model = find_or_insert_model(sesh, cf)
    assert model
    emission = find_or_insert_emission(sesh, cf)
    assert emission
    return insert_run(sesh, cf, model, emission)


# VariableAlias

def usable_name(variable):
    """Returns a usable name for a variable.
    Tries, in order: ``variable.standard_name``, ``variable.name``"""
    try:
        return variable.standard_name
    except AttributeError:
        return variable.name


def find_variable_alias(sesh, cf, var_name):
    """Find a VariableAlias for the named NetCDF variable.
    If none exists, return None.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :return found VariableAlias object or None
    """
    variable = cf.variables[var_name]
    q = (
        sesh.query(VariableAlias)
            .filter(VariableAlias.long_name == variable.long_name)
            .filter(VariableAlias.standard_name == usable_name(variable))
            .filter(VariableAlias.units == variable.units)
    )
    return q.first()


def insert_variable_alias(sesh, cf, var_name):
    """Insert a VariableAlias for the named NetCDF variable.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :return inserted VariableAlias object
    """
    variable = cf.variables[var_name]
    variable_alias = VariableAlias(
        long_name=variable.long_name,
        standard_name=usable_name(variable),
        units=variable.units,
    )
    sesh.add(variable_alias)
    return variable_alias


def find_or_insert_variable_alias(sesh, cf, var_name):
    """Find or insert a VariableAlias for the named NetCDF variable.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :return found VariableAlias object or None
    """
    variable_alias = find_variable_alias(sesh, cf, var_name)
    if variable_alias:
        return variable_alias
    return insert_variable_alias(sesh, cf, var_name)


# LevelSet, Level

def find_level_set(sesh, cf, var_name):
    """Find a LevelSet for a named NetCDF variable.
    If the variable has no Z (level) axis, return None.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: name of NetCDF variable
    :return LevelSet object corresponding to the level set for the provided
        variable, None if no level set (variable has no Z axis)
    """
    info = get_level_set_info(cf, var_name)
    if not info:
        return None
    units = info['level_axis_var'].units
    vertical_levels = info['vertical_levels']
    q = (
        sesh.query(LevelSet).join(Level)
        .filter(LevelSet.level_units == units)
        .filter(Level.vertical_level.in_(vertical_levels))
        .group_by(LevelSet.id)
        .having(func.count(Level.vertical_level) == len(vertical_levels))
    )
    return q.first()


def insert_level_set(sesh, cf, var_name):
    """Insert a LevelSet for a named NetCDF variable.
    If the variable has no Z (level) axis, return None.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: name of NetCDF variable
    :return LevelSet object corresponding to the level set for the provided
        variable, None if no level set (variable has no Z axis)
    """
    info = get_level_set_info(cf, var_name)
    if not info:
        return None
    level_set = LevelSet(level_units=info['level_axis_var'].units)
    sesh.add(level_set)

    sesh.add_all(
        [Level(level_set=level_set,
               level_idx=level_idx,
               vertical_level=vertical_level,
               level_start=level_start,
               level_end=level_end,
               ) for level_idx, (level_start, vertical_level, level_end) in
         enumerate(cf.var_bounds_and_values(info['level_axis_var'].name))
         ]
    )

    return level_set


def find_or_insert_level_set(sesh, cf, var_name):  # get.level.set.id
    """Find or insert a LevelSet for a named NetCDF variable.
    If the variable has no Z (level) axis, return None.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: name of NetCDF variable
    :return LevelSet object corresponding to the level set for the provided
        variable, None if no level set (variable has no Z axis)
    """
    level_set = find_level_set(sesh, cf, var_name)
    if level_set:
        return level_set
    return insert_level_set(sesh, cf, var_name)


# Grid, YCellBound

def find_grid(sesh, cf, var_name):
    """Find existing ``Grid`` record corresponding to spatial dimensions of a
    variable in a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable for which to find or insert grid
    :return: (tuple) (grid, info)
        grid: existing ``Grid`` record or else None
        info: dict containing costly information to compute for
            finding/inserting ``Grid`` record
    """
    def approx_equal(attribute, value, relative_tolerance=1e-6):
        """Return a column expression specifying that ``attribute`` and
        ``value`` are equal within a specified relative tolerance.
        Treat the case when value == 0 specially: require exact equality.
        """
        if value == 0.0:
            return attribute == 0.0
        else:
            return (func.abs((attribute - value) / attribute) <
                    relative_tolerance)

    info = get_grid_info(cf, var_name)

    return (sesh.query(Grid)
            .filter(approx_equal(Grid.xc_origin, info['xc_values'][0]))
            .filter(approx_equal(Grid.yc_origin, info['yc_values'][0]))
            .filter(approx_equal(Grid.xc_grid_step, info['xc_grid_step']))
            .filter(approx_equal(Grid.yc_grid_step, info['yc_grid_step']))
            .filter(Grid.xc_count == len(info['xc_values']))
            .filter(Grid.yc_count == len(info['yc_values']))
            .filter(Grid.evenly_spaced_y == info['evenly_spaced_y'])
            .first()
            )


def insert_grid(sesh, cf, var_name):
    """Insert new ``Grid`` record and associated ``YCellBound`` records
    corresponding to spatial dimensions of a variable in a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable for which to find or insert grid
    :return: existing or new ``Grid`` record
    """
    info = get_grid_info(cf, var_name)

    def cell_avg_area_sq_km():
        """Compute the average area of a grid cell, in sq km."""
        # TODO: Move into nchelpers?
        if all(units == 'm'
               for units in [info['xc_var'].units, info['yc_var'].units]):
            # Assume that grid is regular if specified in meters
            return abs(info['xc_grid_step'] * info['yc_grid_step']) / 1e6
        else:
            # Assume lat-lon coordinates in degrees.
            # Assume that coordinate values are in increasing order,
            # i.e., coord[i} < coord[j] for i < j.
            earth_radius = 6371
            y_vals = np.deg2rad(info['yc_values'])
            # TODO: Improve this computation?
            # See https://github.com/pacificclimate/modelmeta/issues/4
            return (
                np.deg2rad(np.abs(info['xc_values'][1] -
                                  info['xc_values'][0])) *
                np.mean(np.diff(y_vals) * np.cos(y_vals[:-1])) *
                earth_radius ** 2
            )

    grid = Grid(
        xc_origin=info['xc_values'][0],
        yc_origin=info['yc_values'][0],
        xc_grid_step=info['xc_grid_step'],
        yc_grid_step=info['yc_grid_step'],
        xc_count=len(info['xc_values']),
        yc_count=len(info['yc_values']),
        evenly_spaced_y=info['evenly_spaced_y'],
        cell_avg_area_sq_km=cell_avg_area_sq_km(),
        xc_units=info['xc_var'].units,
        yc_units=info['yc_var'].units,
    )
    sesh.add(grid)

    if not info['evenly_spaced_y']:
        y_cell_bounds = [YCellBound(
            grid=grid,
            bottom_bnd=bottom_bnd,
            y_center=y_center,
            top_bnd=top_bnd,
        ) for bottom_bnd, y_center, top_bnd in
                         cf.var_bounds_and_values(info['yc_var'].name)]
        sesh.add_all(y_cell_bounds)

    return grid


def find_or_insert_grid(sesh, cf, var_name):
    """Find existing or insert new ``Grid`` record (and associated ``YCellBound``
    records) corresponding to a variable in a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable for which to find or insert grid
    :return: existing or new ``Grid`` record
    """
    grid = find_grid(sesh, cf, var_name)
    if grid:
        return grid
    return insert_grid(sesh, cf, var_name)


# DataFileVariable

def find_data_file_variable(sesh, cf, var_name, data_file):
    """Find existing ``DataFileVariable`` record corresponding to a named
    variable in a NetCDF file and associated to a specified ``DataFile`` record.

    NOTE: Parameter ``cf`` is not used in this function, but it is retained to
    maintain a consistent signature amongst all ``find_`` functions. This is
    useful in testing, although its absence could be accommodated with more
    complex testing code.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :param data_file: (DataFile) data file to associate this dfv to
    :return: existing ``DataFileVariable`` record or None
    """
    q = (sesh.query(DataFileVariable)
         .filter(DataFileVariable.file == data_file)
         .filter(DataFileVariable.netcdf_variable_name == var_name)
         )
    return q.first()


def insert_data_file_variable(
        sesh, cf, var_name, data_file, variable_alias, level_set, grid):
    """Insert a new ``DataFileVariable`` record corresponding to a named
    variable in a NetCDF file and associated to a specified ``DataFile`` record.

    NOTE: Parameter `cf` is not used in this function, but it is retained to
    maintain a consistent signature amongst all `find_` functions. This is
    useful in testing, although its absence could be accommodated with more
    complex testing code.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :param data_file: (DataFile) data file to associate this dfv to
    :param variable_alias: (VariableAlias) variable alias to associate to
        this dfv
    :param level_set: (LevelSet) level set to associate to this dfv
    :param grid: (Grid) grid to associate to this dfv
    :return: inserted DataFileVariable record
    """
    variable = cf.variables[var_name]
    range_min, range_max = cf.var_range(var_name)
    dfv = DataFileVariable(
        file=data_file,
        variable_alias=variable_alias,
        # TODO: verify no value for this and other unspecified attributes
        # derivation_method=,
        variable_cell_methods=variable.cell_methods,
        level_set=level_set,
        grid=grid,
        netcdf_variable_name=var_name,
        range_min=range_min,
        range_max=range_max,
        disabled=False,
    )
    sesh.add(dfv)
    return dfv


def find_or_insert_data_file_variable(sesh, cf, var_name, data_file):
    """Find or insert a DataFileVariable record corresponding to a named
    variable in a NetCDF file and associated to a specified DataFile record.
    If none exists, return None.

    NOTE: Parameter `cf` is not used in this function, but it is retained to
    maintain a consistent signature amongst all `find_` functions. This is
    useful in testing, although its absence could be accommodated with more
    complex testing code.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param var_name: (str) name of variable
    :param data_file: (DataFile) data file to associate this dfv to
    :return: found or inserted DataFileVariable record
    """
    dfv = find_data_file_variable(sesh, cf, var_name, data_file)
    if dfv:
        return dfv
    variable_alias = find_or_insert_variable_alias(sesh, cf, var_name)
    assert variable_alias
    level_set = find_or_insert_level_set(sesh, cf, var_name)
    grid = find_or_insert_grid(sesh, cf, var_name)
    assert grid
    return insert_data_file_variable(
        sesh, cf, var_name, data_file, variable_alias, level_set, grid)


def find_or_insert_data_file_variables(sesh, cf, data_file):
    """Find or insert DataFileVariables for all dependent variables in a
    NetCDF file, associated to a specified DataFile record.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param data_file: (DataFile) data file to associate this dfv to
    :return: list of found or inserted DataFileVariable records
    """
    return [find_or_insert_data_file_variable(sesh, cf, var_name, data_file)
            for var_name in cf.dependent_varnames()]


# Timeset, Time, ClimatologicalTime

def find_timeset(sesh, cf):
    """Find existing ``TimeSet`` record corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing ``TimeSet`` record or None
    """
    start_date, end_date = to_datetime(
        num2date(cf.nominal_time_span, cf.time_var.units, cf.time_var.calendar)
    )

    # Check for existing TimeSet matching this file's set of time values
    # TODO: Verify encoding for TimeSet.calendar the same as for
    # cf.time_var.calendar
    return (
        sesh.query(TimeSet)
            .filter(TimeSet.start_date == start_date)
            .filter(TimeSet.end_date == end_date)
            .filter(TimeSet.multi_year_mean == cf.is_multi_year_mean)
            .filter(TimeSet.time_resolution == cf.time_resolution)
            .filter(TimeSet.num_times == int(cf.time_var.size))
            .filter(TimeSet.calendar == cf.time_var.calendar)
            .first()
    )


def insert_timeset(sesh, cf):
    """Insert new ``TimeSet`` record and associated ``Time`` and
    ``ClimatologicalTime`` records corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: new ``TimeSet`` record
    """
    start_date, end_date = to_datetime(
        num2date(cf.nominal_time_span, cf.time_var.units, cf.time_var.calendar)
    )

    time_set = TimeSet(
        calendar=cf.time_var.calendar,
        start_date=start_date,
        end_date=end_date,
        multi_year_mean=cf.is_multi_year_mean,
        num_times=int(cf.time_var.size),  # convert from numpy representation
        time_resolution=cf.time_resolution,
    )
    sesh.add(time_set)

    # TODO: Factor out inserts for Time and ClimatologicalTime as separate
    # functions

    times = [Time(
        timeset=time_set,
        time_idx=time_idx,
        timestep=timestep,
    ) for time_idx, timestep
             in enumerate(to_datetime(cf.time_steps['datetime']))]
    sesh.add_all(times)

    if cf.is_multi_year_mean:
        climatology_bounds = to_datetime(
            num2date(cf.climatology_bounds_values,
                     cf.time_var.units, cf.time_var.calendar)
        )
        climatological_times = [ClimatologicalTime(
            timeset=time_set,
            time_idx=time_idx,
            time_start=time_start,
            time_end=time_end,
        ) for time_idx, (time_start, time_end) in enumerate(climatology_bounds)]
        sesh.add_all(climatological_times)

    return time_set


def find_or_insert_timeset(sesh, cf):
    """Find existing or insert new ``TimeSet`` record (and associated `
    `Time`` and ``ClimatologicalTime`` records) corresponding to a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new ``TimeSet`` record
    """
    time_set = find_timeset(sesh, cf)
    if time_set:
        return time_set
    return insert_timeset(sesh, cf)


# DataFile

def find_data_file_by_id_hash_filename(sesh, cf):
    """Find and return DataFile records matching file unique id, file hash,
    and filename.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: tuple of DataFiles matching unique id, hash, filename
        (None in a component if no match)
    """
    q = sesh.query(DataFile).filter(DataFile.unique_id == cf.unique_id)
    id_match = q.first()
    q = (
        sesh.query(DataFile)
            .filter(DataFile.first_1mib_md5sum == cf.first_MiB_md5sum))
    hash_match = q.first()
    q = (
        sesh.query(DataFile)
            .filter(DataFile.filename ==
                    cf.filepath(converter=filepath_converter))
    )
    filename_match = q.first()
    return id_match, hash_match, filename_match


def insert_data_file(sesh, cf):  # create.data.file.id
    """Insert a DataFile (but no associated records) representing a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: inserted DataFile
    """
    logger.info("Creating new DataFile for unique_id {}".format(cf.unique_id))
    # TODO: Parametrize on timeset, run; like run on model, emission
    timeset = find_or_insert_timeset(sesh, cf)
    assert timeset
    run = find_or_insert_run(sesh, cf)
    assert run
    dim_names = cf.axes_dim()

    df = DataFile(
        filename=cf.filepath(converter=filepath_converter),
        first_1mib_md5sum=cf.first_MiB_md5sum,
        unique_id=cf.unique_id,
        index_time=datetime.datetime.utcnow(),
        run=run,
        timeset=timeset,
        x_dim_name=dim_names.get('X', None),
        y_dim_name=dim_names.get('Y', None),
        z_dim_name=dim_names.get('Z', None),
        t_dim_name=dim_names.get('T', None)
    )
    sesh.add(df)
    return df


def delete_data_file(sesh, existing_data_file):
    """Delete existing ``DataFile`` object, associated ``DataFileVariable``s,
    and the associations of those ``DataFileVariable``s to ``Ensemble``s
    (via object ``EnsembleDataFileVariables``).
    Existing ``Ensemble``s are preserved.

    :param sesh: modelmeta database session
    :param existing_data_file: DataFile object representing data file to be
        deleted and re-inserted
    """
    logger.info("Deleting DataFile for unique_id '{}'"
                .format(existing_data_file.unique_id))
    # TODO: Also delete associations with `QCFlag`s?
    # (via `DataFileVariablesQcFlag`)
    existing_data_file_variables = existing_data_file.data_file_variables
    existing_ensemble_data_file_variables = (
        sesh.query(EnsembleDataFileVariables)
            .filter(EnsembleDataFileVariables.data_file_variable_id.in_(
                [edfv.id for edfv in existing_data_file_variables]
            ))
    )
    for obj in existing_ensemble_data_file_variables:
        sesh.delete(obj)
    for obj in existing_data_file_variables:
        sesh.delete(obj)
    sesh.delete(existing_data_file)


# Root functions

def update_data_file_index_time(sesh, data_file):
    """Update the index time recorded for data_file"""
    logger.info('Updating index time (only)')
    data_file.index_time = datetime.datetime.utcnow()
    return data_file


def update_data_file_filename(sesh, data_file, cf):
    """Update the filename recorded for data_file with the cf filename."""
    logger.info('Updating filename (only)')
    data_file.filename = cf.filepath(converter=filepath_converter)
    return data_file


def index_cf_file(sesh, cf):
    """Insert records for a NetCDF known not to be in the database yet.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file
    """
    data_file = insert_data_file(sesh, cf)
    find_or_insert_data_file_variables(sesh, cf, data_file)
    return data_file


def reindex_cf_file(sesh, existing_data_file, cf):
    """Delete the existing modelmeta content for a data file and insert it
    again de novo.
    Return the new DataFile object.

    :param sesh: modelmeta database session
    :param existing_data_file: DataFile object representing data file to be
        deleted and re-inserted
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file
    """
    logger.info('Reindexing file')
    delete_data_file(sesh, existing_data_file)
    return index_cf_file(sesh, cf)


def find_update_or_insert_cf_file(sesh, cf):  # get.data.file.id
    """Find, update, or insert a NetCDF file in the modelmeta database,
    according to whether it is already present and up to date.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file

    The algorithm is a sequence of tests for conditions corresponding to what
    relation the input NetCDF file may bear to the existing database: e.g.,
    a new file, an already-known file, a modified file, etc. This sequence
    deliberately avoids nesting if statements, which has proven confusing and
    hard to maintain. The flip side of this choice is that we may not have
    exhausted all possible cases. This situation is signalled by the final
    statements after all the if statements.
    """
    logger.info('Processing file: {}'
                .format(cf.filepath(converter=filepath_converter)))
    id_match, hash_match, filename_match = \
        find_data_file_by_id_hash_filename(sesh, cf)

    def log_data_files(log):
        def log_data_file(label, df):
            log('{}.id = {}'.format(label, df and df.id))
        log_data_file('id_match', id_match)
        log_data_file('hash_match', hash_match)
        log_data_file('filename_match', filename_match)

    matches = tuple(df for df in (id_match, hash_match, filename_match) if df)

    # new file
    if len(matches) == 0:
        return index_cf_file(sesh, cf)

    # multiple entries for same file: more than one match, but they are
    # not all the same
    if len(set(matches)) != 1:
        logger.error('Multiple entries for same file, not all the same:')
        log_data_files(logger.error)
        raise ValueError('Multiple entries for same file, not all the same. '
                         'See log for details.')

    # At this point, we know that all matches are the same DataFile object,
    # so the following values are valid and consistent for all cases.
    data_file = id_match or hash_match or filename_match
    old_filename_exists = os.path.isfile(data_file.filename)
    # To make testing easier, we call ``os.path.realpath`` explicitly here
    # rather than delegating it to ``cf.filepath()`` as everywhere else.
    normalized_filenames_match = \
        os.path.realpath(data_file.filename) == os.path.realpath(cf.filepath())
    cf_modification_time = os.path.getmtime(cf.filepath())
    data_file_index_time = seconds_since_epoch(data_file.index_time)
    index_up_to_date = data_file_index_time > cf_modification_time

    def skip_file(reason):
        logger.info('Skipping file: {}'.format(reason))
        return data_file

    # same file
    if id_match and hash_match and filename_match and \
        id_match == hash_match == filename_match and \
            index_up_to_date:
        return update_data_file_index_time(sesh, data_file)

    # symlinked file (modified or not)
    if (id_match and not filename_match and old_filename_exists and
            normalized_filenames_match):
        return skip_file('file is symlink to an indexed file')

    # copy of file
    if (id_match and hash_match and not filename_match and
            old_filename_exists and not normalized_filenames_match):
        return skip_file('file is a copy of an indexed file')

    # moved file
    if (id_match and hash_match and not filename_match and
            not old_filename_exists and index_up_to_date):
        return update_data_file_filename(sesh, data_file, cf)

    # indexed under different unique id
    if not id_match and hash_match and filename_match:
        return skip_file(
            'file already already indexed under different unique id')

    # modified file (hash changed)
    if id_match and not hash_match and filename_match:
        return reindex_cf_file(sesh, data_file, cf)

    # modified file (modification time changed, but not hash?)
    if id_match and filename_match and not index_up_to_date:
        return reindex_cf_file(sesh, data_file, cf)

    # moved and modified file (hash changed)
    if (id_match and not hash_match and not filename_match and
            not old_filename_exists):
        return reindex_cf_file(sesh, data_file, cf)

    # moved and modified file (modification time changed)
    if (id_match and not filename_match and not old_filename_exists and
            not index_up_to_date):
        return reindex_cf_file(sesh, data_file, cf)

    # Oops, missed something. We think this won't happen, but ...
    logger.error('Encountered an unanticipated case:')
    log_data_files(logger.error)
    logger.error(
        'old_filename_exists = {}; '
        'normalized_filenames_match = {}; '
        'index_up_to_date = {}'
        .format(old_filename_exists,
                normalized_filenames_match,
                index_up_to_date)
    )
    raise ValueError('Unanticipated case. See log for details.')


def index_netcdf_file(filename, Session):
    """Index a NetCDF file: insert or update records in the modelmeta database
    that identify it.

    :param filename: file name of NetCDF file
    :param Session: database session factory for access to modelmeta database
    :return: database id (``DataFile.id``) for file indexed
    """
    session = Session()
    data_file_id = None
    try:
        with CFDataset(filename) as cf:
            data_file = find_update_or_insert_cf_file(session, cf)
            data_file_id = data_file.id
        session.commit()
    except:
        logger.error(traceback.format_exc())
        session.rollback()
    finally:
        session.close()
    return data_file_id


def index_netcdf_files(filenames, dsn):
    """Index a list of NetCDF files into a modelmeta database.

    :param filenames: list of files to index
    :param dsn: connection info for the modelmeta database to update
    :return: list of DataFile objects for each file indexed
    """
    engine = create_engine(dsn)
    Session = sessionmaker(bind=engine)

    return [index_netcdf_file(f, Session) for f in filenames]
