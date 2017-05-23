import re
import hashlib
import logging
import datetime
import math

import numpy as np
from netCDF4 import Dataset, num2date
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset
from modelmeta import Model, Run, Emission, DataFile, TimeSet, Time, ClimatologicalTime, DataFileVariable, \
    VariableAlias, LevelSet, Level, Grid, YCellBound


formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def index_netcdf_files(filenames, dsn):
    '''Index a list of NetCDF files into a modelmeta database

    :param filenames: list of files to index
    :param dsn: connection info for the modelmeta database to update
    :return: generator yielding database id's of files indexed
    '''
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()

    return (index_netcdf_file(f, session) for f in filenames)


def index_netcdf_file(filename, session):  # index.netcdf
    '''Index a NetCDF file: insert or update records in the modelmeta database that identify it.

    :param filename: file name of NetCDF file
    :param session: database session for access to modelmeta database
    :return: database id of file indexed
    '''
    with CFDataset(filename) as cf:
        id = find_update_or_insert_file(session, cf)
    return id


def find_update_or_insert_file(sesh, cf):  # get.data.file.id
    '''Find, update, or insert a NetCDF file in the modelmeta database, according to whether it is
    already present and up to date.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: database id (primary key into DataFile) of file
    '''
    id_data_file, hash_data_file = find_data_file_by_unique_id_and_hash(sesh, cf)

    if id_data_file and hash_data_file:
        if id_data_file == hash_data_file:
            logger.info("Skipping file {}. Already in the db as id {}.".format(cf.filepath(), id_data_file.id))
        else:
            logger.error("Split brain! We seem to have file {} in the database under multiple entries: data_file_id {} and {}".format(cf.filepath(), id_data_file.id, hash_data_file.id))
        return id_data_file.id

    # File changed. Do an update.
    elif id_data_file and not hash_data_file:
        update_data_file(sesh, cf, id_data_file)
        return id_data_file.id

    # We've indexed this file under a different unique id. Warn and skip.
    elif not id_data_file and hash_data_file:
        logger.warning("Skipping file {}. Already in the db under unique id {}.".format(cf.filepath(), hash_data_file.unique_id))
        return hash_data_file.id

    # Nothing is in the db yet. Our raison d'être. Do the insertion.
    else:
        data_file = insert_data_file(sesh, cf)
        find_or_insert_data_file_variables(sesh, data_file, cf)
        return data_file.id


def update_data_file(sesh, nc, datafile):
    pass


def find_data_file_by_unique_id_and_hash(sesh, cf):
    '''Find and return DataFile records matching file unique id and file hash.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: pair of DataFiles matching unique id, hash (None in a component if no match)
    '''
    q = sesh.query(DataFile).filter(DataFile.unique_id == cf.unique_id)
    id_match = q.first()
    q = sesh.query(DataFile).filter(DataFile.first_1mib_md5sum == cf.first_MiB_md5sum)
    hash_match = q.first()
    return id_match, hash_match


def insert_data_file(sesh, cf):  # create.data.file.id
    timeset = find_or_insert_timeset(sesh, cf)
    run = find_or_insert_run(sesh, cf)
    dim_names = cf.dim_axes_from_names()
    logger.info("Creating new DataFile for unique_id {}".format(cf.unique_id))

    df = DataFile(
        filename=cf.filepath(),
        first_1mib_md5sum=cf.first_MiB_md5sum,
        unique_id=cf.unique_id,
        index_time=datetime.datetime.now(),
        run=run,
        timeset=timeset,
        x_dim_name=dim_names.get('X', None),
        y_dim_name=dim_names.get('Y', None),
        z_dim_name=dim_names.get('Z', None),
        t_dim_name=dim_names.get('T', None)
    )
    sesh.add(df)
    sesh.commit()
    return df


def find_or_insert_data_file_variables(sesh, data_file, cf):  # create.data.file.variables
    '''Find or insert modelmeta `DataFileVariable`s for the NetCDF file.
    One DataFileVariable is found or inserted for every variable in the NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param data_file: DataFile to attach variables to
    :return: list of DataFileVariable found or inserted
    '''
    dfvs = []
    for var_name, variable in cf.variables.items():
        dfv = sesh.query(DataFileVariable)\
            .filter(DataFileVariable.file == data_file)\
            .filter(DataFileVariable.netcdf_variable_name == var_name)\
            .first()
        if not dfv:
            variable_alias = find_or_insert_variable_alias(sesh, variable)
            level_set = find_or_insert_level_set(sesh, cf, variable)
            grid = find_or_insert_grid(sesh, cf, variable)
            range_min, range_max = get_variable_range(variable)
            dfv = DataFileVariable(
                file=data_file,
                variable_alias=variable_alias,
                # derivation_method=,  # TODO: verify no value for this and other unspecified attributes
                variable_cell_methods=variable.cell_method,
                level_set=level_set,
                grid=grid,
                netcdf_variable_name=var_name,
                range_min=range_min,
                range_max=range_max,
                disabled=False,
            )
            sesh.add(dfv)
            sesh.commit()  # should this be outside loop?
        dfvs.append(dfv)
    return dfvs


def find_or_insert_variable_alias(sesh, variable):  # get.variable.alias.id
    variable_alias = sesh.query(VariableAlias)\
        .filter(VariableAlias.variable_long_name == variable.long_name)\
        .filter(VariableAlias.variable_standard_name == variable.standard_name)\
        .filter(VariableAlias.variable_units == variable.units)\
        .first()
    if not variable_alias:
        variable_alias = VariableAlias(
            variable_long_name=variable.long_name,
            variable_standard_name=variable.standard_name,
            variable_units=variable.units,
        )
        sesh.add(variable_alias)
        sesh.commit()
    return variable_alias


def find_or_insert_level_set(sesh, cf, variable):  # get.level.set.id
    '''Find or insert a LevelSet matching a provided NetCDF variable defining a level axis.
    PROBLEM!!! Is the level set associated to `variable` or is it the variable itself (if a level var)?

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param variable: NetCDF variable (in `cf`) that
    '''
    # Find the level dimension if it exists
    # Note: This code is VERY different from the gnarly corresponding R code
    axis_to_dim_name = cf.dim_axes_from_names(variable.dimensions)  # TODO: See PROBLEM above
    level_axis_dim_name = axis_to_dim_name.get('Z', None)

    if not level_axis_dim_name:
        return None
    level_axis_var = cf.variables[level_axis_dim_name]

    # Find LevelSet corresponding to the level axis
    vertical_levels = level_axis_var[:]
    # TODO: WTF? R code queries on non-existent column `pressure_level`:
    #
    #   query <- paste("SELECT level_set_id ",
    #                  "FROM levels NATURAL JOIN level_sets ",
    #                  "WHERE pressure_level IN (",
    #                  paste(levels, collapse=",", sep=","),
    #                  ") AND level_units = '", levels.dim$units, "' ",
    #                  "GROUP BY level_set_id ",
    #                  "HAVING count(vertical_level)=", length(levels), ";", sep="")
    #
    # NB: R variable 'levels' corresponds to this function's 'vertical_levels'
    #
    # Rewritten:
    #   SELECT ls.level_set_id
    #   FROM
    #       levels AS l
    #       NATURAL JOIN level_sets AS ls
    #   WHERE l.pressure_level (??) IN (<vertical_levels>)
    #   AND ls.level_units = <level_axis_var.units>
    #   GROUP BY ls.level_set_id
    #   HAVING count(l.vertical_level) = <len(vertical_levels)>
    #
    # Apparent meaning:
    #   select level sets
    #   such that the aggregate of all levels associated to a given level set match all levels in NC file
    #
    # TODO: Verify pro-tem assumption that pressure_level should be vertical_level.
    # TODO: Verify that the following query works (levels comparison). Requires that LevelSet.levels relationship is
    #   ordered by Level.vertical_level (ORM declaration now modified accordingly)
    level_set = sesh.query(LevelSet)\
        .filter(LevelSet.levels == vertical_levels)\
        .filter(LevelSet.units == level_axis_var.units)\
        .first()

    # Insert appropriate level set (with accompanying levels) if it does not exist
    if not level_set:
        level_set = LevelSet(units=level_axis_var.units)
        sesh.add(level_set)

        sesh.add_all([
                         Level(
                 level_set=level_set,
                 vertical_level=vertical_level,
                 level_start=level_start,
                 level_end=level_end,
             ) for level_start, vertical_level, level_end in get_var_bounds_and_values(cf, level_axis_var)
                         ])
        sesh.commit()

    return level_set


def get_var_bounds_and_values(cf, variable, bounds_var_name=None):  # get.bnds.center.array
    '''Return a list of tuples describing the bounds and values of a NetCDF variable.
    One tuple per variable value, defining (lower_bound, value, upper_bound)

    :param cf: CFDatafile object representing NetCDF file
    :param variable: NetCDF variable that is an level axis
    :param bounds_var_name: name of bounds variable; if not specified, use level_axis_var.bounds
    :return: list of tuples of the form (lower_bound, value, upper_bound)
    '''
    values = variable[:]

    bounds_var_name = bounds_var_name or getattr(variable, 'bounds', None)
    if bounds_var_name:
        # Explicitly defined bounds: use them
        bounds_var = cf.variables.get[bounds_var_name]
        return zip(bounds_var[1,:], values, bounds_var[2, :])
    else:
        # No explicit bounds: manufacture them
        midpoints = (
            [(3*values[0] - values[1]) / 2] +   # fake lower "midpoint", half of previous step below first value
            [(values[i] + values[i+1]) / 2 for i in range(len(values)-1)] +
            [(3*values[-1] - values[-2]) / 2]   # fake upper "midpoint", half of previous step above last value
        )
        return zip(midpoints[:-1], values, midpoints[1:])


def find_or_insert_grid(sesh, cf, variable):  # get.grid.id
    '''Find existing or insert new Grid record (and associated YCellBound records) corresponding to
    a variable in a NetCDF file.
    
    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new Grid record
    '''
    dim_names_to_axes = cf.dim_axes(variable.name)
    axes_to_dim_names = {axis: dim_name for dim_name, axis in dim_names_to_axes.items()}

    if not all (axis in axes_to_dim_names for axis in 'XY'):
        return None

    if 'S' in axes_to_dim_names:
        source = cf.reduced_dims(variable.name)
    else:
        source = axes_to_dim_names

    xc_var, yc_var = (cf.variables[source[axis]] for axis in 'XY')
    xc_values, yc_values = (var[:] for var in [xc_var, yc_var])
    evenly_spaced_y = is_regular_dimension(yc_values)

    def mean_step_size(values):
        '''Return mean of differences between successive elements of values list'''
        return np.mean(np.diff(values))

    xc_grid_step, yc_grid_step = (mean_step_size(values) for values in [xc_values, yc_values])
    xc_origin, yc_origin = (values[0] for values in [xc_values, yc_values])

    def approx_equal(attribute, value, relative_tolerance=1e-6):
        '''Return a column expression specifying that `attribute` and `value` are within a specified relative tolerance.
        Treat the case when value == 0 specially: require exact equality.
        '''
        if value == 0.0:
            return attribute == 0.0
        else:
            return func.abs((attribute - value) / attribute < relative_tolerance)

    grid = (sesh.query(Grid)
            .filter(approx_equal(Grid.xc_origin, xc_origin))
            .filter(approx_equal(Grid.yc_origin, yc_origin))
            .filter(approx_equal(Grid.xc_grid_step, xc_grid_step))
            .filter(approx_equal(Grid.yc_grid_step, yc_grid_step))
            .filter(Grid.xc_count == len(xc_values))
            .filter(Grid.yc_count == len(yc_values))
            .filter(Grid.evenly_spaced_y == evenly_spaced_y)
            .first()
            )

    def cell_avg_area_sq_km():
        if all(units == 'm' for units in [xc_var.units, yc_var.units]):
            # Assume that grid is regular if specified in meters
            return abs(xc_grid_step * yc_grid_step) / 1e6  # TODO: Error in original R script: '/ 10e6'
        else:
            # Assume lat-lon coordinates in degrees for now
            earth_radius = 6371
            y_vals = np.radians(yc_values)
            return np.radians(xc_values[0] - xc_values[1]) * \
                   np.mean(np.diff(y_vals) * np.cos(y_vals[:-1])) * \
                   earth_radius ** 2

    if not grid:
        grid = Grid(
            xc_origin=xc_origin,
            yc_origin=yc_origin,
            xc_grid_step=xc_grid_step,
            yc_grid_step=yc_grid_step,
            xc_count=len(xc_values),
            yc_count=len(yc_values),
            evenly_spaced_y=evenly_spaced_y,
            cell_avg_area_sq_km=cell_avg_area_sq_km(),
            xc_units=xc_var.units,
            yc_units=yc_var.units,
        )
        sesh.add(grid)

        if not evenly_spaced_y:
            y_cell_bounds = [YCellBound(
                grid=grid,
                bottom_bnd=bottom_bnd,
                y_center=y_center,
                top_bound=top_bound,
            ) for bottom_bnd, y_center, top_bound in get_var_bounds_and_values(variable)]
            sesh.add_all(y_cell_bounds)

        sesh.commit()
        
    return grid


def find_or_insert_timeset(sesh, cf):
    '''Find existing or insert new TimeSet record (and associated Time and ClimagologicalTime records)
    corresponding a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new TimeSet record
    '''
    start_date, end_date = cf.time_range_as_dates

    # Check for existing TimeSet matching this file's set of time values
    # TODO: Is the encoding for TimeSet.calendar the same as for cf.time_var.calendar?
    time_set = sesh.query(TimeSet)\
        .filter(TimeSet.start_date == start_date)\
        .filter(TimeSet.end_date == end_date) \
        .filter(TimeSet.multi_year_mean == cf.is_multi_year_mean) \
        .filter(TimeSet.time_resolution == cf.time_resolution)\
        .filter(TimeSet.num_times == cf.time_var.size)\
        .filter(TimeSet.calendar == cf.time_var.calendar)\
        .first()

    if time_set:
        return time_set

    # No matching TimeSet: Create new TimeSet, Time, and ClimatologicalTime records

    # TODO: Convert time values in case of 360_day calendar? See R script, ll. 468-478.
    time_set = TimeSet(
        calendar=cf.time_var.calendar,
        start_date=start_date,
        end_date=end_date,
        multi_year_mean=cf.is_multi_year_mean,
        num_times=cf.time_var.size,
        time_resolution=cf.time_resolution
    )
    sesh.add(time_set)

    times = [Time(
        time_idx=time_idx,
        timestep=timestep,
        timeset=time_set
    ) for time_idx, timestep in enumerate(cf.time_var_values)]
    sesh.add(times)

    if cf.is_multi_year_mean:
        climatology_bounds = cf.variable[cf.climatology_bounds_var_name][:]
        climatological_times = [ClimatologicalTime(
            time_set_id=time_set.id,
            time_idx=time_idx,
            time_start=bounds[0],
            time_end=bounds[1],
        ) for time_idx, bounds in enumerate(climatology_bounds)]
        sesh.add(climatological_times)

    sesh.commit()

    return time_set


def find_emission_nc(sesh, nc):
    meta = get_file_metadata(nc)
    return find_emission(sesh, meta['emission'])


def find_emission(sesh, short_name):
    q = sesh.query(Emission).filter(Emission.short_name == short_name)
    return q.first()


def insert_emission(sesh, name):
    emission = Emission(short_name=name)
    sesh.add(emission)
    return emission


def find_or_insert_emission(sesh, nc):
    meta = get_file_metadata(nc)
    emission = find_emission(sesh, meta['emission'])
    if emission:
        return emission
    else:
        return insert_emission(sesh, meta['emission'])


def find_run_nc(sesh, nc):
    meta = get_file_metadata(nc)
    return find_run(sesh, meta['run'], meta['model'], meta['emission'])


def find_run(sesh, run_name, model_name, emissions_name):
    q = sesh.query(Run).join(Model).join(Emission)\
        .filter(Model.name == model_name)\
        .filter(Emission.name == emissions_name)\
        .filter(Run.name == run_name)
    return q.first()


def insert_run(sesh, run_name, model, emission, project):
    run = Run(name=run_name, model=model, emission=emission, project=project)
    sesh.add(run)
    sesh.commit()
    return run


def find_or_insert_run(sesh, nc):
    meta = get_file_metadata(nc)
    run = find_run(sesh, meta['run'], meta['model'], meta['emission'])
    if run:
        return run
    model = find_or_insert_model(sesh, nc)
    emission = find_or_insert_emission(sesh, nc)
    if not model and emission:
        # FIXME:
        raise Exception('Badness in find_or_insert_run')
    return insert_run(sesh, meta['run'], model, emission, meta['project'])


required_nc_attributes_msg = \
    'In order to insert a climate model into this database, a base amount of '\
    'metadata must exist. A NetCDF file must have the following global '\
    'attributes: {}. The file {} is missing {} and without them, we cannot '\
    'proceed. Please correct this by using the ncatted program (or your '\
    'favorite NetCDF attribute editor, and do familiarize yourself with the '\
    'CMIP5 model output requirements: '\
    'http://cmip-pcmdi.llnl.gov/cmip5/docs/CMIP5_output_metadata_requirements.pdf'


global_to_res_map_cmip5 = {
    'institute_id': 'institution',
    'model_id': 'model',
    'experiment_id': 'emissions',
    'parent_experiment_rip': 'run',
    'project_id': 'project'
}


global_to_res_map_cmip3 = {
    'institute': 'institution',
    'source': 'model',
    'experiment_id': 'emissions',
    'realization': 'run',
    'project_id': 'project'
}


def _get_file_metadata(nc, map_):
    missing = []
    required = map_.keys()
    for key in required:
        if not hasattr(nc, key):
            missing.append(key)
    if missing:
        raise ValueError(required_nc_attributes_msg.format(required, nc.filepath(), missing))

    return {
        to_: getattr(nc, from_)
        for from_, to_ in map_.items()
    }


def format_time_range(min_, max_, resolution):
    map_ = {'yearly': '%Y', 'monthly': '%Y%m', 'daily': '%Y%m%d'}
    if resolution not in map_:
        raise ValueError("I'm not sure how to format a time range with "
                         "resolution '{}' (only yearly, monthly or "
                         "daily)".format(resolution))
    fmt = map_[resolution]

    return '{}-{}'.format(min_.strftime(fmt), max_.strftime(fmt))


def get_file_metadata(nc):
    # Query global attributes from the NetCDF file
    if nc.project_id == 'CMIP5':
        meta = _get_file_metadata(nc, global_to_res_map_cmip5)
    else:
        meta = _get_file_metadata(nc, global_to_res_map_cmip3)

    # Which variable(s) does this file contain?
    meta['var'] = '+'.join(get_important_varnames(nc))

    # Compute time metadata from the time value
    time = get_timeseries(nc)
    meta['tres'] = get_time_resolution(time['numeric'], time['units'])
    tmin, tmax = get_time_range(nc)
    tmin, tmax = num2date([tmin, tmax], time['units'], time['calendar'])
    meta['trange'] = format_time_range(tmin, tmax, meta['tres'])

    return meta


def find_model_nc(sesh, nc):
    meta = get_file_metadata(nc)
    return find_model(sesh, meta['model'])


def find_model(sesh, short_name):
    query = sesh.query(Model).filter(Model.short_name == short_name)
    return query.first()


def insert_model(sesh, short_name, type_, organization):
    m = Model(short_name=short_name, type=type_, organization=organization)
    sesh.add(m)
    sesh.commit()
    return m


def find_or_insert_model(sesh, nc):
    res = find_model_nc(sesh, nc)
    if not res:
        meta = get_file_metadata(nc)
        # Really rudimentary GCM/RCM decision making.
        if (meta['project'] == 'NARCCAP') or \
           (meta['project'] not in ('IPCC Fourth Assessment', 'CMIP5')):
            model_type = 'RCM'
        else:
            model_type = 'GCM'
        org = meta['institution']
        res = insert_model(sesh, meta['model'], model_type, org)
    return res


def nc_get_dim_axes_from_names(nc, dim_names=None):
    if not dim_names:
        dim_names = nc_get_dim_names(nc)
    map_ = {
        'lat': 'Y',
        'latitude': 'Y',
        'lon': 'X',
        'longitude': 'X',
        'xc': 'X',
        'yc': 'Y',
        'x': 'X',
        'y': 'Y',
        'time': 'T',
        'timeofyear': 'T',
        'plev': 'Z',
        'lev': 'Z',
        'level': 'Z'
    }
    return {map_[dim]: dim for dim in dim_names if dim in map_}


def nc_get_dim_names(nc, var_name=None):
    if var_name:
        return nc.variables[var_name].dimensions
    else:
        return tuple(k for k in nc.dimensions.keys())


def nc_get_dim_axes(nc, dim_names=None):
    if not dim_names:
        dim_names = nc_get_dim_names(nc)

    if len(dim_names) == 0:
        return {}

    # Start with our best guess
    dim_axes = nc_get_dim_axes_from_names(nc, dim_names)

    # Then fill in the rest from the 'axis' attributes
    for dim in dim_axes.keys():
        if dim in nc.dimensions and dim in nc.variables \
                and hasattr(nc.variables[dim], 'axis'):
            dim_axes[dim] = nc.variables[dim].axis
            
            # Apparently this is how a "space" dimension is attributed?
            if hasattr(nc.variables[dim], 'compress'):
                dim_axes[dim] = 'S'

    return {ax: dim for dim, ax in dim_axes.items()}


def get_climatology_bounds_var_name(nc):
    axes = nc_get_dim_axes(nc)
    if 'T' in axes:
        time_axis = axes['T']
    else:
        return None

    if 'climatology' in nc.variables[time_axis]:
        return nc.variables[time_axis].climatology
    else:
        return None


def is_multi_year_mean(nc):
    '''Returns True if the netcdf metadata provided indicates that the
    data consists of a multi-year mean
    '''
    return bool(get_climatology_bounds_var_name(nc))


def get_time_step_size(time_series, cf_units='days since 1950-01-01'):

    match = re.match('(days|hours|minutes|seconds) since.*', cf_units)
    if match:
        scale = match.groups()[0]
    else:
        raise ValueError("cf_units param must be a string of the form '<time units> since <reference time>'")
    med = np.median(np.diff(time_series))
    return time_to_seconds(med, scale)


def time_to_seconds(x, units='seconds'):
    map_ = {
        'seconds': 1,
        'minutes': 60,
        'hours': 3600,
        'days': 86400,
    }
    if units in map_:
        return x * map_[units]
    else:
        raise ValueError("No conversions available for unit '{}'"
                         .format(units))


def get_time_resolution(time_series, cf_units='days since 1950-01-01'):
    '''Returns the appropriate time resolution string for the given data
    '''
    #if is_multi_year_mean(nc):
    #    return 'other'

    step_size_seconds = get_time_step_size(time_series, cf_units)
    return convert_time_resolution_string(step_size_seconds)


def convert_time_resolution_string(seconds):
    '''Returns a string given a time resolution in seconds'''
    map_ = {
        60: '1-minute',
        120: '2-minute',
        300: '5-minute',
        900: '15-minute',
        1800: '30-minute',
        3600: '1-hourly',
        10800: '3-hourly',
        21600: '6-hourly',
        43200: '12-hourly',
        86400: 'daily',
        2678400: 'monthly',
        2635200: 'monthly',
        2592000: 'monthly',
        31536000: 'yearly',
        31104000: 'yearly',
    }
    if seconds in map_:
        return map_[seconds]
    else:
        return 'other'


def get_timeseries(nc):
    axes = nc_get_dim_axes_from_names(nc)
    if 'T' in axes:
        time_axis = axes['T']
    else:
        raise ValueError("No axis is attributed with time information")

    t = nc.variables[time_axis]

    assert hasattr(t, 'units') and hasattr(t, 'calendar')

    return {
        'units': t.units,
        'calendar': t.calendar,
        'numeric': t[:],
        'datetime': num2date(t[:], t.units, t.calendar)
    }


def get_time_range(nc):
    t = get_timeseries(nc)['numeric']
    return np.min(t), np.max(t)


def get_first_MiB_md5sum(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        m.update(f.read(2**20))
    return m.digest()


def get_important_varnames(nc):
    '''Returns a list of the primary variables in the netcdf file

    Many of the variables in a NetCDF file describe the *structure* of
    the data and aren't necessarily the values that we actually care
    about. For example a file with temperature data also has to
    include latitude/longitude variables, a time variable, and
    possibly bounds variables for each of the dimensions.

    This function filters out the names of all of the dimensions and
    bounds variables and just gives you the "important" variable names
    (for some value of "important").
    '''
    vars_ = set(nc.variables.keys())
    dims = set(nc.dimensions.keys())
    return [v for v in vars_ - dims if 'bnds' not in v]


def compute_unique_id(nc):
    '''Computes and returns a metadata-based unique id on a NetCDF file'''
    meta = get_file_metadata(nc)
    dim_axes = set(nc_get_dim_axes_from_names(nc).keys())
    # if the file has dims X, Y, T and optionally Z
    if dim_axes == {'X', 'Y', 'T'} or dim_axes == {'X', 'Y', 'Z', 'T'}:
        # we won't put it in the unique id
        meta['axes'] = ''
    else:
        # Otherwise, we will b/c it's a "weird" file
        meta['axes'] = "_dim" + ''.join(sorted(dim_axes))

    return '{var}_{tres}_{model}_{emissions}_{run}_{trange}{axes}'\
        .format(**meta).replace('+', '-')
