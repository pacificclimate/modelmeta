import os
import hashlib
import logging
import datetime

import numpy as np
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset
from modelmeta import Model, Run, Emission, DataFile, TimeSet, Time, ClimatologicalTime, DataFileVariable, \
    VariableAlias, LevelSet, Level, Grid, YCellBound, EnsembleDataFileVariables


formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def index_netcdf_files(filenames, dsn):
    """Index a list of NetCDF files into a modelmeta database.

    :param filenames: list of files to index
    :param dsn: connection info for the modelmeta database to update
    :return: generator yielding DataFile objects for each file indexed
    """
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()

    return (index_netcdf_file(f, session) for f in filenames)


def index_netcdf_file(filename, session):  # index.netcdf
    """Index a NetCDF file: insert or update records in the modelmeta database that identify it.

    :param filename: file name of NetCDF file
    :param session: database session for access to modelmeta database
    :return: DataFile object for file indexed
    """
    with CFDataset(filename) as cf:
        data_file = find_update_or_insert_nc_file(session, cf)
    return data_file


def find_update_or_insert_nc_file(sesh, cf):  # get.data.file.id
    """Find, update, or insert a NetCDF file in the modelmeta database, according to whether it is
    already present and up to date.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file
    """
    id_data_file, hash_data_file = find_data_file_by_unique_id_and_hash(sesh, cf)

    if id_data_file and hash_data_file:
        # File located using both unique id and hash. Check they're indexed by the same DataFile object.
        if id_data_file == hash_data_file:
            logger.info("Skipping file {}. Already in the db as id {}.".format(cf.filepath(), id_data_file.id))
        else:
            logger.error("Split brain! File {} is in the database under multiple entries: data_file_id {} and {}"
                         .format(cf.filepath(), id_data_file.id, hash_data_file.id))
        return id_data_file

    elif id_data_file and not hash_data_file:
        # File changed. Do an update.
        update_data_file(sesh, id_data_file, cf)
        return id_data_file

    elif not id_data_file and hash_data_file:
        # We've indexed this file under a different unique id. Warn and skip.
        logger.warning("Skipping file {}. Already in the db under unique id {}."
                       .format(cf.filepath(), hash_data_file.unique_id))
        return hash_data_file

    else:
        # File is not indexed in the db yet. Our raison d'être. Do the insertion.
        return insert_nc_file(sesh, cf)


def find_data_file_by_unique_id_and_hash(sesh, cf):
    """Find and return DataFile records matching file unique id and file hash.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: pair of DataFiles matching unique id, hash (None in a component if no match)
    """
    q = sesh.query(DataFile).filter(DataFile.unique_id == cf.unique_id)
    id_match = q.first()
    q = sesh.query(DataFile).filter(DataFile.first_1mib_md5sum == cf.first_MiB_md5sum)
    hash_match = q.first()
    return id_match, hash_match


def insert_nc_file(sesh, cf):
    """Insert records for a NetCDF known not to be in the database yet.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file
    """
    data_file = insert_data_file(sesh, cf)
    find_or_insert_data_file_variables(sesh, data_file, cf)
    return data_file


def update_data_file(sesh, data_file, cf):  # not a function in R code; NOT the same as update.data.file.id
    """Update a the modelmeta entry for a file.

    WARNING: `data_file` and `cf` MUST represent the SAME file.

    :param sesh: modelmeta database session
    :param data_file: DataFile entry for NetCDF file
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile object, updated (may be different than the data_file passed in)
    """
    cf_modification_time = os.path.getmtime(cf.filepath())
    if cf.filepath() == data_file.filename:
        if cf.first_MiB_md5sum == data_file.first_1mib_md5sum:
            # TODO (X): Uh-oh, this branch will never be taken, because `find_update_or_insert_file` calls this method
            # only when no match in the database has been made on the file's hash. That's the complement of the if
            # condition. However, if this routine is regarded as general-purpose, then this branch covers a possbile
            # case
            if cf_modification_time < data_file.index_time:
                # Update the index time
                data_file.index_time = datetime.datetime.now()
                sesh.commit()
                return data_file
            else:
                # File has changed w/o hash being updated; log warning, then reindex and update existing records.
                logger.warning("File {}: Hash didn't change, but file was updated.".format(cf.filepath()))
                return reindex_nc_file(sesh, data_file, cf)
        else:
            # TODO: This branch always taken. See TODO (X) above.
            if cf_modification_time < data_file.index_time:
                # Error condition. Should never happen.
                raise ValueError("File {}: Hash changed, but mod time doesn't reflect update.".format(cf.filepath()))
            else:
                # File has changed; re-index it.
                return reindex_nc_file(sesh, data_file, cf)
    else:
        # Name changed and data changed.
        if os.path.isfile(cf.filepath()):
            # Same file (probably a symlink). Ignore the file; we'll hit it later.
            # FIXME: CHECK THE ASSUMPTION HERE.
            logger.info("{} refers to the same file as {}".format(data_file.filename, cf.filename()))
            return data_file
        else:
            if md5(cf.filepath()) == md5(data_file.filename):
                # TODO: Seems unlikely this path will ever be taken for same reason as TODO (X) above.
                # Same content. Scream about a copy.
                logger.warning("File {} is a copy of {}. Figure out why.".format(cf.filepath(), data_file.filename))
                return data_file
            else:
                # Different file content. May be a newer version of the same file. Reindex it.
                return reindex_nc_file(sesh, data_file, cf)

    raise RuntimeError('Error: This function should return from all branches of if statements.')


def reindex_nc_file(sesh, existing_data_file, cf):
    """Delete the existing modelmeta content for a data file and insert it again de novo.
    Return the new DataFile object.

    :param sesh: modelmeta database session
    :param existing_data_file: DataFile object representing data file to be deleted and re-inserted
    :param cf: CFDatafile object representing NetCDF file
    :return: DataFile entry for file
    """
    delete_data_file(sesh, existing_data_file)
    return insert_nc_file(sesh, cf)


def delete_data_file(sesh, existing_data_file):
    """Delete existing `DataFile` object, associated `DataFileVariable`s,
    and the associations of those `DataFileVariable`s with `Ensembles` (via object `EnsembleDataFileVariables`).
    (Existing `Ensemble`s are preserved).

    :param sesh: modelmeta database session
    :param existing_data_file: DataFile object representing data file to be deleted and re-inserted
    """
    # TODO: Also delete associations with `QCFlag`s? (via `DataFileVariablesQcFlag`)
    existing_data_file_variables = existing_data_file.data_file_variables
    existing_ensemble_data_file_variables = (
        sesh.query(EnsembleDataFileVariables)
            .filter(EnsembleDataFileVariables.data_file_variable_id.in_(
                edfv.id for edfv in existing_data_file_variables
            ))
            .all()
    )
    sesh.delete_all(existing_ensemble_data_file_variables)
    sesh.delete_all(existing_data_file_variables)
    sesh.delete(existing_data_file)
    sesh.commit()


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
    """Find or insert modelmeta `DataFileVariable`s for the NetCDF file.
    One DataFileVariable is found or inserted for every variable in the NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param data_file: DataFile to attach variables to
    :return: list of DataFileVariable found or inserted
    """
    dfvs = []
    for var_name in cf.dependent_varnames:
        dfv = (sesh.query(DataFileVariable)
               .filter(DataFileVariable.file == data_file)
               .filter(DataFileVariable.netcdf_variable_name == var_name)
               .first())

        if not dfv:
            variable = cf.variables[var_name]
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
    """Find or insert a LevelSet for a provided NetCDF variable. If the variable has no Z axis, return None.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param variable: NetCDF variable (in `cf`)
    :return LevelSet object corresponding to the level set for the provided varible,
        None if no level set (variable has no Z axis)
    """
    # Find the level dimension if it exists
    axis_to_dim_name = cf.dim_axes_from_names(variable.dimensions)
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

        sesh.add_all([Level(
                 level_set=level_set,
                 vertical_level=vertical_level,
                 level_start=level_start,
                 level_end=level_end,
             ) for level_start, vertical_level, level_end in get_var_bounds_and_values(cf, level_axis_var)
        ])
        sesh.commit()

    return level_set


def get_var_bounds_and_values(cf, variable, bounds_var_name=None):  # get.bnds.center.array
    """Return a list of tuples describing the bounds and values of a NetCDF variable.
    One tuple per variable value, defining (lower_bound, value, upper_bound)

    :param cf: CFDatafile object representing NetCDF file
    :param variable: NetCDF variable that is an level axis
    :param bounds_var_name: name of bounds variable; if not specified, use level_axis_var.bounds
    :return: list of tuples of the form (lower_bound, value, upper_bound)
    """
    # TODO: Should this be in nchelpers?
    values = variable[:]

    bounds_var_name = bounds_var_name or getattr(variable, 'bounds', None)
    if bounds_var_name:
        # Explicitly defined bounds: use them
        bounds_var = cf.variables.get[bounds_var_name]
        return zip(bounds_var[1, :], values, bounds_var[2, :])
    else:
        # No explicit bounds: manufacture them
        midpoints = (
            [(3*values[0] - values[1]) / 2] +   # fake lower "midpoint", half of previous step below first value
            [(values[i] + values[i+1]) / 2 for i in range(len(values)-1)] +
            [(3*values[-1] - values[-2]) / 2]   # fake upper "midpoint", half of previous step above last value
        )
        return zip(midpoints[:-1], values, midpoints[1:])


def find_or_insert_grid(sesh, cf, variable):  # get.grid.id
    """Find existing or insert new Grid record (and associated YCellBound records) corresponding to
    a variable in a NetCDF file.
    
    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param variable: (netCDF4.Variable) variable for which to find or insert grid
    :return: existing or new Grid record
    """
    dim_names_to_axes = cf.dim_axes(variable.name)
    axes_to_dim_names = {axis: dim_name for dim_name, axis in dim_names_to_axes.items()}

    if not all(axis in axes_to_dim_names for axis in 'XY'):
        return None

    if 'S' in axes_to_dim_names:
        source = cf.reduced_dims(variable.name)
    else:
        source = axes_to_dim_names

    xc_var, yc_var = (cf.variables[source[axis]] for axis in 'XY')
    xc_values, yc_values = (var[:] for var in [xc_var, yc_var])

    def is_regular_series(values, relative_tolerance=1e-6):
        """Return True iff the given series of values is regular, i.e., has equal steps between values,
        within a relative tolerance."""
        diffs = np.diff(values)
        return abs((np.max(diffs) / np.min(diffs) - 1) < relative_tolerance)

    evenly_spaced_y = is_regular_series(yc_values)

    def mean_step_size(values):
        """Return mean of differences between successive elements of values list"""
        return np.mean(np.diff(values))

    xc_grid_step, yc_grid_step = (mean_step_size(values) for values in [xc_values, yc_values])
    xc_origin, yc_origin = (values[0] for values in [xc_values, yc_values])

    def approx_equal(attribute, value, relative_tolerance=1e-6):
        """Return a column expression specifying that `attribute` and `value` are equal within a specified
        relative tolerance.
        Treat the case when value == 0 specially: require exact equality.
        """
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
        """Compute the average area of a grid cell, in sq km."""
        if all(units == 'm' for units in [xc_var.units, yc_var.units]):
            # Assume that grid is regular if specified in meters
            return abs(xc_grid_step * yc_grid_step) / 1e6
        else:
            # Assume lat-lon coordinates in degrees.
            # Assume that coordinate values are in increasing order (i.e., coord[i} < coord[j] for i < j).
            earth_radius = 6371
            y_vals = np.deg2rad(yc_values)
            # TODO: Improve this computation? See https://github.com/pacificclimate/modelmeta/issues/4
            return (
                np.deg2rad(np.abs(xc_values[1] - xc_values[0])) *
                np.mean(np.diff(y_vals) * np.cos(y_vals[:-1])) *
                earth_radius ** 2)

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
            ) for bottom_bnd, y_center, top_bound in get_var_bounds_and_values(cf, variable)]
            sesh.add_all(y_cell_bounds)

        sesh.commit()
        
    return grid


def find_or_insert_timeset(sesh, cf):
    """Find existing or insert new TimeSet record (and associated Time and ClimagologicalTime records)
    corresponding a NetCDF file.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :return: existing or new TimeSet record
    """
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
            time_start=time_start,
            time_end=time_end,
        ) for time_idx, (time_start, time_end) in enumerate(climatology_bounds)]
        sesh.add_all(climatological_times)

    sesh.commit()

    return time_set


def find_emission(sesh, cf):
    q = sesh.query(Emission).filter(Emission.short_name == cf.metadata.emissions)
    return q.first()


def insert_emission(sesh, cf):
    emission = Emission(short_name=cf.metadata.emission)
    sesh.add(emission)
    return emission


def find_or_insert_emission(sesh, cf):
    emission = find_emission(sesh, cf)
    if emission:
        return emission
    else:
        return insert_emission(sesh, cf)


def find_run(sesh, cf):
    q = sesh.query(Run).join(Model).join(Emission) \
        .filter(Model.name == cf.metadata.model) \
        .filter(Emission.name == cf.metadata.emissions) \
        .filter(Run.name == cf.metadata.run)
    return q.first()


def insert_run(sesh, cf, model, emission):
    run = Run(name=cf.metadata.run, project=cf.metadata.project, model=model, emission=emission)
    sesh.add(run)
    sesh.commit()
    return run


def find_or_insert_run(sesh, cf):
    run = find_run(sesh, cf)
    if run:
        return run
    model = find_or_insert_model(sesh, cf)
    if not model:
        raise RuntimeError('Model not found or inserted!')
    emission = find_or_insert_emission(sesh, cf)
    if not emission:
        raise RuntimeError('Emission not found or inserted!')
    return insert_run(sesh, cf, model, emission)


def find_model(sesh, cf):
    query = sesh.query(Model).filter(Model.short_name == cf.metadata.model)
    return query.first()


def insert_model(sesh, cf, model_type):
    model = Model(short_name=cf.metadata.model, type=model_type, organization=cf.metadata.institution)
    sesh.add(model)
    sesh.commit()
    return model


def find_or_insert_model(sesh, cf):
    model = find_model(sesh, cf)
    if not model:
        # Really rudimentary GCM/RCM decision making.
        if cf.metadata.project == 'NARCCAP' or \
           cf.metadata.project not in ('IPCC Fourth Assessment', 'CMIP5'):
            model_type = 'RCM'
        else:
            model_type = 'GCM'
        model = insert_model(sesh, cf, model_type)
    return model


def md5(filepath):
    """Return MD5 checksum of entire file.
    Parsimonious with memory. Adopted from https://stackoverflow.com/a/3431838
    """
    # TODO: Should this be in nchelpers? (property of CFDataset)
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_variable_range(variable):  # get.variable.range
    """Return minimum and maximum value taken by variable (over all dimensions).

    :param variable: (netCDF4.Variable)
    :return (tuple) (min, max) minimum and maximum values
    """
    # TODO: Should this be in nchelpers?
    # TODO: What about fill values?
    values = variable[:]
    return np.nanmin(values), np.nanmax(values)
