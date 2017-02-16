import re
import hashlib
import datetime

import numpy as np
from netCDF4 import Dataset, num2date

from modelmeta import Model, Run, Emission, DataFile


def index_netcdf_files(files, dsn):
    pass


def index_netcdf_file(filename, session):
    with Dataset(filename) as nc:
        id_ = find_or_insert_file_id(session, nc)
    return id_


def find_insert_upate_file_id(sesh, nc):
    id_df, hash_df = find_file_id(sesh, nc)

    if id_df and hash_df:
        if id_df == hash_df:
            logger.info("Skipping file {}. Already in the db as id {}.".format(nc.filepath(), id_match.unique_id))
        else:
            logger.error("Split brain! We seem to have file {} in the database under multiple entries: data_file_id {} and {}".format(nc.filepath(), id_match.id, hash_match.id))

    # File changed. Do an update.
    elif id_df and not hash_df:
        update_datafile(sesh, nc, id_df)

    # We've indexed this file under a different id. Warn and skip.
    elif not id_df and hash_df:
        logger.warn("Skipping file {}. Already in the db as id {}.".format(nc.filepath(), hash_df.unique_id))

    # Nothing is in the db yet. Our raison d'être. Do the insertion.
    else:
        insert_data_file(sesh, nc, hash_df.first_1mib_md5sum)


def update_datafile(sesh, nc, datafile):
    pass


def find_file_id(sesh, nc):
    unique_id = compute_unique_id(nc)
    nc_hash = get_first_MiB_md5sum(nc.filepath())
    q = sesh.query(DataFile).filter(DataFile.unique_id == unique_id)
    id_match = q.first()
    q = sesh.query(DataFile).filter(DataFile.first_1mib_md5sum == nc_hash)
    hash_match = q.first()
    return id_match, hash_match


def insert_data_file(sesh, nc, hash_):
    vars_ = get_important_varnames(nc)
    timeset = find_or_insert_timeset(sesh, nc)
    run = find_or_insert_run(sesh, nc)
    unique_id = compute_unique_id(nc)
    dim_names = nc_get_dim_axes_from_names(nc)
    for ax in 'XYZT':
        if ax not in dim_names:
            dim_names[ax] = None
    logger.info("Creating new DataFile for unique_id {}".format(unique_id))

    df = DataFile(
        filename=nc.filepath(),
        first_1mib_md5sum=hash_,
        unique_id=unique_id,
        index_time=datetime.datetime.now(),
        run=run,
        timeset=timeset,
        x_dim_name=dim_names['X'],
        y_dim_name=dim_names['Y'],
        z_dim_name=dim_names['Z'],
        t_dim_name=dim_names['T']
    )
    sesh.add(df)
    sesh.commit()
    return df


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
