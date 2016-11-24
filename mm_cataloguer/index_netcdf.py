import hashlib

from modelmeta import Model, Run, Emission


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


def get_file_metadata(nc):
    if nc.project_id == 'CMIP5':
        return _get_file_metadata(nc, global_to_res_map_cmip5)
    else:
        return _get_file_metadata(nc, global_to_res_map_cmip3)


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

    if 'climatology' in nc.variable[time_axis]:
        return nc.variable[time_axis].climatology
    else:
        return None


def is_multi_year_mean(nc):
    '''Returns True if the netcdf metadata provided indicates that the
    data consists of a multi-year mean
    '''
    return bool(get_climatology_bounds_var_name(nc))


def get_time_step_size(time_series, units='seconds'):
    np.median(np.diff(nc.variables['time']))


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


def get_time_resolution(nc, time_series):
    '''Returns the appropriate time resolution string for the given data
    '''
    if is_multi_year_mean(nc):
        return 'other'

    units = nc.variables['time'].units
    step_size_seconds = get_time_step_size(time_series, units)
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
        return seconds[map_]
    else:
        return 'other'


def get_first_MiB_md5sum(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        m.update(f.read(2**20))
    return m.digest()
