"""
Define v2 modelmeta database in SQLAlchemy using declarative base.
"""
__all__ = '''
    Base
    ClimatologicalTime
    DataFile
    DataFileVariable
    DataFileVariableDSGTimeSeries
    DataFileVariableDSGTimeSeriesXStation
    DataFileVariableGridded
    DataFileVariablesQcFlag
    Emission
    Ensemble
    EnsembleDataFileVariables
    Grid
    Level
    LevelSet
    Model
    QcFlag
    Run
    Station
    Time
    TimeSet
    Variable
    VariableAlias
    YCellBound
    SpatialRefSys
'''.split()

from pkg_resources import resource_filename

from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, \
    Enum, ForeignKey, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, backref, sessionmaker


def obj_repr(attributes, obj):
    if isinstance(attributes, str):
        attributes = attributes.split()
    attr_list = ', '.join(
        ['{name}={value}'.format(name=attr, value=repr(getattr(obj, attr)))
         for attr in attributes]
    )
    return '{}({})'.format(obj.__class__.__name__, attr_list)


print('### Creating modelmeta ORM')
Base = declarative_base()
metadata = Base.metadata


class ClimatologicalTime(Base):
    __tablename__ = 'climatological_times'

    # column definitions
    time_idx = Column(Integer, primary_key=True, nullable=False)
    time_end = Column(DateTime, nullable=False)
    time_start = Column(DateTime, nullable=False)

    # relation definitions
    time_set_id = Column(
        Integer, 
        ForeignKey('time_sets.time_set_id'), 
        primary_key=True, 
        nullable=False
    )

    def __repr__(self):
        return obj_repr('time_set_id time_idx time_start time_end', self)

Index('climatological_times_time_set_id_key', ClimatologicalTime.time_set_id,
      unique=False)


class DataFile(Base):
    __tablename__ = 'data_files'

    # column definitions
    id = Column('data_file_id', Integer, primary_key=True, nullable=False)
    filename = Column(String(length=2048), nullable=False)
    # FIXME: If this db is to be properly normalized, there should probably
    # be a unique constraint on this hash
    first_1mib_md5sum = Column('first_1mib_md5sum', 
                               String(length=32), nullable=False)
    unique_id = Column(String(length=255), nullable=False)
    x_dim_name = Column(String(length=32))
    y_dim_name = Column(String(length=32))
    z_dim_name = Column(String(length=32))
    t_dim_name = Column(String(length=32))
    index_time = Column(DateTime, nullable=False)

    # relation definitions
    run_id = Column(Integer, ForeignKey('runs.run_id'))
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'))

    data_file_variables = relationship(
        "DataFileVariable", backref=backref('file', lazy='joined'), 
        lazy='joined')

    def __str__(self):
        return obj_repr(
            'id filename first_1mib_md5sum unique_id x_dim_name y_dim_name '
            'z_dim_name index_time run_id time_set_id', self)

UniqueConstraint(DataFile.unique_id, name='data_files_unique_id_key')
Index('data_files_run_id_key', DataFile.run_id, unique=False)


class DataFileVariable(Base):
    """Base type for polymorphic DataFileVariable (see `__mapper_args__`).
    Not a valid object by itself, but there is no obvious way to specify
    this in code."""

    __tablename__ = 'data_file_variables'

    # column definitions
    id = Column('data_file_variable_id', 
                Integer, primary_key=True, nullable=False)
    geometry_type = Column(String(50))  # polymorphic discriminator
    derivation_method = Column(String(length=255))
    variable_cell_methods = Column(String(length=255))
    netcdf_variable_name = Column(String(length=32), nullable=False)
    disabled = Column(Boolean)
    range_min = Column(Float, nullable=False)
    range_max = Column(Float, nullable=False)

    # relation definitions
    data_file_id = Column(
        Integer,
        ForeignKey('data_files.data_file_id',
                   name='data_file_variables_data_file_id_fkey',
                   ondelete='CASCADE'),
        nullable=False
    )
    # DataFile defines backref `file` in this class
    variable_alias_id = Column(
        Integer, 
        ForeignKey('variable_aliases.variable_alias_id'), 
        nullable=False
    )
    # VariableAlias defines backref `data_file_variables` in this class

    __mapper_args__ = {
        'polymorphic_identity': 'none',
        'polymorphic_on': geometry_type
    }

    def __repr__(self):
        return obj_repr(
            'id geometry_type derivation_method variable_cell_methods '
            'netcdf_variable_name disabled range_min range_max', self)


class DataFileVariableDSGTimeSeries(DataFileVariable):
    """DataFileVariable of subtype Discrete Sampling Geometry (DSG), subtype
    timeSeries."""

    __tablename__ = 'data_file_variables_dsg_time_series'

    id = Column(
        'data_file_variable_dsg_ts_id',
        Integer,
        ForeignKey(
            'data_file_variables.data_file_variable_id',
            ondelete='CASCADE'),
        primary_key=True,
        nullable=False
    )

    # relations
    stations = relationship(
        'Station',
        secondary='data_file_variables_dsg_time_series_x_stations',
        back_populates='data_file_variables')

    __mapper_args__ = {
        'polymorphic_identity':'dsg_time_series',
    }

    def __repr__(self):
        return obj_repr('''
            id derivation_method variable_cell_methods netcdf_variable_name
            disabled range_min range_max
        ''', self)


class Station(Base):
    __tablename__ = 'stations'

    # columns
    id = Column('station_id', Integer, primary_key=True, nullable=False)
    x = Column(Float, nullable=False)
    x_units = Column(String(64), nullable=False)
    y = Column(Float, nullable=False)
    y_units = Column(String(64), nullable=False)
    name = Column(String(32))
    long_name = Column(String(255))

    # relations
    data_file_variables = relationship(
        'DataFileVariableDSGTimeSeries',
        secondary='data_file_variables_dsg_time_series_x_stations',
        back_populates='stations')


    def __repr__(self):
        return obj_repr('id name long_name x x_units y y_units', self)


class DataFileVariableDSGTimeSeriesXStation(Base):
    """Cross table for many:many relation between DataFileVariableDSGTimeSeries
    and Station."""

    __tablename__ = 'data_file_variables_dsg_time_series_x_stations'

    # columns
    data_file_variable_dsg_ts_id = Column(
        Integer, 
        ForeignKey(
            'data_file_variables_dsg_time_series.data_file_variable_dsg_ts_id',
            name='data_file_variables_dsg_time_series_x_stations_data_file_variable_dsg_ts_id_id_fkey',
            ondelete='CASCADE'),
        primary_key=True,
        nullable=False
    )
    station_id = Column(
        Integer, 
        ForeignKey(
            'stations.station_id',
            name='data_file_variables_dsg_time_series_x_stations_station_id_fkey',
            ondelete='CASCADE'),
        primary_key=True,
        nullable=False
    )

    # relations
    data_file_variable_dsg_ts = relationship('DataFileVariableDSGTimeSeries')
    station = relationship('Station')

    def __repr__(self):
        return obj_repr('data_file_variable_dsg_ts_id station_id', self)


class DataFileVariableGridded(DataFileVariable):
    """DataFileVariable of subtype gridded."""

    __tablename__ = 'data_file_variables_gridded'

    # columns
    id = Column(
        Integer,
        ForeignKey(
            'data_file_variables.data_file_variable_id',
            ondelete='CASCADE'),
        primary_key=True,
        nullable=False
    )

    # relations
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'))
    # LevelSet defines backref `level_set` in this class
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), nullable=False)
    # Grid defines backref `grid` in this class

    __mapper_args__ = {
        'polymorphic_identity':'gridded',
    }

    def __repr__(self):
        return obj_repr(
            'id derivation_method variable_cell_methods netcdf_variable_name '
            'disabled range_min range_max level_set_id grid_id', self)


class DataFileVariablesQcFlag(Base):
    __tablename__ = 'data_file_variables_qc_flags'

    # column definitions
    data_file_variable_id = Column(
        Integer, 
        ForeignKey(
            'data_file_variables.data_file_variable_id', 
            name='data_file_variables_qc_flags_data_file_variable_id_fkey', 
            ondelete='CASCADE'), 
        primary_key=True, 
        nullable=False
    )
    qc_flag_id = Column(
        Integer, 
        ForeignKey('qc_flags.qc_flag_id'), 
        primary_key=True, 
        nullable=False
    )

    def __repr__(self):
        return obj_repr('data_file_variable_id qc_flag_id', self)


class Emission(Base):
    __tablename__ = 'emissions'

    # column definitions
    id = Column('emission_id', Integer, primary_key=True, nullable=False)
    long_name = Column('emission_long_name', String(length=255))
    short_name = Column('emission_short_name', 
                        String(length=255), nullable=False)

    # relation definitions
    runs = relationship("Run", backref=backref('emission', lazy='joined'))

    def __repr__(self):
        return obj_repr('id short_name long_name', self)


class Ensemble(Base):
    __tablename__ = 'ensembles'

    # column definitions
    id = Column('ensemble_id', Integer, primary_key=True, nullable=False)
    changes = Column(String, nullable=False)
    description = Column('ensemble_description', String(length=255))
    name = Column('ensemble_name', String(length=32), nullable=False)
    version = Column(Float, nullable=False)

    # relation definitions
    data_file_variables = relationship('DataFileVariable', 
        primaryjoin='Ensemble.id==ensemble_data_file_variables.c.ensemble_id', 
        secondary='ensemble_data_file_variables', 
        secondaryjoin='ensemble_data_file_variables.c.data_file_variable_id==DataFileVariable.id', 
        backref=backref('ensembles'), lazy='joined')

    def __repr__(self):
        return obj_repr('id name version changes description', self)

UniqueConstraint(Ensemble.name, Ensemble.version,
                 name='ensemble_name_version_key')


class EnsembleDataFileVariables(Base):
    __tablename__ = 'ensemble_data_file_variables'

    # column definitions
    ensemble_id = Column(
        Integer, 
        ForeignKey('ensembles.ensemble_id', 
                    name='ensemble_data_file_variables_ensemble_id_fkey', 
                    ondelete='CASCADE'), 
        primary_key=True, 
        nullable=False
    )
    data_file_variable_id = Column(
        Integer, 
        ForeignKey('data_file_variables.data_file_variable_id',         
                   name='ensemble_data_file_variables_data_file_variable_id_fkey', 
                   ondelete='CASCADE'), 
        primary_key=True, 
        nullable=False
    )

    def __repr__(self):
        return obj_repr('data_file_variable_id ensemble_id', self)


class Grid(Base):
    __tablename__ = 'grids'

    # column definitions
    id = Column('grid_id', Integer, primary_key=True, nullable=False)
    name = Column('grid_name', String(length=255))
    cell_avg_area_sq_km = Column(Float)
    evenly_spaced_y = Column(Boolean, nullable=False)
    xc_count = Column(Integer, nullable=False)
    xc_grid_step = Column(Float, nullable=False)
    xc_origin = Column(Float, nullable=False)
    xc_units = Column(String(length=64), nullable=False)
    yc_count = Column(Integer, nullable=False)
    yc_grid_step = Column(Float, nullable=False)
    yc_origin = Column(Float, nullable=False)
    yc_units = Column(String(length=64), nullable=False)

    # Brace yourself.
    # We'd like to do this:
    # 
    # srid = Column(Integer, ForeignKey('public.spatial_ref_sys.srid'))
    # 
    # but SQLAlchemy (possibly in the context of Alembic) seems incapable of
    # handling cross-schema foreign keys. We get the following error when
    # running
    # `alembic -x db=<empty db> revision --autogenerate -m "initial create"`::
    # 
    #   sqlalchemy.exc.NoReferencedTableError: Foreign key associated with
    #   column 'grids.srid' could not find table 'spatial_ref_sys' with which
    #   to generate a foreign key to target column 'srid'
    # 
    # (Table `public.spatial_ref_sys` was defined in that empty db.
    # And the search_path was OK. And etc.)
    # This despite Michael Bayer's (no less) statement in
    # https://bitbucket.org/zzzeek/alembic/issues/344/cross-database-foreign-key-autogeneration# comment-23989280
    # which suggests there should be no problem so long as we qualify the table
    # name with the schema, as above.
    # 
    # Experimenting with declaring metadata for schema 'public' and
    # reflecting or declaring table 'spatial_ref_sys', and setting
    # options of `alembic.EnvironmentContext.configure`
    # resulted in no joy.
    # 
    # So instead we do this:

    srid = Column(Integer)

    # Now we can run
    # `alembic -x db=<empty db> revision --autogenerate -m "initial create"`
    # without errors.
    # THEN we hand code the following line into the autogenerated migration
    # `upgrade()` code for `create_table('grids', ...)`:
    # 
    # sa.ForeignKeyConstraint(['srid'], ['public.spatial_ref_sys.srid'], )
    # 
    # ... which works (i.e., the migration can be run on an empty database
    # to create the desired tables).
    # 
    # A thoroughly ungodly proceeding, but it seems to be the only way to make
    # SQLAlchemy cooperate here. And at least it only has to happen nominally
    # once, when autogenerating the initial-create migration.

    # relation definitions
    y_cell_bounds = relationship("YCellBound", backref=backref('grid'))
    data_file_variables = relationship("DataFileVariableGridded", 
                                       backref=backref('grid'))

    def __repr__(self):
        return obj_repr(
            'id name cell_avg_area_sq_km '
            'xc_count xc_origin xc_grid_step xc_units '
            'yc_count yc_origin yc_grid_step yc_units evenly_spaced_y '
            'srid',
            self
        )


class Level(Base):
    __tablename__ = 'levels'

    # column definitions
    level_end = Column(Float)
    level_idx = Column(Integer, primary_key=True, nullable=False)
    level_set_id = Column(
        Integer, 
        ForeignKey('level_sets.level_set_id'), 
        primary_key=True, 
        nullable=False
    )
    level_start = Column(Float)
    vertical_level = Column(Float, nullable=False)

    # relations
    # LevelSet defines relation level_set

    def __repr__(self):
        return obj_repr(
            'level_set_id level_idx vertical_level level_start level_end', self)


class LevelSet(Base):
    __tablename__ = 'level_sets'

    # column definitions
    id = Column('level_set_id', Integer, primary_key=True, nullable=False)
    level_units = Column(String(length=32), nullable=False)

    # relation definitions
    levels = relationship("Level",
                          order_by='Level.vertical_level',
                          collection_class=ordering_list('vertical_level'),
                          backref=backref('level_set'))
    data_file_variables = relationship("DataFileVariableGridded", 
                                       backref=backref('level_set'))

    def __repr__(self):
        return obj_repr('id level_units', self)


class Model(Base):
    __tablename__ = 'models'

    # column definitions
    id = Column('model_id', Integer, primary_key=True, nullable=False)
    long_name = Column('model_long_name', String(length=255))
    short_name = Column('model_short_name', String(length=32), nullable=False)
    organization = Column('model_organization', String(length=64))
    type = Column(String(length=32), nullable=False)

    # relation definitions
    runs = relationship("Run", backref=backref('model', lazy='joined'))

    def __repr__(self):
        return obj_repr('id long_name short_name organization type', self)


class QcFlag(Base):
    __tablename__ = 'qc_flags'

    # column definitions
    id = Column('qc_flag_id', Integer, primary_key=True, nullable=False)
    description = Column('qc_flag_description', String(length=2048))
    name = Column('qc_flag_name', String(length=32), nullable=False)

    # relation definitions
    data_file_variables = relationship('DataFileVariable', 
        primaryjoin='QcFlag.id==data_file_variables_qc_flags.c.qc_flag_id', 
        secondary='data_file_variables_qc_flags', 
        secondaryjoin='data_file_variables_qc_flags.c.data_file_variable_id==DataFileVariable.id', 
        backref=backref('qc_flags'))

    def __repr__(self):
        return obj_repr('id name description', self)


class Run(Base):
    __tablename__ = 'runs'

    # column definitions
    id = Column('run_id', Integer, primary_key=True, nullable=False)
    name = Column('run_name', String(length=32), nullable=False)
    model_id = Column(Integer, ForeignKey('models.model_id'), nullable=False)
    emission_id = Column(Integer, 
                         ForeignKey('emissions.emission_id'), nullable=False)
    project = Column(String(length=64))

    # relation definitions
    driving_run_id = Column(
        'driving_run', Integer, ForeignKey('runs.run_id'))
    initialized_from_id = Column(
        'initialized_from', Integer, ForeignKey('runs.run_id'))

    driving_run = relationship("Run", foreign_keys="Run.driving_run_id")
    initialized_from_run = relationship("Run", foreign_keys="Run.initialized_from_id")

    time_set = relationship('TimeSet', 
        primaryjoin='Run.id==DataFile.run_id', 
        secondary='data_files', 
        secondaryjoin='DataFile.time_set_id==TimeSet.id')
    files = relationship("DataFile", 
                         backref=backref('run', lazy='joined'), 
                         lazy='joined')

    def __repr__(self):
        return obj_repr('id name project model_id emission_id '
                        'driving_run_id initialized_from_id', self)

UniqueConstraint(Run.name, Run.model_id, Run.emission_id,
                 name='unique_run_model_emissions_constraint')
Index('runs_model_id_key', Run.model_id, unique=False)
Index('runs_emission_id_key', Run.emission_id, unique=False)



class Time(Base):
    __tablename__ = 'times'

    # column definitions
    time_idx = Column(Integer, nullable=False)
    timestep = Column(DateTime, primary_key=True, nullable=False)

    # relation definitions
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'), 
                         primary_key=True, nullable=False)

    def __repr__(self):
        return obj_repr('time_set_id time_idx timestep', self)

Index('time_set_id_key', Time.time_set_id, unique=False)


class TimeSet(Base):
    __tablename__ = 'time_sets'

    # column definitions
    id = Column('time_set_id', Integer, primary_key=True, nullable=False)
    calendar = Column(String(length=32), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    multi_year_mean = Column(Boolean, nullable=False)
    num_times = Column(Integer, nullable=False)
    time_resolution = Column(
        Enum(
            '1-minute', '2-minute', '5-minute', '15-minute', '30-minute', 
            '1-hourly', '3-hourly', '6-hourly', '12-hourly', 
            'daily', 'monthly', 'seasonal', 'yearly', 'other', 'irregular', 
            name='timescale'), 
        nullable=False
    )

    # relation definitions
    files = relationship("DataFile", backref=backref('timeset'))
    climatological_times = relationship("ClimatologicalTime", 
                                        backref=backref('timeset'))
    times = relationship("Time", backref=backref('timeset'))

    def __repr__(self):
        return obj_repr(
            'id calendar start_date end_date multi_year_mean '
            'num_times time_resolution',
            self
        )


class Variable(Base):
    __tablename__ = 'variables'

    # column definitions
    id = Column('variable_id', Integer, primary_key=True, nullable=False)
    variable_alias_id = Column(
        Integer, ForeignKey('variable_aliases.variable_alias_id'), 
        nullable=False)
    description = Column('variable_description', 
                         String(length=255), nullable=False)
    name = Column('variable_name', String(length=64), nullable=False)

    # relation definitions
    variable_aliases = relationship('VariableAlias', primaryjoin='Variable.variable_alias_id==VariableAlias.id')
    data_files_variables = relationship('DataFileVariable', 
        primaryjoin='Variable.variable_alias_id==VariableAlias.id', 
        secondary='variable_aliases', 
        secondaryjoin='VariableAlias.id==DataFileVariable.variable_alias_id')

    def __repr__(self):
        return obj_repr('id name description variable_alias_id', self)


class VariableAlias(Base):
    __tablename__ = 'variable_aliases'

    # column definitions
    id = Column('variable_alias_id', Integer, primary_key=True, nullable=False)
    long_name = Column('variable_long_name', String(length=255), nullable=False)
    standard_name = Column('variable_standard_name', 
                           String(length=64), nullable=False)
    units = Column('variable_units', String(length=32), nullable=False)

    # relation definitions
    data_file_variables = relationship("DataFileVariable", 
                                       backref=backref('variable_alias'))
    data_files = relationship('DataFile', 
        primaryjoin='VariableAlias.id==DataFileVariable.variable_alias_id', 
        secondary='data_file_variables', 
        secondaryjoin='DataFileVariable.data_file_id==DataFile.id', 
        backref=backref('variable_aliases'))
    variable = relationship("Variable", backref=backref('variable_alias'))

    def __repr__(self):
        return obj_repr('id long_name standard_name units', self)


class YCellBound(Base):
    __tablename__ = 'y_cell_bounds'

    # column definitions
    bottom_bnd = Column(Float)
    grid_id = Column(
        Integer, 
        ForeignKey('grids.grid_id', name='y_cell_bounds_grid_id_fkey', 
                   ondelete='CASCADE'), 
        primary_key=True, 
        nullable=False
    )
    top_bnd = Column(Float)
    y_center = Column(Float, primary_key=True, nullable=False)

    def __repr__(self):
        return obj_repr('grid_id y_center bottom_bnd top_bnd', self)

Index('y_c_b_grid_id_key', YCellBound.grid_id, unique=False)


class SpatialRefSys(Base):
    """This table is established by the Postgis plugin."""
    __tablename__ = 'spatial_ref_sys'

    # column definitions
    id = Column('srid', Integer, primary_key=True, nullable=False)
    auth_name = Column(String(length=256))
    auth_srid = Column(Integer)
    srtext = Column(String(length=2048))
    proj4text = Column(String(length=2048))

    def __repr__(self):
        return obj_repr('id auth_name auth_srid srtext proj4text', self)

# We don't declare constraints on SpatialRefSys because the Postgis plugin is
# responsible for creating it.


class StreamflowOrder(Base):
    __tablename__ = 'streamflow_orders'

    # column definitions
    id = Column('streamflow_order_id', Integer, primary_key=True, nullable=False)
    hydromodel_output_id = Column(
        Integer, ForeignKey('data_files.data_file_id'),
        nullable=False)
    streamflow_result_id = Column(
        Integer, ForeignKey('streamflow_results.streamflow_result_id'),
        nullable=False)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    notification_method = Column(
        Enum('none', 'email'),
        nullable=False
    )
    notification_address = Column(String(length=255), nullable=True)
    status = Column(
        Enum('accepted', 'fulfilled', 'cancelled', 'error'),
        nullable=False
    )

    # relationships
    hydromodel = relationship('DataFile')
    result = relationship('StreamflowResult', back_populates='orders')


class StreamflowResult(Base):
    __tablename__ = 'streamflow_results'

    # column definitions
    id = Column('streamflow_result_id', Integer, primary_key=True, nullable=False)
    data_file_id = Column(
        Integer, ForeignKey('data_files.data_file_id'),
        nullable=False)
    station_id = Column(
        Integer, ForeignKey('stations.station_id'),
        nullable=False)
    status = Column(
        Enum('queued', 'processing', 'error', 'cancelled', 'ready', 'removed'),
        nullable=False
    )

    # relationships
    data_file = relationship('DataFile')
    station = relationship('Station')
    orders = relationship('StreamflowOrder', back_populates='result')