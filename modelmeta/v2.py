"""
Define v2 modelmeta database in SQLAlchemy using declarative base.
"""
__all__ = ['ClimatologicalTime', 'DataFile', 'DataFileVariable',
           'DataFileVariablesQcFlag', 'Emission', 'Ensemble', 'EnsembleDataFileVariables',
           'Grid', 'Level', 'LevelSet', 'Model', 'QcFlag', 'Run',
           'Time', 'TimeSet', 'Variable', 'VariableAlias', 'YCellBound',
           'SpatialRefSys',
           'test_dsn', 'test_session']

from pkg_resources import resource_filename

from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, \
    Enum, ForeignKey, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, backref, sessionmaker

print('### Creating modelmeta ORM')
Base = declarative_base()
metadata = Base.metadata


class ClimatologicalTime(Base):
    __tablename__ = 'climatological_times'

    #column definitions
    time_idx = Column(Integer, primary_key=True, nullable=False)
    time_end = Column(DateTime, nullable=False)
    time_start = Column(DateTime, nullable=False)

    #relation definitions
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'), primary_key=True, nullable=False)

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(self.__class__.__name__,
                self.time_idx, self.time_end, self.time_start, self.time_set_id)

Index('climatological_times_time_set_id_key', ClimatologicalTime.time_set_id,
      unique=False)


class DataFile(Base):
    __tablename__ = 'data_files'

    #column definitions
    id = Column('data_file_id', Integer, primary_key=True, nullable=False)
    filename = Column(String(length=2048), nullable=False)
    # FIXME: If this db is to be properly normalized, there should probably
    # be a unique constraint on this hash
    first_1mib_md5sum = Column('first_1mib_md5sum', String(length=32), nullable=False)
    unique_id = Column(String(length=255), nullable=False)
    x_dim_name = Column(String(length=32), nullable=False)
    y_dim_name = Column(String(length=32), nullable=False)
    z_dim_name = Column(String(length=32))
    t_dim_name = Column(String(length=32))
    index_time = Column(DateTime, nullable=False)

    #relation definitions
    run_id = Column(Integer, ForeignKey('runs.run_id'))
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'))

    data_file_variables = relationship("DataFileVariable", backref=backref('file', lazy='joined'), lazy='joined')

    def __str__(self):
        return '<DataFile %s>' % self.filename

UniqueConstraint(DataFile.unique_id, name='data_files_unique_id_key')
Index('data_files_run_id_key', DataFile.run_id, unique=False)


class DataFileVariable(Base):
    __tablename__ = 'data_file_variables'

    #column definitions
    id = Column('data_file_variable_id', Integer, primary_key=True, nullable=False)
    derivation_method = Column(String(length=255))
    variable_cell_methods = Column(String(length=255))
    netcdf_variable_name = Column(String(length=32), nullable=False)
    disabled = Column(Boolean)
    range_min = Column(Float, nullable=False)
    range_max = Column(Float, nullable=False)

    #relation definitions
    data_file_id = Column(Integer, ForeignKey('data_files.data_file_id', name='data_file_variables_data_file_id_fkey', ondelete='CASCADE'), nullable=False)
    variable_alias_id = Column(Integer, ForeignKey('variable_aliases.variable_alias_id'), nullable=False)
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'))
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), nullable=False)


class DataFileVariablesQcFlag(Base):
    __tablename__ = 'data_file_variables_qc_flags'

    #column definitions
    data_file_variable_id = Column(Integer, ForeignKey('data_file_variables.data_file_variable_id', name='data_file_variables_qc_flags_data_file_variable_id_fkey', ondelete='CASCADE'), primary_key=True, nullable=False)
    qc_flag_id = Column(Integer, ForeignKey('qc_flags.qc_flag_id'), primary_key=True, nullable=False)


class Emission(Base):
    __tablename__ = 'emissions'

    #column definitions
    id = Column('emission_id', Integer, primary_key=True, nullable=False)
    long_name = Column('emission_long_name', String(length=255))
    short_name = Column('emission_short_name', String(length=255), nullable=False)

    #relation definitions
    runs = relationship("Run", backref=backref('emission', lazy='joined'))

    def __repr__(self):
        return '{}({}, {}, {})'.format(self.__class__.__name__, self.id,
                                       self.long_name, self.short_name)

class Ensemble(Base):
    __tablename__ = 'ensembles'

    #column definitions
    id = Column('ensemble_id', Integer, primary_key=True, nullable=False)
    changes = Column(String, nullable=False)
    description = Column('ensemble_description', String(length=255))
    name = Column('ensemble_name', String(length=32), nullable=False)
    version = Column(Float, nullable=False)

    #relation definitions
    data_file_variables = relationship('DataFileVariable', 
        primaryjoin='Ensemble.id==ensemble_data_file_variables.c.ensemble_id', 
        secondary='ensemble_data_file_variables', 
        secondaryjoin='ensemble_data_file_variables.c.data_file_variable_id==DataFileVariable.id', 
        backref=backref('ensembles'), lazy='joined')

UniqueConstraint(Ensemble.name, Ensemble.version,
                 name='ensemble_name_version_key')


class EnsembleDataFileVariables(Base):
    __tablename__ = 'ensemble_data_file_variables'

    #column definitions
    ensemble_id = Column(Integer, ForeignKey('ensembles.ensemble_id', name='ensemble_data_file_variables_ensemble_id_fkey', ondelete='CASCADE'), primary_key=True, nullable=False)
    data_file_variable_id = Column(Integer, ForeignKey('data_file_variables.data_file_variable_id', name='ensemble_data_file_variables_data_file_variable_id_fkey', ondelete='CASCADE'), primary_key=True, nullable=False)


class Grid(Base):
    __tablename__ = 'grids'

    #column definitions
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
    # https://bitbucket.org/zzzeek/alembic/issues/344/cross-database-foreign-key-autogeneration#comment-23989280
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

    #relation definitions
    y_cell_bounds = relationship("YCellBound", backref=backref('grid'))
    data_file_variables = relationship("DataFileVariable", backref=backref('grid'))


class Level(Base):
    __tablename__ = 'levels'

    #column definitions
    level_end = Column(Float)
    level_idx = Column(Integer, primary_key=True, nullable=False)
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'), primary_key=True, nullable=False)
    level_start = Column(Float)
    vertical_level = Column(Float, nullable=False)

    #relation definitions


class LevelSet(Base):
    __tablename__ = 'level_sets'

    #column definitions
    id = Column('level_set_id', Integer, primary_key=True, nullable=False)
    level_units = Column(String(length=32), nullable=False)

    #relation definitions
    levels = relationship("Level",
                          order_by='Level.vertical_level',
                          collection_class=ordering_list('vertical_level'),
                          backref=backref('level_set'))
    data_file_variables = relationship("DataFileVariable", backref=backref('level_set'))


class Model(Base):
    __tablename__ = 'models'

    #column definitions
    id = Column('model_id', Integer, primary_key=True, nullable=False)
    long_name = Column('model_long_name', String(length=255))
    short_name = Column('model_short_name', String(length=32), nullable=False)
    organization = Column('model_organization', String(length=64))
    type = Column(String(length=32), nullable=False)

    #relation definitions
    runs = relationship("Run", backref=backref('model', lazy='joined'))

    def __repr__(self):
        return '{}(id={}, long_name={}, short_name={}, organization={}, ' \
               'type={})'.format(self.__class__.__name__, self.id,
                                  self.long_name, self.short_name,
                                  self.organization, self.type)

class QcFlag(Base):
    __tablename__ = 'qc_flags'

    #column definitions
    id = Column('qc_flag_id', Integer, primary_key=True, nullable=False)
    description = Column('qc_flag_description', String(length=2048))
    name = Column('qc_flag_name', String(length=32), nullable=False)

    #relation definitions
    data_file_variables = relationship('DataFileVariable', 
        primaryjoin='QcFlag.id==data_file_variables_qc_flags.c.qc_flag_id', 
        secondary='data_file_variables_qc_flags', 
        secondaryjoin='data_file_variables_qc_flags.c.data_file_variable_id==DataFileVariable.id', 
        backref=backref('qc_flags'))


class Run(Base):
    __tablename__ = 'runs'

    #column definitions
    id = Column('run_id', Integer, primary_key=True, nullable=False)
    name = Column('run_name', String(length=32), nullable=False)
    model_id = Column(Integer, ForeignKey('models.model_id'), nullable=False)
    emission_id = Column(Integer, ForeignKey('emissions.emission_id'), nullable=False)
    project = Column(String(length=64))

    #relation definitions
    driving_run_id = Column('driving_run', Integer, ForeignKey('runs.run_id'))
    initialized_from_id = Column('initialized_from', Integer, ForeignKey('runs.run_id'))

    driving_run = relationship("Run", foreign_keys="Run.driving_run_id")
    initialized_from_run = relationship("Run", foreign_keys="Run.initialized_from_id")

    time_set = relationship('TimeSet', 
        primaryjoin='Run.id==DataFile.run_id', 
        secondary='data_files', 
        secondaryjoin='DataFile.time_set_id==TimeSet.id')
    files = relationship("DataFile", backref=backref('run', lazy='joined'), lazy='joined')

UniqueConstraint(Run.name, Run.model_id, Run.emission_id,
                 name='unique_run_model_emissions_constraint')
Index('runs_model_id_key', Run.model_id, unique=False)
Index('runs_emission_id_key', Run.emission_id, unique=False)



class Time(Base):
    __tablename__ = 'times'

    #column definitions
    time_idx = Column(Integer, nullable=False)
    timestep = Column(DateTime, primary_key=True, nullable=False)

    #relation definitions
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'), primary_key=True, nullable=False)

    def __repr__(self):
        return '{}({}, {}, {})'.format(self.__class__.__name__, self.time_idx,
                                      repr(self.timestep), self.time_set_id)

Index('time_set_id_key', Time.time_set_id, unique=False)


class TimeSet(Base):
    __tablename__ = 'time_sets'

    #column definitions
    id = Column('time_set_id', Integer, primary_key=True, nullable=False)
    calendar = Column(String(length=32), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    multi_year_mean = Column(Boolean, nullable=False)
    num_times = Column(Integer, nullable=False)
    # time_resolution = Column(Enum('1-minute', '2-minute', '5-minute', '15-minute', '30-minute', '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily', 'monthly', 'yearly', 'other', 'irregular', name='timescale'), nullable=False)
    time_resolution = Column(Enum('1-minute', '2-minute', '5-minute', '15-minute', '30-minute', '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily', 'monthly', 'seasonal', 'yearly', 'other', 'irregular', name='timescale'), nullable=False)

    #relation definitions
    files = relationship("DataFile", backref=backref('timeset'))
    climatological_times = relationship("ClimatologicalTime", backref=backref('timeset'))
    times = relationship("Time", backref=backref('timeset'))

    def __repr__(self):
        return '{}({}, {}, {}, {}, {}, {}, {})'.format(self.__class__.__name,
                self.calendar, repr(self.start_date), repr(self.end_date),
                self.multi_year_mean, self.num_times, self.time_resolution)

class Variable(Base):
    __tablename__ = 'variables'

    #column definitions
    id = Column('variable_id', Integer, primary_key=True, nullable=False)
    variable_alias_id = Column(Integer, ForeignKey('variable_aliases.variable_alias_id'), nullable=False)
    description = Column('variable_description', String(length=255), nullable=False)
    name = Column('variable_name', String(length=64), nullable=False)

    #relation definitions
    variable_aliases = relationship('VariableAlias', primaryjoin='Variable.variable_alias_id==VariableAlias.id')
    data_files_variables = relationship('DataFileVariable', 
        primaryjoin='Variable.variable_alias_id==VariableAlias.id', 
        secondary='variable_aliases', 
        secondaryjoin='VariableAlias.id==DataFileVariable.variable_alias_id')

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(self.__class__.__name__, self.id,
                self.variable_alias_id, self.description, self.name)

class VariableAlias(Base):
    __tablename__ = 'variable_aliases'

    #column definitions
    id = Column('variable_alias_id', Integer, primary_key=True, nullable=False)
    long_name = Column('variable_long_name', String(length=255), nullable=False)
    standard_name = Column('variable_standard_name', String(length=64), nullable=False)
    units = Column('variable_units', String(length=32), nullable=False)

    #relation definitions
    data_file_variables = relationship("DataFileVariable", backref=backref('variable_alias'))
    data_files = relationship('DataFile', 
        primaryjoin='VariableAlias.id==DataFileVariable.variable_alias_id', 
        secondary='data_file_variables', 
        secondaryjoin='DataFileVariable.data_file_id==DataFile.id', 
        backref=backref('variable_aliases'))
    variable = relationship("Variable", backref=backref('variable_alias'))

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(self.__class__.__name__, self.id,
                self.long_name, self.standard_name, self.units)


class YCellBound(Base):
    __tablename__ = 'y_cell_bounds'

    #column definitions
    bottom_bnd = Column(Float)
    grid_id = Column(Integer, ForeignKey('grids.grid_id', name='y_cell_bounds_grid_id_fkey', ondelete='CASCADE'), primary_key=True, nullable=False)
    top_bnd = Column(Float)
    y_center = Column(Float, primary_key=True, nullable=False)

Index('y_c_b_grid_id_key', YCellBound.grid_id, unique=False)


class SpatialRefSys(Base):
    """This table is established by the Postgis plugin."""
    __tablename__ = 'spatial_ref_sys'

    #column definitions
    id = Column('srid', Integer, primary_key=True, nullable=False)
    auth_name = Column(String(length=256))
    auth_srid = Column(Integer)
    srtext = Column(String(length=2048))
    proj4text = Column(String(length=2048))

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(
            self.__class__.__name__, self.id,
            self.auth_name, self.auth_srid, self.srtext, self.proj4text
        )

# We don't declare constraints on SpatialRefSys because the Postgis plugin is
# responsible for creating it.


# TODO: Move this out to conftest. How did this even get here?

test_dsn = 'sqlite+pysqlite:///{0}'.format(resource_filename('modelmeta', 'data/mddb-v2.sqlite'))

def test_session():
    '''This creates a testing database session that can be used as a test fixture.
    '''
    from sqlalchemy import create_engine
    engine = create_engine(test_dsn)
    Session = sessionmaker(bind=engine)
    return Session()
