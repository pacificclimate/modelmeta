__all__ = ['ClimatologicalTime', 'DataFile', 'DataFileVariable', 
           'DataFileVariablesQcFlag', 'Emission', 'Ensemble', 'EnsembleDataFileVariables',
           'Grid', 'Level', 'LevelSet', 'Model', 'QcFlag', 'Run',
           'Time', 'TimeSet', 'Variable', 'VariableAlias', 'YCellBound',
           'test_dsn', 'test_session']

from pkg_resources import resource_filename

from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, Enum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

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


class DataFile(Base):
    __tablename__ = 'data_files'

    #column definitions
    id = Column('data_file_id', Integer, primary_key=True, nullable=False)
    filename = Column(String(length=2048), nullable=False)
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
    data_file_id = Column(Integer, ForeignKey('data_files.data_file_id'), nullable=False)
    variable_alias_id = Column(Integer, ForeignKey('variable_aliases.variable_alias_id'), nullable=False)
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'))
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), nullable=False)


class DataFileVariablesQcFlag(Base):
    __tablename__ = 'data_file_variables_qc_flags'

    #column definitions
    data_file_variable_id = Column(Integer, ForeignKey('data_file_variables.data_file_variable_id'), primary_key=True, nullable=False)
    qc_flag_id = Column(Integer, ForeignKey('qc_flags.qc_flag_id'), primary_key=True, nullable=False)


class Emission(Base):
    __tablename__ = 'emissions'

    #column definitions
    id = Column('emission_id', Integer, primary_key=True, nullable=False)
    long_name = Column('emission_long_name', String(length=255))
    short_name = Column('emission_short_name', String(length=255), nullable=False)

    #relation definitions
    runs = relationship("Run", backref=backref('emission', lazy='joined'))


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

class EnsembleDataFileVariables(Base):
    __tablename__ = 'ensemble_data_file_variables'

    #column definitions
    ensemble_id = Column(Integer, ForeignKey('ensembles.ensemble_id'), primary_key=True, nullable=False)
    data_file_variable_id = Column(Integer, ForeignKey('data_file_variables.data_file_variable_id'), primary_key=True, nullable=False)


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
    levels = relationship("Level", backref=backref('level_set'))
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

class Time(Base):
    __tablename__ = 'times'

    #column definitions
    time_idx = Column(Integer, nullable=False)
    timestep = Column(DateTime, primary_key=True, nullable=False)

    #relation definitions
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'), primary_key=True, nullable=False)


class TimeSet(Base):
    __tablename__ = 'time_sets'

    #column definitions
    id = Column('time_set_id', Integer, primary_key=True, nullable=False)
    calendar = Column(String(length=32), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    multi_year_mean = Column(Boolean, nullable=False)
    num_times = Column(Integer, nullable=False)
    time_resolution = Column(Enum(u'1-minute', u'2-minute', u'5-minute', u'15-minute', u'30-minute', u'1-hourly', u'3-hourly', u'6-hourly', u'12-hourly', u'daily', u'monthly', u'yearly', u'other', u'irregular', name='timescale'), nullable=False)

    #relation definitions
    files = relationship("DataFile", backref=backref('timeset'))
    climatological_times = relationship("ClimatologicalTime", backref=backref('timeset'))
    times = relationship("Time", backref=backref('timeset'))


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

class YCellBound(Base):
    __tablename__ = 'y_cell_bounds'

    #column definitions
    bottom_bnd = Column(Float)
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), primary_key=True, nullable=False)
    top_bnd = Column(Float)
    y_center = Column(Float, primary_key=True, nullable=False)


test_dsn = 'sqlite+pysqlite:///{0}'.format(resource_filename('modelmeta', 'data/mddb-v2.sqlite'))

def test_session():
    '''This creates a testing database session that can be used as a test fixture.
    '''
    from sqlalchemy import create_engine
    engine = create_engine(test_dsn)
    Session = sessionmaker(bind=engine)
    return Session()
