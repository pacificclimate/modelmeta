__all__ = ['DataFile', 'Run', 'TimeSet', 'Model', 'Emission', 'Grid', 'Ensemble',\
           'EnsembleRun', 'XCellBounds', 'YCellBounds', 'Presentation', 'Variable',\
           'DataFileVariable', 'Level', 'LevelSet', 'QCFlag',\
           'test_dsn', 'test_session']

from importlib.resources import files
           
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base()

class DataFile(Base):
    __tablename__ = 'data_files'
    id = Column('data_file_id', Integer, primary_key=True)
    filename = Column(String)
    first_1mib_md5sum = Column(String)
    unique_id = Column(String)
    x_dim_name = Column(String)
    y_dim_name = Column(String)
    z_dim_name = Column(String)
    t_dim_name = Column(String)

    run_id = Column(Integer, ForeignKey('runs.run_id'))
    time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'))

    data_file_variables = relationship("DataFileVariable", backref=backref('file', lazy='joined'), lazy='joined')

    def __str__(self):
        return '<DataFile %s>' % self.filename

class Run(Base):
    __tablename__ = 'runs'
    id = Column('run_id', Integer, primary_key=True)
    name = Column('run_name', String)
    model_id = Column(Integer, ForeignKey('models.model_id'))
    emission_id = Column(Integer, ForeignKey('emissions.emission_id'))
    project = Column(String)

    driving_run_id = Column('driving_run', Integer, ForeignKey('runs.run_id'))
    initialized_from_id = Column('initialized_from', Integer, ForeignKey('runs.run_id'))

    initialized_from_run = relationship("Run", foreign_keys="Run.initialized_from_id")
    driving_run = relationship("Run", foreign_keys="Run.driving_run_id")
    
    files = relationship("DataFile", backref=backref('run'), lazy='joined')
    ensembles = relationship("EnsembleRun", backref=backref('run', lazy='joined'), lazy='joined')

class TimeSet(Base):
    __tablename__ = 'time_sets'
    id = Column('time_set_id', Integer, primary_key=True)
    calendar = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    #time_resolution = Column('time_resolution', ???)
    multi_year_mean = Column(Boolean)
    num_times = Column(Integer)

    files = relationship("DataFile", backref=backref('timeset'))
    #times = relationship("Time", backref=backref('timeset'))


class Model(Base):
    __tablename__ = 'models'
    id = Column('model_id', Integer, primary_key=True)
    long_name = Column('model_long_name', String)
    short_name = Column('model_short_name', String)
    type = Column(String)
    org = Column('model_organization', String)

    runs = relationship("Run", backref=backref('model', lazy='joined'))

class Emission(Base):
    __tablename__ = 'emissions'
    id = Column('emission_id', Integer, primary_key=True)
    long_name = Column('emission_long_name', String)
    short_name = Column('emission_short_name', String)

    runs = relationship("Run", backref=backref('emission', lazy='joined'))

class Grid(Base):
    __tablename__ = 'grids'
    id = Column('grid_id', Integer, primary_key=True)
    srid = Column(Integer) #FIXME: references spatial_ref_sys
    name = Column('grid_name', String)
    xc_res = Column(Float)
    yc_res = Column(Float)
    xc_center_left = Column(Float)
    yc_center_left = Column(Float)
    xc_size = Column(Integer)
    yc_size = Column(Integer)
    cell_avg_area_sq_km = Column(Float)
    evenly_spaced_y = Column(Boolean)
    xc_units = Column(String)
    yc_units = Column(String)

    x_cell_bounds = relationship("XCellBounds", backref=backref('grid'))
    y_cell_bounds = relationship("YCellBounds", backref=backref('grid'))
    
class Ensemble(Base):
    __tablename__ = 'ensembles'
    id = Column('ensemble_id', Integer, primary_key=True)
    name = Column('ensemble_name', String)
    description = Column('ensemble_description', String)
    version = Column(Float)
    changes = Column(String)

    ensemble_runs = relationship("EnsembleRun", backref=backref('ensemble'), lazy='joined')

class EnsembleRun(Base):
    __tablename__ = 'ensemble_runs'
    ensemble_id = Column(Integer, ForeignKey('ensembles.ensemble_id'), primary_key=True)
    run_id = Column(Integer, ForeignKey('runs.run_id'), primary_key=True)

class XCellBounds(Base):
    __tablename__ = 'x_cell_bounds'
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), primary_key=True)
    left_bnd = Column(Float)
    x_center = Column(Float)
    right_bnd = Column(Float)

class YCellBounds(Base):
    __tablename__ = 'y_cell_bounds'
    grid_id = Column(Integer, ForeignKey('grids.grid_id'), primary_key=True)
    left_bnd = Column(Float)
    x_center = Column(Float)
    right_bnd = Column(Float)

class Presentation(Base):
    __tablename__ = 'presentations'
    id = Column('presentation_id', Integer, primary_key=True)
    palette = Column(String)
    scaling = Column(String)
    num_color_bands = Column(Integer)

    variables = relationship("Variable", backref=backref('presentation'))

class Variable(Base):
    __tablename__ = 'variables'
    id = Column('variable_id', Integer, primary_key=True)
    long_name = Column('variable_long_name', String)
    short_name = Column('variable_short_name', String)
    derived_from_id = Column('derived_from', Integer, ForeignKey('variables.variable_id'))
    presentation_id = Column(Integer, ForeignKey('presentations.presentation_id'))
    range_min = Column('var_range_min', Float)
    range_max = Column('var_range_max', Float)

    data_file_variables = relationship("DataFileVariable", backref=backref('variable'))
    derived_from = relationship("Variable", foreign_keys="Variable.derived_from_id")

    
class DataFileVariable(Base):
    __tablename__ = 'data_file_variables'
    id = Column('data_file_variable_id', Integer, primary_key=True)
    variable_units = Column(String)
    variable_cell_methods = Column(String)
    netcdf_variable_name = Column(String)
    disabled = Column(Boolean)
    range_min = Column(Float)
    range_max = Column(Float)

    data_file_id = Column(Integer, ForeignKey('data_files.data_file_id'))
    variable_id = Column(Integer, ForeignKey('variables.variable_id'))
    grid_id = Column(Integer, ForeignKey('grids.grid_id'))
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'))
    qc_flag = Column(Integer, ForeignKey('qc_flags.qc_flag'))
    anomaly_from_id = Column('anomaly_from', Integer, ForeignKey('data_file_variables.data_file_variable_id'))

    anomaly_from = relationship("DataFileVariable", foreign_keys="DataFileVariable.anomaly_from_id")
            
class Level(Base):
    __tablename__ = 'levels'
    id = Column('level_id', Integer, primary_key=True)
    vertical_level = Column(Float)
    level_set_id = Column(Integer, ForeignKey('level_sets.level_set_id'))

class LevelSet(Base):
    __tablename__ = 'level_sets'
    id = Column('level_set_id', Integer, primary_key=True)
    units = Column('level_units', String)

    levels = relationship("Level", backref=backref('level_set'))
    data_file_variables = relationship("DataFileVariable", backref=backref('level_set'))

class QCFlag(Base):
    __tablename__ = 'qc_flags'
    id = Column('qc_flag', Integer, primary_key=True)
    name = Column('qc_flag_name', String)
    description = Column('qc_flag_description', String)

# FIXME: There is no primary key for this table which this is, by definition, impossible for an ORM to handle
# class Time(Base):
#     __tablename__ = 'times'
#     timestep = Column(DateTime)
#     time_set_id = Column(Integer, ForeignKey('time_sets.time_set_id'))

test_dsn = 'sqlite+pysqlite:///{0}'.format((files('modelmeta') / 'data/mddb-v1.sqlite').resolve())

def test_session():
    '''This creates a testing database session that can be used as a test fixture.
    '''
    engine = create_engine(test_dsn)
    Session = sessionmaker(bind=engine)
    return Session()
