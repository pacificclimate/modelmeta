from pkg_resources import resource_filename

import modelmeta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

@pytest.fixture
def test_session():
    f = resource_filename('modelmeta', 'data/mddb-v2.sqlite')
    engine = create_engine('sqlite:///{0}'.format(f))
    Session = sessionmaker(bind=engine)
    return Session()


def test_have_data(test_session):
    q = test_session.query(modelmeta.Ensemble)
    assert q.count() == 2
    assert [x.name for x in q.all()] == ['bcsd_downscale_canada', 'bc_prism']

def test_data_file_vars(test_session):
    ensemble_name = 'bcsd_downscale_canada'
    mydatafilevars = test_session.query(modelmeta.DataFileVariable).\
                     join(modelmeta.EnsembleDataFileVariables).\
                     join(modelmeta.Ensemble).\
                     filter(modelmeta.Ensemble.name == ensemble_name).all()
    assert len(mydatafilevars) == 198

def test_relations(test_session):
    myensemble = test_session.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == 'bcsd_downscale_canada').first()
    assert len(myensemble.data_file_variables) == 198

def test_deep_mapping(test_session):
    myensemble = test_session.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == 'bcsd_downscale_canada').first()
    mymodel = myensemble.data_file_variables[0].file.run.model
    assert mymodel.short_name == 'BCSD+ANUSPLIN300+ACCESS1-0'

def test_circular_mapping(test_session):
    myensemble = test_session.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == 'bcsd_downscale_canada').first()
    mymodel = myensemble.data_file_variables[0].file.run.model
    assert mymodel.runs[0].files[0].data_file_variables[0].ensembles[0].name == 'bcsd_downscale_canada'

def test_more_circles(test_session):

    myensemble = test_session.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == 'bcsd_downscale_canada').first()
    my_filename = myensemble.data_file_variables[0].file.run.emission.\
              runs[0].files[0].data_file_variables[0].file.filename
    assert my_filename == '/home/data/climate/downscale/CMIP5/BCSD/pr+tasmax+tasmin_day_BCSD+ANUSPLIN300+ACCESS1-0_historical+rcp45_r1i1p1_19500101-21001231.nc'

def test_relationships(test_session):
    q = test_session.query(modelmeta.Ensemble).join(modelmeta.EnsembleDataFileVariables).join(modelmeta.DataFileVariable).join(modelmeta.DataFile).filter(modelmeta.Ensemble.name == 'bcsd_downscale_canada')
    assert q.all()
