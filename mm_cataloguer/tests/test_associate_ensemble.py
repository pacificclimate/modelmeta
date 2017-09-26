"""Test functions for associating an ensemble to a file.
"""
import pytest
from pkg_resources import resource_filename
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset

from mm_cataloguer.associate_ensemble import \
    find_ensemble, associate_ensemble_to_cf, \
    associate_ensemble_to_file, associate_ensemble_to_files
from mm_cataloguer.index_netcdf import index_cf_file, index_netcdf_file, index_netcdf_files, find_update_or_insert_cf_file

from modelmeta import create_test_database
from modelmeta import DataFile, Ensemble, EnsembleDataFileVariables


# Test helper functions

def index_test_files(Session):
    session = Session()
    test_files = [
        'data/tiny_gcm.nc',
        'data/tiny_downscaled.nc',
        'data/tiny_hydromodel_gcm.nc',
        'data/tiny_gcm_climo_monthly.nc',
    ]
    filenames = [resource_filename('modelmeta', f) for f in test_files]
    for filename in filenames:
        with CFDataset(filename) as cf:
            find_update_or_insert_cf_file(session, cf)
    session.commit()


def add_objects(Session, objects):
    session = Session()
    session.add_all(objects)
    session.commit()


def find_or_insert_ensemble(dsn, new_ensemble):
    """Helper function to find or insert an ensemble into the test database."""
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    ensemble = find_ensemble(session, new_ensemble.name, new_ensemble.version)
    if ensemble:
        return ensemble
    session.add(new_ensemble)
    return new_ensemble


def check_associations(session, data_file, ensemble):
    for data_file_variable in data_file.data_file_variables:
        ensembles = (
            session.query(Ensemble).join(EnsembleDataFileVariables)
                .filter(EnsembleDataFileVariables.data_file_variable_id ==
                        data_file_variable.id)
                .all()
        )
        assert any(
            ensemble.name == e.name and ensemble.version == e.version
            for e in ensembles
        )


# Tests

def test_find_ensemble(test_session_with_ensembles, ensemble1):
    assert ensemble1 == find_ensemble(
        test_session_with_ensembles, ensemble1.name, ensemble1.version)
    assert not find_ensemble(
        test_session_with_ensembles, 'foo', ensemble1.version)


def test_associate_ensemble_to_cf(
        test_session_with_ensembles, ensemble1, tiny_dataset):
    """Test association against serveral different test datasets.
    Note: ``tiny_dataset`` is pre-parametrized in conftest.py
    """
    sesh = test_session_with_ensembles
    data_file = index_cf_file(sesh, tiny_dataset)
    assoc_df = associate_ensemble_to_cf(
        sesh, tiny_dataset, ensemble1.name, ensemble1.version)
    assert assoc_df == data_file
    check_associations(sesh, data_file, ensemble1)


@pytest.mark.parametrize('rel_filepath, in_db', [
    ('data/tiny_gcm.nc', True),
    ('data/tiny_downscaled.nc', True),
    ('data/tiny_hydromodel_gcm.nc', True),
    ('data/tiny_gcm_climo_monthly.nc', True),
    ('data/tiny_gcm_climo_seasonal.nc', False),
])
def test_associate_ensemble_to_file(
        test_engine_fs, test_session_factory_fs, 
        ensemble1, ensemble2, 
        rel_filepath, in_db
):
    # Set up test database
    create_test_database(test_engine_fs)
    index_test_files(test_session_factory_fs)
    add_objects(test_session_factory_fs, [ensemble1, ensemble2])
    
    # Associate an ensemble to a data file
    filepath = resource_filename('modelmeta', rel_filepath)
    data_file_id = associate_ensemble_to_file(
        filepath, test_session_factory_fs, ensemble1.name, ensemble1.version)
    
    # Check associations
    if in_db:
        session = test_session_factory_fs()
        data_file = (
            session.query(DataFile)
                .filter(DataFile.id == data_file_id)
                .one()
        )
        assert data_file is not None
        assert data_file.filename == filepath
        check_associations(session, data_file, ensemble1)
        session.close()
    else:
        assert data_file_id is None
    


def test_associate_ensemble_to_files(
        test_dsn_fs, test_engine_fs, test_session_factory_fs,
        ensemble1, ensemble2,
):
    # Set up test database
    create_test_database(test_engine_fs)
    index_test_files(test_session_factory_fs)
    add_objects(test_session_factory_fs, [ensemble1, ensemble2])

    # Associate an ensemble to files
    files_to_associate = [
        'data/tiny_gcm.nc',
        'data/tiny_downscaled.nc',
        'data/tiny_hydromodel_gcm.nc',
        'data/tiny_gcm_climo_monthly.nc',
    ]
    filepaths = [resource_filename('modelmeta', f) for f in files_to_associate]
    data_file_ids = associate_ensemble_to_files(
        filepaths, test_dsn_fs, ensemble1.name, ensemble1.version)

    # Check associations
    # Use a minimalist check, since ``test_associate_ensemble_to_file``
    # checks associations in detail. This test is mainly to confirm that
    # ``associate_ensemble_to_files`` correctly calls
    # ``associate_ensemble_to_file``.
    assert all(data_file_ids)
