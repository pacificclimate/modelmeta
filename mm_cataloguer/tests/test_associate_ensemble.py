"""Test functions for associating an ensemble to a file.
"""
import pytest
from pkg_resources import resource_filename

from nchelpers import CFDataset

from mm_cataloguer.associate_ensemble import \
    find_ensemble, associate_ensemble_to_cf, \
    associate_ensemble_to_file, associate_ensemble_to_files
from mm_cataloguer.index_netcdf import \
    index_cf_file, find_update_or_insert_cf_file

from modelmeta import create_test_database
from modelmeta import DataFile, DataFileVariable, \
    Ensemble, EnsembleDataFileVariables


# Test helper functions

def index_test_files(Session):
    """Add some test files to a database, using a session factory.
    Since this is done in a local session, these database records must be
    committed.
    """
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
    """Add objects to a database.
    Since this is done in a local session, these database records must be
    committed.
    """
    session = Session()
    session.add_all(objects)
    session.commit()


def check_ensemble_associations(session, data_file_variables, ensemble):
    """Check that the ensemble associations to a ``DataFile`` include
    a specified ensemble."""
    for data_file_variable in data_file_variables:
        ensembles = (
            session.query(Ensemble).join(EnsembleDataFileVariables)
            .filter(EnsembleDataFileVariables.data_file_variable_id ==
                    data_file_variable.id)
            .all()
        )
        # Can't just do assert ``ensemble in ensembles`` because they may not
        # be attached to the same database session.
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


@pytest.mark.parametrize('tiny_dataset, var_names, expected_var_names', [
    ('gcm', None, {'tasmax'}),
    ('gcm', set(), {'tasmax'}),
    ('gcm', {'tasmax'}, {'tasmax'}),
    ('gcm', {'tasmax', 'foo'}, {'tasmax'}),
    ('gcm', {'foo'}, set()),
    ('hydromodel_gcm', None, {'BASEFLOW', 'EVAP', 'RUNOFF', 'GLAC_AREA_BAND',
                              'GLAC_MBAL_BAND', 'SWE_BAND'}),
    ('hydromodel_gcm', {'BASEFLOW', 'EVAP'}, {'BASEFLOW', 'EVAP'}),
    ('hydromodel_gcm', {'BASEFLOW', 'foo'}, {'BASEFLOW'}),
], indirect=['tiny_dataset'])
def test_associate_ensemble_to_cf(
        test_session_with_ensembles, ensemble1, tiny_dataset,
        var_names, expected_var_names):
    """Test association against serveral different test datasets.
    Note: ``tiny_dataset`` is pre-parametrized in conftest.py
    """
    session = test_session_with_ensembles

    # Set up test database
    data_file = index_cf_file(session, tiny_dataset)

    # Associate ensemble to file
    assoc_dfvs = associate_ensemble_to_cf(
        session, ensemble1.name, ensemble1.version, tiny_dataset, var_names)

    # Check associations
    assert all(dfv.file == data_file for dfv in assoc_dfvs)
    assert set(dfv.netcdf_variable_name for dfv in assoc_dfvs) == \
           expected_var_names
    check_ensemble_associations(session, assoc_dfvs, ensemble1)
    # Since this is an isolated test, we can also test that no extraneous
    # associations were created.
    # TODO: Enable the following test when bug is fixed
    # The following ought to work ...
    # assert set(dfv.netcdf_variable_name
    #            for dfv in ensemble1.data_file_variables) == \
    #        expected_var_names
    # ... but there's a bug in the definition of Ensemble.data_file_variables
    # and the following print statements prove it
    print('\nBug in Ensemble.data_file_variables:')
    for dfv in assoc_dfvs:
        print(dfv.netcdf_variable_name, 'dfv.ensembles', dfv.ensembles)
    print('ensemble1.data_file_variables', ensemble1.data_file_variables)


@pytest.mark.parametrize('rel_filepath, var_names, expected_var_names', [
    ('data/tiny_gcm.nc', None, {'tasmax'}),
    ('data/tiny_gcm.nc', {'tasmax'}, {'tasmax'}),
    ('data/tiny_gcm.nc', {'foo'}, set()),
    # ('data/tiny_downscaled.nc', ),
    # ('data/tiny_hydromodel_gcm.nc', ),
    # ('data/tiny_gcm_climo_monthly.nc', ),
    # ('data/tiny_gcm_climo_seasonal.nc', ),
])
def test_associate_ensemble_to_file(
        test_engine_fs, test_session_factory_fs, 
        ensemble1, ensemble2, 
        rel_filepath, var_names, expected_var_names
):
    # Set up test database
    create_test_database(test_engine_fs)
    index_test_files(test_session_factory_fs)
    add_objects(test_session_factory_fs, [ensemble1, ensemble2])
    
    # Associate an ensemble to a data file
    filepath = resource_filename('modelmeta', rel_filepath)
    assoc_dfv_ids = associate_ensemble_to_file(
        test_session_factory_fs, ensemble1.name, ensemble1.version,
        filepath, var_names
    )
    
    # Check associations
    session = test_session_factory_fs()
    data_file = session.query(DataFile).filter_by(filename=filepath).one()
    assoc_dfvs = (
        session.query(DataFileVariable)
        .filter(DataFileVariable.id.in_(assoc_dfv_ids))
        .all()
    )
    assert all(dfv.file == data_file for dfv in assoc_dfvs)
    assert set(dfv.netcdf_variable_name for dfv in assoc_dfvs) == \
           expected_var_names
    check_ensemble_associations(session, assoc_dfvs, ensemble1)
    session.close()


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
    var_names = {'tasmax'}
    result = associate_ensemble_to_files(
        test_dsn_fs, ensemble1.name, ensemble1.version, filepaths, var_names)

    # Check associations
    assert len(result) == len(files_to_associate)
    session = test_session_factory_fs()
    for dfv_ids in result:
        assoc_dfvs = (
            session.query(DataFileVariable)
            .filter(DataFileVariable.id.in_(dfv_ids))
            .all()
        )
        check_ensemble_associations(session, assoc_dfvs, ensemble1)
    session.close()
