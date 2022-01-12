"""Test functions for associating an ensemble to a file.
"""
import pytest
from pkg_resources import resource_filename

from nchelpers import CFDataset

from mm_cataloguer.associate_ensemble import \
    associate_ensemble_to_data_file_variable, \
    associate_ensemble_to_data_file, \
    associate_ensemble_to_filepath, \
    associate_ensemble_to_filepaths, \
    find_ensemble

from mm_cataloguer.index_netcdf import \
    index_cf_file, find_update_or_insert_cf_file

from modelmeta.util import create_test_database
from modelmeta import DataFile, DataFileVariable, \
    Ensemble, EnsembleDataFileVariables


# Helper functions

def index_test_files(Session):
    """Add some test files to a database, using a session factory.
    Since this is done in a local session, these database records must be
    committed.
    """
    session = Session(expire_on_commit=False)
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
    session = Session(expire_on_commit=False)
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


# Tests proper
# test function names end in __ so that they can be used to uniquely
# select tests with pytest -k option

def test_find_ensemble__(test_session_with_ensembles, ensemble1):
    assert ensemble1 == find_ensemble(
        test_session_with_ensembles, ensemble1.name, ensemble1.version)
    assert not find_ensemble(
        test_session_with_ensembles, 'foo', ensemble1.version)


# associate_ensemble_to_data_file_variable

@pytest.mark.slow
@pytest.mark.parametrize('tiny_gridded_dataset, var_names', [
    ('gcm', {'tasmax'}),
    ('downscaled', {'tasmax'}),
    ('hydromodel_gcm', {'BASEFLOW', 'EVAP', 'GLAC_AREA_BAND', 'GLAC_MBAL_BAND', 'RUNOFF', 'SWE_BAND', }),
    ('gcm_climo_monthly', {'tasmax'}),
], indirect=['tiny_gridded_dataset'])
def test_associate_ensemble_to_data_file_variable__(
        test_session_with_ensembles,
        ensemble1,
        tiny_gridded_dataset,
        var_names,
):
    session = test_session_with_ensembles

    # Set up test database
    data_file = index_cf_file(session, tiny_gridded_dataset)

    # Extract some info
    assert set(
        dfv.netcdf_variable_name for dfv in data_file.data_file_variables
    ) == var_names
    assert len(data_file.data_file_variables) > 0
    data_file_variable = data_file.data_file_variables[0]

    # Associate
    ensemble_dfv = associate_ensemble_to_data_file_variable(
        session, ensemble1, data_file_variable)

    # Verify
    assert ensemble_dfv.ensemble_id == ensemble1.id
    assert ensemble_dfv.data_file_variable_id == data_file_variable.id


# associate_ensemble_to_data_file

@pytest.mark.slow
@pytest.mark.parametrize('tiny_gridded_dataset, var_names, expected_var_names', [
    ('gcm', None, {'tasmax'}),
    ('gcm', {'tasmax'}, {'tasmax'}),
    ('gcm', {'tasmax', 'foo'}, {'tasmax'}),
    ('gcm', {'foo'}, set()),

    ('downscaled', None, {'tasmax'}),
    ('hydromodel_gcm', None, {'BASEFLOW', 'EVAP', 'GLAC_AREA_BAND', 'GLAC_MBAL_BAND', 'RUNOFF', 'SWE_BAND', }),
    ('hydromodel_gcm', {'foo'}, set()),
    ('gcm_climo_monthly', None, {'tasmax'}),
], indirect=['tiny_gridded_dataset'])
def test_associate_ensemble_to_data_file__(
        test_session_with_ensembles,
        ensemble1,
        tiny_gridded_dataset,
        var_names,
        expected_var_names
):
    session = test_session_with_ensembles

    # Set up test database
    data_file = index_cf_file(session, tiny_gridded_dataset)

    # Associate
    associated_dfvs = associate_ensemble_to_data_file(
        session, ensemble1, data_file, var_names)

    # Verify
    assert set(dfv.netcdf_variable_name for dfv in associated_dfvs) == \
           expected_var_names

    check_ensemble_associations(session, associated_dfvs, ensemble1)


# associate_ensemble_to_filepath

fp_gcm = resource_filename('modelmeta', 'data/tiny_gcm.nc')
fp_downscaled = resource_filename('modelmeta', 'data/tiny_downscaled.nc')
fp_hydromodel_gcm = resource_filename('modelmeta', 'data/tiny_hydromodel_gcm.nc')
fp_gcm_climo_monthly = resource_filename('modelmeta', 'data/tiny_gcm_climo_monthly.nc')

@pytest.mark.slow
@pytest.mark.parametrize(
    'tiny_gridded_dataset, regex_filepath, filepath, var_names, expected_filepaths, expected_var_names',
    [
        ('gcm', False, fp_gcm, None, {fp_gcm}, {'tasmax'}),
        ('gcm', True, fp_gcm, None, {fp_gcm}, {'tasmax'}),
        ('gcm', True, r'tiny_gcm', None, {fp_gcm}, {'tasmax'}),
        ('gcm', True, r'tiny_.*\.nc', None, {fp_gcm}, {'tasmax'}),
        ('gcm', True, r'this wont match', None, set(), set()),

        ('gcm', False, fp_gcm, {'tasmax'}, {fp_gcm}, {'tasmax'}),
        ('gcm', True, fp_gcm, {'tasmax'}, {fp_gcm}, {'tasmax'}),

        ('gcm', False, fp_gcm, {'foo'}, {fp_gcm}, set()),
        ('gcm', True, fp_gcm, {'foo'}, {fp_gcm}, set()),

        ('downscaled', False, fp_downscaled, None, {fp_downscaled}, {'tasmax'}),
        ('hydromodel_gcm', False, fp_hydromodel_gcm, None, {fp_hydromodel_gcm}, {'BASEFLOW', 'EVAP', 'GLAC_AREA_BAND', 'GLAC_MBAL_BAND', 'RUNOFF', 'SWE_BAND', }),
        ('hydromodel_gcm', False, fp_hydromodel_gcm, {'foo'}, {fp_hydromodel_gcm}, set()),
        ('gcm_climo_monthly', False, fp_gcm_climo_monthly, None, {fp_gcm_climo_monthly}, {'tasmax'}),
    ],
    indirect=['tiny_gridded_dataset']
)
def test_associate_ensemble_to_filepath__(
        test_session_with_ensembles,
        ensemble1,
        tiny_gridded_dataset,
        regex_filepath,
        filepath,
        var_names,
        expected_filepaths,
        expected_var_names,
):
    session = test_session_with_ensembles

    # Set up test database
    data_file = index_cf_file(session, tiny_gridded_dataset)

    # Associate
    associated_items = associate_ensemble_to_filepath(
        session, ensemble1.name, ensemble1.version,
        regex_filepath, filepath, var_names
    )

    # Verify
    assert set(
        df.filename for df, _ in associated_items
    ) == expected_filepaths

    assert set(
        dfv.netcdf_variable_name
        for _, data_file_variables in associated_items
        for dfv in data_file_variables
    ) == expected_var_names

    for _, data_file_variables in associated_items:
        check_ensemble_associations(session, data_file_variables, ensemble1)


# associate_ensemble_to_filepaths

files_to_associate = [
    'data/tiny_gcm.nc',
    'data/tiny_downscaled.nc',
    'data/tiny_hydromodel_gcm.nc',
    'data/tiny_gcm_climo_monthly.nc',
]
fps = [
    [resource_filename('modelmeta', f) for f in files_to_associate[:number]]
    for number in [1,2,4]
]

@pytest.mark.slow
@pytest.mark.parametrize(
    'regex_filepath, filepaths, var_names, '
    'expected_filepaths, expected_var_names',
    [
        (False, fps[0], None, set(fps[0]), {'tasmax'}),
        (False, fps[1], None, set(fps[1]), {'tasmax'}),
        (False, fps[2], None, set(fps[2]), {'tasmax', 'BASEFLOW', 'EVAP', 'GLAC_AREA_BAND', 'GLAC_MBAL_BAND', 'RUNOFF', 'SWE_BAND', }),
        (False, fps[2], {'tasmax'}, set(fps[2]), {'tasmax'}),
        (False, fps[2], {'foo'}, set(fps[2]), set()),

        (True, fps[0], None, set(fps[0]), {'tasmax'}),
        (True, fps[1], None, set(fps[1]), {'tasmax'}),
        (True, fps[2], {'tasmax'}, set(fps[2]), {'tasmax'}),

        (True, [r'tiny_.*\.nc'], {'tasmax'}, set(fps[2]), {'tasmax'}),
    ]
)
def test_associate_ensemble_to_filepaths__(
        test_engine_fs, test_session_factory_fs,
        ensemble1, ensemble2,
        regex_filepath, filepaths, var_names,
        expected_filepaths, expected_var_names
):
    # Set up test database
    create_test_database(test_engine_fs)
    index_test_files(test_session_factory_fs)
    add_objects(test_session_factory_fs, [ensemble1, ensemble2])

    # Associate
    associated_ids = associate_ensemble_to_filepaths(
        test_session_factory_fs, ensemble1.name, ensemble1.version,
        regex_filepath, filepaths, var_names)

    # Verify
    session = test_session_factory_fs()

    associated_data_files = (
        session.query(DataFile)
        .filter(DataFile.id.in_(df_id for df_id, _ in associated_ids))
        .all()
    )
    assert set(
        df.filename for df in associated_data_files
    ) == expected_filepaths

    associated_data_file_variables = (
        session.query(DataFileVariable)
        .filter(DataFileVariable.id.in_(
            dfv_id for _, dfv_ids in associated_ids for dfv_id in dfv_ids
        ))
        .all()
    )
    assert set(
        dfv.netcdf_variable_name for dfv in associated_data_file_variables
    ) == expected_var_names

    check_ensemble_associations(session, associated_data_file_variables, ensemble1)

    session.close()
