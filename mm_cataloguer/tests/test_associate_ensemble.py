"""Test functions for associating an ensemble to a file.
"""
from pkg_resources import resource_filename
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mm_cataloguer.associate_ensemble import \
    find_ensemble, associate_ensemble_to_cf, associate_ensemble_to_files
from mm_cataloguer.index_netcdf import index_cf_file, index_netcdf_files
from modelmeta import Ensemble, EnsembleDataFileVariables


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
    for data_file_variable in data_file.data_file_variables:
        ensembles = (
            sesh.query(Ensemble).join(EnsembleDataFileVariables)
            .filter(EnsembleDataFileVariables.data_file_variable_id ==
                    data_file_variable.id)
            .all()
        )
        assert ensemble1 in ensembles


def find_or_insert_ensemble(dsn, new_ensemble):
    """Helper function to find or insert an ensemble into the test database."""
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    ensemble = find_ensemble(session, new_ensemble.name, new_ensemble.version)
    if ensemble:
        return ensemble
    session.add(new_ensemble)
    session.commit()
    return new_ensemble


def test_associate_ensemble_to_files(ensemble1):
    """End to end test of main function"""
    # Set up
    f = resource_filename('modelmeta', 'data/mddb-v2.sqlite')
    dsn = 'sqlite:///{0}'.format(f)
    test_files = [
        'data/tiny_gcm.nc',
        'data/tiny_downscaled.nc',
        'data/tiny_hydromodel_gcm.nc',
        'data/tiny_gcm_climo_monthly.nc',
    ]
    filenames = [resource_filename('modelmeta', f) for f in test_files]

    # Index a bunch of files
    index_netcdf_files(filenames, dsn)
    # Ensure there is an ensemble there to associate them to
    find_or_insert_ensemble(dsn, ensemble1)
    # Associate them
    assoc_data_files = associate_ensemble_to_files(
        filenames, dsn, ensemble1.name, ensemble1.version)
    assert all(adf and adf.filename == f
               for (adf, f) in zip(assoc_data_files, filenames))
