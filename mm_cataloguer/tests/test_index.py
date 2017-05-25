import pytest

from mm_cataloguer.index_netcdf import insert_model, insert_emission, \
    insert_run


def test_insert_model(blank_test_session, mock_cf):
    model = insert_model(blank_test_session, mock_cf, 'GCM')
    assert model.short_name == mock_cf.metadata.model
    assert model.organization == mock_cf.metadata.institution
    assert model.type == 'GCM'


def test_insert_emission(blank_test_session, mock_cf):
    em = insert_emission(blank_test_session, mock_cf)
    assert em.short_name == mock_cf.metadata.emission


def test_insert_run(blank_test_session, mock_cf):
    emission = insert_emission(blank_test_session, mock_cf)
    model = insert_model(blank_test_session, mock_cf, 'GCM')
    run = insert_run(blank_test_session, mock_cf, model, emission)
    assert run.name == mock_cf.metadata.run
    assert run.project == mock_cf.metadata.project
    assert run.model == model
    assert run.emission == emission
