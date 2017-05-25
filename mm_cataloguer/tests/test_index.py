import pytest

from mm_cataloguer.index_netcdf import \
    insert_model, find_model, find_or_insert_model, \
    insert_emission, find_emission, find_or_insert_emission, \
    insert_run, find_run, find_or_insert_run


def cond_insert_model(blank_test_session, mock_cf, model_type, insert=True):
    if not insert:
        return None
    return insert_model(blank_test_session, mock_cf, model_type)


def test_insert_model(blank_test_session, mock_cf):
    model = cond_insert_model(blank_test_session, mock_cf, 'GCM')
    assert model.short_name == mock_cf.metadata.model
    assert model.organization == mock_cf.metadata.institution
    assert model.type == 'GCM'


@pytest.mark.parametrize('insert', [False, True])
def test_find_model(blank_test_session, mock_cf, insert):
    model_inserted = cond_insert_model(blank_test_session, mock_cf, 'GCM', insert)
    model_found = find_model(blank_test_session, mock_cf)
    assert model_inserted == model_found


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_model(blank_test_session, mock_cf, insert):
    model_inserted = cond_insert_model(blank_test_session, mock_cf, 'GCM', insert)
    model_found_or_inserted = find_or_insert_model(blank_test_session, mock_cf)
    if insert:
        assert model_found_or_inserted == model_inserted
    else:
        assert model_found_or_inserted


def cond_insert_emission(blank_test_session, mock_cf, insert=True):
    if not insert:
        return None
    return insert_emission(blank_test_session, mock_cf)


def test_insert_emission(blank_test_session, mock_cf):
    emission = cond_insert_emission(blank_test_session, mock_cf)
    assert emission.short_name == mock_cf.metadata.emissions


@pytest.mark.parametrize('insert', [False, True])
def test_find_emission(blank_test_session, mock_cf, insert):
    emission_inserted = cond_insert_emission(blank_test_session, mock_cf)
    emission_found = find_emission(blank_test_session, mock_cf)
    assert emission_inserted == emission_found


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_emission(blank_test_session, mock_cf, insert):
    emission_inserted = cond_insert_emission(blank_test_session, mock_cf, insert)
    emission_found_or_inserted = find_or_insert_emission(blank_test_session, mock_cf)
    if insert:
        assert emission_found_or_inserted == emission_inserted
    else:
        assert emission_found_or_inserted


def cond_insert_run(blank_test_session, mock_cf, insert=True):
    if not insert:
        return None, None, None
    emission = insert_emission(blank_test_session, mock_cf)
    model = insert_model(blank_test_session, mock_cf, 'GCM')
    run = insert_run(blank_test_session, mock_cf, model, emission)
    return run, model, emission


def test_insert_run(blank_test_session, mock_cf):
    run, model, emission = cond_insert_run(blank_test_session, mock_cf)
    assert run.name == mock_cf.metadata.run
    assert run.project == mock_cf.metadata.project
    assert run.model == model
    assert run.emission == emission


@pytest.mark.parametrize('insert', [False, True])
def test_find_run(blank_test_session, mock_cf, insert):
    run_inserted, _, _ = cond_insert_run(blank_test_session, mock_cf, insert)
    run_found = find_run(blank_test_session, mock_cf)
    assert run_inserted == run_found


@pytest.mark.parametrize('insert', [False, True])
def test_find_or_insert_run(blank_test_session, mock_cf, insert):
    run_inserted, _, _ = cond_insert_run(blank_test_session, mock_cf, insert)
    run_found_or_inserted = find_or_insert_run(blank_test_session, mock_cf)
    if insert:
        assert run_found_or_inserted == run_inserted
    else:
        assert run_found_or_inserted
