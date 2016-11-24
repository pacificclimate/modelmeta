import pytest

from mm_cataloguer.index_netcdf import insert_model, insert_emission, \
    insert_run, get_file_metadata, nc_get_dim_axes, nc_get_dim_names, \
    nc_get_dim_axes_from_names


def test_insert_model(blank_test_session):
    model = insert_model(blank_test_session, "CGCM3", "GCM", "CCCMA")
    assert model.short_name == "CGCM3"


def test_insert_emission(blank_test_session):
    em = insert_emission(blank_test_session, "SRESA2")
    assert em


def test_insert_run(blank_test_session):
    em = insert_emission(blank_test_session, "SRESA2")
    model = insert_model(blank_test_session, "CGCM3", "GCM", "CCCMA")
    run = insert_run(blank_test_session, "r1e3v3", model, em, "CMIP5")
    assert run


def test_get_file_metadata(tiny_gcm):
    res = get_file_metadata(tiny_gcm)
    for key in ('institution', 'model', 'emissions', 'run', 'project'):
        assert key in res


@pytest.mark.parametrize(('ax_name', 'rv'), (
    (['time'], {'time': 'T'}),
    (['time', 'lon'], {'time': 'T', 'lon': 'X'}),
    (None, {'time': 'T', 'lon': 'X', 'lat': 'Y'})
))
def test_nc_get_dim_axes(tiny_gcm, ax_name, rv):
    ax = nc_get_dim_axes(tiny_gcm, ax_name)
    assert ax == rv


@pytest.mark.parametrize(('ax_name', 'rv'), (
    (['time'], {'T': 'time'}),
    (['time', 'lon'], {'T': 'time', 'X': 'lon'}),
    (None, {'T': 'time', 'X': 'lon', 'Y': 'lat'})
))
def test_nc_get_dim_axes_from_names(tiny_gcm, ax_name, rv):
    ax = nc_get_dim_axes_from_names(tiny_gcm, ax_name)
    assert ax == rv


def test_nc_get_dim_names(tiny_gcm):
    set1 = set(nc_get_dim_names(tiny_gcm))
    set2 = set(('time', 'lon', 'lat', 'nb2'))
    assert set1 == set2
