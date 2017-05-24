from pkg_resources import resource_filename

import pytest

from mm_cataloguer.index_netcdf import insert_model, insert_emission, \
    insert_run


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
