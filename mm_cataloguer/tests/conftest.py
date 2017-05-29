import sys
import os
# Add helpers directory to pythonpath: See https://stackoverflow.com/a/33515264
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
import datetime

from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
from nchelpers import CFDataset

from modelmeta import create_test_database

from mock_helper import Mock


@pytest.fixture
def test_session():
    f = resource_filename('modelmeta', 'data/mddb-v2.sqlite')
    engine = create_engine('sqlite:///{0}'.format(f))
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture
def blank_test_session():
    engine = create_engine('sqlite:///')
    create_test_database(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture
def tiny_gcm():
    return CFDataset(resource_filename('modelmeta', 'data/tiny_gcm.nc'))


# @pytest.fixture
# def mock_cf():
#     num_time_values = 99
#     start_date = datetime.datetime(2000, 1, 1)
#     end_date = start_date + datetime.timedelta(days=num_time_values-1)
#     return Mock(
#         metadata=Mock(
#             project="CMIP5",
#             run="r1e3v3",
#             model="CGCM3",
#             institution="CCCMA",
#             emissions="SRESA2"
#         ),
#         is_multi_year_mean=False,
#         time_resolution='daily',
#         time_range_as_dates=(start_date, end_date),
#         time_var=Mock(size=num_time_values, calendar='standard'),
#         time_steps={
#             'datetime': (start_date + datetime.timedelta(days=d) for d in range(num_time_values))
#         }
#     )


@pytest.fixture
def mock_cf(monkeypatch, tiny_gcm):
    yield tiny_gcm

