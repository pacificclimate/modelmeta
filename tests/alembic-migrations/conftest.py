import os
import pytest

from sqlalchemydiff.util import get_temporary_uri


@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(
            os.path.dirname(__file__), '..', '..', 'alembic'
        )
    )


@pytest.fixture(scope='module')
def uri_left(db_uri):
    return get_temporary_uri(db_uri)


@pytest.fixture(scope='module')
def uri_right(db_uri):
    return get_temporary_uri(db_uri)


