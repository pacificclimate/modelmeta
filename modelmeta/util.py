from .v2 import Base


def create_test_database(engine):
    Base.metadata.create_all(bind=engine)
