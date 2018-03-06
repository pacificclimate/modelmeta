# -*- coding: utf-8 -*-
import datetime

import pytest

from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker

from alembic import command

from sqlalchemydiff import compare
from sqlalchemydiff.util import (
    prepare_schema_from_models,
)

from alembicverify.util import (
    get_current_revision,
    get_head_revision,
    prepare_schema_from_migrations,
)
from modelmeta import \
    Base, DataFile, VariableAlias, Grid, \
    DataFileVariable, DataFileVariableGridded


@pytest.mark.usefixtures('new_db_left')
def test_upgrade_and_downgrade(uri_left, alembic_config_left):
    """Test all migrations up and down.

    Tests that we can apply all migrations from a brand new empty
    database, and also that we can remove them all.
    """
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left)

    head = get_head_revision(alembic_config_left, engine, script)
    current = get_current_revision(alembic_config_left, engine, script)

    assert head == current

    while current is not None:
        command.downgrade(alembic_config_left, '-1')
        current = get_current_revision(alembic_config_left, engine, script)


@pytest.mark.usefixtures('new_db_left')
@pytest.mark.usefixtures('new_db_right')
def test_model_and_migration_schemas_are_the_same(
        uri_left, uri_right, alembic_config_left):
    """Compare two databases.

    Compares the database obtained with all migrations against the
    one we get out of the models.
    """
    prepare_schema_from_migrations(uri_left, alembic_config_left)
    engine = create_engine(uri_right)
    engine.execute('create extension postgis')
    prepare_schema_from_models(uri_right, Base)

    result = compare(
        uri_left, uri_right,
        # Ignore grids.srid fkey because of the flaky way it has to be set up;
        # for details see comments in definiton of `Grid` in `v2.py`.
        ignores={'alembic_version', 'grids.fk.grids_srid_fkey'}
    )

    assert result.is_match



def name(base, i):
    return '{}{}'.format(base, i)


@pytest.mark.usefixtures('new_db_left')
def test_12f290b63791_upgrade_data_migration(uri_left, alembic_config_left):
    """
    Test the data migration from 614911daf883 to 12f290b63791.
    Note: Cannot use relative revisions because they will become invalid when
    later revisions are added.

    TODO: It would be better to pull the revision numbers out of the migration
    script (.revision, .down_revision), but they are not (yet) part of the
    modelmeta package.
    """
    # Set up database in pre-migration schema
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left, revision='614911daf883')

    # Define minimal set of tables needed to test migration
    meta_data = MetaData(bind=engine)
    variable_aliases = Table('variable_aliases', meta_data, autoload=True)
    grids = Table('grids', meta_data, autoload=True)
    data_files = Table('data_files', meta_data, autoload=True)
    data_file_variables = Table('data_file_variables', meta_data, autoload=True)

    # Insert minimal data needed to test migration: Several instances of each of
    # variable_aliases, grids, data_files, associated to a data_file_variables.
    num_test_records = 3
    for i in range(0, num_test_records):
        for stmt in [
            variable_aliases.insert().values(
                variable_alias_id=i,
                variable_long_name=name('var', i),
                variable_standard_name=name('var', i),
                variable_units='foo',
            ),
            grids.insert().values(
                grid_id=i,
                xc_origin=0.0,
                xc_grid_step=1.0,
                xc_count=10,
                xc_units='xc_units',
                yc_origin=0.0,
                yc_grid_step=1.0,
                yc_count=10,
                yc_units='yc_units',
                evenly_spaced_y=True,
            ),
            data_files.insert().values(
                data_file_id=i,
                filename=name('filename', i),
                first_1mib_md5sum='first_1mib_md5sum',
                unique_id=name('unique_id', i),
                x_dim_name='x_dim_name',
                y_dim_name='y_dim_name',
                index_time=datetime.datetime.now(),
            ),
            data_file_variables.insert().values(
                data_file_variable_id=i,
                data_file_id=i,
                variable_alias_id=i,
                grid_id=i,
                netcdf_variable_name=name('var', i),
                range_min=0.0,
                range_max=10.0,
            )
        ]:
            engine.execute(stmt)

    # Run upgrade migration
    command.upgrade(alembic_config_left, '+1')

    # Check data results of migration. We can use current ORM for this.
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    sesh = Session()

    dfvs = sesh.query(DataFileVariable).all()
    assert len(dfvs) == num_test_records
    assert all(dfv.geometry_type == 'gridded' for dfv in dfvs)

    dfvs_gridded = sesh.query(DataFileVariableGridded).all()
    assert len(dfvs_gridded) == num_test_records
    assert all(dfv.grid_id == dfv.id for dfv in dfvs_gridded)
    assert all(dfv.variable_alias_id == dfv.id for dfv in dfvs_gridded)
    assert all(dfv.data_file_id == dfv.id for dfv in dfvs_gridded)

    sesh.close()


@pytest.mark.usefixtures('new_db_left')
def test_12f290b63791_downgrade_data_migration(uri_left, alembic_config_left):
    """
    Test the data migration from 12f290b63791 to 614911daf883.
    Note: Cannot use relative revisions because they will become invalid when
    later revisions are added.

    TODO: It would be better to pull the revision numbers out of the migration
    script (.revision, .down_revision), but they are not (yet) part of the
    modelmeta package.
    """
    # Prepare database in post-migration schema
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left)

    Session = sessionmaker(bind=engine)
    sesh = Session()

    # Insert minimal data needed to test migration: Several instances of each of
    # variable_aliases, grids, data_files, associated to a data_file_variables.
    # We can use current ORM for this.
    num_test_records = 3
    for i in range(0, num_test_records):
        variable_alias = VariableAlias(
            id=i,
            long_name=name('var', i),
            standard_name=name('var', i),
            units='foo',
        )
        grid = Grid(
            id=i,
            xc_origin=0.0,
            xc_grid_step=1.0,
            xc_count=10,
            xc_units='xc_units',
            yc_origin=0.0,
            yc_grid_step=1.0,
            yc_count=10,
            yc_units='yc_units',
            evenly_spaced_y=True,
        )
        data_file = DataFile(
            id=i,
            filename=name('filename', i),
            first_1mib_md5sum='first_1mib_md5sum',
            unique_id=name('unique_id', i),
            x_dim_name='x_dim_name',
            y_dim_name='y_dim_name',
            index_time=datetime.datetime.now(),
        )
        data_file_variable = DataFileVariableGridded(
            id=i,
            file=data_file,
            variable_alias=variable_alias,
            grid=grid,
            netcdf_variable_name=name('var', i),
            range_min=0.0,
            range_max=10.0,
        )
        sesh.add(data_file_variable)
        sesh.commit()

    # Run downgrade migration
    command.downgrade(alembic_config_left, '-1')

    # Define minimal set of tables needed to test migration
    meta_data = MetaData(bind=engine)
    data_file_variables = Table('data_file_variables', meta_data, autoload=True)

    # Check data results of migration.
    results = list(engine.execute(
        data_file_variables.select()
    ))

    assert results is not None
    assert len(results) == num_test_records
    assert all(r['variable_alias_id'] == r['data_file_variable_id'] for r in results)
    assert all(r['grid_id'] == r['data_file_variable_id'] for r in results)
    assert all(r['data_file_id'] == r['data_file_variable_id'] for r in results)
