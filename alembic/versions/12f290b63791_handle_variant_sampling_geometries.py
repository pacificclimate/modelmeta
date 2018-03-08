"""handle variant sampling geometries

Revision ID: 12f290b63791
Revises: 614911daf883
Create Date: 2018-03-01 17:08:12.325506

"""
from warnings import warn

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '12f290b63791'
down_revision = '614911daf883'
branch_labels = None
depends_on = None


def get_dialect():
    connection = op.get_bind()
    dialect = connection.dialect.name
    return dialect


def copy_level_set_id_and_grid_id_to_data_file_variables_gridded():
    """
    Copy ``level_set_id`` and ``grid_id`` from ``data_file_variables`` to
    ``data_file_variables_gridded``.
    """
    # Adapted from https://gist.github.com/mafrosis/5e456eb16bf4cc619c959f4d6e1aa8e1
    dfvs = sa.Table(
        'data_file_variables',
        sa.MetaData(bind=op.get_bind()),
        autoload=True
    )
    dfvs_gridded = sa.Table(
        'data_file_variables_gridded',
        sa.MetaData(bind=op.get_bind()),
        autoload=True
    )
    op.get_bind().execute(
        dfvs.update().values(geometry_type='gridded')
    )
    op.get_bind().execute(
        dfvs_gridded.insert().from_select(
            ['id', 'level_set_id', 'grid_id'],
            sa.select([dfvs.c.data_file_variable_id, dfvs.c.level_set_id, dfvs.c.grid_id])
        )
    )


def upgrade():
    # Start schema migration - don't drop columns that need to be copied to
    # data_file_variables_gridded

    op.create_table('stations',
    sa.Column('station_id', sa.Integer(), nullable=False),
    sa.Column('x', sa.Float(), nullable=False),
    sa.Column('x_units', sa.String(length=64), nullable=False),
    sa.Column('y', sa.Float(), nullable=False),
    sa.Column('y_units', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=32), nullable=True),
    sa.Column('long_name', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('station_id')
    )

    op.create_table('data_file_variables_dsg_time_series',
    sa.Column('data_file_variable_dsg_ts_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_file_variable_dsg_ts_id'], ['data_file_variables.data_file_variable_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('data_file_variable_dsg_ts_id')
    )

    op.create_table('data_file_variables_gridded',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('level_set_id', sa.Integer(), nullable=True),
    sa.Column('grid_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['grid_id'], ['grids.grid_id'], ),
    sa.ForeignKeyConstraint(
        ['id'], ['data_file_variables.data_file_variable_id'],
        ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['level_set_id'], ['level_sets.level_set_id'], ),
    sa.PrimaryKeyConstraint('id')
    )

    op.create_table('data_file_variables_dsg_time_series_x_stations',
    sa.Column('data_file_variable_dsg_ts_id', sa.Integer(), nullable=False),
    sa.Column('station_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(
        ['data_file_variable_dsg_ts_id'],
        ['data_file_variables_dsg_time_series.data_file_variable_dsg_ts_id'],
        name='data_file_variables_dsg_time_series_x_stations_data_file_variable_dsg_ts_id_id_fkey',
        ondelete='CASCADE'),
    sa.ForeignKeyConstraint(
        ['station_id'], ['stations.station_id'],
        name='data_file_variables_dsg_time_series_x_stations_station_id_fkey',
        ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('data_file_variable_dsg_ts_id', 'station_id')
    )

    with op.batch_alter_table('data_files', schema=None) as batch_op:
        batch_op.alter_column('x_dim_name', nullable=True)
        batch_op.alter_column('y_dim_name', nullable=True)

    with op.batch_alter_table('data_file_variables', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('geometry_type',
                      sa.String(length=50),
                      nullable=True)
        )

    # Do data migration
    copy_level_set_id_and_grid_id_to_data_file_variables_gridded()


    # Finish schema migration - drop columns copied to
    # data_file_variables_gridded

    with op.batch_alter_table('data_file_variables', schema=None) as batch_op:
        batch_op.drop_constraint('data_file_variables_grid_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('data_file_variables_level_set_id_fkey', type_='foreignkey')
        batch_op.drop_column('grid_id')
        batch_op.drop_column('level_set_id')


def copy_level_set_id_and_grid_id_from_data_file_variables_gridded():
    """
    Copy ``level_set_id`` and ``grid_id`` from ``data_file_variables_gridded`` to
    ``data_file_variables``.
    """
    # Adapted from https://gist.github.com/mafrosis/5e456eb16bf4cc619c959f4d6e1aa8e1
    dfvs = sa.Table(
        'data_file_variables',
        sa.MetaData(bind=op.get_bind()),
        autoload=True
    )
    dfvs_gridded = sa.Table(
        'data_file_variables_gridded',
        sa.MetaData(bind=op.get_bind()),
        autoload=True
    )
    if get_dialect() == 'sqlite':
        # SQLite doesn't support the (standard) form ``UPDATE ... SET ... FROM ...``
        # (https://stackoverflow.com/questions/3845718/), so we have to use an
        # alternate form of UPDATE. This could be really inefficient.
        op.get_bind().execute(
            dfvs.update().values(
                level_set_id=
                    sa.select([dfvs_gridded.c.level_set_id])
                    .where(dfvs_gridded.c.id == dfvs.c.data_file_variable_id),
                grid_id=
                    sa.select([dfvs_gridded.c.grid_id])
                    .where(dfvs_gridded.c.id == dfvs.c.data_file_variable_id),
            )
        )
    else:
        # Standard form, efficient
        op.get_bind().execute(
            dfvs.update().values(
                level_set_id=dfvs_gridded.c.level_set_id,
                grid_id=dfvs_gridded.c.grid_id,
            ).where(
                dfvs_gridded.c.id == dfvs.c.data_file_variable_id
            )
        )


def delete_dsg_time_series_data_file_variables():
    dfvs = sa.Table(
        'data_file_variables',
        sa.MetaData(bind=op.get_bind()),
        autoload=True
    )
    dfvs.delete().where(dfvs.c.geometry_type == 'dsg_time_series')



def downgrade():
    # This downgrade will dump all discrete geometry data.
    # TODO: Add user confirmation of downgrade?

    # Start schema migration - don't drop geometry discriminator until after
    # data migration

    with op.batch_alter_table('data_file_variables', schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            'level_set_id', sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.add_column(sa.Column(
            'grid_id', sa.INTEGER(), autoincrement=False,
            nullable=False, server_default=sa.text('0')))
        batch_op.create_foreign_key(
            'data_file_variables_level_set_id_fkey', 'level_sets',
            ['level_set_id'], ['level_set_id'])
        batch_op.create_foreign_key(
            'data_file_variables_grid_id_fkey', 'grids',
            ['grid_id'], ['grid_id'])

    # Do data migration
    copy_level_set_id_and_grid_id_from_data_file_variables_gridded()
    delete_dsg_time_series_data_file_variables()

    # Finish schema migration - drop geometry discriminator at very end

    op.drop_table('data_file_variables_dsg_time_series_x_stations')
    op.drop_table('data_file_variables_gridded')
    op.drop_table('data_file_variables_dsg_time_series')
    op.drop_table('stations')

    with op.batch_alter_table('data_file_variables', schema=None) as batch_op:
        batch_op.drop_column('geometry_type')

    with op.batch_alter_table('data_files', schema=None) as batch_op:
        batch_op.alter_column('x_dim_name', nullable=False)
        batch_op.alter_column('y_dim_name', nullable=False)

