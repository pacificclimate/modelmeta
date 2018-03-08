"""initial create

Revision ID: 7847aa3c1b39
Revises: 
Create Date: 2017-08-01 15:25:29.289114

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7847aa3c1b39'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # `create extension postgis` was originally done externally to the migration
    # but it proved easier (a.k.a. apparently necessary) for testing to do it
    # here. Also, it makes eminent sense to remove an external manual step in
    # setting up a new database. Hence:
    op.execute('create extension postgis')

    op.create_table('emissions',
    sa.Column('emission_id', sa.Integer(), nullable=False),
    sa.Column('emission_long_name', sa.String(length=255), nullable=True),
    sa.Column('emission_short_name', sa.String(length=255), nullable=False),
    sa.PrimaryKeyConstraint('emission_id')
    )
    op.create_table('ensembles',
    sa.Column('ensemble_id', sa.Integer(), nullable=False),
    sa.Column('changes', sa.String(), nullable=False),
    sa.Column('ensemble_description', sa.String(length=255), nullable=True),
    sa.Column('ensemble_name', sa.String(length=32), nullable=False),
    sa.Column('version', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('ensemble_id'),
    sa.UniqueConstraint('ensemble_name', 'version', name='ensemble_name_version_key')
    )
    op.create_table('grids',
    sa.Column('grid_id', sa.Integer(), nullable=False),
    sa.Column('grid_name', sa.String(length=255), nullable=True),
    sa.Column('cell_avg_area_sq_km', sa.Float(), nullable=True),
    sa.Column('evenly_spaced_y', sa.Boolean(), nullable=False),
    sa.Column('xc_count', sa.Integer(), nullable=False),
    sa.Column('xc_grid_step', sa.Float(), nullable=False),
    sa.Column('xc_origin', sa.Float(), nullable=False),
    sa.Column('xc_units', sa.String(length=64), nullable=False),
    sa.Column('yc_count', sa.Integer(), nullable=False),
    sa.Column('yc_grid_step', sa.Float(), nullable=False),
    sa.Column('yc_origin', sa.Float(), nullable=False),
    sa.Column('yc_units', sa.String(length=64), nullable=False),
    sa.Column('srid', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('grid_id'),
    # IMPORTANT: This FK constraint is hand-coded. See comments in SQLAlchemy
    # declaration of Grid in v2.py.
    sa.ForeignKeyConstraint(['srid'], ['public.spatial_ref_sys.srid'], )
    )
    op.create_table('level_sets',
    sa.Column('level_set_id', sa.Integer(), nullable=False),
    sa.Column('level_units', sa.String(length=32), nullable=False),
    sa.PrimaryKeyConstraint('level_set_id')
    )
    op.create_table('models',
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.Column('model_long_name', sa.String(length=255), nullable=True),
    sa.Column('model_short_name', sa.String(length=32), nullable=False),
    sa.Column('model_organization', sa.String(length=64), nullable=True),
    sa.Column('type', sa.String(length=32), nullable=False),
    sa.PrimaryKeyConstraint('model_id')
    )
    op.create_table('qc_flags',
    sa.Column('qc_flag_id', sa.Integer(), nullable=False),
    sa.Column('qc_flag_description', sa.String(length=2048), nullable=True),
    sa.Column('qc_flag_name', sa.String(length=32), nullable=False),
    sa.PrimaryKeyConstraint('qc_flag_id')
    )
    op.create_table('time_sets',
    sa.Column('time_set_id', sa.Integer(), nullable=False),
    sa.Column('calendar', sa.String(length=32), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=False),
    sa.Column('end_date', sa.DateTime(), nullable=False),
    sa.Column('multi_year_mean', sa.Boolean(), nullable=False),
    sa.Column('num_times', sa.Integer(), nullable=False),
    sa.Column('time_resolution', sa.Enum('1-minute', '2-minute', '5-minute', '15-minute', '30-minute', '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily', 'monthly', 'yearly', 'other', 'irregular', name='timescale'), nullable=False),
    sa.PrimaryKeyConstraint('time_set_id')
    )
    op.create_table('variable_aliases',
    sa.Column('variable_alias_id', sa.Integer(), nullable=False),
    sa.Column('variable_long_name', sa.String(length=255), nullable=False),
    sa.Column('variable_standard_name', sa.String(length=64), nullable=False),
    sa.Column('variable_units', sa.String(length=32), nullable=False),
    sa.PrimaryKeyConstraint('variable_alias_id')
    )
    op.create_table('climatological_times',
    sa.Column('time_idx', sa.Integer(), nullable=False),
    sa.Column('time_end', sa.DateTime(), nullable=False),
    sa.Column('time_start', sa.DateTime(), nullable=False),
    sa.Column('time_set_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['time_set_id'], ['time_sets.time_set_id'], ),
    sa.PrimaryKeyConstraint('time_idx', 'time_set_id')
    )
    with op.batch_alter_table('climatological_times', schema=None) as batch_op:
        batch_op.create_index('climatological_times_time_set_id_key', ['time_set_id'], unique=False)

    op.create_table('levels',
    sa.Column('level_end', sa.Float(), nullable=True),
    sa.Column('level_idx', sa.Integer(), nullable=False),
    sa.Column('level_set_id', sa.Integer(), nullable=False),
    sa.Column('level_start', sa.Float(), nullable=True),
    sa.Column('vertical_level', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['level_set_id'], ['level_sets.level_set_id'], ),
    sa.PrimaryKeyConstraint('level_idx', 'level_set_id')
    )
    op.create_table('runs',
    sa.Column('run_id', sa.Integer(), nullable=False),
    sa.Column('run_name', sa.String(length=32), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.Column('emission_id', sa.Integer(), nullable=False),
    sa.Column('project', sa.String(length=64), nullable=True),
    sa.Column('driving_run', sa.Integer(), nullable=True),
    sa.Column('initialized_from', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['driving_run'], ['runs.run_id'], ),
    sa.ForeignKeyConstraint(['emission_id'], ['emissions.emission_id'], ),
    sa.ForeignKeyConstraint(['initialized_from'], ['runs.run_id'], ),
    sa.ForeignKeyConstraint(['model_id'], ['models.model_id'], ),
    sa.PrimaryKeyConstraint('run_id'),
    sa.UniqueConstraint('run_name', 'model_id', 'emission_id', name='unique_run_model_emissions_constraint')
    )
    with op.batch_alter_table('runs', schema=None) as batch_op:
        batch_op.create_index('runs_emission_id_key', ['emission_id'], unique=False)
        batch_op.create_index('runs_model_id_key', ['model_id'], unique=False)

    op.create_table('times',
    sa.Column('time_idx', sa.Integer(), nullable=False),
    sa.Column('timestep', sa.DateTime(), nullable=False),
    sa.Column('time_set_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['time_set_id'], ['time_sets.time_set_id'], ),
    sa.PrimaryKeyConstraint('timestep', 'time_set_id')
    )
    with op.batch_alter_table('times', schema=None) as batch_op:
        batch_op.create_index('time_set_id_key', ['time_set_id'], unique=False)

    op.create_table('variables',
    sa.Column('variable_id', sa.Integer(), nullable=False),
    sa.Column('variable_alias_id', sa.Integer(), nullable=False),
    sa.Column('variable_description', sa.String(length=255), nullable=False),
    sa.Column('variable_name', sa.String(length=64), nullable=False),
    sa.ForeignKeyConstraint(['variable_alias_id'], ['variable_aliases.variable_alias_id'], ),
    sa.PrimaryKeyConstraint('variable_id')
    )
    op.create_table('y_cell_bounds',
    sa.Column('bottom_bnd', sa.Float(), nullable=True),
    sa.Column('grid_id', sa.Integer(), nullable=False),
    sa.Column('top_bnd', sa.Float(), nullable=True),
    sa.Column('y_center', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['grid_id'], ['grids.grid_id'], name='y_cell_bounds_grid_id_fkey', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('grid_id', 'y_center')
    )
    with op.batch_alter_table('y_cell_bounds', schema=None) as batch_op:
        batch_op.create_index('y_c_b_grid_id_key', ['grid_id'], unique=False)

    op.create_table('data_files',
    sa.Column('data_file_id', sa.Integer(), nullable=False),
    sa.Column('filename', sa.String(length=2048), nullable=False),
    sa.Column('first_1mib_md5sum', sa.String(length=32), nullable=False),
    sa.Column('unique_id', sa.String(length=255), nullable=False),
    sa.Column('x_dim_name', sa.String(length=32), nullable=False),
    sa.Column('y_dim_name', sa.String(length=32), nullable=False),
    sa.Column('z_dim_name', sa.String(length=32), nullable=True),
    sa.Column('t_dim_name', sa.String(length=32), nullable=True),
    sa.Column('index_time', sa.DateTime(), nullable=False),
    sa.Column('run_id', sa.Integer(), nullable=True),
    sa.Column('time_set_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['run_id'], ['runs.run_id'], ),
    sa.ForeignKeyConstraint(['time_set_id'], ['time_sets.time_set_id'], ),
    sa.PrimaryKeyConstraint('data_file_id'),
    sa.UniqueConstraint('unique_id', name='data_files_unique_id_key')
    )
    with op.batch_alter_table('data_files', schema=None) as batch_op:
        batch_op.create_index('data_files_run_id_key', ['run_id'], unique=False)

    op.create_table('data_file_variables',
    sa.Column('data_file_variable_id', sa.Integer(), nullable=False),
    sa.Column('derivation_method', sa.String(length=255), nullable=True),
    sa.Column('variable_cell_methods', sa.String(length=255), nullable=True),
    sa.Column('netcdf_variable_name', sa.String(length=32), nullable=False),
    sa.Column('disabled', sa.Boolean(), nullable=True),
    sa.Column('range_min', sa.Float(), nullable=False),
    sa.Column('range_max', sa.Float(), nullable=False),
    sa.Column('data_file_id', sa.Integer(), nullable=False),
    sa.Column('variable_alias_id', sa.Integer(), nullable=False),
    sa.Column('level_set_id', sa.Integer(), nullable=True),
    sa.Column('grid_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_file_id'], ['data_files.data_file_id'], name='data_file_variables_data_file_id_fkey', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['grid_id'], ['grids.grid_id'], ),
    sa.ForeignKeyConstraint(['level_set_id'], ['level_sets.level_set_id'], ),
    sa.ForeignKeyConstraint(['variable_alias_id'], ['variable_aliases.variable_alias_id'], ),
    sa.PrimaryKeyConstraint('data_file_variable_id')
    )
    op.create_table('data_file_variables_qc_flags',
    sa.Column('data_file_variable_id', sa.Integer(), nullable=False),
    sa.Column('qc_flag_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_file_variable_id'], ['data_file_variables.data_file_variable_id'], name='data_file_variables_qc_flags_data_file_variable_id_fkey', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['qc_flag_id'], ['qc_flags.qc_flag_id'], ),
    sa.PrimaryKeyConstraint('data_file_variable_id', 'qc_flag_id')
    )
    op.create_table('ensemble_data_file_variables',
    sa.Column('ensemble_id', sa.Integer(), nullable=False),
    sa.Column('data_file_variable_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['data_file_variable_id'], ['data_file_variables.data_file_variable_id'], name='ensemble_data_file_variables_data_file_variable_id_fkey', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['ensemble_id'], ['ensembles.ensemble_id'], name='ensemble_data_file_variables_ensemble_id_fkey', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('ensemble_id', 'data_file_variable_id')
    )


def downgrade():
    op.drop_table('ensemble_data_file_variables')
    op.drop_table('data_file_variables_qc_flags')
    op.drop_table('data_file_variables')
    with op.batch_alter_table('data_files', schema=None) as batch_op:
        batch_op.drop_index('data_files_run_id_key')

    op.drop_table('data_files')
    with op.batch_alter_table('y_cell_bounds', schema=None) as batch_op:
        batch_op.drop_index('y_c_b_grid_id_key')

    op.drop_table('y_cell_bounds')
    op.drop_table('variables')
    with op.batch_alter_table('times', schema=None) as batch_op:
        batch_op.drop_index('time_set_id_key')

    op.drop_table('times')
    with op.batch_alter_table('runs', schema=None) as batch_op:
        batch_op.drop_index('runs_model_id_key')
        batch_op.drop_index('runs_emission_id_key')

    op.drop_table('runs')
    op.drop_table('levels')
    with op.batch_alter_table('climatological_times', schema=None) as batch_op:
        batch_op.drop_index('climatological_times_time_set_id_key')

    op.drop_table('climatological_times')
    op.drop_table('variable_aliases')
    op.drop_table('time_sets')
    op.drop_table('qc_flags')
    op.drop_table('models')
    op.drop_table('level_sets')
    op.drop_table('grids')
    op.drop_table('ensembles')
    op.drop_table('emissions')
