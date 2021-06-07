"""Add streamflow order and results tables

Revision ID: c0810e121564
Revises: 12f290b63791
Create Date: 2018-09-05 11:41:58.245456

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c0810e121564'
down_revision = '12f290b63791'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('streamflow_results',
    sa.Column('streamflow_result_id', sa.Integer(), nullable=False),
    sa.Column('data_file_id', sa.Integer(), nullable=True),
    sa.Column('station_id', sa.Integer(), nullable=True),
    sa.Column('status', sa.Enum('queued', 'processing', 'error', 'cancelled', 'ready', 'removed', name='streamflow_result_statuses'), nullable=False),
    sa.ForeignKeyConstraint(['data_file_id'], ['data_files.data_file_id'], ),
    sa.ForeignKeyConstraint(['station_id'], ['stations.station_id'], ),
    sa.PrimaryKeyConstraint('streamflow_result_id')
    )

    op.create_table('streamflow_orders',
    sa.Column('streamflow_order_id', sa.Integer(), nullable=False),
    sa.Column('hydromodel_output_id', sa.Integer(), nullable=False),
    sa.Column('streamflow_result_id', sa.Integer(), nullable=False),
    sa.Column('longitude', sa.Float(), nullable=False),
    sa.Column('latitude', sa.Float(), nullable=False),
    sa.Column('notification_method', sa.Enum('none', 'email', name='notification_methods'), nullable=False),
    sa.Column('notification_address', sa.String(length=255), nullable=True),
    sa.Column('status', sa.Enum('accepted', 'fulfilled', 'cancelled', 'error', name='streamflow_order_statuses'), nullable=False),
    sa.ForeignKeyConstraint(['hydromodel_output_id'], ['data_files.data_file_id'], ),
    sa.ForeignKeyConstraint(['streamflow_result_id'], ['streamflow_results.streamflow_result_id'], ),
    sa.PrimaryKeyConstraint('streamflow_order_id')
    )


def downgrade():
    op.drop_table('streamflow_orders')
    op.drop_table('streamflow_results')
