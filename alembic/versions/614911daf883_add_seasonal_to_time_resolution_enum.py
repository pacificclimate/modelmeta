"""add 'seasonal' to time_resolution enum

Revision ID: 614911daf883
Revises: 
Create Date: 2017-07-17 17:00:43.066818

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '614911daf883'
down_revision = None
branch_labels = None
depends_on = None

# Because we use SQLite for our test databases, we must use "bach mode" to
# do most table alterations, including the one that changes the Enum type
# for time_resolution. See http://alembic.zzzcomputing.com/en/latest/batch.html
#
# Specifically, that documentation says:
#   On other backends, we’d see the usual ALTER statements done as though
#   there were no batch directive - the batch context by default only does
#   the “move and copy” process if SQLite is in use, and if there are migration
#   directives other than Operations.add_column() present, which is the one
#   kind of column-level ALTER statement that SQLite supports.
#
# Therefore, this code, while created to accommodate SQLite, should also work
# for our production Postgres databases.

def upgrade():
    with op.batch_alter_table('time_sets') as batch_op:
        batch_op.alter_column(
            'time_resolution',
            type_=sa.Enum(
                '1-minute', '2-minute', '5-minute', '15-minute', '30-minute',
                '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily',
                'monthly', 'seasonal', 'yearly', 'other', 'irregular',
                name='timescale'
            ),
            existing_type=sa.Enum(
                '1-minute', '2-minute', '5-minute', '15-minute', '30-minute',
                '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily',
                'monthly', 'yearly', 'other', 'irregular',
                name='timescale'
            )
        )


def downgrade():
    with op.batch_alter_table('time_sets') as batch_op:
        batch_op.alter_column(
            'time_resolution',
            type_=sa.Enum(
                '1-minute', '2-minute', '5-minute', '15-minute', '30-minute',
                '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily',
                'monthly', 'yearly', 'other', 'irregular',
                name='timescale'
            ),
            existing_type=sa.Enum(
                '1-minute', '2-minute', '5-minute', '15-minute', '30-minute',
                '1-hourly', '3-hourly', '6-hourly', '12-hourly', 'daily',
                'monthly', 'seasonal', 'yearly', 'other', 'irregular',
                name='timescale'
            )
        )
