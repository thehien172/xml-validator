"""add compare_mode and compare_field_id to rule_detail

Revision ID: a6e12e957f15
Revises: 
Create Date: 2026-03-23 09:41:30.331217

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a6e12e957f15'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('rule_detail', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('compare_mode', sa.String(length=20), nullable=False, server_default='VALUE')
        )
        batch_op.add_column(
            sa.Column('compare_field_id', sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            'fk_rule_detail_compare_field_id',
            'danh_muc_truong_du_lieu',
            ['compare_field_id'],
            ['id']
        )

    # bỏ server_default sau khi đã tạo xong dữ liệu cũ
    with op.batch_alter_table('rule_detail', schema=None) as batch_op:
        batch_op.alter_column('compare_mode', server_default=None)


def downgrade():
    with op.batch_alter_table('rule_detail', schema=None) as batch_op:
        batch_op.drop_constraint('fk_rule_detail_compare_field_id', type_='foreignkey')
        batch_op.drop_column('compare_field_id')
        batch_op.drop_column('compare_mode')

    # ### end Alembic commands ###
