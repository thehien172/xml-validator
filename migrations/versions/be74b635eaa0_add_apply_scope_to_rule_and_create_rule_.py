from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260326_01"
down_revision = "a6e12e957f15"   # sửa lại đúng revision trước đó của bạn
branch_labels = None
depends_on = None


def upgrade():
    # 1. Thêm cột apply_scope vào bảng rule
    op.add_column(
        "rule",
        sa.Column(
            "apply_scope",
            sa.String(length=20),
            nullable=False,
            server_default="ALL"
        )
    )

    # 2. Tạo bảng rule_unit
    op.create_table(
        "rule_unit",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("don_vi_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["rule.id"]),
        sa.ForeignKeyConstraint(["don_vi_id"], ["don_vi.id"]),
        sa.UniqueConstraint("rule_id", "don_vi_id", name="uq_rule_unit_rule_don_vi")
    )

    # 3. Nếu muốn bỏ server_default sau khi đã tạo xong thì có thể giữ nguyên luôn cũng được.
    # Với SQLite thường cứ để vậy cũng không sao.


def downgrade():
    # rollback theo thứ tự ngược lại
    op.drop_table("rule_unit")
    op.drop_column("rule", "apply_scope")