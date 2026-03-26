from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a6e12e957f15"
down_revision = None   # hoặc revision trước đó của file này
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "rule_detail",
        sa.Column("compare_mode", sa.String(length=20), nullable=False, server_default="VALUE")
    )

    op.add_column(
        "rule_detail",
        sa.Column("compare_field_id", sa.Integer(), nullable=True)
    )

    op.add_column(
        "rule_detail",
        sa.Column("date_part", sa.String(length=50), nullable=True)
    )

    op.add_column(
        "rule_detail",
        sa.Column("group_no", sa.Integer(), nullable=False, server_default="1")
    )

    # SQLite không hỗ trợ thêm foreign key vào bảng cũ bằng ALTER TABLE như DB khác.
    # Vì vậy chỗ compare_field_id nếu chưa có FK cũng tạm chấp nhận được.
    # App vẫn chạy bình thường nếu code validate dữ liệu ở tầng ứng dụng.


def downgrade():
    op.drop_column("rule_detail", "group_no")
    op.drop_column("rule_detail", "date_part")
    op.drop_column("rule_detail", "compare_field_id")
    op.drop_column("rule_detail", "compare_mode")