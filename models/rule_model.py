from models import db


class BoRule(db.Model):
    __tablename__ = "bo_rule"

    id = db.Column(db.Integer, primary_key=True)
    ma_bo_rule = db.Column(db.String(100), nullable=False, unique=True)
    ten_bo_rule = db.Column(db.String(255), nullable=False)
    mo_ta = db.Column(db.String(500), nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    def __repr__(self):
        return f"<BoRule {self.ma_bo_rule}>"


class Rule(db.Model):
    __tablename__ = "rule"

    id = db.Column(db.Integer, primary_key=True)
    bo_rule_id = db.Column(db.Integer, db.ForeignKey("bo_rule.id"), nullable=False)
    ten_rule = db.Column(db.String(255), nullable=False)
    thong_bao = db.Column(db.String(500), nullable=False)
    severity = db.Column(db.String(50), nullable=False, default="WARNING")
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    apply_scope = db.Column(db.String(20), nullable=False, default="ALL")
    run_scope = db.Column(db.String(20), nullable=False, default="ONE_HOSO")

    bo_rule = db.relationship("BoRule", backref="rules")

    def __repr__(self):
        return f"<Rule {self.ten_rule}>"


class RuleUnit(db.Model):
    __tablename__ = "rule_unit"

    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    don_vi_id = db.Column(db.Integer, db.ForeignKey("don_vi.id"), nullable=False)

    rule = db.relationship("Rule", backref="rule_units")
    don_vi = db.relationship("DonVi")

    __table_args__ = (
        db.UniqueConstraint("rule_id", "don_vi_id", name="uq_rule_unit_rule_don_vi"),
    )

    def __repr__(self):
        return f"<RuleUnit rule_id={self.rule_id} don_vi_id={self.don_vi_id}>"


class RuleDetail(db.Model):
    __tablename__ = "rule_detail"

    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey("danh_muc_truong_du_lieu.id"), nullable=False)
    condition_id = db.Column(db.Integer, db.ForeignKey("danh_muc_dieu_kien.id"), nullable=False)

    gia_tri = db.Column(db.String(255), nullable=True)
    condition_role = db.Column(db.String(50), nullable=False)
    sort_order = db.Column(db.Integer, nullable=False, default=1)

    compare_mode = db.Column(db.String(20), nullable=False, default="VALUE")

    # FIELD
    compare_field_id = db.Column(db.Integer, db.ForeignKey("danh_muc_truong_du_lieu.id"), nullable=True)

    # CATEGORY (NEW)
    compare_category_id = db.Column(db.Integer, db.ForeignKey("danh_muc.id"), nullable=True)
    compare_category_field_id = db.Column(db.Integer, db.ForeignKey("danh_muc_field.id"), nullable=True)

    date_part = db.Column(db.String(50), nullable=True)

    group_no = db.Column(db.Integer, nullable=False, default=1)

    rule = db.relationship("Rule", backref="details")
    field = db.relationship("DanhMucTruongDuLieu", foreign_keys=[field_id])
    condition = db.relationship("DanhMucDieuKien")
    compare_field = db.relationship("DanhMucTruongDuLieu", foreign_keys=[compare_field_id])

    # NEW
    compare_category = db.relationship("DanhMuc")
    compare_category_field = db.relationship("DanhMucField")

    def __repr__(self):
        return f"<RuleDetail rule_id={self.rule_id}>"

    def __repr__(self):
        return f"<RuleDetail rule_id={self.rule_id} field_id={self.field_id}>"