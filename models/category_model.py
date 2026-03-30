from models import db


class DanhMuc(db.Model):
    __tablename__ = "danh_muc"

    id = db.Column(db.Integer, primary_key=True)
    ten_danh_muc = db.Column(db.String(255), nullable=False)

    # COMMON / UNIT
    # COMMON: danh mục dùng chung -> dữ liệu nằm trực tiếp ở 1 bộ dữ liệu chung
    # UNIT: danh mục riêng -> dữ liệu nằm theo từng bộ dữ liệu đơn vị
    scope = db.Column(db.String(20), nullable=False, default="COMMON")

    fields = db.relationship(
        "DanhMucField",
        back_populates="danh_muc",
        cascade="all, delete-orphan",
        lazy="select"
    )

    datasets = db.relationship(
        "DanhMucDataset",
        back_populates="danh_muc",
        cascade="all, delete-orphan",
        lazy="select"
    )

    def __repr__(self):
        return f"<DanhMuc {self.ten_danh_muc}>"


class DanhMucField(db.Model):
    __tablename__ = "danh_muc_field"

    id = db.Column(db.Integer, primary_key=True)
    danh_muc_id = db.Column(db.Integer, db.ForeignKey("danh_muc.id"), nullable=False)

    ma_truong = db.Column(db.String(100), nullable=False)
    ten_truong = db.Column(db.String(255), nullable=False)

    danh_muc = db.relationship("DanhMuc", back_populates="fields")

    __table_args__ = (
        db.UniqueConstraint("danh_muc_id", "ma_truong", name="uq_dm_field"),
    )

    def __repr__(self):
        return f"<DanhMucField {self.ma_truong}>"


class DanhMucDataset(db.Model):
    __tablename__ = "danh_muc_dataset"

    id = db.Column(db.Integer, primary_key=True)
    danh_muc_id = db.Column(db.Integer, db.ForeignKey("danh_muc.id"), nullable=False)

    # với danh mục chung thì null
    # với danh mục riêng thì bắt buộc có
    don_vi_id = db.Column(db.Integer, db.ForeignKey("don_vi.id"), nullable=True)

    ten_bo_du_lieu = db.Column(db.String(255), nullable=False)

    danh_muc = db.relationship("DanhMuc", back_populates="datasets")
    don_vi = db.relationship("DonVi")

    records = db.relationship(
        "DanhMucRecord",
        back_populates="dataset",
        cascade="all, delete-orphan",
        lazy="select"
    )

    __table_args__ = (
        db.UniqueConstraint("danh_muc_id", "don_vi_id", name="uq_dm_dataset_unit"),
    )

    def __repr__(self):
        return f"<DanhMucDataset {self.ten_bo_du_lieu}>"


class DanhMucRecord(db.Model):
    __tablename__ = "danh_muc_record"

    id = db.Column(db.Integer, primary_key=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey("danh_muc_dataset.id"), nullable=False)

    dataset = db.relationship("DanhMucDataset", back_populates="records")

    values = db.relationship(
        "DanhMucRecordValue",
        back_populates="record",
        cascade="all, delete-orphan",
        lazy="select"
    )


class DanhMucRecordValue(db.Model):
    __tablename__ = "danh_muc_record_value"

    id = db.Column(db.Integer, primary_key=True)
    record_id = db.Column(db.Integer, db.ForeignKey("danh_muc_record.id"), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey("danh_muc_field.id"), nullable=False)

    value = db.Column(db.String(500), nullable=True)

    record = db.relationship("DanhMucRecord", back_populates="values")
    field = db.relationship("DanhMucField")

    __table_args__ = (
        db.UniqueConstraint("record_id", "field_id", name="uq_record_field"),
    )