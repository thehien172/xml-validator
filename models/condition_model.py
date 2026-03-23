from models import db


class DanhMucDieuKien(db.Model):
    __tablename__ = "danh_muc_dieu_kien"

    id = db.Column(db.Integer, primary_key=True)
    ma_dieu_kien = db.Column(db.String(50), nullable=False, unique=True)
    ten_dieu_kien = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f"<Condition {self.ma_dieu_kien}>"