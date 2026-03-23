from models import db


class HeThong(db.Model):
    __tablename__ = "he_thong"

    id = db.Column(db.Integer, primary_key=True)
    ten_he_thong = db.Column(db.String(100), nullable=False, unique=True)

    def __repr__(self):
        return f"<HeThong {self.ten_he_thong}>"


class DonVi(db.Model):
    __tablename__ = "don_vi"

    id = db.Column(db.Integer, primary_key=True)
    ma_don_vi = db.Column(db.String(50), nullable=False, unique=True)
    ten_don_vi = db.Column(db.String(255), nullable=False)
    he_thong_id = db.Column(db.Integer, db.ForeignKey("he_thong.id"), nullable=False)

    he_thong = db.relationship("HeThong", backref="don_vis")

    def __repr__(self):
        return f"<DonVi {self.ma_don_vi} - {self.ten_don_vi}>"