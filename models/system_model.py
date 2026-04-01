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

    # HIS
    api_username = db.Column(db.String(100))
    api_password = db.Column(db.String(100))

    # BHYT login
    bhyt_username = db.Column(db.String(100))
    bhyt_password = db.Column(db.String(100))

    # cookie BHYT
    bhyt_aspxauth = db.Column(db.Text)
    bhyt_aspnet_session_id = db.Column(db.String(255))
    bhyt_bigipserver = db.Column(db.String(255))
    bhyt_ts015ef943 = db.Column(db.String(255))

    api_base_url = db.Column(db.String(255))
    api_uuid = db.Column(db.Text)
    api_his_token = db.Column(db.Text)
    api_token_expire_at = db.Column(db.DateTime)

    api_jsessionid = db.Column(db.String(255))
    api_sessionid = db.Column(db.Text)

    he_thong = db.relationship("HeThong", backref="don_vis")

    def __repr__(self):
        return f"<DonVi {self.ma_don_vi} - {self.ten_don_vi}>"