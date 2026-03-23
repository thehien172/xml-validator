from models import db


class DanhMucXml(db.Model):
    __tablename__ = "danh_muc_xml"

    id = db.Column(db.Integer, primary_key=True)
    ma_xml = db.Column(db.String(50), nullable=False, unique=True)   # XML1, XML2, XML3
    ten_xml = db.Column(db.String(255), nullable=False)
    list_path = db.Column(db.String(500), nullable=False)            # path tính từ NOIDUNGFILE

    def __repr__(self):
        return f"<DanhMucXml {self.ma_xml}>"