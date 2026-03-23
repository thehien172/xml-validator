from models import db


class DanhMucTruongDuLieu(db.Model):
    __tablename__ = "danh_muc_truong_du_lieu"

    id = db.Column(db.Integer, primary_key=True)
    ten_truong = db.Column(db.String(255), nullable=False)
    xml_id = db.Column(db.Integer, db.ForeignKey("danh_muc_xml.id"), nullable=False)
    xml_path = db.Column(db.String(500), nullable=False)  # path bên trong từng phần tử của list
    data_type = db.Column(db.String(50), nullable=False, default="STRING")

    xml = db.relationship("DanhMucXml", backref="fields")

    def __repr__(self):
        return f"<Field {self.ten_truong}>"