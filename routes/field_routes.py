from flask import Blueprint, render_template, request, redirect, url_for
from sqlalchemy import or_

from models import db, DanhMucTruongDuLieu, DanhMucXml

field_bp = Blueprint("field_bp", __name__, url_prefix="/fields")


@field_bp.route("/", methods=["GET"])
def list_fields():
    keyword = (request.args.get("keyword") or "").strip()
    xml_id = (request.args.get("xml_id") or "").strip()

    query = DanhMucTruongDuLieu.query.join(DanhMucXml)

    if keyword:
        query = query.filter(
            or_(
                DanhMucTruongDuLieu.ten_truong.ilike(f"%{keyword}%"),
                DanhMucTruongDuLieu.xml_path.ilike(f"%{keyword}%"),
                DanhMucXml.ma_xml.ilike(f"%{keyword}%"),
                DanhMucXml.ten_xml.ilike(f"%{keyword}%")
            )
        )

    if xml_id:
        query = query.filter(DanhMucTruongDuLieu.xml_id == int(xml_id))

    fields = query.order_by(DanhMucTruongDuLieu.id.asc()).all()
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()

    return render_template(
        "fields.html",
        fields=fields,
        xmls=xmls,
        keyword=keyword,
        xml_id=xml_id
    )


@field_bp.route("/create", methods=["GET", "POST"])
def create_field():
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()

    if request.method == "POST":
        field = DanhMucTruongDuLieu(
            ten_truong=request.form.get("ten_truong", "").strip(),
            xml_id=int(request.form.get("xml_id")),
            xml_path=request.form.get("xml_path", "").strip(),
            data_type=request.form.get("data_type", "").strip()
        )
        db.session.add(field)
        db.session.commit()
        return redirect(url_for("field_bp.list_fields"))

    return render_template("field_form.html", item=None, xmls=xmls)


@field_bp.route("/<int:field_id>/edit", methods=["GET", "POST"])
def edit_field(field_id):
    item = DanhMucTruongDuLieu.query.get_or_404(field_id)
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()

    if request.method == "POST":
        item.ten_truong = request.form.get("ten_truong", "").strip()
        item.xml_id = int(request.form.get("xml_id"))
        item.xml_path = request.form.get("xml_path", "").strip()
        item.data_type = request.form.get("data_type", "").strip()
        db.session.commit()
        return redirect(url_for("field_bp.list_fields"))

    return render_template("field_form.html", item=item, xmls=xmls)


@field_bp.route("/<int:field_id>/delete", methods=["POST"])
def delete_field(field_id):
    item = DanhMucTruongDuLieu.query.get_or_404(field_id)
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("field_bp.list_fields"))