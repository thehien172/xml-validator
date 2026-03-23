from flask import Blueprint, render_template, request, redirect, url_for
from sqlalchemy import or_

from models import db, DanhMucXml

xml_bp = Blueprint("xml_bp", __name__, url_prefix="/xmls")


@xml_bp.route("/", methods=["GET"])
def list_xmls():
    keyword = (request.args.get("keyword") or "").strip()

    query = DanhMucXml.query
    if keyword:
        query = query.filter(
            or_(
                DanhMucXml.ma_xml.ilike(f"%{keyword}%"),
                DanhMucXml.ten_xml.ilike(f"%{keyword}%"),
                DanhMucXml.list_path.ilike(f"%{keyword}%")
            )
        )

    xmls = query.order_by(DanhMucXml.id.asc()).all()
    return render_template("xmls.html", xmls=xmls, keyword=keyword)


@xml_bp.route("/create", methods=["GET", "POST"])
def create_xml():
    if request.method == "POST":
        xml = DanhMucXml(
            ma_xml=request.form.get("ma_xml", "").strip().upper(),
            ten_xml=request.form.get("ten_xml", "").strip(),
            list_path=request.form.get("list_path", "").strip()
        )
        db.session.add(xml)
        db.session.commit()
        return redirect(url_for("xml_bp.list_xmls"))

    return render_template("xml_form.html", item=None)


@xml_bp.route("/<int:xml_id>/edit", methods=["GET", "POST"])
def edit_xml(xml_id):
    item = DanhMucXml.query.get_or_404(xml_id)

    if request.method == "POST":
        item.ma_xml = request.form.get("ma_xml", "").strip().upper()
        item.ten_xml = request.form.get("ten_xml", "").strip()
        item.list_path = request.form.get("list_path", "").strip()
        db.session.commit()
        return redirect(url_for("xml_bp.list_xmls"))

    return render_template("xml_form.html", item=item)


@xml_bp.route("/<int:xml_id>/delete", methods=["POST"])
def delete_xml(xml_id):
    item = DanhMucXml.query.get_or_404(xml_id)
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("xml_bp.list_xmls"))