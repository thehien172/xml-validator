from flask import Blueprint, render_template, request, redirect, url_for
from sqlalchemy import or_

from models import db, HeThong, DonVi

system_bp = Blueprint("system_bp", __name__, url_prefix="/systems")


@system_bp.route("/", methods=["GET"])
def list_systems():
    keyword = (request.args.get("keyword") or "").strip()

    query = HeThong.query
    if keyword:
        query = query.filter(HeThong.ten_he_thong.ilike(f"%{keyword}%"))

    items = query.order_by(HeThong.id.asc()).all()
    return render_template("unit/systems.html", items=items, keyword=keyword)


@system_bp.route("/create", methods=["GET", "POST"])
def create_system():
    if request.method == "POST":
        item = HeThong(
            ten_he_thong=request.form.get("ten_he_thong", "").strip()
        )
        db.session.add(item)
        db.session.commit()
        return redirect(url_for("system_bp.list_systems"))

    return render_template("unit/system_form.html", item=None)


@system_bp.route("/<int:item_id>/edit", methods=["GET", "POST"])
def edit_system(item_id):
    item = HeThong.query.get_or_404(item_id)

    if request.method == "POST":
        item.ten_he_thong = request.form.get("ten_he_thong", "").strip()
        db.session.commit()
        return redirect(url_for("system_bp.list_systems"))

    return render_template("unit/system_form.html", item=item)


@system_bp.route("/<int:item_id>/delete", methods=["POST"])
def delete_system(item_id):
    item = HeThong.query.get_or_404(item_id)

    # Không cho xóa nếu đã có đơn vị
    if item.don_vis:
        return "Không thể xóa hệ thống vì đang có đơn vị thuộc hệ thống này.", 400

    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("system_bp.list_systems"))


unit_bp = Blueprint("unit_bp", __name__, url_prefix="/units")


@unit_bp.route("/", methods=["GET"])
def list_units():
    keyword = (request.args.get("keyword") or "").strip()
    he_thong_id = (request.args.get("he_thong_id") or "").strip()

    query = DonVi.query.join(HeThong)

    if keyword:
        query = query.filter(
            or_(
                DonVi.ma_don_vi.ilike(f"%{keyword}%"),
                DonVi.ten_don_vi.ilike(f"%{keyword}%"),
                HeThong.ten_he_thong.ilike(f"%{keyword}%")
            )
        )

    if he_thong_id:
        query = query.filter(DonVi.he_thong_id == int(he_thong_id))

    items = query.order_by(DonVi.id.asc()).all()
    systems = HeThong.query.order_by(HeThong.id.asc()).all()

    return render_template(
        "unit/units.html",
        items=items,
        systems=systems,
        keyword=keyword,
        he_thong_id=he_thong_id
    )


@unit_bp.route("/create", methods=["GET", "POST"])
def create_unit():
    systems = HeThong.query.order_by(HeThong.id.asc()).all()

    if request.method == "POST":
        item = DonVi(
            ma_don_vi=request.form.get("ma_don_vi", "").strip(),
            ten_don_vi=request.form.get("ten_don_vi", "").strip(),
            he_thong_id=int(request.form.get("he_thong_id")),
            api_username=request.form.get("api_username"),
            api_password=request.form.get("api_password"),
            bhyt_username=request.form.get("bhyt_username"),
            bhyt_password=request.form.get("bhyt_password"),
        )
        db.session.add(item)
        db.session.commit()
        return redirect(url_for("unit_bp.list_units"))

    return render_template("unit/unit_form.html", item=None, systems=systems)


@unit_bp.route("/<int:item_id>/edit", methods=["GET", "POST"])
def edit_unit(item_id):
    item = DonVi.query.get_or_404(item_id)
    systems = HeThong.query.order_by(HeThong.id.asc()).all()

    if request.method == "POST":
        item.ma_don_vi = request.form.get("ma_don_vi", "").strip()
        item.ten_don_vi = request.form.get("ten_don_vi", "").strip()
        item.he_thong_id = int(request.form.get("he_thong_id"))
        item.api_username = request.form.get("api_username")
        item.api_password = request.form.get("api_password")
        item.bhyt_username = request.form.get("bhyt_username")
        item.bhyt_password = request.form.get("bhyt_password")
        db.session.commit()
        return redirect(url_for("unit_bp.list_units"))

    return render_template("unit/unit_form.html", item=item, systems=systems)


@unit_bp.route("/<int:item_id>/delete", methods=["POST"])
def delete_unit(item_id):
    item = DonVi.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("unit_bp.list_units"))