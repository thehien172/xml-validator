from flask import Blueprint, render_template, request, redirect, url_for, jsonify
from sqlalchemy import or_

from models import (
    db,
    BoRule,
    Rule,
    RuleDetail,
    DanhMucTruongDuLieu,
    DanhMucDieuKien,
    DanhMucXml
)

rule_bp = Blueprint("rule_bp", __name__, url_prefix="/rules")


@rule_bp.route("/api/fields-by-xml", methods=["GET"])
def get_fields_by_xml():
    xml_id = request.args.get("xml_id", type=int)

    if not xml_id:
        return jsonify([])

    fields = (
        DanhMucTruongDuLieu.query
        .filter_by(xml_id=xml_id)
        .order_by(DanhMucTruongDuLieu.id.asc())
        .all()
    )

    result = []
    for f in fields:
        result.append({
            "id": f.id,
            "ten_truong": f.ten_truong,
            "xml_path": f.xml_path,
            "xml_code": f.xml.ma_xml if f.xml else "",
            "data_type": (f.data_type or "STRING").upper()
        })

    return jsonify(result)


@rule_bp.route("/", methods=["GET"])
def list_rules():
    keyword = (request.args.get("keyword") or "").strip()
    status = (request.args.get("status") or "").strip()
    bo_rule_id = (request.args.get("bo_rule_id") or "").strip()

    query = Rule.query.join(BoRule)

    if keyword:
        query = query.filter(
            or_(
                Rule.ten_rule.ilike(f"%{keyword}%"),
                Rule.thong_bao.ilike(f"%{keyword}%"),
                Rule.severity.ilike(f"%{keyword}%"),
                BoRule.ma_bo_rule.ilike(f"%{keyword}%"),
                BoRule.ten_bo_rule.ilike(f"%{keyword}%")
            )
        )

    if status == "active":
        query = query.filter(Rule.is_active.is_(True))
    elif status == "inactive":
        query = query.filter(Rule.is_active.is_(False))

    if bo_rule_id:
        query = query.filter(Rule.bo_rule_id == int(bo_rule_id))

    rules = query.order_by(Rule.id.asc()).all()
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()

    return render_template(
        "rules.html",
        rules=rules,
        keyword=keyword,
        status=status,
        bo_rule_id=bo_rule_id,
        bo_rules=bo_rules
    )


@rule_bp.route("/create", methods=["GET", "POST"])
def create_rule():
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()

    if request.method == "POST":
        rule = Rule(
            bo_rule_id=int(request.form.get("bo_rule_id")),
            ten_rule=request.form.get("ten_rule", "").strip(),
            thong_bao=request.form.get("thong_bao", "").strip(),
            severity=request.form.get("severity", "WARNING").strip(),
            is_active=True if request.form.get("is_active") == "1" else False
        )
        db.session.add(rule)
        db.session.commit()
        return redirect(url_for("rule_bp.list_rules"))

    return render_template("rule_form.html", item=None, bo_rules=bo_rules)


@rule_bp.route("/<int:rule_id>/edit", methods=["GET", "POST"])
def edit_rule(rule_id):
    item = Rule.query.get_or_404(rule_id)
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()

    if request.method == "POST":
        item.bo_rule_id = int(request.form.get("bo_rule_id"))
        item.ten_rule = request.form.get("ten_rule", "").strip()
        item.thong_bao = request.form.get("thong_bao", "").strip()
        item.severity = request.form.get("severity", "WARNING").strip()
        item.is_active = True if request.form.get("is_active") == "1" else False
        db.session.commit()
        return redirect(url_for("rule_bp.list_rules"))

    return render_template("rule_form.html", item=item, bo_rules=bo_rules)


@rule_bp.route("/<int:rule_id>/delete", methods=["POST"])
def delete_rule(rule_id):
    item = Rule.query.get_or_404(rule_id)

    RuleDetail.query.filter_by(rule_id=item.id).delete()
    db.session.delete(item)
    db.session.commit()

    return redirect(url_for("rule_bp.list_rules"))


@rule_bp.route("/<int:rule_id>/details", methods=["GET"])
def list_rule_details(rule_id):
    rule = Rule.query.get_or_404(rule_id)

    keyword = (request.args.get("keyword") or "").strip()
    xml_id = (request.args.get("xml_id") or "").strip()
    role = (request.args.get("role") or "").strip()

    query = (
        RuleDetail.query
        .join(DanhMucTruongDuLieu, RuleDetail.field_id == DanhMucTruongDuLieu.id)
        .join(DanhMucXml, DanhMucTruongDuLieu.xml_id == DanhMucXml.id)
        .join(DanhMucDieuKien, RuleDetail.condition_id == DanhMucDieuKien.id)
        .filter(RuleDetail.rule_id == rule_id)
    )

    if keyword:
        query = query.filter(
            or_(
                DanhMucTruongDuLieu.ten_truong.ilike(f"%{keyword}%"),
                DanhMucTruongDuLieu.xml_path.ilike(f"%{keyword}%"),
                DanhMucDieuKien.ma_dieu_kien.ilike(f"%{keyword}%"),
                DanhMucDieuKien.ten_dieu_kien.ilike(f"%{keyword}%"),
                RuleDetail.gia_tri.ilike(f"%{keyword}%")
            )
        )

    if xml_id:
        query = query.filter(DanhMucTruongDuLieu.xml_id == int(xml_id))

    if role:
        query = query.filter(RuleDetail.condition_role == role)

    details = query.order_by(RuleDetail.sort_order.asc(), RuleDetail.id.asc()).all()
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()

    return render_template(
        "rule_details.html",
        rule=rule,
        details=details,
        keyword=keyword,
        xml_id=xml_id,
        role=role,
        xmls=xmls
    )


@rule_bp.route("/<int:rule_id>/details/create", methods=["GET", "POST"])
def create_rule_detail(rule_id):
    rule = Rule.query.get_or_404(rule_id)
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    conditions = DanhMucDieuKien.query.order_by(DanhMucDieuKien.id.asc()).all()

    selected_xml_id = (request.form.get("xml_id") or "").strip()
    compare_xml_id = (request.form.get("compare_xml_id") or "").strip()

    selected_xml_id_int = to_int_or_none(selected_xml_id)
    compare_xml_id_int = to_int_or_none(compare_xml_id)

    fields = []
    compare_fields = []
    error = None

    if selected_xml_id_int is not None:
        fields = (
            DanhMucTruongDuLieu.query
            .filter_by(xml_id=selected_xml_id_int)
            .order_by(DanhMucTruongDuLieu.id.asc())
            .all()
        )

    if compare_xml_id_int is not None:
        compare_fields = (
            DanhMucTruongDuLieu.query
            .filter_by(xml_id=compare_xml_id_int)
            .order_by(DanhMucTruongDuLieu.id.asc())
            .all()
        )

    if request.method == "POST":
        compare_mode = (request.form.get("compare_mode") or "VALUE").strip()
        field_id = to_int_or_none(request.form.get("field_id"))
        condition_id = to_int_or_none(request.form.get("condition_id"))
        sort_order = to_int_or_none(request.form.get("sort_order")) or 1
        compare_field_id = to_int_or_none(request.form.get("compare_field_id"))
        gia_tri = (request.form.get("gia_tri") or "").strip() or None
        condition_role = (request.form.get("condition_role") or "VALIDATE").strip()
        date_part = (request.form.get("date_part") or "").strip() or None

        if selected_xml_id_int is None:
            error = "Bạn chưa chọn XML nguồn."
        elif field_id is None:
            error = "Bạn chưa chọn field nguồn."
        elif condition_id is None:
            error = "Bạn chưa chọn điều kiện."
        elif compare_mode == "FIELD" and compare_xml_id_int is None:
            error = "Bạn chưa chọn XML so sánh."
        elif compare_mode == "FIELD" and compare_field_id is None:
            error = "Bạn chưa chọn field so sánh."
        else:
            detail = RuleDetail(
                rule_id=rule.id,
                field_id=field_id,
                condition_id=condition_id,
                gia_tri=None if compare_mode == "FIELD" else gia_tri,
                condition_role=condition_role,
                sort_order=sort_order,
                compare_mode=compare_mode,
                compare_field_id=compare_field_id if compare_mode == "FIELD" else None,
                date_part=date_part
            )
            db.session.add(detail)
            db.session.commit()
            return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))

    return render_template(
        "rule_detail_form.html",
        rule=rule,
        item=None,
        xmls=xmls,
        fields=fields,
        compare_fields=compare_fields,
        conditions=conditions,
        selected_xml_id=selected_xml_id,
        compare_xml_id=compare_xml_id,
        error=error
    )


@rule_bp.route("/details/<int:detail_id>/edit", methods=["GET", "POST"])
def edit_rule_detail(detail_id):
    item = RuleDetail.query.get_or_404(detail_id)
    rule = item.rule
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    conditions = DanhMucDieuKien.query.order_by(DanhMucDieuKien.id.asc()).all()

    selected_xml_id = (
        request.form.get("xml_id")
        or str(item.field.xml_id)
    ).strip()

    compare_xml_id = (
        request.form.get("compare_xml_id")
        or (str(item.compare_field.xml_id) if item.compare_field else "")
    ).strip()

    selected_xml_id_int = to_int_or_none(selected_xml_id)
    compare_xml_id_int = to_int_or_none(compare_xml_id)

    fields = []
    compare_fields = []
    error = None

    if selected_xml_id_int is not None:
        fields = (
            DanhMucTruongDuLieu.query
            .filter_by(xml_id=selected_xml_id_int)
            .order_by(DanhMucTruongDuLieu.id.asc())
            .all()
        )

    if compare_xml_id_int is not None:
        compare_fields = (
            DanhMucTruongDuLieu.query
            .filter_by(xml_id=compare_xml_id_int)
            .order_by(DanhMucTruongDuLieu.id.asc())
            .all()
        )

    if request.method == "POST":
        compare_mode = (request.form.get("compare_mode") or "VALUE").strip()
        field_id = to_int_or_none(request.form.get("field_id"))
        condition_id = to_int_or_none(request.form.get("condition_id"))
        sort_order = to_int_or_none(request.form.get("sort_order")) or 1
        compare_field_id = to_int_or_none(request.form.get("compare_field_id"))
        gia_tri = (request.form.get("gia_tri") or "").strip() or None
        condition_role = (request.form.get("condition_role") or "VALIDATE").strip()
        date_part = (request.form.get("date_part") or "").strip() or None

        if selected_xml_id_int is None:
            error = "Bạn chưa chọn XML nguồn."
        elif field_id is None:
            error = "Bạn chưa chọn field nguồn."
        elif condition_id is None:
            error = "Bạn chưa chọn điều kiện."
        elif compare_mode == "FIELD" and compare_xml_id_int is None:
            error = "Bạn chưa chọn XML so sánh."
        elif compare_mode == "FIELD" and compare_field_id is None:
            error = "Bạn chưa chọn field so sánh."
        else:
            item.field_id = field_id
            item.condition_id = condition_id
            item.gia_tri = None if compare_mode == "FIELD" else gia_tri
            item.condition_role = condition_role
            item.sort_order = sort_order
            item.compare_mode = compare_mode
            item.compare_field_id = compare_field_id if compare_mode == "FIELD" else None
            item.date_part = date_part

            db.session.commit()
            return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))

    return render_template(
        "rule_detail_form.html",
        rule=rule,
        item=item,
        xmls=xmls,
        fields=fields,
        compare_fields=compare_fields,
        conditions=conditions,
        selected_xml_id=selected_xml_id,
        compare_xml_id=compare_xml_id,
        error=error
    )


@rule_bp.route("/details/<int:detail_id>/delete", methods=["POST"])
def delete_rule_detail(detail_id):
    item = RuleDetail.query.get_or_404(detail_id)
    rule_id = item.rule_id
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("rule_bp.list_rule_details", rule_id=rule_id))


def to_int_or_none(value):
    value = (value or "").strip()
    return int(value) if value else None