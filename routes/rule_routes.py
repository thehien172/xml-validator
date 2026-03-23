from flask import Blueprint, render_template, request, redirect, url_for
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

    selected_xml_id = request.args.get("xml_id") or request.form.get("xml_id") or ""
    compare_xml_id = request.args.get("compare_xml_id") or request.form.get("compare_xml_id") or ""

    fields = []
    compare_fields = []

    if selected_xml_id:
        fields = DanhMucTruongDuLieu.query.filter_by(xml_id=int(selected_xml_id)).order_by(DanhMucTruongDuLieu.id.asc()).all()

    if compare_xml_id:
        compare_fields = DanhMucTruongDuLieu.query.filter_by(xml_id=int(compare_xml_id)).order_by(DanhMucTruongDuLieu.id.asc()).all()

    if request.method == "POST":
        compare_mode = request.form.get("compare_mode", "VALUE").strip()

        detail = RuleDetail(
            rule_id=rule.id,
            field_id=int(request.form.get("field_id")),
            condition_id=int(request.form.get("condition_id")),
            gia_tri=(request.form.get("gia_tri") or "").strip() or None,
            condition_role=request.form.get("condition_role", "VALIDATE").strip(),
            sort_order=int(request.form.get("sort_order") or 1),
            compare_mode=compare_mode,
            compare_field_id=int(request.form.get("compare_field_id")) if compare_mode == "FIELD" and request.form.get("compare_field_id") else None
        )

        if compare_mode == "FIELD":
            detail.gia_tri = None

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
        compare_xml_id=compare_xml_id
    )


@rule_bp.route("/details/<int:detail_id>/edit", methods=["GET", "POST"])
def edit_rule_detail(detail_id):
    item = RuleDetail.query.get_or_404(detail_id)
    rule = item.rule
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    conditions = DanhMucDieuKien.query.order_by(DanhMucDieuKien.id.asc()).all()

    selected_xml_id = request.args.get("xml_id") or request.form.get("xml_id") or str(item.field.xml_id)
    compare_xml_id = request.args.get("compare_xml_id") or request.form.get("compare_xml_id") or (str(item.compare_field.xml_id) if item.compare_field else "")

    fields = []
    compare_fields = []

    if selected_xml_id:
        fields = DanhMucTruongDuLieu.query.filter_by(xml_id=int(selected_xml_id)).order_by(DanhMucTruongDuLieu.id.asc()).all()

    if compare_xml_id:
        compare_fields = DanhMucTruongDuLieu.query.filter_by(xml_id=int(compare_xml_id)).order_by(DanhMucTruongDuLieu.id.asc()).all()

    if request.method == "POST":
        compare_mode = request.form.get("compare_mode", "VALUE").strip()

        item.field_id = int(request.form.get("field_id"))
        item.condition_id = int(request.form.get("condition_id"))
        item.gia_tri = (request.form.get("gia_tri") or "").strip() or None
        item.condition_role = request.form.get("condition_role", "VALIDATE").strip()
        item.sort_order = int(request.form.get("sort_order") or 1)
        item.compare_mode = compare_mode
        item.compare_field_id = int(request.form.get("compare_field_id")) if compare_mode == "FIELD" and request.form.get("compare_field_id") else None

        if compare_mode == "FIELD":
            item.gia_tri = None

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
        compare_xml_id=compare_xml_id
    )


@rule_bp.route("/details/<int:detail_id>/delete", methods=["POST"])
def delete_rule_detail(detail_id):
    item = RuleDetail.query.get_or_404(detail_id)
    rule_id = item.rule_id
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("rule_bp.list_rule_details", rule_id=rule_id))