import json

from flask import Blueprint, render_template, request, redirect, url_for, jsonify
from sqlalchemy import or_, func

from models import (
    db,
    BoRule,
    Rule,
    RuleUnit,
    RuleDetail,
    DonVi,
    DanhMucTruongDuLieu,
    DanhMucDieuKien,
    DanhMucXml
)

rule_bp = Blueprint("rule_bp", __name__, url_prefix="/rules")

@rule_bp.route("/<int:rule_id>/toggle-status", methods=["POST"])
def toggle_rule_status(rule_id):
    rule = Rule.query.get_or_404(rule_id)

    # checkbox có gửi thì là bật, không gửi là tắt
    rule.is_active = "is_active" in request.form

    db.session.commit()

    # quay lại trang list (giữ filter nếu có)
    return redirect(request.referrer or url_for("rule_bp.list_rules"))

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
            "data_type": (getattr(f, "data_type", None) or "STRING").upper()
        })

    return jsonify(result)


@rule_bp.route("/", methods=["GET"])
def list_rules():
    keyword = (request.args.get("keyword") or "").strip()
    status = (request.args.get("status") or "").strip()
    bo_rule_id = (request.args.get("bo_rule_id") or "").strip()
    apply_scope = (request.args.get("apply_scope") or "").strip().upper()
    don_vi_id = (request.args.get("don_vi_id") or "").strip()

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

    if apply_scope in ["ALL", "UNIT"]:
        query = query.filter(Rule.apply_scope == apply_scope)

    if don_vi_id:
        query = query.join(RuleUnit, RuleUnit.rule_id == Rule.id).filter(RuleUnit.don_vi_id == int(don_vi_id))

    rules = query.order_by(Rule.id.asc()).all()
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()
    units = DonVi.query.order_by(DonVi.id.asc()).all()

    rule_unit_map = {}
    for rule in rules:
        rule_unit_map[rule.id] = [ru.don_vi for ru in getattr(rule, "rule_units", []) if ru.don_vi]

    return render_template(
        "rules.html",
        rules=rules,
        keyword=keyword,
        status=status,
        bo_rule_id=bo_rule_id,
        bo_rules=bo_rules,
        apply_scope=apply_scope,
        don_vi_id=don_vi_id,
        units=units,
        rule_unit_map=rule_unit_map
    )


@rule_bp.route("/create", methods=["GET", "POST"])
def create_rule():
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()
    units = DonVi.query.order_by(DonVi.id.asc()).all()
    error = None

    if request.method == "POST":
        try:
            bo_rule_id = request.form.get("bo_rule_id", type=int)
            ten_rule = (request.form.get("ten_rule") or "").strip()
            thong_bao = (request.form.get("thong_bao") or "").strip()
            severity = (request.form.get("severity") or "WARNING").strip()
            is_active = True if request.form.get("is_active") == "1" else False
            apply_scope = ((request.form.get("apply_scope") or "ALL").strip().upper())
            unit_ids = request.form.getlist("unit_ids")

            if not bo_rule_id:
                raise ValueError("Vui lòng chọn bộ rule.")
            if not ten_rule:
                raise ValueError("Vui lòng nhập tên rule.")
            if not thong_bao:
                raise ValueError("Vui lòng nhập thông báo.")
            if apply_scope not in ["ALL", "UNIT"]:
                raise ValueError("Phạm vi áp dụng không hợp lệ.")
            if apply_scope == "UNIT" and not unit_ids:
                raise ValueError("Vui lòng chọn ít nhất 1 đơn vị.")

            rule = Rule(
                bo_rule_id=bo_rule_id,
                ten_rule=ten_rule,
                thong_bao=thong_bao,
                severity=severity,
                is_active=is_active,
                apply_scope=apply_scope
            )
            db.session.add(rule)
            db.session.flush()

            if apply_scope == "UNIT":
                for uid in unit_ids:
                    uid_int = int(uid)
                    db.session.add(RuleUnit(rule_id=rule.id, don_vi_id=uid_int))

            db.session.commit()
            return redirect(url_for("rule_bp.list_rules"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "rule_form.html",
        item=None,
        bo_rules=bo_rules,
        units=units,
        selected_unit_ids=[],
        error=error
    )


@rule_bp.route("/<int:rule_id>/edit", methods=["GET", "POST"])
def edit_rule(rule_id):
    item = Rule.query.get_or_404(rule_id)
    bo_rules = BoRule.query.order_by(BoRule.id.asc()).all()
    units = DonVi.query.order_by(DonVi.id.asc()).all()
    error = None

    if request.method == "POST":
        try:
            bo_rule_id = request.form.get("bo_rule_id", type=int)
            ten_rule = (request.form.get("ten_rule") or "").strip()
            thong_bao = (request.form.get("thong_bao") or "").strip()
            severity = (request.form.get("severity") or "WARNING").strip()
            is_active = True if request.form.get("is_active") == "1" else False
            apply_scope = ((request.form.get("apply_scope") or "ALL").strip().upper())
            unit_ids = request.form.getlist("unit_ids")

            if not bo_rule_id:
                raise ValueError("Vui lòng chọn bộ rule.")
            if not ten_rule:
                raise ValueError("Vui lòng nhập tên rule.")
            if not thong_bao:
                raise ValueError("Vui lòng nhập thông báo.")
            if apply_scope not in ["ALL", "UNIT"]:
                raise ValueError("Phạm vi áp dụng không hợp lệ.")
            if apply_scope == "UNIT" and not unit_ids:
                raise ValueError("Vui lòng chọn ít nhất 1 đơn vị.")

            item.bo_rule_id = bo_rule_id
            item.ten_rule = ten_rule
            item.thong_bao = thong_bao
            item.severity = severity
            item.is_active = is_active
            item.apply_scope = apply_scope

            RuleUnit.query.filter_by(rule_id=item.id).delete()

            if apply_scope == "UNIT":
                for uid in unit_ids:
                    uid_int = int(uid)
                    db.session.add(RuleUnit(rule_id=item.id, don_vi_id=uid_int))

            db.session.commit()
            return redirect(url_for("rule_bp.list_rules"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    selected_unit_ids = [str(ru.don_vi_id) for ru in getattr(item, "rule_units", [])]

    return render_template(
        "rule_form.html",
        item=item,
        bo_rules=bo_rules,
        units=units,
        selected_unit_ids=selected_unit_ids,
        error=error
    )


@rule_bp.route("/<int:rule_id>/delete", methods=["POST"])
def delete_rule(rule_id):
    item = Rule.query.get_or_404(rule_id)

    RuleUnit.query.filter_by(rule_id=item.id).delete()
    RuleDetail.query.filter_by(rule_id=item.id).delete()
    db.session.delete(item)
    db.session.commit()

    return redirect(url_for("rule_bp.list_rules"))


@rule_bp.route("/<int:rule_id>/details", methods=["GET"])
def list_rule_details(rule_id):
    rule = Rule.query.get_or_404(rule_id)

    keyword = (request.args.get("keyword") or "").strip()
    xml_id = (request.args.get("xml_id") or "").strip()
    role = (request.args.get("role") or "").strip().upper()

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

    if role in ["TRIGGER", "VALIDATE"]:
        query = query.filter(RuleDetail.condition_role == role)

    details = query.order_by(
        RuleDetail.condition_role.asc(),
        RuleDetail.group_no.asc(),
        RuleDetail.sort_order.asc(),
        RuleDetail.id.asc()
    ).all()

    grouped_details = build_grouped_details(details)
    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    applied_units = [ru.don_vi for ru in getattr(rule, "rule_units", []) if ru.don_vi]

    return render_template(
        "rule_details.html",
        rule=rule,
        details=details,
        grouped_details=grouped_details,
        keyword=keyword,
        xml_id=xml_id,
        role=role,
        xmls=xmls,
        applied_units=applied_units
    )


@rule_bp.route("/<int:rule_id>/details/create/<string:role>", methods=["GET", "POST"])
def create_rule_detail_group(rule_id, role):
    rule = Rule.query.get_or_404(rule_id)
    role = normalize_role_or_404(role)

    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    conditions = DanhMucDieuKien.query.order_by(DanhMucDieuKien.id.asc()).all()
    error = None

    if request.method == "POST":
        raw_payload = request.form.get("group_payload") or ""

        try:
            payload = json.loads(raw_payload)
            conditions_payload = payload.get("conditions") or []

            if not isinstance(conditions_payload, list) or not conditions_payload:
                raise ValueError(f"Phải có ít nhất 1 {role.lower()}.")

            next_group_no = get_next_group_no(rule.id, role)

            normalized_details = normalize_group_details(
                rule_id=rule.id,
                role=role,
                group_no=next_group_no,
                conditions_payload=conditions_payload
            )

            for row in normalized_details:
                detail = RuleDetail(
                    rule_id=row["rule_id"],
                    field_id=row["field_id"],
                    condition_id=row["condition_id"],
                    gia_tri=row["gia_tri"],
                    condition_role=row["condition_role"],
                    sort_order=row["sort_order"],
                    compare_mode=row["compare_mode"],
                    compare_field_id=row["compare_field_id"],
                    date_part=row["date_part"],
                    group_no=row["group_no"]
                )
                db.session.add(detail)

            db.session.commit()
            return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "rule_detail_form.html",
        rule=rule,
        item=None,
        role=role,
        group_no=None,
        xmls=xmls,
        conditions=conditions,
        initial_payload=json.dumps({"conditions": []}, ensure_ascii=False),
        error=error
    )


@rule_bp.route("/<int:rule_id>/details/<string:role>/<int:group_no>/edit", methods=["GET", "POST"])
def edit_rule_detail_group(rule_id, role, group_no):
    rule = Rule.query.get_or_404(rule_id)
    role = normalize_role_or_404(role)

    group_details = (
        RuleDetail.query
        .filter_by(rule_id=rule.id, condition_role=role, group_no=group_no)
        .order_by(RuleDetail.sort_order.asc(), RuleDetail.id.asc())
        .all()
    )

    if not group_details:
        return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))

    xmls = DanhMucXml.query.order_by(DanhMucXml.id.asc()).all()
    conditions = DanhMucDieuKien.query.order_by(DanhMucDieuKien.id.asc()).all()
    error = None

    if request.method == "POST":
        raw_payload = request.form.get("group_payload") or ""

        try:
            payload = json.loads(raw_payload)
            conditions_payload = payload.get("conditions") or []

            if not isinstance(conditions_payload, list) or not conditions_payload:
                raise ValueError(f"Nhóm {role.lower()} phải có ít nhất 1 điều kiện.")

            normalized_details = normalize_group_details(
                rule_id=rule.id,
                role=role,
                group_no=group_no,
                conditions_payload=conditions_payload
            )

            RuleDetail.query.filter_by(
                rule_id=rule.id,
                condition_role=role,
                group_no=group_no
            ).delete()

            for row in normalized_details:
                detail = RuleDetail(
                    rule_id=row["rule_id"],
                    field_id=row["field_id"],
                    condition_id=row["condition_id"],
                    gia_tri=row["gia_tri"],
                    condition_role=row["condition_role"],
                    sort_order=row["sort_order"],
                    compare_mode=row["compare_mode"],
                    compare_field_id=row["compare_field_id"],
                    date_part=row["date_part"],
                    group_no=row["group_no"]
                )
                db.session.add(detail)

            db.session.commit()
            return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    initial_payload = build_initial_group_payload(group_details)

    return render_template(
        "rule_detail_form.html",
        rule=rule,
        item=group_details,
        role=role,
        group_no=group_no,
        xmls=xmls,
        conditions=conditions,
        initial_payload=json.dumps(initial_payload, ensure_ascii=False),
        error=error
    )


@rule_bp.route("/<int:rule_id>/details/<string:role>/<int:group_no>/delete", methods=["POST"])
def delete_rule_detail_group(rule_id, role, group_no):
    rule = Rule.query.get_or_404(rule_id)
    role = normalize_role_or_404(role)

    RuleDetail.query.filter_by(
        rule_id=rule.id,
        condition_role=role,
        group_no=group_no
    ).delete()
    db.session.commit()

    return redirect(url_for("rule_bp.list_rule_details", rule_id=rule.id))


def normalize_role_or_404(role):
    role = (role or "").strip().upper()
    if role not in ["TRIGGER", "VALIDATE"]:
        raise ValueError("Role không hợp lệ.")
    return role


def get_next_group_no(rule_id, role):
    max_group = (
        db.session.query(func.max(RuleDetail.group_no))
        .filter(
            RuleDetail.rule_id == rule_id,
            RuleDetail.condition_role == role
        )
        .scalar()
    )
    return (max_group or 0) + 1


def normalize_group_details(rule_id, role, group_no, conditions_payload):
    details = []

    for idx, payload in enumerate(conditions_payload, start=1):
        details.append(build_detail_row(
            rule_id=rule_id,
            payload=payload,
            condition_role=role,
            group_no=group_no,
            sort_order=idx
        ))

    return details


def build_detail_row(rule_id, payload, condition_role, group_no, sort_order):
    field_id = to_int_or_none(payload.get("field_id"))
    condition_id = to_int_or_none(payload.get("condition_id"))
    compare_mode = (payload.get("compare_mode") or "VALUE").strip().upper()
    compare_field_id = to_int_or_none(payload.get("compare_field_id"))
    gia_tri = (payload.get("gia_tri") or "").strip() or None
    date_part = (payload.get("date_part") or "").strip() or None

    if field_id is None:
        raise ValueError(f"{condition_role}: thiếu field.")
    if condition_id is None:
        raise ValueError(f"{condition_role}: thiếu condition.")
    if compare_mode not in ["VALUE", "FIELD"]:
        raise ValueError(f"{condition_role}: compare_mode không hợp lệ.")

    field = DanhMucTruongDuLieu.query.get(field_id)
    if not field:
        raise ValueError(f"{condition_role}: field không tồn tại.")

    condition = DanhMucDieuKien.query.get(condition_id)
    if not condition:
        raise ValueError(f"{condition_role}: condition không tồn tại.")

    if compare_mode == "FIELD":
        if compare_field_id is None:
            raise ValueError(f"{condition_role}: thiếu field so sánh.")
        compare_field = DanhMucTruongDuLieu.query.get(compare_field_id)
        if not compare_field:
            raise ValueError(f"{condition_role}: field so sánh không tồn tại.")
        gia_tri = None
    else:
        compare_field_id = None

    condition_code = (condition.ma_dieu_kien or "").upper()
    if condition_code in ["BETWEEN", "NOT_BETWEEN"] and compare_mode == "VALUE":
        if not gia_tri or "-" not in gia_tri:
            raise ValueError(
                f"{condition_role}: điều kiện khoảng yêu cầu giá trị dạng start-end, ví dụ 18-06."
            )

    return {
        "rule_id": rule_id,
        "field_id": field_id,
        "condition_id": condition_id,
        "gia_tri": gia_tri,
        "condition_role": condition_role,
        "sort_order": sort_order,
        "compare_mode": compare_mode,
        "compare_field_id": compare_field_id,
        "date_part": date_part,
        "group_no": group_no
    }


def build_initial_group_payload(group_details):
    conditions = []

    for d in group_details:
        conditions.append({
            "xml_id": d.field.xml_id if d.field else "",
            "field_id": d.field_id or "",
            "condition_id": d.condition_id or "",
            "compare_mode": d.compare_mode or "VALUE",
            "compare_xml_id": d.compare_field.xml_id if d.compare_field else "",
            "compare_field_id": d.compare_field_id or "",
            "gia_tri": d.gia_tri or "",
            "date_part": d.date_part or ""
        })

    return {"conditions": conditions}


def build_grouped_details(details):
    grouped = {
        "TRIGGER": [],
        "VALIDATE": []
    }

    map_by_role = {
        "TRIGGER": {},
        "VALIDATE": {}
    }

    for d in details:
        role = (d.condition_role or "").upper()
        if role not in map_by_role:
            continue

        group_no = d.group_no or 1
        if group_no not in map_by_role[role]:
            map_by_role[role][group_no] = {
                "role": role,
                "group_no": group_no,
                "details": []
            }

        map_by_role[role][group_no]["details"].append(d)

    for role in ["TRIGGER", "VALIDATE"]:
        grouped[role] = [
            map_by_role[role][key]
            for key in sorted(map_by_role[role].keys())
        ]

    return grouped


def to_int_or_none(value):
    if value is None:
        return None
    value = str(value).strip()
    return int(value) if value else None