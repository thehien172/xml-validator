from flask import Blueprint, render_template, request, redirect, url_for
from sqlalchemy import or_

from models import db, Rule, RuleGroup, BoRule

rule_group_bp = Blueprint("rule_group_bp", __name__, url_prefix="/rule-groups")


@rule_group_bp.route("/tree", methods=["GET"])
def tree_view():
    keyword = (request.args.get("keyword") or "").strip()

    query = (
        Rule.query
        .join(BoRule, Rule.bo_rule_id == BoRule.id)
        .join(RuleGroup, Rule.rule_group_id == RuleGroup.id)
        .filter(
            Rule.is_active.is_(True),
            BoRule.is_active.is_(True),
            Rule.rule_group_id.isnot(None)
        )
    )

    if keyword:
        query = query.filter(
            or_(
                Rule.ten_rule.ilike(f"%{keyword}%"),
                Rule.thong_bao.ilike(f"%{keyword}%"),
                Rule.severity.ilike(f"%{keyword}%"),
                RuleGroup.ten_nhom.ilike(f"%{keyword}%"),
                BoRule.ten_bo_rule.ilike(f"%{keyword}%"),
                BoRule.ma_bo_rule.ilike(f"%{keyword}%")
            )
        )

    active_rules = query.order_by(Rule.id.asc()).all()

    all_active_groups = (
        RuleGroup.query
        .order_by(RuleGroup.sort_order.asc(), RuleGroup.id.asc())
        .all()
    )

    tree_nodes = build_rule_tree(all_active_groups, active_rules)

    return render_template(
        "rule/rule_group/tree_rules.html",
        tree_nodes=tree_nodes,
        keyword=keyword,
        total_rules=len(active_rules)
    )


@rule_group_bp.route("/", methods=["GET"])
def list_groups():
    keyword = (request.args.get("keyword") or "").strip()

    query = RuleGroup.query

    if keyword:
        query = query.filter(
            or_(
                RuleGroup.ten_nhom.ilike(f"%{keyword}%")
            )
        )

    groups = query.order_by(
        RuleGroup.sort_order.asc(),
        RuleGroup.id.asc()
    ).all()

    return render_template(
        "rule/rule_group/rule_groups.html",
        groups=groups,
        keyword=keyword
    )


@rule_group_bp.route("/create", methods=["GET", "POST"])
def create_group():
    error = None
    groups = RuleGroup.query.order_by(RuleGroup.sort_order.asc(), RuleGroup.id.asc()).all()

    if request.method == "POST":
        try:
            ten_nhom = (request.form.get("ten_nhom") or "").strip()
            parent_id = request.form.get("parent_id", type=int)
            sort_order = request.form.get("sort_order", type=int) or 1

            if not ten_nhom:
                raise ValueError("Vui lòng nhập tên nhóm.")

            if parent_id:
                parent = RuleGroup.query.get(parent_id)
                if not parent:
                    raise ValueError("Nhóm cha không tồn tại.")

            item = RuleGroup(
                ten_nhom=ten_nhom,
                parent_id=parent_id,
                sort_order=sort_order
            )
            db.session.add(item)
            db.session.commit()
            return redirect(url_for("rule_group_bp.list_groups"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "rule/rule_group/rule_group_form.html",
        item=None,
        groups=groups,
        error=error
    )


@rule_group_bp.route("/<int:group_id>/edit", methods=["GET", "POST"])
def edit_group(group_id):
    item = RuleGroup.query.get_or_404(group_id)
    error = None
    groups = (
        RuleGroup.query
        .filter(RuleGroup.id != group_id)
        .order_by(RuleGroup.sort_order.asc(), RuleGroup.id.asc())
        .all()
    )

    if request.method == "POST":
        try:
            ten_nhom = (request.form.get("ten_nhom") or "").strip()
            parent_id = request.form.get("parent_id", type=int)
            sort_order = request.form.get("sort_order", type=int) or 1

            if not ten_nhom:
                raise ValueError("Vui lòng nhập tên nhóm.")

            if parent_id:
                if parent_id == item.id:
                    raise ValueError("Không thể chọn chính nó làm nhóm cha.")

                parent = RuleGroup.query.get(parent_id)
                if not parent:
                    raise ValueError("Nhóm cha không tồn tại.")

                if is_descendant(parent_id, item.id):
                    raise ValueError("Không thể chọn nhóm con làm nhóm cha.")

            item.ten_nhom = ten_nhom
            item.parent_id = parent_id
            item.sort_order = sort_order

            db.session.commit()
            return redirect(url_for("rule_group_bp.list_groups"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "rule/rule_group/rule_group_form.html",
        item=item,
        groups=groups,
        error=error
    )


@rule_group_bp.route("/<int:group_id>/delete", methods=["POST"])
def delete_group(group_id):
    item = RuleGroup.query.get_or_404(group_id)

    child_exists = RuleGroup.query.filter_by(parent_id=item.id).first()
    if child_exists:
        return "Không thể xóa nhóm: còn nhóm con.", 400

    rule_exists = Rule.query.filter_by(rule_group_id=item.id).first()
    if rule_exists:
        return "Không thể xóa nhóm: còn rule đang gán.", 400

    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("rule_group_bp.list_groups"))


@rule_group_bp.route("/<int:group_id>/assign", methods=["GET", "POST"])
def assign_rules(group_id):
    group = RuleGroup.query.get_or_404(group_id)
    error = None

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        try:
            if action == "add":
                selected_ids = request.form.getlist("free_rule_ids")
                for rid in selected_ids:
                    rule = Rule.query.get(int(rid))
                    if rule and rule.rule_group_id is None:
                        rule.rule_group_id = group.id

            elif action == "remove":
                selected_ids = request.form.getlist("group_rule_ids")
                for rid in selected_ids:
                    rule = Rule.query.get(int(rid))
                    if rule and rule.rule_group_id == group.id:
                        rule.rule_group_id = None
            else:
                raise ValueError("Thao tác không hợp lệ.")

            db.session.commit()
            return redirect(url_for("rule_group_bp.assign_rules", group_id=group.id))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    rules_in_group = (
        Rule.query
        .join(BoRule, Rule.bo_rule_id == BoRule.id)
        .filter(Rule.rule_group_id == group.id)
        .order_by(Rule.id.asc())
        .all()
    )

    free_rules = (
        Rule.query
        .join(BoRule, Rule.bo_rule_id == BoRule.id)
        .filter(Rule.rule_group_id.is_(None))
        .order_by(Rule.id.asc())
        .all()
    )

    return render_template(
        "rule/rule_group/rule_group_assign.html",
        group=group,
        rules_in_group=rules_in_group,
        free_rules=free_rules,
        error=error
    )


def is_descendant(candidate_parent_id, current_group_id):
    current = RuleGroup.query.get(candidate_parent_id)
    while current:
        if current.id == current_group_id:
            return True
        if not current.parent_id:
            return False
        current = RuleGroup.query.get(current.parent_id)
    return False


def build_rule_tree(groups, rules):
    group_map = {}
    roots = []

    for group in groups:
        group_map[group.id] = {
            "id": group.id,
            "type": "group",
            "group": group,
            "children": [],
            "rule_count": 0
        }

    for group in groups:
        node = group_map[group.id]
        if group.parent_id and group.parent_id in group_map:
            group_map[group.parent_id]["children"].append(node)
        else:
            roots.append(node)

    for rule in rules:
        if rule.rule_group_id in group_map:
            group_map[rule.rule_group_id]["children"].append({
                "id": f"rule-{rule.id}",
                "type": "rule",
                "rule": rule
            })

    filtered_roots = []
    for root in roots:
        filtered = prune_empty_group(root)
        if filtered:
            filtered_roots.append(filtered)

    filtered_roots.sort(key=lambda x: ((x["group"].sort_order or 1), x["group"].id))
    return filtered_roots


def prune_empty_group(node):
    kept_children = []
    total_rules = 0

    for child in node["children"]:
        if child["type"] == "rule":
            kept_children.append(child)
            total_rules += 1
        else:
            filtered_child = prune_empty_group(child)
            if filtered_child:
                kept_children.append(filtered_child)
                total_rules += filtered_child.get("rule_count", 0)

    if total_rules == 0:
        return None

    kept_children.sort(
        key=lambda x: (
            0 if x["type"] == "group" else 1,
            (x["group"].sort_order if x["type"] == "group" else 0),
            (x["group"].id if x["type"] == "group" else x["rule"].id)
        )
    )

    node["children"] = kept_children
    node["rule_count"] = total_rules
    return node