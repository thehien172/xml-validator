from flask import Blueprint, render_template, request, redirect, url_for
from sqlalchemy import or_

from models import db, BoRule, Rule

rule_set_bp = Blueprint("rule_set_bp", __name__, url_prefix="/rule-sets")


@rule_set_bp.route("/", methods=["GET"])
def list_rule_sets():
    keyword = (request.args.get("keyword") or "").strip()
    status = (request.args.get("status") or "").strip()

    query = BoRule.query

    if keyword:
        query = query.filter(
            or_(
                BoRule.ma_bo_rule.ilike(f"%{keyword}%"),
                BoRule.ten_bo_rule.ilike(f"%{keyword}%"),
                BoRule.mo_ta.ilike(f"%{keyword}%")
            )
        )

    if status == "active":
        query = query.filter(BoRule.is_active.is_(True))
    elif status == "inactive":
        query = query.filter(BoRule.is_active.is_(False))

    items = query.order_by(BoRule.id.asc()).all()
    return render_template("rule/rule_set/rule_sets.html", items=items, keyword=keyword, status=status)


@rule_set_bp.route("/create", methods=["GET", "POST"])
def create_rule_set():
    if request.method == "POST":
        item = BoRule(
            ma_bo_rule=request.form.get("ma_bo_rule", "").strip(),
            ten_bo_rule=request.form.get("ten_bo_rule", "").strip(),
            mo_ta=(request.form.get("mo_ta") or "").strip() or None,
            is_active=True if request.form.get("is_active") == "1" else False
        )
        db.session.add(item)
        db.session.commit()
        return redirect(url_for("rule_set_bp.list_rule_sets"))

    return render_template("rule/rule_set/rule_set_form.html", item=None)


@rule_set_bp.route("/<int:item_id>/edit", methods=["GET", "POST"])
def edit_rule_set(item_id):
    item = BoRule.query.get_or_404(item_id)

    if request.method == "POST":
        item.ma_bo_rule = request.form.get("ma_bo_rule", "").strip()
        item.ten_bo_rule = request.form.get("ten_bo_rule", "").strip()
        item.mo_ta = (request.form.get("mo_ta") or "").strip() or None
        item.is_active = True if request.form.get("is_active") == "1" else False
        db.session.commit()
        return redirect(url_for("rule_set_bp.list_rule_sets"))

    return render_template("rule/rule_set/rule_set_form.html", item=item)


@rule_set_bp.route("/<int:item_id>/delete", methods=["POST"])
def delete_rule_set(item_id):
    item = BoRule.query.get_or_404(item_id)

    Rule.query.filter_by(bo_rule_id=item.id).delete()
    db.session.delete(item)
    db.session.commit()

    return redirect(url_for("rule_set_bp.list_rule_sets"))