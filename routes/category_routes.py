import io
import json

from flask import Blueprint, render_template, request, redirect, url_for, jsonify
from openpyxl import load_workbook
from sqlalchemy import or_

from models import (
    db,
    DonVi,
    DanhMuc,
    DanhMucField,
    DanhMucRecord,
    DanhMucRecordValue,
    DanhMucDataset
)

category_bp = Blueprint("category_bp", __name__, url_prefix="/categories")


@category_bp.route("/", methods=["GET"])
def list_categories():
    keyword = (request.args.get("keyword") or "").strip()
    scope = (request.args.get("scope") or "").strip().upper()

    query = DanhMuc.query

    if keyword:
        query = query.filter(
            or_(
                DanhMuc.ten_danh_muc.ilike(f"%{keyword}%")
            )
        )

    if scope in ["COMMON", "UNIT"]:
        query = query.filter(DanhMuc.scope == scope)

    items = query.order_by(DanhMuc.id.asc()).all()

    return render_template(
        "categories.html",
        items=items,
        keyword=keyword,
        scope=scope
    )


@category_bp.route("/create", methods=["GET", "POST"])
def create_category():
    error = None

    if request.method == "POST":
        try:
            ten_danh_muc = (request.form.get("ten_danh_muc") or "").strip()
            scope = (request.form.get("scope") or "COMMON").strip().upper()

            if not ten_danh_muc:
                raise ValueError("Vui lòng nhập tên danh mục.")

            if scope not in ["COMMON", "UNIT"]:
                raise ValueError("Phạm vi danh mục không hợp lệ.")

            item = DanhMuc(
                ten_danh_muc=ten_danh_muc,
                scope=scope
            )
            db.session.add(item)
            db.session.flush()

            if scope == "COMMON":
                db.session.add(DanhMucDataset(
                    danh_muc_id=item.id,
                    don_vi_id=None,
                    ten_bo_du_lieu="Bộ dữ liệu chung"
                ))

            db.session.commit()
            return redirect(url_for("category_bp.list_categories"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "category_form.html",
        item=None,
        error=error
    )


@category_bp.route("/<int:category_id>/edit", methods=["GET", "POST"])
def edit_category(category_id):
    item = DanhMuc.query.get_or_404(category_id)
    error = None

    if request.method == "POST":
        try:
            ten_danh_muc = (request.form.get("ten_danh_muc") or "").strip()
            scope = (request.form.get("scope") or "COMMON").strip().upper()

            if not ten_danh_muc:
                raise ValueError("Vui lòng nhập tên danh mục.")

            if scope not in ["COMMON", "UNIT"]:
                raise ValueError("Phạm vi danh mục không hợp lệ.")

            old_scope = (item.scope or "COMMON").upper()

            if old_scope == "COMMON" and scope == "UNIT":
                common_dataset = (
                    DanhMucDataset.query
                    .filter(DanhMucDataset.danh_muc_id == item.id)
                    .filter(DanhMucDataset.don_vi_id.is_(None))
                    .first()
                )
                if common_dataset:
                    has_records = DanhMucRecord.query.filter_by(dataset_id=common_dataset.id).first()
                    if has_records:
                        raise ValueError(
                            "Danh mục đang có dữ liệu chung. Vui lòng xóa/chuyển dữ liệu trước khi đổi sang danh mục riêng."
                        )

            item.ten_danh_muc = ten_danh_muc
            item.scope = scope

            if scope == "COMMON":
                get_or_create_common_dataset(item)

            db.session.commit()
            return redirect(url_for("category_bp.list_categories"))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    return render_template(
        "category_form.html",
        item=item,
        error=error
    )


@category_bp.route("/<int:category_id>/delete", methods=["POST"])
def delete_category(category_id):
    item = DanhMuc.query.get_or_404(category_id)
    db.session.delete(item)
    db.session.commit()
    return redirect(url_for("category_bp.list_categories"))


@category_bp.route("/api/fields", methods=["GET"])
def get_category_fields_api():
    category_id = request.args.get("category_id", type=int)

    if not category_id:
        return jsonify([])

    fields = (
        DanhMucField.query
        .filter_by(danh_muc_id=category_id)
        .order_by(DanhMucField.id.asc())
        .all()
    )

    result = []
    for f in fields:
        result.append({
            "id": f.id,
            "ma_truong": f.ma_truong,
            "ten_truong": f.ten_truong
        })

    return jsonify(result)


@category_bp.route("/<int:category_id>/fields", methods=["GET", "POST"])
def manage_category_fields(category_id):
    item = DanhMuc.query.get_or_404(category_id)
    error = None

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        try:
            if action == "create":
                ma_truong = (request.form.get("ma_truong") or "").strip()
                ten_truong = (request.form.get("ten_truong") or "").strip()

                if not ma_truong:
                    raise ValueError("Vui lòng nhập mã trường.")
                if not ten_truong:
                    raise ValueError("Vui lòng nhập tên trường.")

                existed = DanhMucField.query.filter_by(
                    danh_muc_id=item.id,
                    ma_truong=ma_truong
                ).first()
                if existed:
                    raise ValueError("Mã trường đã tồn tại trong danh mục này.")

                db.session.add(DanhMucField(
                    danh_muc_id=item.id,
                    ma_truong=ma_truong,
                    ten_truong=ten_truong
                ))
                db.session.commit()

                return redirect(url_for("category_bp.manage_category_fields", category_id=item.id))

            if action == "delete":
                field_id = to_int_or_none(request.form.get("field_id"))
                field = DanhMucField.query.filter_by(id=field_id, danh_muc_id=item.id).first()
                if not field:
                    raise ValueError("Không tìm thấy field.")

                DanhMucRecordValue.query.filter_by(field_id=field.id).delete()
                db.session.delete(field)
                db.session.commit()

                return redirect(url_for("category_bp.manage_category_fields", category_id=item.id))

            raise ValueError("Thao tác không hợp lệ.")

        except Exception as e:
            db.session.rollback()
            error = str(e)

    fields = (
        DanhMucField.query
        .filter_by(danh_muc_id=item.id)
        .order_by(DanhMucField.id.asc())
        .all()
    )

    return render_template(
        "category_fields.html",
        item=item,
        fields=fields,
        error=error
    )


@category_bp.route("/<int:category_id>/datasets", methods=["GET", "POST"])
def manage_category_datasets(category_id):
    item = DanhMuc.query.get_or_404(category_id)
    error = None

    if (item.scope or "COMMON").upper() != "UNIT":
        return redirect(url_for("category_bp.manage_category_records", category_id=item.id))

    units = DonVi.query.order_by(DonVi.id.asc()).all()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        try:
            if action != "create":
                raise ValueError("Thao tác không hợp lệ.")

            don_vi_id = to_int_or_none(request.form.get("don_vi_id"))
            ten_bo_du_lieu = (request.form.get("ten_bo_du_lieu") or "").strip()

            if not don_vi_id:
                raise ValueError("Vui lòng chọn đơn vị.")

            unit = DonVi.query.get(don_vi_id)
            if not unit:
                raise ValueError("Đơn vị không tồn tại.")

            existed = (
                DanhMucDataset.query
                .filter_by(danh_muc_id=item.id, don_vi_id=don_vi_id)
                .first()
            )
            if existed:
                raise ValueError("Đơn vị này đã có bộ dữ liệu.")

            if not ten_bo_du_lieu:
                ten_bo_du_lieu = f"Bộ dữ liệu - {unit.ten_don_vi}"

            db.session.add(DanhMucDataset(
                danh_muc_id=item.id,
                don_vi_id=don_vi_id,
                ten_bo_du_lieu=ten_bo_du_lieu
            ))
            db.session.commit()

            return redirect(url_for("category_bp.manage_category_datasets", category_id=item.id))

        except Exception as e:
            db.session.rollback()
            error = str(e)

    datasets = (
        DanhMucDataset.query
        .filter_by(danh_muc_id=item.id)
        .order_by(DanhMucDataset.id.asc())
        .all()
    )

    return render_template(
        "category_datasets.html",
        item=item,
        datasets=datasets,
        units=units,
        error=error
    )


@category_bp.route("/<int:category_id>/datasets/<int:dataset_id>/delete", methods=["POST"])
def delete_category_dataset(category_id, dataset_id):
    item = DanhMuc.query.get_or_404(category_id)

    dataset = (
        DanhMucDataset.query
        .filter_by(id=dataset_id, danh_muc_id=item.id)
        .first_or_404()
    )

    db.session.delete(dataset)
    db.session.commit()

    return redirect(url_for("category_bp.manage_category_datasets", category_id=item.id))


@category_bp.route("/<int:category_id>/records", methods=["GET", "POST"])
def manage_category_records(category_id):
    item = DanhMuc.query.get_or_404(category_id)

    if (item.scope or "COMMON").upper() == "UNIT":
        return redirect(url_for("category_bp.manage_category_datasets", category_id=item.id))

    dataset = get_or_create_common_dataset(item)
    db.session.commit()

    return handle_dataset_records(item=item, dataset=dataset)


@category_bp.route("/<int:category_id>/datasets/<int:dataset_id>/records", methods=["GET", "POST"])
def manage_category_dataset_records(category_id, dataset_id):
    item = DanhMuc.query.get_or_404(category_id)

    dataset = (
        DanhMucDataset.query
        .filter_by(id=dataset_id, danh_muc_id=item.id)
        .first_or_404()
    )

    return handle_dataset_records(item=item, dataset=dataset)


def handle_dataset_records(item, dataset):
    fields = (
        DanhMucField.query
        .filter_by(danh_muc_id=item.id)
        .order_by(DanhMucField.id.asc())
        .all()
    )
    error = None
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        try:
            if not fields:
                raise ValueError("Danh mục chưa có field, vui lòng tạo field trước.")

            if action == "create_manual":
                record = DanhMucRecord(dataset_id=dataset.id)
                db.session.add(record)
                db.session.flush()

                cell_values = []
                for field in fields:
                    val = (request.form.get(f"field_{field.id}") or "").strip()
                    norm_val = val or None
                    db.session.add(DanhMucRecordValue(
                        record_id=record.id,
                        field_id=field.id,
                        value=norm_val
                    ))
                    cell_values.append(val)

                db.session.commit()

                if is_ajax:
                    return jsonify({
                        "success": True,
                        "record_id": record.id,
                        "cells": cell_values
                    })

                return redirect(request.path)

            if action == "update_record":
                record_id = to_int_or_none(request.form.get("record_id"))
                record = DanhMucRecord.query.filter_by(id=record_id, dataset_id=dataset.id).first()
                if not record:
                    raise ValueError("Không tìm thấy dòng dữ liệu cần sửa.")

                existing_values = {v.field_id: v for v in record.values}
                cell_values = []

                for field in fields:
                    val = (request.form.get(f"field_{field.id}") or "").strip()
                    norm_val = val or None
                    value_obj = existing_values.get(field.id)

                    if value_obj:
                        value_obj.value = norm_val
                    else:
                        db.session.add(DanhMucRecordValue(
                            record_id=record.id,
                            field_id=field.id,
                            value=norm_val
                        ))

                    cell_values.append(val)

                db.session.commit()

                if is_ajax:
                    return jsonify({
                        "success": True,
                        "record_id": record.id,
                        "cells": cell_values
                    })

                return redirect(request.path)

            if action == "delete_record":
                record_id = to_int_or_none(request.form.get("record_id"))
                record = DanhMucRecord.query.filter_by(id=record_id, dataset_id=dataset.id).first()
                if not record:
                    raise ValueError("Không tìm thấy dòng dữ liệu.")

                DanhMucRecordValue.query.filter_by(record_id=record.id).delete()
                db.session.delete(record)
                db.session.commit()

                if is_ajax:
                    return jsonify({
                        "success": True,
                        "record_id": record_id
                    })

                return redirect(request.path)

            if action == "import_json":
                json_text = (request.form.get("json_text") or "").strip()
                if not json_text:
                    raise ValueError("Vui lòng nhập JSON.")

                import_json_rows(dataset, fields, json_text)
                db.session.commit()
                return redirect(request.path)

            if action == "import_excel":
                excel_file = request.files.get("excel_file")
                if not excel_file or excel_file.filename == "":
                    raise ValueError("Vui lòng chọn file Excel.")

                import_excel_rows(dataset, fields, excel_file.read())
                db.session.commit()
                return redirect(request.path)

            raise ValueError("Thao tác không hợp lệ.")

        except Exception as e:
            db.session.rollback()
            if is_ajax:
                return jsonify({
                    "success": False,
                    "error": str(e)
                }), 400
            error = str(e)

    records = (
        DanhMucRecord.query
        .filter_by(dataset_id=dataset.id)
        .order_by(DanhMucRecord.id.asc())
        .all()
    )

    record_rows = build_record_rows(records, fields)

    return render_template(
        "category_records.html",
        item=item,
        dataset=dataset,
        fields=fields,
        records=records,
        record_rows=record_rows,
        error=error
    )


def get_or_create_common_dataset(category):
    dataset = (
        DanhMucDataset.query
        .filter(DanhMucDataset.danh_muc_id == category.id)
        .filter(DanhMucDataset.don_vi_id.is_(None))
        .first()
    )
    if dataset:
        return dataset

    dataset = DanhMucDataset(
        danh_muc_id=category.id,
        don_vi_id=None,
        ten_bo_du_lieu="Bộ dữ liệu chung"
    )
    db.session.add(dataset)
    db.session.flush()
    return dataset


def import_json_rows(dataset, fields, json_text):
    data = json.loads(json_text)

    if isinstance(data, dict):
        if "rows" in data and isinstance(data["rows"], list):
            rows = data["rows"]
        else:
            rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        raise ValueError("JSON phải là object hoặc list object.")

    field_map = build_field_map(fields)

    for row in rows:
        if not isinstance(row, dict):
            continue

        record = DanhMucRecord(dataset_id=dataset.id)
        db.session.add(record)
        db.session.flush()

        for field in fields:
            raw_value = find_value_by_field(row, field, field_map)
            db.session.add(DanhMucRecordValue(
                record_id=record.id,
                field_id=field.id,
                value=normalize_cell_value(raw_value)
            ))


def import_excel_rows(dataset, fields, file_bytes):
    workbook = load_workbook(filename=io.BytesIO(file_bytes), data_only=True)
    sheet = workbook.active

    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        raise ValueError("File Excel không có dữ liệu.")

    headers = [str(x).strip() if x is not None else "" for x in rows[0]]
    if not any(headers):
        raise ValueError("Dòng đầu tiên của file Excel phải là tiêu đề cột.")

    field_map = build_field_map(fields)

    for data_row in rows[1:]:
        row_dict = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            value = data_row[idx] if idx < len(data_row) else None
            row_dict[header] = value

        if not row_dict:
            continue

        record = DanhMucRecord(dataset_id=dataset.id)
        db.session.add(record)
        db.session.flush()

        for field in fields:
            raw_value = find_value_by_field(row_dict, field, field_map)
            db.session.add(DanhMucRecordValue(
                record_id=record.id,
                field_id=field.id,
                value=normalize_cell_value(raw_value)
            ))


def build_field_map(fields):
    result = {}
    for field in fields:
        result[(field.ma_truong or "").strip().lower()] = field
        result[(field.ten_truong or "").strip().lower()] = field
    return result


def find_value_by_field(row_dict, field, field_map):
    if not isinstance(row_dict, dict):
        return None

    ma_key = (field.ma_truong or "").strip()
    ten_key = (field.ten_truong or "").strip()

    if ma_key in row_dict:
        return row_dict.get(ma_key)

    if ten_key in row_dict:
        return row_dict.get(ten_key)

    lowered = {}
    for k, v in row_dict.items():
        lowered[str(k).strip().lower()] = v

    if ma_key.lower() in lowered:
        return lowered.get(ma_key.lower())

    if ten_key.lower() in lowered:
        return lowered.get(ten_key.lower())

    return None


def build_record_rows(records, fields):
    rows = []

    for record in records:
        value_map = {}
        for val in record.values:
            value_map[val.field_id] = val.value or ""

        row = {
            "record": record,
            "cells": []
        }

        for field in fields:
            row["cells"].append(value_map.get(field.id, ""))

        rows.append(row)

    return rows


def normalize_cell_value(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_int_or_none(value):
    if value is None:
        return None
    value = str(value).strip()
    return int(value) if value else None