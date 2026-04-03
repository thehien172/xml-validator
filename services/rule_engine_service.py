from collections import defaultdict
from datetime import datetime

from lxml import etree
from sqlalchemy import or_

from models import (
    db,
    DanhMucXml,
    Rule,
    RuleUnit,
    DanhMuc,
    DanhMucField,
    DanhMucDataset,
    DanhMucRecord,
    DanhMucRecordValue
)
from services.xml_parser_service import (
    get_hoso_nodes,
    build_xml_data_map_for_hoso,
    get_value_from_item,
    get_item_label,
    get_hoso_identity,
    get_hoso_raw_xml
)

CATEGORY_COMPARE_SKIP = "__CATEGORY_COMPARE_SKIP__"


def diff_minutes(dt1, dt2):
    if not dt1 or not dt2:
        return None
    return (dt1 - dt2).total_seconds() / 60.0


def parse_date(value):
    if not value:
        return None

    value = str(value).strip()

    formats = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d%H%M",
        "%Y%m%d",
        "%Y%m%d%H%M%S"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue

    return None


def extract_date_part(value, part):
    dt = parse_date(value)
    if not dt:
        return None

    if part == "DAY_OF_WEEK":
        return dt.weekday() + 2
    if part == "DAY":
        return dt.day
    if part == "MONTH":
        return dt.month
    if part == "YEAR":
        return dt.year
    if part == "HOUR":
        return dt.hour

    return None


def normalize_value(value):
    if value is None:
        return None
    return str(value).strip()


def normalize_list(values):
    result = []
    for v in values:
        nv = normalize_value(v)
        if nv is not None and nv != "":
            result.append(nv)
    return result


def try_parse_number(value):
    if value is None:
        return None

    text = str(value).strip().replace(",", ".")
    if text == "":
        return None

    try:
        return float(text)
    except Exception:
        return None


def split_range_value(expected_value):
    if expected_value is None:
        return None, None

    text = normalize_value(expected_value)
    if not text or "-" not in text:
        return None, None

    left, right = text.split("-", 1)
    return normalize_value(left), normalize_value(right)


def is_between_value(actual_value, expected_value):
    actual_num = try_parse_number(actual_value)
    start_raw, end_raw = split_range_value(expected_value)

    if actual_num is None or start_raw is None or end_raw is None:
        return False

    start_num = try_parse_number(start_raw)
    end_num = try_parse_number(end_raw)

    if start_num is None or end_num is None:
        return False

    if start_num <= end_num:
        return start_num <= actual_num <= end_num

    return actual_num >= start_num or actual_num <= end_num


def evaluate_datetime_diff_condition(actual_value_raw, compare_value_raw, condition_code, expected_value):
    dt_a = parse_date(actual_value_raw)
    dt_b = parse_date(compare_value_raw)

    if not dt_a or not dt_b:
        return True

    diff = diff_minutes(dt_a, dt_b)
    if diff is None:
        return True

    if condition_code == "DATETIME_NOT_GT_MINUTES":
        offset_minutes = try_parse_number(expected_value)
        if offset_minutes is None:
            return True
        return diff <= offset_minutes

    if condition_code == "DATETIME_GT_MINUTES":
        offset_minutes = try_parse_number(expected_value)
        if offset_minutes is None:
            return True
        return diff >= offset_minutes

    if condition_code == "DATETIME_BETWEEN_MINUTES":
        start_raw, end_raw = split_range_value(expected_value)
        start_num = try_parse_number(start_raw)
        end_num = try_parse_number(end_raw)

        if start_num is None or end_num is None:
            return True

        return start_num <= diff <= end_num

    if condition_code == "DATETIME_NOT_BETWEEN_MINUTES":
        start_raw, end_raw = split_range_value(expected_value)
        start_num = try_parse_number(start_raw)
        end_num = try_parse_number(end_raw)

        if start_num is None or end_num is None:
            return True

        return not (start_num <= diff <= end_num)

    return True


def build_datetime_compare_text(detail):
    compare_field_text = ""
    if detail.compare_field:
        compare_field_text = (
            f"{detail.compare_field.xml.ma_xml}:{detail.compare_field.xml_path}"
        )

    if detail.condition and detail.condition.ma_dieu_kien == "DATETIME_NOT_GT_MINUTES":
        return f"Không được lớn hơn field {compare_field_text} quá {detail.gia_tri or 0} phút"

    if detail.condition and detail.condition.ma_dieu_kien == "DATETIME_GT_MINUTES":
        return f"Phải lớn hơn field {compare_field_text} ít nhất {detail.gia_tri or 0} phút"

    if detail.condition and detail.condition.ma_dieu_kien == "DATETIME_BETWEEN_MINUTES":
        return f"Chênh lệch với field {compare_field_text} phải nằm trong khoảng {detail.gia_tri}"
    
    if detail.condition and detail.condition.ma_dieu_kien == "DATETIME_NOT_BETWEEN_MINUTES":
        return f"Chênh lệch với field {compare_field_text} KHÔNG được nằm trong khoảng {detail.gia_tri}"

    return None


def check_condition(actual_value, condition_code, expected_value=None):
    actual_value = normalize_value(actual_value)

    if condition_code in ("LENGTH_EQ", "LENGTH_GT", "LENGTH_LT", "LENGTH_BETWEEN", "LENGTH_NOT_BETWEEN"):
        length = len(actual_value) if actual_value else 0

        if condition_code == "LENGTH_EQ":
            num = try_parse_number(expected_value)
            return length == num if num is not None else False
        
        if condition_code == "LENGTH_GT":
            num = try_parse_number(expected_value)
            return length > num if num is not None else False

        if condition_code == "LENGTH_LT":
            num = try_parse_number(expected_value)
            return length < num if num is not None else False

        if condition_code == "LENGTH_BETWEEN":
            start_raw, end_raw = split_range_value(expected_value)
            start = try_parse_number(start_raw)
            end = try_parse_number(end_raw)

            if start is None or end is None:
                return False

            return start <= length <= end

        if condition_code == "LENGTH_NOT_BETWEEN":
            start_raw, end_raw = split_range_value(expected_value)
            start = try_parse_number(start_raw)
            end = try_parse_number(end_raw)

            if start is None or end is None:
                return False

            return not (start <= length <= end)
        
    if isinstance(expected_value, list):
        expected_list = normalize_list(expected_value)

        if condition_code == "EQUAL":
            return actual_value in expected_list
        if condition_code == "NOT_EQUAL":
            return actual_value not in expected_list
        if condition_code == "IN_LIST":
            return actual_value in expected_list
        if condition_code == "NOT_IN_LIST":
            return actual_value not in expected_list
        if condition_code == "NOT_NULL":
            return actual_value is not None and actual_value != ""
        if condition_code == "IS_NULL":
            return actual_value is None or actual_value == ""

        return False

    expected_value = normalize_value(expected_value)

    if condition_code == "IS_NULL":
        return actual_value is None or actual_value == ""

    if condition_code == "NOT_NULL":
        return actual_value is not None and actual_value != ""

    if condition_code == "EQUAL":
        return actual_value == expected_value

    if condition_code == "NOT_EQUAL":
        return actual_value != expected_value

    if condition_code == "CONTAINS":
        if actual_value is None or expected_value is None:
            return False
        return expected_value in actual_value

    if condition_code == "NOT_CONTAINS":
        if actual_value is None or expected_value is None:
            return True
        return expected_value not in actual_value

    if condition_code == "IN_LIST":
        if actual_value is None or expected_value is None:
            return False
        values = [v.strip() for v in expected_value.split(",") if v.strip()]
        return actual_value in values

    if condition_code == "NOT_IN_LIST":
        if actual_value is None or expected_value is None:
            return True
        values = [v.strip() for v in expected_value.split(",") if v.strip()]
        return actual_value not in values

    if condition_code == "BETWEEN":
        return is_between_value(actual_value, expected_value)

    if condition_code == "NOT_BETWEEN":
        return not is_between_value(actual_value, expected_value)

    return False


def get_xml_item_type(xml_obj):
    value = getattr(xml_obj, "item_type", "MULTI") or "MULTI"
    return str(value).strip().upper()


def group_details_by_group_no(details):
    grouped = defaultdict(list)
    for d in details:
        grouped[d.group_no or 1].append(d)

    result = []
    for group_no in sorted(grouped.keys()):
        result.append((group_no, sorted(grouped[group_no], key=lambda x: (x.sort_order, x.id))))
    return result


def build_compare_text(detail):
    if detail.condition and detail.condition.ma_dieu_kien in (
        "DATETIME_NOT_GT_MINUTES",
        "DATETIME_GT_MINUTES",
        "DATETIME_BETWEEN_MINUTES"
    ):
        if detail.compare_mode == "FIELD" and detail.compare_field:
            return build_datetime_compare_text(detail)

    if detail.compare_mode == "FIELD" and detail.compare_field:
        return f"So sánh với field {detail.compare_field.xml.ma_xml}:{detail.compare_field.xml_path}"

    if detail.compare_mode == "CATEGORY" and detail.compare_category and detail.compare_category_field:
        return (
            f"So sánh với danh mục {detail.compare_category.ten_danh_muc}"
            f" / field {detail.compare_category_field.ma_truong}"
        )

    if detail.gia_tri:
        if detail.condition.ma_dieu_kien == "BETWEEN":
            return f"Giá trị phải nằm trong khoảng {detail.gia_tri}"
        if detail.condition.ma_dieu_kien == "NOT_BETWEEN":
            return f"Giá trị không được nằm trong khoảng {detail.gia_tri}"
        return f"So sánh với giá trị {detail.gia_tri}"

    return None


def apply_date_part_if_needed(value, date_part):
    if date_part:
        return extract_date_part(value, date_part)
    return value


def get_category_dataset(category, don_vi_id=None):
    if not category:
        return None

    scope = (category.scope or "COMMON").strip().upper()

    if scope == "COMMON":
        return (
            DanhMucDataset.query
            .filter(DanhMucDataset.danh_muc_id == category.id)
            .filter(DanhMucDataset.don_vi_id.is_(None))
            .order_by(DanhMucDataset.id.asc())
            .first()
        )

    if not don_vi_id:
        return None

    return (
        DanhMucDataset.query
        .filter(DanhMucDataset.danh_muc_id == category.id)
        .filter(DanhMucDataset.don_vi_id == don_vi_id)
        .order_by(DanhMucDataset.id.asc())
        .first()
    )


def ensure_common_dataset(category):
    if not category:
        return None

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


def get_category_expected_values(detail, don_vi_id=None):
    if detail.compare_mode != "CATEGORY":
        return CATEGORY_COMPARE_SKIP

    if not detail.compare_category_id or not detail.compare_category_field_id:
        return CATEGORY_COMPARE_SKIP

    category = DanhMuc.query.get(detail.compare_category_id)
    category_field = DanhMucField.query.get(detail.compare_category_field_id)

    if not category or not category_field:
        return CATEGORY_COMPARE_SKIP

    if category_field.danh_muc_id != category.id:
        return CATEGORY_COMPARE_SKIP

    dataset = get_category_dataset(category, don_vi_id=don_vi_id)
    if not dataset:
        return CATEGORY_COMPARE_SKIP

    records = (
        DanhMucRecord.query
        .filter_by(dataset_id=dataset.id)
        .order_by(DanhMucRecord.id.asc())
        .all()
    )
    if not records:
        return CATEGORY_COMPARE_SKIP

    record_ids = [r.id for r in records]
    values = (
        DanhMucRecordValue.query
        .filter(DanhMucRecordValue.record_id.in_(record_ids))
        .filter(DanhMucRecordValue.field_id == category_field.id)
        .all()
    )

    result = []
    for val in values:
        text = normalize_value(val.value)
        if text:
            result.append(text)

    if not result:
        return CATEGORY_COMPARE_SKIP

    return result


def get_expected_value_for_detail(detail, xml_data_map, current_item, don_vi_id=None):
    if detail.compare_mode == "FIELD" and detail.compare_field:
        source_xml_code = detail.field.xml.ma_xml
        target_xml_code = detail.compare_field.xml.ma_xml

        if source_xml_code == target_xml_code and detail.field_id != detail.compare_field_id:
            compare_val = get_value_from_item(current_item, detail.compare_field.xml_path)
            return apply_date_part_if_needed(compare_val, detail.date_part)

        target_xml_info = xml_data_map.get(target_xml_code, {})
        target_items = target_xml_info.get("items", [])

        values = []
        for target_item in target_items:
            val = get_value_from_item(target_item, detail.compare_field.xml_path)
            val = apply_date_part_if_needed(val, detail.date_part)
            if val is not None and str(val).strip() != "":
                values.append(val)

        return values

    if detail.compare_mode == "CATEGORY":
        values = get_category_expected_values(detail, don_vi_id=don_vi_id)
        if values == CATEGORY_COMPARE_SKIP:
            return CATEGORY_COMPARE_SKIP
        return [apply_date_part_if_needed(v, detail.date_part) for v in values]

    return detail.gia_tri


def evaluate_detail_on_item(detail, xml_data_map, item, don_vi_id=None):
    actual_value_raw = get_value_from_item(item, detail.field.xml_path)

    if detail.condition.ma_dieu_kien in (
        "DATETIME_NOT_GT_MINUTES",
        "DATETIME_GT_MINUTES",
        "DATETIME_BETWEEN_MINUTES",
        "DATETIME_NOT_BETWEEN_MINUTES"
    ):
        if detail.compare_mode != "FIELD" or not detail.compare_field:
            return True

        compare_value_raw = get_value_from_item(item, detail.compare_field.xml_path)

        return evaluate_datetime_diff_condition(
            actual_value_raw=actual_value_raw,
            compare_value_raw=compare_value_raw,
            condition_code=detail.condition.ma_dieu_kien,
            expected_value=detail.gia_tri
        )

    actual_value = apply_date_part_if_needed(actual_value_raw, detail.date_part)

    expected_value = get_expected_value_for_detail(
        detail=detail,
        xml_data_map=xml_data_map,
        current_item=item,
        don_vi_id=don_vi_id
    )

    if expected_value == CATEGORY_COMPARE_SKIP:
        return True

    return check_condition(
        actual_value=actual_value,
        condition_code=detail.condition.ma_dieu_kien,
        expected_value=expected_value
    )


def evaluate_detail_group_on_item(detail_group, xml_data_map, item, don_vi_id=None):
    for detail in detail_group:
        if not evaluate_detail_on_item(detail, xml_data_map, item, don_vi_id=don_vi_id):
            return False
    return True


def evaluate_trigger_group_any_item(detail_group, xml_data_map, don_vi_id=None):
    if not detail_group:
        return True

    xml_code = detail_group[0].field.xml.ma_xml
    xml_info = xml_data_map.get(xml_code, {})
    items = xml_info.get("items", [])

    for item in items:
        if evaluate_detail_group_on_item(detail_group, xml_data_map, item, don_vi_id=don_vi_id):
            return True

    return False


def detail_is_pairwise_multi_same_field(detail):
    if detail.compare_mode != "FIELD" or not detail.compare_field:
        return False

    if detail.field_id != detail.compare_field_id:
        return False

    return get_xml_item_type(detail.field.xml) == "MULTI"


def rule_uses_pairwise_mode(rule):
    for detail in rule.details:
        if detail_is_pairwise_multi_same_field(detail):
            return True
    return False


def serialize_item_xml(item):
    try:
        return etree.tostring(item, encoding="unicode", pretty_print=True)
    except Exception:
        return ""


def build_hoso_contexts(tree, xml_configs):
    hoso_nodes = get_hoso_nodes(tree)
    contexts = []
    total_xml_read = 0

    for hoso_index, hoso_node in enumerate(hoso_nodes, start=1):
        xml_data_map = build_xml_data_map_for_hoso(hoso_node, xml_configs)

        for _, xml_info in xml_data_map.items():
            total_xml_read += len(xml_info.get("items", []))

        hoso_info = get_hoso_identity(xml_data_map, hoso_index)
        raw_xml = get_hoso_raw_xml(hoso_node)

        contexts.append({
            "hoso_index": hoso_index,
            "hoso_node": hoso_node,
            "xml_data_map": xml_data_map,
            "raw_xml": raw_xml,
            "warnings": [],
            "patient_code": hoso_info["patient_code"],
            "patient_name": hoso_info["patient_name"],
            "ngay_vao": hoso_info["ngay_vao"],
            "ngay_vao_raw": hoso_info["ngay_vao_raw"],
        })

    stats = {
        "total_xml_read": total_xml_read,
        "total_hoso_read": len(hoso_nodes),
        "error_hoso_count": 0
    }

    return contexts, stats


def make_occurrence(context, xml_code, item, item_index):
    return {
        "context": context,
        "xml_code": xml_code,
        "item": item,
        "item_index": item_index,
        "label": f"{get_item_label(xml_code, item)}({item_index})",
        "item_xml": serialize_item_xml(item)
    }


def collect_occurrences_for_xml(contexts, xml_code):
    occurrences = []

    for context in contexts:
        xml_info = context["xml_data_map"].get(xml_code, {})
        items = xml_info.get("items", []) or []

        for item_index, item in enumerate(items, start=1):
            occurrences.append(make_occurrence(context, xml_code, item, item_index))

    return occurrences


def evaluate_pairwise_detail_on_pair(detail, left_occ, right_occ, don_vi_id=None):
    actual_raw = get_value_from_item(left_occ["item"], detail.field.xml_path)

    if detail.condition.ma_dieu_kien in (
        "DATETIME_NOT_GT_MINUTES",
        "DATETIME_GT_MINUTES",
        "DATETIME_BETWEEN_MINUTES",
        "DATETIME_NOT_BETWEEN_MINUTES"
    ):
        if detail.compare_mode != "FIELD" or not detail.compare_field:
            return True, None

        compare_raw = get_value_from_item(right_occ["item"], detail.compare_field.xml_path)

        ok = evaluate_datetime_diff_condition(
            actual_value_raw=actual_raw,
            compare_value_raw=compare_raw,
            condition_code=detail.condition.ma_dieu_kien,
            expected_value=detail.gia_tri
        )

        if ok:
            return True, None

        return False, {
            "field_name": detail.field.ten_truong,
            "field_path": detail.field.xml_path,
            "actual_value": actual_raw,
            "compare_text": build_compare_text(detail),
            "pair_left": left_occ["label"],
            "pair_right": right_occ["label"],
            "compare_value": detail.gia_tri
        }

    actual_value = apply_date_part_if_needed(actual_raw, detail.date_part)

    if detail.compare_mode == "FIELD" and detail.compare_field:
        expected_value = get_value_from_item(right_occ["item"], detail.compare_field.xml_path)
        expected_value = apply_date_part_if_needed(expected_value, detail.date_part)
    elif detail.compare_mode == "CATEGORY":
        expected_value = get_category_expected_values(detail, don_vi_id=don_vi_id)
        if expected_value == CATEGORY_COMPARE_SKIP:
            return True, None
        expected_value = [apply_date_part_if_needed(v, detail.date_part) for v in expected_value]
    else:
        expected_value = detail.gia_tri

    ok = check_condition(
        actual_value=actual_value,
        condition_code=detail.condition.ma_dieu_kien,
        expected_value=expected_value
    )

    if ok:
        return True, None

    return False, {
        "field_name": detail.field.ten_truong,
        "field_path": detail.field.xml_path,
        "actual_value": actual_value,
        "compare_text": build_compare_text(detail),
        "pair_left": left_occ["label"],
        "pair_right": right_occ["label"],
        "compare_value": expected_value
    }


def evaluate_pairwise_group_on_pair(detail_group, left_occ, right_occ, don_vi_id=None):
    failed_fields = []

    for detail in detail_group:
        ok, failed = evaluate_pairwise_detail_on_pair(detail, left_occ, right_occ, don_vi_id=don_vi_id)
        if not ok:
            failed_fields.append(failed)

    return len(failed_fields) == 0, failed_fields


def get_occurrence_owner_key(occ):
    context = occ["context"]
    return (
        context.get("patient_code") or "",
        occ.get("xml_code") or "",
        occ.get("item_index") or 0
    )


def pick_warning_owner(left_occ, right_occ):
    if get_occurrence_owner_key(left_occ) <= get_occurrence_owner_key(right_occ):
        return left_occ["context"]
    return right_occ["context"]


def extract_field_names(failed_fields):
    result = []
    seen = set()

    for field in failed_fields or []:
        name = (field.get("field_name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        result.append(name)

    return result


def extract_field_paths(failed_fields):
    result = []
    seen = set()

    for field in failed_fields or []:
        path = (field.get("field_path") or "").strip()
        if not path or path in seen:
            continue
        seen.add(path)
        result.append(path)

    return result


def build_xml_target(context, xml_code, highlights):
    return {
        "patient_code": context.get("patient_code") or "",
        "patient_name": context.get("patient_name") or "",
        "ngay_vao": context.get("ngay_vao") or "",
        "ngay_vao_raw": context.get("ngay_vao_raw") or "",
        "raw_xml": context.get("raw_xml") or "",
        "xml_code": xml_code,
        "highlights": highlights
    }


def build_highlight_payload(occ, failed_fields=None, title_prefix=None):
    field_names = extract_field_names(failed_fields)
    field_paths = extract_field_paths(failed_fields)

    title = occ["label"]
    if title_prefix:
        title = f"{title_prefix}: {title}"

    return {
        "title": title,
        "xml_code": occ["xml_code"],
        "item_index": occ["item_index"],
        "item_label": occ["label"],
        "item_xml": occ.get("item_xml") or "",
        "field_names": field_names,
        "field_paths": field_paths
    }


def append_pairwise_warning_to_context(owner_context, warning_payload):
    owner_context["warnings"].append(warning_payload)


def build_pairwise_warning_payload(rule, left_occ, right_occ, failed_fields):
    same_context = left_occ["context"] is right_occ["context"]

    if same_context:
        xml_targets = [
            build_xml_target(
                left_occ["context"],
                left_occ["xml_code"],
                [
                    build_highlight_payload(left_occ, failed_fields, "Đối tượng 1"),
                    build_highlight_payload(right_occ, failed_fields, "Đối tượng 2"),
                ]
            )
        ]
    else:
        xml_targets = [
            build_xml_target(
                left_occ["context"],
                left_occ["xml_code"],
                [build_highlight_payload(left_occ, failed_fields, "Đối tượng 1")]
            ),
            build_xml_target(
                right_occ["context"],
                right_occ["xml_code"],
                [build_highlight_payload(right_occ, failed_fields, "Đối tượng 2")]
            )
        ]

    return {
        "bo_rule": rule.bo_rule.ma_bo_rule,
        "bo_rule_name": rule.bo_rule.ten_bo_rule,
        "rule_name": rule.ten_rule,
        "message": rule.thong_bao,
        "severity": rule.severity,
        "warning_type": "PAIRWISE",
        "failed_fields": failed_fields,
        "xml_targets": xml_targets
    }


def evaluate_pairwise_trigger_group_on_occurrence(detail_group, occ, don_vi_id=None):
    if not detail_group:
        return True

    xml_data_map = occ["context"]["xml_data_map"]
    source_xml_code = occ["xml_code"]
    item = occ["item"]

    local_trigger_details = []
    external_trigger_by_xml = defaultdict(list)

    for trigger in detail_group:
        trigger_xml_code = trigger.field.xml.ma_xml
        if trigger_xml_code == source_xml_code:
            local_trigger_details.append(trigger)
        else:
            external_trigger_by_xml[trigger_xml_code].append(trigger)

    local_ok = evaluate_detail_group_on_item(local_trigger_details, xml_data_map, item, don_vi_id=don_vi_id)

    external_ok = True
    for _, ext_group in external_trigger_by_xml.items():
        if not evaluate_trigger_group_any_item(ext_group, xml_data_map, don_vi_id=don_vi_id):
            external_ok = False
            break

    return local_ok and external_ok


def occurrence_passes_pairwise_trigger(trigger_groups, occ, don_vi_id=None):
    if not trigger_groups:
        return True

    for _, trigger_group_details in trigger_groups:
        if not trigger_group_details:
            return True

        if evaluate_pairwise_trigger_group_on_occurrence(trigger_group_details, occ, don_vi_id=don_vi_id):
            return True

    return False


def validate_pairwise_rule(rule, contexts, don_vi_id=None):
    details = sorted(rule.details, key=lambda x: (x.group_no or 1, x.sort_order, x.id))
    if not details:
        return

    trigger_details = [d for d in details if d.condition_role == "TRIGGER"]
    validate_details = [d for d in details if d.condition_role == "VALIDATE"]

    if not validate_details:
        return

    driver_detail = next((d for d in validate_details if detail_is_pairwise_multi_same_field(d)), None)
    if not driver_detail:
        return

    driver_xml_code = driver_detail.field.xml.ma_xml
    occurrences = collect_occurrences_for_xml(contexts, driver_xml_code)

    if len(occurrences) <= 1:
        return

    occurrence_trigger_details = []
    pairwise_trigger_details = []

    for d in trigger_details:
        if detail_is_pairwise_multi_same_field(d):
            pairwise_trigger_details.append(d)
        else:
            occurrence_trigger_details.append(d)

    occurrence_trigger_groups = group_details_by_group_no(occurrence_trigger_details) if occurrence_trigger_details else []
    pairwise_trigger_groups = group_details_by_group_no(pairwise_trigger_details) if pairwise_trigger_details else []
    validate_groups = group_details_by_group_no(validate_details)

    if occurrence_trigger_groups:
        filtered_occurrences = [
            occ for occ in occurrences
            if occurrence_passes_pairwise_trigger(occurrence_trigger_groups, occ, don_vi_id=don_vi_id)
        ]
    else:
        filtered_occurrences = occurrences

    if len(filtered_occurrences) <= 1:
        return

    for i in range(len(filtered_occurrences)):
        left_occ = filtered_occurrences[i]

        for j in range(i + 1, len(filtered_occurrences)):
            right_occ = filtered_occurrences[j]

            pairwise_trigger_pass = False

            if not pairwise_trigger_groups:
                pairwise_trigger_pass = True
            else:
                for _, trigger_group_details in pairwise_trigger_groups:
                    if not trigger_group_details:
                        pairwise_trigger_pass = True
                        break

                    ok, _ = evaluate_pairwise_group_on_pair(
                        trigger_group_details,
                        left_occ,
                        right_occ,
                        don_vi_id=don_vi_id
                    )
                    if ok:
                        pairwise_trigger_pass = True
                        break

            if not pairwise_trigger_pass:
                continue

            validate_pass = False
            failed_fields = []

            for _, validate_group_details in validate_groups:
                ok, current_failed = evaluate_pairwise_group_on_pair(
                    validate_group_details,
                    left_occ,
                    right_occ,
                    don_vi_id=don_vi_id
                )
                if ok:
                    validate_pass = True
                    failed_fields = []
                    break

                if not failed_fields:
                    failed_fields = current_failed

            if validate_pass:
                continue

            warning_payload = build_pairwise_warning_payload(rule, left_occ, right_occ, failed_fields)
            owner_context = pick_warning_owner(left_occ, right_occ)
            append_pairwise_warning_to_context(owner_context, warning_payload)


def build_single_warning_payload(rule, context, source_xml_code, item, index, failed_fields):
    item_occ = make_occurrence(context, source_xml_code, item, index)

    return {
        "bo_rule": rule.bo_rule.ma_bo_rule,
        "bo_rule_name": rule.bo_rule.ten_bo_rule,
        "rule_name": rule.ten_rule,
        "message": rule.thong_bao,
        "severity": rule.severity,
        "warning_type": "SINGLE",
        "failed_fields": failed_fields,
        "xml_targets": [
            build_xml_target(
                context,
                source_xml_code,
                [build_highlight_payload(item_occ, failed_fields, "Đối tượng")]
            )
        ]
    }


def validate_one_hoso(context, active_rules, don_vi_id=None):
    warnings = []
    xml_data_map = context["xml_data_map"]

    for rule in active_rules:
        details = sorted(rule.details, key=lambda x: (x.group_no or 1, x.sort_order, x.id))
        if not details:
            continue

        trigger_details = [d for d in details if d.condition_role == "TRIGGER"]
        validate_details = [d for d in details if d.condition_role == "VALIDATE"]

        if not validate_details:
            continue

        validate_group_by_xml = defaultdict(list)
        for detail in validate_details:
            validate_group_by_xml[detail.field.xml.ma_xml].append(detail)

        trigger_groups = group_details_by_group_no(trigger_details) if trigger_details else [(1, [])]

        for source_xml_code, source_validate_details in validate_group_by_xml.items():
            source_xml_info = xml_data_map.get(source_xml_code, {})
            source_items = source_xml_info.get("items", [])

            if not source_items:
                continue

            validate_groups = group_details_by_group_no(source_validate_details)

            for index, item in enumerate(source_items, start=1):
                trigger_pass = False

                for _, trigger_group_details in trigger_groups:
                    if not trigger_group_details:
                        trigger_pass = True
                        break

                    local_trigger_details = []
                    external_trigger_by_xml = defaultdict(list)

                    for trigger in trigger_group_details:
                        trigger_xml_code = trigger.field.xml.ma_xml
                        if trigger_xml_code == source_xml_code:
                            local_trigger_details.append(trigger)
                        else:
                            external_trigger_by_xml[trigger_xml_code].append(trigger)

                    local_ok = evaluate_detail_group_on_item(
                        local_trigger_details,
                        xml_data_map,
                        item,
                        don_vi_id=don_vi_id
                    )

                    external_ok = True
                    for _, ext_group in external_trigger_by_xml.items():
                        if not evaluate_trigger_group_any_item(ext_group, xml_data_map, don_vi_id=don_vi_id):
                            external_ok = False
                            break

                    if local_ok and external_ok:
                        trigger_pass = True
                        break

                if not trigger_pass:
                    continue

                validate_pass = False
                failed_fields = []

                for _, validate_group_details in validate_groups:
                    current_failed = []

                    for detail in validate_group_details:
                        ok = evaluate_detail_on_item(detail, xml_data_map, item, don_vi_id=don_vi_id)

                        if not ok:
                            actual_value = get_value_from_item(item, detail.field.xml_path)
                            actual_value = apply_date_part_if_needed(actual_value, detail.date_part)

                            current_failed.append({
                                "field_name": detail.field.ten_truong,
                                "field_path": detail.field.xml_path,
                                "actual_value": actual_value,
                                "compare_text": build_compare_text(detail)
                            })

                    if not current_failed:
                        validate_pass = True
                        failed_fields = []
                        break

                    if not failed_fields:
                        failed_fields = current_failed

                if not validate_pass and failed_fields:
                    warnings.append(
                        build_single_warning_payload(
                            rule=rule,
                            context=context,
                            source_xml_code=source_xml_code,
                            item=item,
                            index=index,
                            failed_fields=failed_fields
                        )
                    )

    return warnings


def get_active_rules_for_unit(don_vi_id=None):
    query = (
        Rule.query
        .join(Rule.bo_rule)
        .filter(Rule.is_active.is_(True))
        .filter(Rule.bo_rule.has(is_active=True))
    )

    if don_vi_id:
        query = (
            query
            .outerjoin(RuleUnit, RuleUnit.rule_id == Rule.id)
            .filter(
                or_(
                    Rule.apply_scope == "ALL",
                    (Rule.apply_scope == "UNIT") & (RuleUnit.don_vi_id == don_vi_id)
                )
            )
            .distinct()
        )
    else:
        query = query.filter(Rule.apply_scope == "ALL")

    return query.all()


def run_validation(tree, don_vi_id=None):
    xml_configs = DanhMucXml.query.all()
    active_rules = get_active_rules_for_unit(don_vi_id=don_vi_id)

    contexts, stats = build_hoso_contexts(tree, xml_configs)

    normal_rules = [r for r in active_rules if not rule_uses_pairwise_mode(r)]
    pairwise_rules_one_hoso = [
        r for r in active_rules
        if rule_uses_pairwise_mode(r) and (getattr(r, "run_scope", "ONE_HOSO") or "ONE_HOSO").strip().upper() == "ONE_HOSO"
    ]
    pairwise_rules_all_hoso = [
        r for r in active_rules
        if rule_uses_pairwise_mode(r) and (getattr(r, "run_scope", "ONE_HOSO") or "ONE_HOSO").strip().upper() == "ALL_HOSO"
    ]

    for context in contexts:
        context["warnings"].extend(validate_one_hoso(context, normal_rules, don_vi_id=don_vi_id))

    for context in contexts:
        for rule in pairwise_rules_one_hoso:
            validate_pairwise_rule(rule, [context], don_vi_id=don_vi_id)

    for rule in pairwise_rules_all_hoso:
        validate_pairwise_rule(rule, contexts, don_vi_id=don_vi_id)

    result_by_hoso = []

    for context in contexts:
        if context["warnings"]:
            result_by_hoso.append({
                "patient_code": context["patient_code"],
                "patient_name": context["patient_name"],
                "ngay_vao": context["ngay_vao"],
                "ngay_vao_raw": context["ngay_vao_raw"],
                "raw_xml": context["raw_xml"],
                "warnings": context["warnings"]
            })

    stats["error_hoso_count"] = len(result_by_hoso)
    return result_by_hoso, stats