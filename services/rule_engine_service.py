from collections import defaultdict
from datetime import datetime

from sqlalchemy import or_

from models import DanhMucXml, Rule, RuleUnit
from services.xml_parser_service import (
    get_hoso_nodes,
    build_xml_data_map_for_hoso,
    get_value_from_item,
    get_item_label,
    get_hoso_identity,
    get_hoso_raw_xml
)


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


def check_condition(actual_value, condition_code, expected_value=None):
    actual_value = normalize_value(actual_value)

    if isinstance(expected_value, list):
        expected_list = normalize_list(expected_value)

        if condition_code == "EQUAL":
            return actual_value in expected_list

        if condition_code == "NOT_EQUAL":
            return actual_value not in expected_list

        if condition_code == "IN_LIST":
            return actual_value in expected_list

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

    if condition_code == "IN_LIST":
        if actual_value is None or expected_value is None:
            return False
        values = [v.strip() for v in expected_value.split(",") if v.strip()]
        return actual_value in values

    if condition_code == "BETWEEN":
        return is_between_value(actual_value, expected_value)

    if condition_code == "NOT_BETWEEN":
        return not is_between_value(actual_value, expected_value)

    return False


def get_expected_value_for_detail(detail, xml_data_map, current_item):
    if detail.compare_mode == "FIELD" and detail.compare_field:
        source_xml_code = detail.field.xml.ma_xml
        target_xml_code = detail.compare_field.xml.ma_xml

        if source_xml_code == target_xml_code:
            return get_value_from_item(current_item, detail.compare_field.xml_path)

        target_xml_info = xml_data_map.get(target_xml_code, {})
        target_items = target_xml_info.get("items", [])

        values = []
        for target_item in target_items:
            val = get_value_from_item(target_item, detail.compare_field.xml_path)
            if val is not None and str(val).strip() != "":
                values.append(val)

        return values

    return detail.gia_tri


def evaluate_detail_on_item(detail, xml_data_map, item):
    actual_value = get_value_from_item(item, detail.field.xml_path)

    if detail.date_part:
        actual_value = extract_date_part(actual_value, detail.date_part)

    expected_value = get_expected_value_for_detail(detail, xml_data_map, item)

    return check_condition(
        actual_value=actual_value,
        condition_code=detail.condition.ma_dieu_kien,
        expected_value=expected_value
    )


def evaluate_detail_group_on_item(detail_group, xml_data_map, item):
    for detail in detail_group:
        if not evaluate_detail_on_item(detail, xml_data_map, item):
            return False
    return True


def evaluate_trigger_group_any_item(detail_group, xml_data_map):
    if not detail_group:
        return True

    xml_code = detail_group[0].field.xml.ma_xml
    xml_info = xml_data_map.get(xml_code, {})
    items = xml_info.get("items", [])

    for item in items:
        if evaluate_detail_group_on_item(detail_group, xml_data_map, item):
            return True

    return False


def group_details_by_group_no(details):
    grouped = defaultdict(list)
    for d in details:
        grouped[d.group_no or 1].append(d)

    result = []
    for group_no in sorted(grouped.keys()):
        result.append((group_no, sorted(grouped[group_no], key=lambda x: (x.sort_order, x.id))))
    return result


def build_compare_text(detail):
    if detail.compare_mode == "FIELD" and detail.compare_field:
        return f"So sánh với field {detail.compare_field.xml.ma_xml}:{detail.compare_field.xml_path}"

    if detail.gia_tri:
        if detail.condition.ma_dieu_kien == "BETWEEN":
            return f"Giá trị phải nằm trong khoảng {detail.gia_tri}"
        if detail.condition.ma_dieu_kien == "NOT_BETWEEN":
            return f"Giá trị không được nằm trong khoảng {detail.gia_tri}"
        return f"So sánh với giá trị {detail.gia_tri}"

    return None


def validate_one_hoso(xml_data_map, active_rules):
    warnings = []

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

                for trigger_group_no, trigger_group_details in trigger_groups:
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

                    local_ok = evaluate_detail_group_on_item(local_trigger_details, xml_data_map, item)

                    external_ok = True
                    for _, ext_group in external_trigger_by_xml.items():
                        if not evaluate_trigger_group_any_item(ext_group, xml_data_map):
                            external_ok = False
                            break

                    if local_ok and external_ok:
                        trigger_pass = True
                        break

                if not trigger_pass:
                    continue

                validate_pass = False
                failed_fields = []

                for validate_group_no, validate_group_details in validate_groups:
                    current_failed = []

                    for detail in validate_group_details:
                        ok = evaluate_detail_on_item(detail, xml_data_map, item)

                        if not ok:
                            actual_value = get_value_from_item(item, detail.field.xml_path)
                            if detail.date_part:
                                actual_value = extract_date_part(actual_value, detail.date_part)

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
                    item_label = get_item_label(source_xml_code, item)

                    warnings.append({
                        "bo_rule": rule.bo_rule.ma_bo_rule,
                        "bo_rule_name": rule.bo_rule.ten_bo_rule,
                        "rule_name": rule.ten_rule,
                        "message": f"{item_label}({index}): {rule.thong_bao}",
                        "severity": rule.severity,
                        "failed_fields": failed_fields
                    })

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

    hoso_nodes = get_hoso_nodes(tree)
    result_by_hoso = []
    total_xml_read = 0

    for hoso_index, hoso_node in enumerate(hoso_nodes, start=1):
        xml_data_map = build_xml_data_map_for_hoso(hoso_node, xml_configs)

        for xml_code, xml_info in xml_data_map.items():
            total_xml_read += len(xml_info.get("items", []))

        hoso_info = get_hoso_identity(xml_data_map, hoso_index)
        warnings = validate_one_hoso(xml_data_map, active_rules)
        raw_xml = get_hoso_raw_xml(hoso_node)

        if warnings:
            result_by_hoso.append({
                "patient_code": hoso_info["patient_code"],
                "patient_name": hoso_info["patient_name"],
                "ngay_vao": hoso_info["ngay_vao"],
                "ngay_vao_raw": hoso_info["ngay_vao_raw"],
                "raw_xml": raw_xml,
                "warnings": warnings
            })

    stats = {
        "total_xml_read": total_xml_read,
        "total_hoso_read": len(hoso_nodes),
        "error_hoso_count": len(result_by_hoso)
    }

    return result_by_hoso, stats