from collections import defaultdict

from models import DanhMucXml, Rule
from services.xml_parser_service import (
    get_hoso_nodes,
    build_xml_data_map_for_hoso,
    get_value_from_item,
    get_item_label,
    get_hoso_identity
)


def normalize_value(value):
    if value is None:
        return None
    return str(value).strip()


def check_condition(actual_value, condition_code, expected_value=None):
    actual_value = normalize_value(actual_value)
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

        values = [v.strip() for v in expected_value.split(",") if v.strip() != ""]
        return actual_value in values

    return False


def validate_one_hoso(xml_data_map, active_rules):
    warnings = []

    for rule in active_rules:
        details = sorted(rule.details, key=lambda x: (x.sort_order, x.id))
        if not details:
            continue

        grouped_by_xml = defaultdict(list)
        for detail in details:
            xml_code = detail.field.xml.ma_xml
            grouped_by_xml[xml_code].append(detail)

        for xml_code, xml_details in grouped_by_xml.items():
            xml_info = xml_data_map.get(xml_code)
            if not xml_info:
                continue

            items = xml_info["items"]

            trigger_details = [d for d in xml_details if d.condition_role == "TRIGGER"]
            validate_details = [d for d in xml_details if d.condition_role == "VALIDATE"]

            for index, item in enumerate(items, start=1):
                trigger_pass = True

                for detail in trigger_details:
                    actual_value = get_value_from_item(item, detail.field.xml_path)
                    ok = check_condition(
                        actual_value=actual_value,
                        condition_code=detail.condition.ma_dieu_kien,
                        expected_value=detail.gia_tri
                    )
                    if not ok:
                        trigger_pass = False
                        break

                if not trigger_pass:
                    continue

                validate_failed_fields = []

                for detail in validate_details:
                    actual_value = get_value_from_item(item, detail.field.xml_path)
                    ok = check_condition(
                        actual_value=actual_value,
                        condition_code=detail.condition.ma_dieu_kien,
                        expected_value=detail.gia_tri
                    )
                    if not ok:
                        validate_failed_fields.append({
                            "field_name": detail.field.ten_truong,
                            "field_path": detail.field.xml_path,
                            "actual_value": actual_value
                        })

                if validate_failed_fields:
                    item_label = get_item_label(xml_code, item)

                    warnings.append({
                        "bo_rule": rule.bo_rule.ma_bo_rule,
                        "bo_rule_name": rule.bo_rule.ten_bo_rule,
                        "rule_name": rule.ten_rule,
                        # "message": f"{item_label}({index}): {rule.thong_bao}",
                        "message": rule.thong_bao,
                        "severity": rule.severity,
                        "failed_fields": validate_failed_fields
                    })

    return warnings


def run_validation(tree):
    xml_configs = DanhMucXml.query.all()

    active_rules = (
        Rule.query
        .join(Rule.bo_rule)
        .filter(Rule.is_active.is_(True))
        .filter(Rule.bo_rule.has(is_active=True))
        .all()
    )

    hoso_nodes = get_hoso_nodes(tree)
    result_by_hoso = []

    for hoso_index, hoso_node in enumerate(hoso_nodes, start=1):
        xml_data_map = build_xml_data_map_for_hoso(hoso_node, xml_configs)
        hoso_info = get_hoso_identity(xml_data_map, hoso_index)
        warnings = validate_one_hoso(xml_data_map, active_rules)

        if len(warnings) > 0:
            result_by_hoso.append({
                "patient_code": hoso_info["patient_code"],
                "patient_name": hoso_info["patient_name"],
                "warnings": warnings
            })

    return result_by_hoso