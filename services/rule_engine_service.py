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


def normalize_list(values):
    result = []
    for v in values:
        nv = normalize_value(v)
        if nv is not None and nv != "":
            result.append(nv)
    return result


def check_condition(actual_value, condition_code, expected_value=None):
    actual_value = normalize_value(actual_value)

    # expected_value có thể là list khi compare field khác XML
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

    return False


def get_expected_value_for_detail(detail, xml_data_map, current_item):
    """
    compare_mode = VALUE:
        dùng detail.gia_tri

    compare_mode = FIELD:
        - nếu compare_field cùng XML với field hiện tại: lấy trên cùng item
        - nếu compare_field khác XML: lấy toàn bộ giá trị field đó trong XML đích
    """
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
    expected_value = get_expected_value_for_detail(detail, xml_data_map, item)

    return check_condition(
        actual_value=actual_value,
        condition_code=detail.condition.ma_dieu_kien,
        expected_value=expected_value
    )


def evaluate_trigger_any_item(detail, xml_data_map):
    """
    Trigger ở XML khác validate:
    chỉ cần có ít nhất 1 item trong XML đó thỏa điều kiện là pass.
    """
    xml_code = detail.field.xml.ma_xml
    xml_info = xml_data_map.get(xml_code, {})
    items = xml_info.get("items", [])

    for item in items:
        if evaluate_detail_on_item(detail, xml_data_map, item):
            return True

    return False


def validate_one_hoso(xml_data_map, active_rules):
    warnings = []

    for rule in active_rules:
        details = sorted(rule.details, key=lambda x: (x.sort_order, x.id))
        if not details:
            continue

        trigger_details = [d for d in details if d.condition_role == "TRIGGER"]
        validate_details = [d for d in details if d.condition_role == "VALIDATE"]

        if not validate_details:
            continue

        # Gom validate theo XML nguồn
        validate_group_by_xml = defaultdict(list)
        for detail in validate_details:
            validate_group_by_xml[detail.field.xml.ma_xml].append(detail)

        for source_xml_code, source_validate_details in validate_group_by_xml.items():
            source_xml_info = xml_data_map.get(source_xml_code, {})
            source_items = source_xml_info.get("items", [])

            if not source_items:
                continue

            # Tách trigger:
            # - local trigger: cùng XML với validate -> check trên từng item
            # - external trigger: khác XML -> check toàn hồ sơ
            local_triggers = []
            external_triggers = []

            for trigger in trigger_details:
                trigger_xml_code = trigger.field.xml.ma_xml
                if trigger_xml_code == source_xml_code:
                    local_triggers.append(trigger)
                else:
                    external_triggers.append(trigger)

            # Nếu có trigger ngoài XML mà không đạt -> bỏ qua toàn bộ validate group này
            external_trigger_pass = True
            for trigger in external_triggers:
                if not evaluate_trigger_any_item(trigger, xml_data_map):
                    external_trigger_pass = False
                    break

            if not external_trigger_pass:
                continue

            # Chạy từng item của XML nguồn
            for index, item in enumerate(source_items, start=1):
                # local trigger phải pass trên chính item hiện tại
                local_trigger_pass = True
                for trigger in local_triggers:
                    if not evaluate_detail_on_item(trigger, xml_data_map, item):
                        local_trigger_pass = False
                        break

                if not local_trigger_pass:
                    continue

                validate_failed_fields = []

                for detail in source_validate_details:
                    ok = evaluate_detail_on_item(detail, xml_data_map, item)

                    if not ok:
                        compare_text = None
                        if detail.compare_mode == "FIELD" and detail.compare_field:
                            compare_text = (
                                f"So sánh với field "
                                f"{detail.compare_field.xml.ma_xml}:{detail.compare_field.xml_path}"
                            )
                        elif detail.gia_tri:
                            compare_text = f"So sánh với giá trị {detail.gia_tri}"

                        actual_value = get_value_from_item(item, detail.field.xml_path)

                        validate_failed_fields.append({
                            "field_name": detail.field.ten_truong,
                            "field_path": detail.field.xml_path,
                            "actual_value": actual_value,
                            "compare_text": compare_text
                        })

                if validate_failed_fields:
                    item_label = get_item_label(source_xml_code, item)

                    warnings.append({
                        "bo_rule": rule.bo_rule.ma_bo_rule,
                        "bo_rule_name": rule.bo_rule.ten_bo_rule,
                        "rule_name": rule.ten_rule,
                        "message": f"{item_label}({index}): {rule.thong_bao}",
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

        if warnings:
            result_by_hoso.append({
                "patient_code": hoso_info["patient_code"],
                "patient_name": hoso_info["patient_name"],
                "warnings": warnings
            })

    return result_by_hoso