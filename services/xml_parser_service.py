from lxml import etree

def parse_xml_content(xml_content):
    parser = etree.XMLParser(remove_blank_text=True, recover=True, encoding="utf-8")
    return etree.fromstring(xml_content.encode("utf-8"), parser)

def parse_xml_file(file_path):
    parser = etree.XMLParser(remove_blank_text=True, recover=True, encoding="utf-8")
    tree = etree.parse(file_path, parser)
    return tree


def get_hoso_nodes(tree):
    """
    Lấy danh sách HOSO trong file đầu vào.
    Mỗi HOSO tương ứng 1 bệnh nhân.
    """
    return tree.xpath(".//HOSO")

def build_xml_data_map_for_hoso(hoso_node, xml_configs):
    """
    Trong 1 HOSO, map các FILEHOSO theo LOAIHOSO.
    Trả về dict:
    {
        "XML1": {
            "xml_id": ...,
            "xml_code": "XML1",
            "xml_name": "...",
            "items": [...]
        },
        ...
    }
    """
    result = {}

    for xml_config in xml_configs:
        result[xml_config.ma_xml] = {
            "xml_id": xml_config.id,
            "xml_code": xml_config.ma_xml,
            "xml_name": xml_config.ten_xml,
            "items": []
        }

    filehoso_nodes = hoso_node.xpath("./FILEHOSO")

    for filehoso in filehoso_nodes:
        loaihoso = (filehoso.findtext("./LOAIHOSO") or "").strip().upper()
        noidungfile = filehoso.find("./NOIDUNGFILE")

        if not loaihoso or noidungfile is None:
            continue

        xml_config = next((x for x in xml_configs if x.ma_xml == loaihoso), None)
        if not xml_config:
            continue

        items = noidungfile.xpath(xml_config.list_path)
        if not items:
            list_path = (xml_config.list_path or "").strip()

            # bỏ ./ ở đầu nếu có
            normalized_path = list_path[2:] if list_path.startswith("./") else list_path

            # lấy tag cuối cùng của path
            fallback_tag = normalized_path.split("/")[-1] if normalized_path else "EMPTY_ITEM"

            # tạo node rỗng giả
            empty_item = etree.Element(fallback_tag)
            items = [empty_item]

        result[loaihoso]["items"] = items

    return result


def get_value_from_item(item, relative_path):
    result = item.xpath(relative_path)

    if not result:
        return None

    first = result[0]

    if isinstance(first, etree._Element):
        return (first.text or "").strip()

    return str(first).strip()


def get_item_label(xml_code, item):
    return f"{xml_code}-{item.tag}"


def get_hoso_identity(xml_data_map, hoso_index):
    """
    Lấy thông tin nhận diện hồ sơ/bệnh nhân để hiển thị kết quả.
    Ưu tiên đọc từ XML1/TONG_HOP nếu có.
    """
    xml1_info = xml_data_map.get("XML1", {})
    xml1_items = xml1_info.get("items", [])

    patient_code = ""
    patient_name = ""

    if xml1_items:
        tong_hop = xml1_items[0]
        patient_code = get_value_from_item(tong_hop, "./MA_BN") or ""
        patient_name = get_value_from_item(tong_hop, "./HO_TEN") or ""

    return {
        "hoso_index": hoso_index,
        "patient_code": patient_code,
        "patient_name": patient_name
    }