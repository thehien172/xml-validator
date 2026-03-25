import os
from lxml import etree

from models import (
    db,
    DanhMucXml,
    DanhMucTruongDuLieu,
    DanhMucDieuKien,
    BoRule,
    Rule,
    RuleDetail,
    HeThong,
    DonVi
)


SAMPLE_XML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "sample_seed_3176.xml"
)


XML_CONFIGS = [
    ("XML1", "Thông tin tổng hợp hồ sơ", "./TONG_HOP"),
    ("XML2", "Danh sách chi tiết thuốc", "./CHITIEU_CHITIET_THUOC/DSACH_CHI_TIET_THUOC/CHI_TIET_THUOC"),
    ("XML3", "Danh sách DVKT", "./CHITIEU_CHITIET_DVKT_VTYT/DSACH_CHI_TIET_DVKT/CHI_TIET_DVKT"),
    ("XML4", "Danh sách CLS", "./CHITIEU_CHITIET_DICHVUCANLAMSANG/DSACH_CHI_TIET_CLS/CHI_TIET_CLS"),
    ("XML5", "Danh sách DV khác", "./CHITIEU_CHITIET_DIENBIENLAMSANG/DSACH_CHI_TIET_DIEN_BIEN_BENH/CHI_TIET_DIEN_BIEN_BENH"),
    ("XML6", "Danh sách diễn biến bệnh", "./CHI_TIEU_HO_SO_BENH_AN_CHAM_SOC_VA_DIEU_TRI_HIV_AIDS/DSACH_HO_SO_BENH_AN_CHAM_SOC_VA_DIEU_TRI_HIV_AIDS"),
    ("XML7", "Thông tin giấy ra viện", "./CHI_TIEU_DU_LIEU_GIAY_RA_VIEN"),
    ("XML8", "Thông tin giấy chuyển tuyến", "./CHI_TIEU_DU_LIEU_TOM_TAT_HO_SO_BENH_AN"),
    # ("XML9", "Danh sách toa thuốc", "./"),
    # ("XML10", "Thông tin y lệnh", "./"),
    # ("XML11", "Thông tin phiếu công khai", "./"),
    # ("XML13", "Thông tin giấy hẹn khám lại", "./"),
    # ("XML14", "Thông tin giấy chứng nhận nghỉ việc", "./"),
    # ("XML15", "Danh sách diễn biến khác", "./"),
]


def add_condition_if_not_exists(ma, ten):
    existing = DanhMucDieuKien.query.filter_by(ma_dieu_kien=ma).first()
    if not existing:
        db.session.add(DanhMucDieuKien(ma_dieu_kien=ma, ten_dieu_kien=ten))


def seed_systems_and_units():
    if not HeThong.query.filter_by(ten_he_thong="L2").first():
        db.session.add(HeThong(ten_he_thong="L2"))
    if not HeThong.query.filter_by(ten_he_thong="L3").first():
        db.session.add(HeThong(ten_he_thong="L3"))
    db.session.commit()

    l2 = HeThong.query.filter_by(ten_he_thong="L2").first()

    units = [
        ("62126", "Bệnh xá 24", l2.id, 'BX.HIENLTADMIN', 'Thehien@172'),
        ("62058", "BV YHCT - CS1", l2.id, '', ''),
        ("62134", "BV YHCT - CS2", l2.id, '', ''),
    ]

    for ma_don_vi, ten_don_vi, he_thong_id, api_username, api_password in units:
        if not DonVi.query.filter_by(ma_don_vi=ma_don_vi).first():
            db.session.add(DonVi(
                ma_don_vi=ma_don_vi,
                ten_don_vi=ten_don_vi,
                he_thong_id=he_thong_id,
                api_username = api_username,
                api_password = api_password
            ))
    db.session.commit()


def seed_xml_configs():
    for ma_xml, ten_xml, list_path in XML_CONFIGS:
        existing = DanhMucXml.query.filter_by(ma_xml=ma_xml).first()
        if not existing:
            db.session.add(DanhMucXml(
                ma_xml=ma_xml,
                ten_xml=ten_xml,
                list_path=list_path
            ))
    db.session.commit()


def parse_sample_xml():
    if not os.path.exists(SAMPLE_XML_PATH):
        return None

    parser = etree.XMLParser(remove_blank_text=True, recover=True, encoding="utf-8")
    return etree.parse(SAMPLE_XML_PATH, parser)


def get_first_hoso(tree):
    if tree is None:
        return None
    hosos = tree.xpath(".//HOSO")
    return hosos[0] if hosos else None


def get_noidungfile_by_xml(hoso_node, ma_xml):
    if hoso_node is None:
        return None

    filehoso = hoso_node.xpath(f'./FILEHOSO[LOAIHOSO="{ma_xml}"]')
    if not filehoso:
        return None

    return filehoso[0].find("./NOIDUNGFILE")


def collect_leaf_paths(elem, prefix=""):
    if elem is None:
        return []

    current_path = f"{prefix}/{elem.tag}" if prefix else elem.tag
    children = [c for c in elem if isinstance(c.tag, str)]

    if not children:
        return [current_path]

    result = []
    for child in children:
        result.extend(collect_leaf_paths(child, current_path))
    return result


def deduplicate_keep_order(items):
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def infer_data_type(field_name):
    upper_name = field_name.upper()

    if upper_name.startswith("NGAY_") or upper_name.endswith("_NGAY"):
        return "DATE"

    number_prefixes = (
        "T_", "TYLE_", "SO_", "DON_GIA", "THANH_TIEN", "MUC_HUONG",
        "CAN_NANG", "STT"
    )
    if upper_name.startswith(number_prefixes):
        return "NUMBER"

    return "STRING"


def seed_fields_from_sample():
    tree = parse_sample_xml()
    hoso_node = get_first_hoso(tree)
    if hoso_node is None:
        return

    xmls = DanhMucXml.query.all()
    xml_map = {x.ma_xml: x for x in xmls}

    for ma_xml, _, list_path in XML_CONFIGS:
        xml_row = xml_map.get(ma_xml)
        if not xml_row:
            continue

        noidungfile = get_noidungfile_by_xml(hoso_node, ma_xml)
        if noidungfile is None:
            continue

        leaf_paths = collect_leaf_paths(noidungfile)
        leaf_paths = [p.replace("NOIDUNGFILE/", "") for p in leaf_paths]
        leaf_paths = deduplicate_keep_order(leaf_paths)

        # chỉ seed những field nằm đúng dưới list_path hoặc chính path đơn item
        for full_path in leaf_paths:
            if ma_xml in ("XML10", "XML11", "XML13", "XML14"):
                continue

            # Nếu list_path là "./TONG_HOP" thì field path sẽ là "./HO_TEN"
            base = list_path.replace("./", "")
            if full_path == base:
                continue

            if base == "":
                continue

            if full_path.startswith(base + "/"):
                relative_path = "./" + full_path[len(base) + 1:]
            elif full_path == base:
                continue
            else:
                # XML có thể là node đơn, ví dụ XML7, XML8
                if "/" not in full_path:
                    continue
                root_node = full_path.split("/")[0]
                if base == root_node:
                    relative_path = "./" + "/".join(full_path.split("/")[1:])
                else:
                    continue

            if relative_path == "./":
                continue

            field_name = relative_path.replace("./", "").split("/")[-1]
            existing = DanhMucTruongDuLieu.query.filter_by(
                xml_id=xml_row.id,
                xml_path=relative_path
            ).first()

            if not existing:
                db.session.add(DanhMucTruongDuLieu(
                    ten_truong=field_name,
                    xml_id=xml_row.id,
                    xml_path=relative_path,
                    data_type=infer_data_type(field_name)
                ))

    db.session.commit()


def seed_conditions():
    add_condition_if_not_exists("IS_NULL", "Trống")
    add_condition_if_not_exists("NOT_NULL", "Không trống")
    add_condition_if_not_exists("EQUAL", "Bằng")
    add_condition_if_not_exists("NOT_EQUAL", "Khác")
    add_condition_if_not_exists("CONTAINS", "Chứa")
    add_condition_if_not_exists("IN_LIST", "Nằm trong danh sách")
    add_condition_if_not_exists("BETWEEN", "Nằm trong khoảng")
    add_condition_if_not_exists("NOT_BETWEEN", "Không nằm trong khoảng")
    db.session.commit()


def seed_rule_sets():
    if not BoRule.query.filter_by(ma_bo_rule="3176").first():
        db.session.add(BoRule(
            ma_bo_rule="3176",
            ten_bo_rule="Bộ rule theo chuẩn 3176",
            mo_ta="Các rule kiểm tra theo chuẩn 3176",
            is_active=True
        ))

    db.session.commit()

def seed_data():
    seed_systems_and_units()
    seed_xml_configs()
    seed_fields_from_sample()
    seed_conditions()
    seed_rule_sets()