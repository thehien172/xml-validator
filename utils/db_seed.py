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


def seed_data():
    # Seed hệ thống
    if not HeThong.query.first():
        l2 = HeThong(ten_he_thong="L2")
        l3 = HeThong(ten_he_thong="L3")
        db.session.add_all([l2, l3])
        db.session.commit()
    else:
        l2 = HeThong.query.filter_by(ten_he_thong="L2").first()
        l3 = HeThong.query.filter_by(ten_he_thong="L3").first()

    # Seed đơn vị
    if not DonVi.query.first():
        don_vis = [
            DonVi(ma_don_vi="62058", ten_don_vi="BV YHCT - CS1", he_thong_id=l2.id),
            DonVi(ma_don_vi="62134", ten_don_vi="BV YHCT - CS2", he_thong_id=l2.id),
            DonVi(ma_don_vi="62126", ten_don_vi="Bệnh xá 24", he_thong_id=l2.id),
        ]
        db.session.add_all(don_vis)
        db.session.commit()

    # Seed XML
    if not DanhMucXml.query.first():
        xml1 = DanhMucXml(
            ma_xml="XML1",
            ten_xml="XML1 - Thông tin tổng hợp",
            list_path="./TONG_HOP"
        )

        xml2 = DanhMucXml(
            ma_xml="XML2",
            ten_xml="XML2 - Chi tiết thuốc",
            list_path="./CHITIEU_CHITIET_THUOC/DSACH_CHI_TIET_THUOC/CHI_TIET_THUOC"
        )

        xml3 = DanhMucXml(
            ma_xml="XML3",
            ten_xml="XML3 - Chi tiết DVKT",
            list_path="./CHITIEU_CHITIET_DVKT_VTYT/DSACH_CHI_TIET_DVKT/CHI_TIET_DVKT"
        )

        db.session.add_all([xml1, xml2, xml3])
        db.session.commit()
    else:
        xml1 = DanhMucXml.query.filter_by(ma_xml="XML1").first()
        xml3 = DanhMucXml.query.filter_by(ma_xml="XML3").first()

    # Seed field
    if not DanhMucTruongDuLieu.query.first():
        field_ho_ten = DanhMucTruongDuLieu(
            ten_truong="Họ tên",
            xml_id=xml1.id,
            xml_path="./HO_TEN",
            data_type="STRING"
        )

        field_ma_nhom = DanhMucTruongDuLieu(
            ten_truong="Mã nhóm",
            xml_id=xml3.id,
            xml_path="./MA_NHOM",
            data_type="STRING"
        )

        field_ma_bac_si = DanhMucTruongDuLieu(
            ten_truong="Mã bác sĩ",
            xml_id=xml3.id,
            xml_path="./MA_BAC_SI",
            data_type="STRING"
        )

        db.session.add_all([field_ho_ten, field_ma_nhom, field_ma_bac_si])
        db.session.commit()
    else:
        field_ho_ten = DanhMucTruongDuLieu.query.filter_by(ten_truong="Họ tên").first()
        field_ma_nhom = DanhMucTruongDuLieu.query.filter_by(ten_truong="Mã nhóm").first()
        field_ma_bac_si = DanhMucTruongDuLieu.query.filter_by(ten_truong="Mã bác sĩ").first()

    # Seed điều kiện
    if not DanhMucDieuKien.query.first():
        conditions = [
            DanhMucDieuKien(ma_dieu_kien="IS_NULL", ten_dieu_kien="Trống"),
            DanhMucDieuKien(ma_dieu_kien="NOT_NULL", ten_dieu_kien="Không trống"),
            DanhMucDieuKien(ma_dieu_kien="EQUAL", ten_dieu_kien="Bằng"),
            DanhMucDieuKien(ma_dieu_kien="NOT_EQUAL", ten_dieu_kien="Khác"),
            DanhMucDieuKien(ma_dieu_kien="CONTAINS", ten_dieu_kien="Chứa"),
            DanhMucDieuKien(ma_dieu_kien="IN_LIST", ten_dieu_kien="Nằm trong danh sách")
        ]
        db.session.add_all(conditions)
        db.session.commit()

    # Seed bộ rule
    if not BoRule.query.first():
        bo_rule_4750 = BoRule(
            ma_bo_rule="4750",
            ten_bo_rule="Bộ rule theo Quyết định 4750",
            mo_ta="Các rule kiểm tra theo chuẩn QĐ 4750",
            is_active=True
        )

        bo_rule_3176 = BoRule(
            ma_bo_rule="3176",
            ten_bo_rule="Bộ rule theo chuẩn 3176",
            mo_ta="Các rule kiểm tra theo chuẩn 3176",
            is_active=True
        )

        db.session.add_all([bo_rule_4750, bo_rule_3176])
        db.session.commit()
    else:
        bo_rule_4750 = BoRule.query.filter_by(ma_bo_rule="4750").first()

    # Seed rule
    if not Rule.query.first():
        cond_not_null = DanhMucDieuKien.query.filter_by(ma_dieu_kien="NOT_NULL").first()
        cond_equal = DanhMucDieuKien.query.filter_by(ma_dieu_kien="EQUAL").first()

        rule_1 = Rule(
            bo_rule_id=bo_rule_4750.id,
            ten_rule="Check họ và tên",
            thong_bao="Thiếu họ và tên",
            severity="WARNING",
            is_active=True
        )

        rule_2 = Rule(
            bo_rule_id=bo_rule_4750.id,
            ten_rule="Check mã bác sĩ",
            thong_bao="Thiếu mã bác sĩ",
            severity="WARNING",
            is_active=True
        )

        db.session.add_all([rule_1, rule_2])
        db.session.commit()

        db.session.add_all([
            RuleDetail(
                rule_id=rule_1.id,
                field_id=field_ho_ten.id,
                condition_id=cond_not_null.id,
                gia_tri=None,
                condition_role="VALIDATE",
                sort_order=1
            ),
            RuleDetail(
                rule_id=rule_2.id,
                field_id=field_ma_nhom.id,
                condition_id=cond_equal.id,
                gia_tri="12",
                condition_role="TRIGGER",
                sort_order=1
            ),
            RuleDetail(
                rule_id=rule_2.id,
                field_id=field_ma_bac_si.id,
                condition_id=cond_not_null.id,
                gia_tri=None,
                condition_role="VALIDATE",
                sort_order=2
            )
        ])
        db.session.commit()