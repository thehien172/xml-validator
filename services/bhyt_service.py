import re
import requests

from models import db


BHYT_BASE_URL = "https://gdbhyt.baohiemxahoi.gov.vn"


def build_bhyt_cookie_header(unit):
    cookies = []

    if getattr(unit, "bhyt_aspnet_session_id", None):
        cookies.append(f"ASP.NET_SessionId={unit.bhyt_aspnet_session_id}")

    if getattr(unit, "bhyt_bigipserver", None):
        cookies.append(f"BIGipServerP-GD-APP-134={unit.bhyt_bigipserver}")

    if getattr(unit, "bhyt_aspxauth", None):
        cookies.append(f".ASPXAUTH={unit.bhyt_aspxauth}")

    if getattr(unit, "bhyt_ts015ef943", None):
        cookies.append(f"TS015ef943={unit.bhyt_ts015ef943}")

    return "; ".join(cookies)


def build_bhyt_headers(unit=None, extra_headers=None):
    headers = {
        "Accept": "*/*",
    }

    if unit is not None:
        cookie_str = build_bhyt_cookie_header(unit)
        if cookie_str:
            headers["Cookie"] = cookie_str

    if extra_headers:
        headers.update(extra_headers)

    return headers


def update_unit_cookie_from_response(unit, resp):
    cookie_map = resp.cookies.get_dict()

    if cookie_map.get("ASP.NET_SessionId"):
        unit.bhyt_aspnet_session_id = cookie_map.get("ASP.NET_SessionId")

    if cookie_map.get("BIGipServerP-GD-APP-134"):
        unit.bhyt_bigipserver = cookie_map.get("BIGipServerP-GD-APP-134")

    if cookie_map.get("TS015ef943"):
        unit.bhyt_ts015ef943 = cookie_map.get("TS015ef943")

    if cookie_map.get(".ASPXAUTH"):
        unit.bhyt_aspxauth = cookie_map.get(".ASPXAUTH")


def check_bhyt_root_status(unit):
    resp = requests.get(
        BHYT_BASE_URL + "/",
        headers=build_bhyt_headers(unit),
        allow_redirects=False,
        timeout=20
    )

    return resp


def refresh_bhyt_captcha(unit):
    resp = requests.post(
        BHYT_BASE_URL + "/Account/_RefreshCaptchaPartial",
        headers=build_bhyt_headers(
            unit,
            {
                "X-Requested-With": "XMLHttpRequest"
            }
        ),
        timeout=20
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()

    return resp.text


def extract_captcha_base64(html_text):
    if not html_text:
        return None

    match = re.search(r'src="data:image\/png;base64,([^"]+)"', html_text)
    if match:
        return match.group(1)

    return None


def login_bhyt(unit, captcha_value):
    if not unit.bhyt_username or not unit.bhyt_password:
        raise ValueError("Đơn vị chưa cấu hình tài khoản/mật khẩu cổng BHYT.")

    if not unit.ma_don_vi:
        raise ValueError("Đơn vị chưa có mã đơn vị để gửi macskcb.")

    payload = {
        "macskcb": unit.ma_don_vi,
        "username": unit.bhyt_username,
        "password": unit.bhyt_password,
        "captcha": (captcha_value or "").strip(),
    }

    resp = requests.post(
        BHYT_BASE_URL + "/Account/login",
        data=payload,
        headers=build_bhyt_headers(
            unit,
            {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
            }
        ),
        timeout=20
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()

    data = resp.json()

    return {
        "response": resp,
        "data": data
    }


def _clean_url(url):
    return (url or "").strip() or None


def validate_sync_config(api_config):
    if not _clean_url(getattr(api_config, "api_danh_muc_url", None)):
        raise ValueError("Danh mục chưa cấu hình API danh mục.")

    if not _clean_url(getattr(api_config, "api_tong_hop_url", None)):
        raise ValueError("Danh mục chưa cấu hình API tổng hợp Excel.")

    if not _clean_url(getattr(api_config, "api_xuat_file_url", None)):
        raise ValueError("Danh mục chưa cấu hình API xuất file.")


def warmup_sync_category(unit, api_danh_muc_url):
    resp = requests.get(
        _clean_url(api_danh_muc_url),
        headers=build_bhyt_headers(unit),
        timeout=60,
        allow_redirects=True
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()
    return resp


def call_sync_search(unit, api_tim_kiem_url, api_tim_kiem_body=None):
    api_tim_kiem_url = _clean_url(api_tim_kiem_url)
    if not api_tim_kiem_url:
        return None

    resp = requests.post(
        api_tim_kiem_url,
        headers=build_bhyt_headers(
            unit,
            {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
            }
        ),
        data=(api_tim_kiem_body or ""),
        timeout=60,
        allow_redirects=True
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()
    return resp


def post_sync_export(unit, api_config):
    validate_sync_config(api_config)

    warmup_sync_category(unit, api_config.api_danh_muc_url)

    if _clean_url(getattr(api_config, "api_tim_kiem_url", None)):
        call_sync_search(
            unit,
            api_config.api_tim_kiem_url,
            getattr(api_config, "api_tim_kiem_body", None)
        )

    resp = requests.post(
        _clean_url(api_config.api_tong_hop_url),
        headers=build_bhyt_headers(
            unit,
            {
                "X-Requested-With": "XMLHttpRequest",
            }
        ),
        timeout=60
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()

    try:
        data = resp.json()
    except Exception:
        raise ValueError(f"API tổng hợp Excel không trả JSON hợp lệ. Nội dung: {resp.text[:500]}")

    return {
        "response": resp,
        "data": data
    }


def get_sync_stream(unit, api_xuat_file_url):
    api_xuat_file_url = _clean_url(api_xuat_file_url)
    if not api_xuat_file_url:
        raise ValueError("Danh mục chưa cấu hình API xuất file.")

    resp = requests.get(
        api_xuat_file_url,
        headers=build_bhyt_headers(unit),
        timeout=120
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()

    if resp.status_code != 200:
        raise ValueError(f"Tải file xuất thất bại. HTTP {resp.status_code}")

    return resp.content


def clear_dataset_records(dataset):
    from models import DanhMucRecord, DanhMucRecordValue

    record_ids = [r.id for r in dataset.records]
    if record_ids:
        DanhMucRecordValue.query.filter(DanhMucRecordValue.record_id.in_(record_ids)).delete(synchronize_session=False)
        DanhMucRecord.query.filter(DanhMucRecord.id.in_(record_ids)).delete(synchronize_session=False)
