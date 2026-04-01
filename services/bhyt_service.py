import re
import requests

from urllib.parse import urlsplit, urlunsplit

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


def normalize_sync_export_url(api_sync_url):
    """
    Chuẩn hóa URL export:
    - Nếu truyền vào .../ExportExcelStream -> đổi thành .../ExportExcel
    - Nếu đã là .../ExportExcel -> giữ nguyên
    """
    if not api_sync_url:
        return api_sync_url

    url = api_sync_url.strip().rstrip("/")

    if url.endswith("ExportExcelStream"):
        return url[:-6]  # bỏ chữ "Stream"

    return url


def build_sync_parent_url(api_sync_url):
    export_url = normalize_sync_export_url(api_sync_url)
    parsed = urlsplit(export_url)

    path = (parsed.path or "").rstrip("/")
    segments = [seg for seg in path.split("/") if seg]

    if segments:
        last_seg = segments[-1].lower()
        if last_seg in ["exportexcel", "exportexcelstream"]:
            segments = segments[:-1]

    parent_path = "/" + "/".join(segments) if segments else "/"

    return urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parent_path,
        "",
        ""
    ))


def build_sync_stream_url(api_sync_url):
    export_url = normalize_sync_export_url(api_sync_url)
    return export_url.rstrip("/") + "Stream"


def warmup_sync_parent(unit, parent_url):
    resp = requests.get(
        parent_url,
        headers=build_bhyt_headers(unit),
        timeout=60,
        allow_redirects=True
    )

    return resp

def warmup_sync_ft_timkiem(unit, parent_url, api_ft_timkiem_body):
    url = parent_url.rstrip("/") + "/ft_TimKiem"

    resp = requests.post(
        url,
        headers=build_bhyt_headers(unit, {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}),
        data=api_ft_timkiem_body,
        timeout=60,
        allow_redirects=True
    )

    return resp

def post_sync_export(unit, api_config):
    api_ft_timkiem_body = api_config.api_ft_timkiem_body
    api_sync_url = api_config.api_sync_url
    
    if not api_sync_url:
        raise ValueError("Danh mục chưa cấu hình API đồng bộ.")

    export_url = normalize_sync_export_url(api_sync_url)
    parent_url = build_sync_parent_url(api_sync_url)
    warmup_sync_parent(unit, parent_url)
    warmup_sync_ft_timkiem(unit, parent_url, api_ft_timkiem_body)

    resp = requests.post(
        export_url,
        headers=build_bhyt_headers(
            unit,
            {
                "X-Requested-With": "XMLHttpRequest",
            }
        ),
        timeout=60
    )

    try:
        data = resp.json()
    except Exception:
        raise ValueError(f"API đồng bộ POST không trả JSON hợp lệ. Nội dung: {resp.text[:500]}")

    return {
        "response": resp,
        "data": data
    }


def get_sync_stream(unit, api_sync_url):
    if not api_sync_url:
        raise ValueError("Danh mục chưa cấu hình API đồng bộ.")

    stream_url = build_sync_stream_url(api_sync_url)

    resp = requests.get(
        stream_url,
        headers=build_bhyt_headers(unit),
        timeout=120
    )

    update_unit_cookie_from_response(unit, resp)
    db.session.commit()

    if resp.status_code != 200:
        raise ValueError(f"Tải file stream thất bại. HTTP {resp.status_code}")

    return resp.content


def clear_dataset_records(dataset):
    from models import DanhMucRecord, DanhMucRecordValue

    record_ids = [r.id for r in dataset.records]
    if record_ids:
        DanhMucRecordValue.query.filter(DanhMucRecordValue.record_id.in_(record_ids)).delete(synchronize_session=False)
        DanhMucRecord.query.filter(DanhMucRecord.id.in_(record_ids)).delete(synchronize_session=False)