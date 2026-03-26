import base64
from http.cookies import SimpleCookie
import json
import re
from datetime import datetime, timezone

import requests

from models import db
from services.xml_parser_service import parse_xml_content
from services.rule_engine_service import run_validation


LOGIN_URL = "https://bvquangngai.vncare.vn/vnpthis/servlet/login.ValidateUser"
RESTSERVICE_URL = "https://bvquangngai.vncare.vn/vnpthis/RestService"

DEFAULT_SRCWIDTH = "1024"
DEFAULT_REAL = ""
DEFAULT_REAL_HASH = "-1223478800"

LIST_QUERY_CODE = "NTU02D061.03.RG"

DEFAULT_LOCTHEO = "1"
DEFAULT_TUYEN = "-1"
DEFAULT_LOAITHE = "-1"
DEFAULT_LOAIXML = "130"
DEFAULT_DTBNID = "100"
DEFAULT_PHAMVI = "-1"
DEFAULT_MUCHUONG = "-1"
DEFAULT_NHOMBHXH = "-1"
DEFAULT_NHOMDV = "-1"
DEFAULT_KHOA = "-1"
DEFAULT_PHONG = "-1"
DEFAULT_LOAITIEPNHAN = "01,02,03,04,05,06,07,08,09,10"
DEFAULT_DS_BA = ""
DEFAULT_GUI_CONG = "-1"
DEFAULT_GUI_BHXH = "-1"
DEFAULT_TRANGTHAI_KYSO = "-1"

DEFAULT_LOAI_HS = "01,02,03,04,05,06,07,08,09,10"
DEFAULT_MODE = "1"
DEFAULT_DTBN = "100"
DEFAULT_QD3176 = "1"
DEFAULT_MAHOA = "0"

DEFAULT_MA_THE = (
    "AK,AK2,BA,BA3,BA4,BT,BT2,BT4,BT5,CA1,CA2,CA3,CA5,CB,CB2,CB4,CB7,CC,CC1,"
    "CD,CD2,CD4,CH1,CH2,CH3,CH4,CH7,CK,CK1,CK2,CN,CN2,CN3,CN6,CS,CS1,CS2,CS3,CS4,"
    "CT,CT1,CT2,CT3,CT4,CY5,DC,DC2,DD,DD4,DK,DK2,DN,DN1,DN2,DN3,DN4,DN7,DQ,DQ4,DS,"
    "DS1,DS2,DS3,DT,DT2,GB,GB2,GB4,GD,GD2,GD4,GD7,GH,GH4,GK,GK4,GT,GT4,HC,HC1,HC2,"
    "HC3,HC4,HC7,HD,HD2,HD3,HD4,HD7,HG,HG2,HG3,HG4,HG7,HK,HK3,HN,HN2,HN4,HS,HS3,HS4,"
    "HS6,HS7,HT,HT1,HT2,HT3,HT4,HT5,HX,HX1,HX2,HX3,HX4,HX7,KC,KC2,KC3,KC4,KC7,KD,KD2,"
    "KT,KT4,LH,LH2,LH3,LH4,LS,LS3,LS4,LS7,LT,LT4,MS,MS1,MS2,MS3,MS4,MS7,NB,NB4,ND,ND4,"
    "NK,NK2,NM,NM4,NN,NN1,NN2,NN3,NN4,NN7,NO,NO1,NO2,NO3,NO4,NO7,NT,NT4,NTH,NU,NU4,PV,"
    "PV2,PV3,PV4,QD,QD5,QN2,QN5,SV,SV3,SV4,TA2,TA3,TA4,TA7,TB,TB1,TB2,TB3,TB4,TB7,TC,"
    "TC2,TC3,TC7,TD4,TE,TE1,TG,TG2,TG3,TH4,TK1,TK2,TK3,TK4,TK7,TL7,TN,TN1,TN2,TN3,TN4,"
    "TN7,TQ2,TQ3,TQ4,TQ7,TS,TS2,TU4,TV4,TY2,TY3,TY4,TY7,XB,XB1,XB2,XB3,XB4,XB7,XD,XD2,"
    "XK,XK1,XK2,XK3,XK4,XK7,XN,XN1,XN2,XN3,XN4,XN7,XV7,YT,YT1,YT4,bt41,ma"
)

def is_token_valid(don_vi):
    if (
        not don_vi.api_uuid
        or not don_vi.api_token_expire_at
        or not don_vi.api_jsessionid
        or not don_vi.api_sessionid
    ):
        return False

    return don_vi.api_token_expire_at > datetime.utcnow()

def init_gateway_session():
    session = requests.Session()

    response = session.get(
        "https://bvquangngai.vncare.vn/",
        timeout=30,
        allow_redirects=True
    )

    if response.status_code != 200:
        raise Exception(f"Không khởi tạo được session ban đầu. HTTP {response.status_code}")

    sessionid = get_cookie_value(session, "SESSIONID")
    return session, sessionid

def get_cookie_value(session, cookie_name):
    for c in session.cookies:
        if c.name == cookie_name:
            return c.value
    return None

def decode_jwt_payload(token):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}

        payload_part = parts[1]
        padding = "=" * (-len(payload_part) % 4)
        decoded = base64.urlsafe_b64decode(payload_part + padding)
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}


def get_token_expire_datetime(token):
    payload = decode_jwt_payload(token)
    exp = payload.get("exp")
    if not exp:
        return None

    try:
        return datetime.fromtimestamp(exp, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def extract_rest_info_from_html(html_text):
    result = {
        "base_url": None,
        "uuid": None,
        "his_token": None,
        "ssid": None
    }

    patterns = {
        "base_url": r"base_url\s*:\s*'([^']*)'",
        "uuid": r"uuid\s*:\s*'([^']*)'",
        "his_token": r"his_token\s*:\s*'([^']*)'",
        "ssid": r"var\s+ssid\s*=\s*'([^']*)'"
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match:
            result[key] = match.group(1).strip()

    return result


def is_token_valid(don_vi):
    if not don_vi.api_uuid or not don_vi.api_token_expire_at or not don_vi.api_jsessionid:
        return False
    return don_vi.api_token_expire_at > datetime.utcnow()


def convert_yyyy_mm_dd_to_dd_mm_yyyy(value):
    if not value:
        raise Exception("Thiếu giá trị ngày.")

    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        raise Exception(f"Ngày không đúng định dạng yyyy-mm-dd: {value}")


def build_cookie_string_from_response(response, session):
    """
    Trả về cookie dạng:
    JSESSIONID=...; Path=/vnpthis/; HttpOnly;
    """

    # Ưu tiên đọc trực tiếp từ Set-Cookie header
    set_cookie = response.headers.get("Set-Cookie", "")
    if set_cookie:
        cookie = SimpleCookie()
        cookie.load(set_cookie)

        morsel = cookie.get("JSESSIONID")
        if morsel:
            jsessionid = morsel.value
            path = morsel["path"] or "/"
            httponly = "HttpOnly;" if morsel["httponly"] else ""

            cookie_str = f"JSESSIONID={jsessionid}; Path={path};"
            if httponly:
                cookie_str += f" {httponly}"
            return cookie_str.strip()

    # Fallback: đọc từ session.cookies nếu header không có/khó parse
    for c in session.cookies:
        if c.name == "JSESSIONID":
            cookie_str = f"{c.name}={c.value}; Path={c.path or '/'};"
            # requests cookiejar không luôn giữ cờ httponly rõ ràng,
            # nên thử đọc trong rest trước
            if getattr(c, "_rest", None):
                has_httponly = any(str(k).lower() == "httponly" for k in c._rest.keys())
                if has_httponly:
                    cookie_str += " HttpOnly;"
            return cookie_str.strip()

    return None

def l2_login(don_vi):
    if not don_vi.api_username or not don_vi.api_password:
        raise Exception(f"Đơn vị {don_vi.ten_don_vi} chưa cấu hình tài khoản API.")

    payload = {
        "srcwidth": DEFAULT_SRCWIDTH,
        "txtName": don_vi.api_username,
        "txtPass": don_vi.api_password,
        "defaultReal": DEFAULT_REAL,
        "defaultRealHash": DEFAULT_REAL_HASH
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Bước 1: vào trang chủ để lấy SESSIONID ban đầu
    session, initial_sessionid = init_gateway_session()

    # Bước 2: login bằng cùng session
    response = session.post(
        LOGIN_URL,
        data=payload,
        headers=headers,
        timeout=30,
        allow_redirects=True
    )

    html_text = response.text or ""
    rest_info = extract_rest_info_from_html(html_text)

    uuid = rest_info.get("uuid")
    his_token = rest_info.get("his_token")
    base_url = rest_info.get("base_url")

    if not uuid:
        raise Exception("Không lấy được uuid từ HTML login. Có thể sai tài khoản/mật khẩu hoặc response đã thay đổi.")

    expire_at = get_token_expire_datetime(uuid)

    # Sau login, lấy cookie thật từ cookie jar
    jsessionid = get_cookie_value(session, "JSESSIONID")
    sessionid = get_cookie_value(session, "SESSIONID") or initial_sessionid

    if not jsessionid:
        raise Exception("Không lấy được JSESSIONID sau login.")

    if not sessionid:
        raise Exception("Không lấy được SESSIONID ban đầu hoặc sau login.")

    don_vi.api_uuid = uuid
    don_vi.api_his_token = his_token
    don_vi.api_base_url = base_url or RESTSERVICE_URL
    don_vi.api_jsessionid = jsessionid
    don_vi.api_sessionid = sessionid
    don_vi.api_token_expire_at = expire_at
    db.session.commit()

    return uuid

def build_api_cookie_header(don_vi):
    parts = []

    if don_vi.api_jsessionid:
        parts.append(f"JSESSIONID={don_vi.api_jsessionid}")

    if don_vi.api_sessionid:
        parts.append(f"SESSIONID={don_vi.api_sessionid}")

    return "; ".join(parts)

def get_valid_uuid(don_vi, force_refresh=False):
    if not force_refresh and is_token_valid(don_vi):
        return don_vi.api_uuid
    return l2_login(don_vi)


def get_rest_base_url(don_vi):
    if don_vi.api_base_url:
        return don_vi.api_base_url

    l2_login(don_vi)
    return don_vi.api_base_url or RESTSERVICE_URL


def build_list_options_value(don_vi, tu_ngay_ddmmyyyy, den_ngay_ddmmyyyy):
    value_obj = {
        "TU_NGAY": f"{tu_ngay_ddmmyyyy} 00:00:00",
        "DEN_NGAY": f"{den_ngay_ddmmyyyy} 23:59:59",
        "DS_MALK": "",
        "TUYEN": DEFAULT_TUYEN,
        "LOCTHEO": DEFAULT_LOCTHEO,
        "LOAITHE": DEFAULT_LOAITHE,
        "LOAIHSMS": "[{}]",
        "MATHE": DEFAULT_MA_THE,
        "LOAIXML": DEFAULT_LOAIXML,
        "DTBNID": DEFAULT_DTBNID,
        "PHAMVI": DEFAULT_PHAMVI,
        "MUCHUONG": DEFAULT_MUCHUONG,
        "NHOMBHXH": DEFAULT_NHOMBHXH,
        "NHOMDV": DEFAULT_NHOMDV,
        "CSKCB": don_vi.ma_don_vi,
        "KHOA": DEFAULT_KHOA,
        "PHONG": DEFAULT_PHONG,
        "LOAITIEPNHAN": DEFAULT_LOAITIEPNHAN,
        "DS_BA": DEFAULT_DS_BA,
        "GUI_CONG": DEFAULT_GUI_CONG,
        "GUI_BHXH": DEFAULT_GUI_BHXH,
        "TRANGTHAI_KYSO": DEFAULT_TRANGTHAI_KYSO
    }
    return json.dumps(value_obj, ensure_ascii=False, separators=(",", ":"))


def build_list_request_params(uuid_value, don_vi, tu_ngay_ddmmyyyy, den_ngay_ddmmyyyy, page=1, rows=10000):
    post_data_obj = {
        "func": "ajaxExecuteQueryPaging",
        "uuid": uuid_value,
        "params": [LIST_QUERY_CODE],
        "options": [
            {
                "name": "[0]",
                "value": build_list_options_value(don_vi, tu_ngay_ddmmyyyy, den_ngay_ddmmyyyy)
            }
        ]
    }

    return {
        "postData": json.dumps(post_data_obj, ensure_ascii=False, separators=(",", ":")),
        "_search": "false",
        "nd": str(int(datetime.utcnow().timestamp() * 1000)),
        "rows": str(rows),
        "page": str(page),
        "sidx": "",
        "sord": "asc",
        "filters": json.dumps({"groupOp": "AND", "rules": []}, separators=(",", ":"))
    }


def _request_list_api_once(uuid_value, don_vi, tu_ngay_ddmmyyyy, den_ngay_ddmmyyyy, page=1, rows=10000):
    base_url = get_rest_base_url(don_vi)
    params = build_list_request_params(
        uuid_value=uuid_value,
        don_vi=don_vi,
        tu_ngay_ddmmyyyy=tu_ngay_ddmmyyyy,
        den_ngay_ddmmyyyy=den_ngay_ddmmyyyy,
        page=page,
        rows=rows
    )

    headers = {
        "Accept": "*/*",
        "Cookie": build_api_cookie_header(don_vi)
    }

    response = requests.get(
        base_url,
        params=params,
        headers=headers,
        timeout=60
    )

    return response


def l2_get_hoso_list(don_vi, tu_ngay, den_ngay):
    tu_ngay_ddmmyyyy = convert_yyyy_mm_dd_to_dd_mm_yyyy(tu_ngay)
    den_ngay_ddmmyyyy = convert_yyyy_mm_dd_to_dd_mm_yyyy(den_ngay)

    uuid_value = get_valid_uuid(don_vi)

    response = _request_list_api_once(
        uuid_value=uuid_value,
        don_vi=don_vi,
        tu_ngay_ddmmyyyy=tu_ngay_ddmmyyyy,
        den_ngay_ddmmyyyy=den_ngay_ddmmyyyy,
        page=1,
        rows=10000
    )

    if response.status_code in (401, 403):
        uuid_value = get_valid_uuid(don_vi, force_refresh=True)
        response = _request_list_api_once(
            uuid_value=uuid_value,
            don_vi=don_vi,
            tu_ngay_ddmmyyyy=tu_ngay_ddmmyyyy,
            den_ngay_ddmmyyyy=den_ngay_ddmmyyyy,
            page=1,
            rows=10000
        )

    if response.status_code != 200:
        raise Exception(f"Gọi API danh sách hồ sơ thất bại. HTTP {response.status_code}. Body: {response.text[:500]}")

    try:
        data = response.json()
    except Exception:
        raise Exception(f"API danh sách hồ sơ không trả JSON hợp lệ. Body: {response.text[:500]}")

    rows = data.get("rows", [])
    if not isinstance(rows, list):
        rows = []

    return rows



def build_export_xml_params(don_vi, tu_ngay_ddmmyyyy, den_ngay_ddmmyyyy, ds_malk):
    tu_ngay_dt = datetime.strptime(tu_ngay_ddmmyyyy, "%d/%m/%Y")

    qd3176 = DEFAULT_QD3176
    if tu_ngay_dt.month < 2:
        qd3176 = 0

    return {
        "LOCTHEO": DEFAULT_LOCTHEO,
        "TU_NGAY": f"{tu_ngay_ddmmyyyy} 00:00:00",
        "DEN_NGAY": f"{den_ngay_ddmmyyyy} 23:59:59",
        "LoaiHS": DEFAULT_LOAI_HS,
        "Tuyen": DEFAULT_TUYEN,
        "MaThe": DEFAULT_MA_THE,
        "LoaiThe": DEFAULT_LOAITHE,
        "MAHOA": DEFAULT_MAHOA,
        "DS_MALK": ds_malk,
        "HCODE": don_vi.ma_don_vi,
        "MODE": DEFAULT_MODE,
        "DTBN": DEFAULT_DTBN,
        "LOAIXML": DEFAULT_LOAIXML,
        "QD3176": qd3176
    }


def l2_get_xml_content(don_vi, tu_ngay, den_ngay, ds_malk):
    if not ds_malk:
        raise Exception("Danh sách mã liên kết DS_MALK đang rỗng.")

    uuid_value = get_valid_uuid(don_vi)
    base_url = get_rest_base_url(don_vi)

    tu_ngay_ddmmyyyy = convert_yyyy_mm_dd_to_dd_mm_yyyy(tu_ngay)
    den_ngay_ddmmyyyy = convert_yyyy_mm_dd_to_dd_mm_yyyy(den_ngay)

    inner_params = build_export_xml_params(
        don_vi=don_vi,
        tu_ngay_ddmmyyyy=tu_ngay_ddmmyyyy,
        den_ngay_ddmmyyyy=den_ngay_ddmmyyyy,
        ds_malk=ds_malk
    )

    payload = {
        "func": "ajaxCALL_SP_X",
        "params": [
            "XUAT.XML",
            json.dumps(inner_params, ensure_ascii=False, separators=(",", ":"))
        ],
        "uuid": uuid_value
    }

    headers = {
        "Accept": "*/*",
        "Cookie": build_api_cookie_header(don_vi)
    }

    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    response = requests.post(
        base_url,
        headers=headers,
        data=body,
        timeout=120
    )

    if response.status_code in (401, 403):
        uuid_value = get_valid_uuid(don_vi, force_refresh=True)
        payload["uuid"] = uuid_value

        response = requests.post(
            base_url,
            headers=headers,
            json=payload,
            timeout=120
        )

    if response.status_code != 200:
        raise Exception(f"Gọi API xuất XML thất bại. HTTP {response.status_code}. Body: {response.text[:500]}")

    xml_text = response.text or ""
    if not xml_text.strip():
        raise Exception("API xuất XML trả về rỗng.")

    return xml_text


def extract_ds_malk_from_hoso_list(hoso_list):
    values = []

    for item in hoso_list:
        ma_lk = (item.get("MA_LK") or "").strip()
        if ma_lk:
            values.append(ma_lk)

    seen = set()
    result = []
    for x in values:
        if x not in seen:
            seen.add(x)
            result.append(x)

    return ",".join(result)


def validate_from_l2(don_vi, tu_ngay, den_ngay):
    hoso_list = l2_get_hoso_list(don_vi, tu_ngay, den_ngay)
    ds_malk = extract_ds_malk_from_hoso_list(hoso_list)

    if not ds_malk:
        return [], {
            "total_xml_read": 0,
            "total_hoso_read": 0,
            "error_hoso_count": 0
        }

    xml_content = l2_get_xml_content(don_vi, tu_ngay, den_ngay, ds_malk)
    tree = parse_xml_content(xml_content)

    return run_validation(tree)