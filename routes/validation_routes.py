import os

from flask import Blueprint, current_app, render_template, request
from werkzeug.utils import secure_filename

from services.xml_parser_service import parse_xml_file
from services.rule_engine_service import run_validation

from models import HeThong, DonVi
from services.l2_api_service import validate_from_l2

validation_bp = Blueprint("validation_bp", __name__)


@validation_bp.route("/", methods=["GET", "POST"])
def index():
    hoso_results = []
    stats = {
        "total_xml_read": 0,
        "total_hoso_read": 0,
        "error_hoso_count": 0
    }
    error = None

    systems = HeThong.query.all()
    units = DonVi.query.all()

    if request.method == "POST":
        mode = request.form.get("mode")

        try:
            # ====== MODE FILE ======
            if mode == "file":
                file = request.files.get("xml_file")

                if not file or file.filename == "":
                    error = "Vui lòng chọn file XML."
                else:
                    filename = secure_filename(file.filename)
                    upload_folder = current_app.config["UPLOAD_FOLDER"]
                    os.makedirs(upload_folder, exist_ok=True)
                    file_path = os.path.join(upload_folder, filename)
                    file.save(file_path)

                    tree = parse_xml_file(file_path)
                    hoso_results = run_validation(tree)

            # ====== MODE L2 ======
            elif mode == "l2":
                don_vi_id = request.form.get("don_vi_id")
                tu_ngay = request.form.get("tu_ngay")
                den_ngay = request.form.get("den_ngay")

                don_vi = DonVi.query.get(don_vi_id)

                if not don_vi:
                    raise Exception("Không tìm thấy đơn vị")

                try:
                    hoso_results, stats = validate_from_l2(don_vi, tu_ngay, den_ngay)
                except Exception as e:
                    error = f"Lỗi: {str(e)}"

        except Exception as e:
            error = f"Lỗi: {str(e)}"

    return render_template(
        "index.html",
        hoso_results=hoso_results,
        stats=stats,
        error=error,
        systems=systems,
        units=units
    )