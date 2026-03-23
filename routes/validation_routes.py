import os

from flask import Blueprint, current_app, render_template, request
from werkzeug.utils import secure_filename

from services.xml_parser_service import parse_xml_file
from services.rule_engine_service import run_validation

validation_bp = Blueprint("validation_bp", __name__)


@validation_bp.route("/", methods=["GET", "POST"])
def index():
    hoso_results = []
    error = None

    if request.method == "POST":
        file = request.files.get("xml_file")

        if not file or file.filename == "":
            error = "Vui lòng chọn file XML."
            return render_template("index.html", hoso_results=hoso_results, error=error)

        filename = secure_filename(file.filename)
        upload_folder = current_app.config["UPLOAD_FOLDER"]
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)

        try:
            tree = parse_xml_file(file_path)
            hoso_results = run_validation(tree)
        except Exception as e:
            error = f"Lỗi xử lý XML: {str(e)}"

    return render_template("index.html", hoso_results=hoso_results, error=error)