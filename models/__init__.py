from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

from models.xml_model import DanhMucXml
from models.field_model import DanhMucTruongDuLieu
from models.condition_model import DanhMucDieuKien
from models.rule_model import BoRule, Rule, RuleDetail
from models.system_model import HeThong, DonVi