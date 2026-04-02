import os

from flask import Flask
from flask_migrate import Migrate

from config import Config
from models import db
from routes.validation_routes import validation_bp
from routes.field_routes import field_bp
from routes.xml_routes import xml_bp
from routes.rule_routes import rule_bp
from routes.rule_set_routes import rule_set_bp
from routes.system_routes import system_bp, unit_bp
from routes.category_routes import category_bp
from utils.db_seed import seed_data
from routes.rule_group_routes import rule_group_bp

migrate = Migrate()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    db.init_app(app)
    migrate.init_app(app, db)

    app.register_blueprint(validation_bp)
    app.register_blueprint(xml_bp)
    app.register_blueprint(field_bp)
    app.register_blueprint(rule_set_bp)
    app.register_blueprint(rule_bp)
    app.register_blueprint(system_bp)
    app.register_blueprint(unit_bp)
    app.register_blueprint(category_bp)
    app.register_blueprint(rule_group_bp)
    
    with app.app_context():
        db.create_all()
        seed_data()

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)