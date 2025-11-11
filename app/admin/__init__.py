"""Admin blueprint initialization"""
from flask import Blueprint

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# Import routes to register them with the blueprint
from app.admin import routes