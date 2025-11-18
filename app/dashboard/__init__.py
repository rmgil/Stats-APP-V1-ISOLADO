# Dashboard module initialization
from .api import bp_dashboard, bp_dashboard_debug, bp_dashboard_internal
from .aggregate import build_overview

__all__ = [
    'bp_dashboard',
    'bp_dashboard_debug',
    'bp_dashboard_internal',
    'build_overview',
]
