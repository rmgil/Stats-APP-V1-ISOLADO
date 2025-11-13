#!/usr/bin/env python3
"""
WSGI entry point for deployment
This file ensures proper initialization for gunicorn deployment
"""

import os
import sys
from pathlib import Path

# Add the current directory to the Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import the Flask application
from main import app

# Ensure application is initialized
if __name__ != "__main__":
    # This runs when imported by gunicorn
    pass

# Export the application for gunicorn
application = app