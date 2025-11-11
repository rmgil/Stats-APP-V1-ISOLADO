"""User model for Flask-Login"""
from flask_login import UserMixin
from datetime import datetime
import json

class User(UserMixin):
    """User class for Flask-Login"""
    
    def __init__(self, user_data=None):
        if user_data:
            # From Supabase auth response
            self.id = user_data.get('id')
            self.email = user_data.get('email')
            self.created_at = user_data.get('created_at')
            self.updated_at = user_data.get('updated_at')
            # User metadata from Supabase
            metadata = user_data.get('user_metadata', {})
            self.username = metadata.get('username', self.email.split('@')[0] if self.email else '')
        else:
            self.id = None
            self.email = None
            self.username = None
            self.created_at = None
            self.updated_at = None
    
    def get_id(self):
        """Return the user ID as a string for Flask-Login"""
        return str(self.id) if self.id else None
    
    def to_dict(self):
        """Convert user to dictionary"""
        return {
            'id': self.id,
            'email': self.email,
            'username': self.username,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @staticmethod
    def from_session_data(session_data):
        """Create User from Supabase session data"""
        if session_data and 'user' in session_data:
            return User(session_data['user'])
        return None