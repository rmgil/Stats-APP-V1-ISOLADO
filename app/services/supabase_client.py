"""Supabase client service"""
import os
from supabase import create_client, Client
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class SupabaseService:
    def __init__(self):
        # Get Supabase URL and Anon Key from environment
        self.url = os.getenv('SUPABASE_URL', '')
        self.key = os.getenv('SUPABASE_ANON_KEY', '')
        self.service_role_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
        self.client: Optional[Client] = None
        self.admin_client: Optional[Client] = None
        
        if self.url and self.key:
            self.client = create_client(self.url, self.key)
        
        # Create admin client with service role key if available
        if self.url and self.service_role_key:
            try:
                self.admin_client = create_client(self.url, self.service_role_key)
                logger.info("Admin client initialized with service role key")
            except Exception as e:
                logger.error(f"Failed to create admin client: {e}")
                self.admin_client = None
        else:
            logger.info(f"Admin client not initialized - URL: {bool(self.url)}, Service Key: {bool(self.service_role_key)}")
    
    def get_client(self) -> Optional[Client]:
        """Get Supabase client instance"""
        return self.client
    
    def sign_up(self, email: str, password: str, metadata: dict = None):
        """Register a new user"""
        if not self.client:
            return None, "Supabase client not configured"
        
        try:
            response = self.client.auth.sign_up({
                "email": email,
                "password": password,
                "options": {"data": metadata or {}}
            })
            return response, None
        except Exception as e:
            return None, str(e)
    
    def sign_in(self, email: str, password: str):
        """Sign in a user"""
        if not self.client:
            return None, "Supabase client not configured"
        
        try:
            response = self.client.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            return response, None
        except Exception as e:
            return None, str(e)
    
    def sign_out(self):
        """Sign out the current user"""
        if not self.client:
            return False, "Supabase client not configured"
        
        try:
            self.client.auth.sign_out()
            return True, None
        except Exception as e:
            return False, str(e)
    
    def get_user(self):
        """Get current authenticated user"""
        if not self.client:
            return None
        
        try:
            user = self.client.auth.get_user()
            return user
        except:
            return None
    
    def get_session(self):
        """Get current session"""
        if not self.client:
            return None
        
        try:
            session = self.client.auth.get_session()
            return session
        except:
            return None
    
    def admin_list_users(self):
        """List all users (requires service role key)"""
        if not self.admin_client:
            logger.warning("Admin client not initialized - service role key missing")
            return []
        
        try:
            # Use admin client to list users
            response = self.admin_client.auth.admin.list_users()
            if response:
                logger.info(f"Listed {len(response)} users")
                return response
            return []
        except Exception as e:
            logger.error(f"Error listing users: {e}")
            return []
    
    def admin_delete_user(self, user_id: str):
        """Delete a user by ID (requires service role key)"""
        if not self.admin_client:
            logger.warning("Admin client not initialized - service role key missing")
            return False
        
        try:
            # Use admin client to delete user
            self.admin_client.auth.admin.delete_user(user_id)
            logger.info(f"Successfully deleted user: {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting user {user_id}: {e}")
            return False
    
    def admin_get_user_by_email(self, email: str):
        """Get user by email (requires service role key)"""
        if not self.admin_client:
            logger.warning("Admin client not initialized - service role key missing")
            return None
        
        try:
            # List all users and find by email
            users = self.admin_client.auth.admin.list_users()
            if users:
                for user in users:
                    if user.email and user.email.lower() == email.lower():
                        return user
            return None
        except Exception as e:
            logger.error(f"Error getting user by email {email}: {e}")
            return None

# Global instance
supabase_service = SupabaseService()