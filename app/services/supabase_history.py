"""
Service for storing processing history in Supabase
"""
import os
import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from supabase import create_client, Client

logger = logging.getLogger(__name__)

class SupabaseHistoryService:
    """
    Service for storing and retrieving processing history from Supabase
    
    Usage:
        history_service = SupabaseHistoryService()
        
        # Save processing result
        history_service.save_processing(token="abc123", user_id="user@example.com", 
                                       filename="poker_hands.zip", pipeline_result={...})
        
        # Get user history
        history = history_service.get_user_history(user_id="user@example.com", limit=10)
    """
    
    def __init__(self, use_service_role: bool = True):
        """
        Initialize Supabase client
        
        Args:
            use_service_role: If True, use SERVICE_ROLE_KEY to bypass RLS policies (default)
        """
        supabase_url = os.getenv('SUPABASE_URL')
        
        if use_service_role:
            supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
        else:
            supabase_key = os.getenv('SUPABASE_KEY')
        
        self.client: Optional[Client] = None
        self.enabled = False
        
        if not supabase_url or not supabase_key:
            logger.warning("Supabase credentials not configured. History will not be saved.")
        else:
            try:
                self.client = create_client(supabase_url, supabase_key)
                self.enabled = True
                logger.info("Supabase history service initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Supabase client: {e}")
    
    def save_processing(
        self, 
        token: str,
        filename: str,
        pipeline_result: Dict[str, Any],
        user_id: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        file_hash: Optional[str] = None,
        storage_path: Optional[str] = None
    ) -> bool:
        """
        Save processing result to Supabase
        
        Args:
            token: Unique processing token
            filename: Name of uploaded file
            pipeline_result: Complete pipeline result dictionary
            user_id: User identifier (email or Flask-Login user ID)
            file_size_bytes: Size of uploaded file in bytes
            file_hash: SHA256 hash of file for deduplication
            storage_path: Path to file in Supabase Storage
            
        Returns:
            True if saved successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Supabase not enabled, skipping history save")
            return False
        
        try:
            # Extract summary data from pipeline result
            status = pipeline_result.get('status', 'completed')
            
            # Extract statistics
            total_hands = 0
            total_sites = 0
            sites_list = []
            pko_count = 0
            mystery_count = 0
            nonko_count = 0
            overall_score = None
            
            # Handle multi-site results
            if pipeline_result.get('multi_site'):
                sites_data = pipeline_result.get('sites', {})
                total_sites = len(sites_data)
                sites_list = list(sites_data.keys())
                
                # Aggregate hands from all sites
                for site_name, site_data in sites_data.items():
                    for table_format, table_data in site_data.items():
                        if isinstance(table_data, dict):
                            # Try summary first (new format), fallback to direct key (old format)
                            if 'summary' in table_data and 'total_hands' in table_data['summary']:
                                total_hands += table_data['summary'].get('total_hands', 0)
                            elif 'total_hands' in table_data:
                                total_hands += table_data.get('total_hands', 0)
                
                # Get classification counts
                classification = pipeline_result.get('classification', {})
                pko_count = classification.get('pko', 0)
                mystery_count = classification.get('mystery', 0)
                nonko_count = classification.get('nonko', 0)
                
                # Get overall score
                overall_score = pipeline_result.get('score_overall')
            
            # Create summary without full hand data (too large for database)
            result_summary = {
                'status': pipeline_result.get('status'),
                'multi_site': pipeline_result.get('multi_site'),
                'classification': pipeline_result.get('classification'),
                'score_overall': pipeline_result.get('score_overall'),
                'sites': {}
            }
            
            # Add site summaries without detailed hand data
            if pipeline_result.get('sites'):
                for site_name, site_data in pipeline_result['sites'].items():
                    result_summary['sites'][site_name] = {}
                    for table_format, format_data in site_data.items():
                        if isinstance(format_data, dict):
                            # Extract from summary or top-level
                            hands = 0
                            if 'summary' in format_data:
                                hands = format_data['summary'].get('total_hands', 0)
                            else:
                                hands = format_data.get('total_hands', 0)
                                
                            result_summary['sites'][site_name][table_format] = {
                                'total_hands': hands,
                                'stats_count': len(format_data.get('preflop_stats', {}))
                            }
            
            # Extract months_summary if multi-month processing
            months_summary = pipeline_result.get('months_manifest')
            
            # Insert into processing_history
            history_data = {
                'token': token,
                'user_id': user_id,
                'filename': filename,
                'file_size_bytes': file_size_bytes,
                'file_hash': file_hash,
                'storage_path': storage_path,
                'status': status,
                'completed_at': datetime.now().isoformat() if status == 'completed' else None,
                'total_hands': total_hands,
                'total_sites': total_sites,
                'sites_processed': sites_list,
                'pko_count': pko_count,
                'mystery_count': mystery_count,
                'nonko_count': nonko_count,
                'overall_score': overall_score,
                'months_summary': months_summary,  # NEW: Monthly manifest
                'full_result': result_summary  # Store only summary, not full data
            }
            
            response = self.client.table('processing_history').insert(history_data).execute()
            
            if not response.data:
                logger.error("Failed to insert processing history")
                return False
            
            processing_id = response.data[0]['id']
            logger.info(f"Saved processing history: token={token}, id={processing_id}")
            
            # Insert detailed stats (handles both monthly and aggregate data)
            stats_inserted = self._save_detailed_stats(
                processing_id=processing_id,
                token=token,
                pipeline_result=pipeline_result
            )
            
            logger.info(f"Saved {stats_inserted} detailed stats for token={token}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving processing to Supabase: {e}", exc_info=True)
            return False
    
    def _save_detailed_stats(
        self,
        processing_id: int,
        token: str,
        pipeline_result: Dict[str, Any]
    ) -> int:
        """
        Save detailed poker statistics to poker_stats_detail table.
        Handles both monthly and aggregate data.
        
        For multi-month uploads:
        - Saves stats from each month with month='YYYY-MM'
        - Also saves aggregate stats with month=NULL
        
        Returns:
            Number of stats inserted
        """
        if not self.enabled:
            return 0
        
        stats_rows = []
        
        try:
            # Check if this is multi-month processing
            is_multi_month = pipeline_result.get('multi_month', False)
            
            if is_multi_month:
                # Process monthly data
                months_data = pipeline_result.get('months', {})
                
                for month, month_result in months_data.items():
                    # Extract stats for this month
                    sites_data = month_result.get('sites', {})
                    
                    for site_name, site_data in sites_data.items():
                        for table_format, table_data in site_data.items():
                            if not isinstance(table_data, dict):
                                continue
                            
                            stats = table_data.get('stats', {})
                            
                            for stat_name, stat_data in stats.items():
                                if not isinstance(stat_data, dict):
                                    continue
                                
                                stats_rows.append({
                                    'processing_id': processing_id,
                                    'token': token,
                                    'month': month,  # Set month for monthly stats
                                    'site': site_name,
                                    'table_format': table_format,
                                    'stat_name': stat_name,
                                    'opportunities': stat_data.get('opportunities', 0),
                                    'attempts': stat_data.get('attempts', 0),
                                    'percentage': stat_data.get('percentage')
                                })
            
            # Always save aggregate/combined stats (month=NULL)
            # For multi-month: Extract from 'combined' key (aggregated across all months)
            # For single-month/multi-site: Extract from 'sites' key
            if is_multi_month:
                # Multi-month: Use 'combined' key for aggregate stats
                combined_data = pipeline_result.get('combined', {})
                
                for group_key, group_data in combined_data.items():
                    if not isinstance(group_data, dict):
                        continue
                    
                    stats = group_data.get('stats', {})
                    
                    # Extract table_format from group_key (e.g., 'nonko_6max_pref' -> '6max')
                    if '6max' in group_key:
                        table_format = '6max'
                    elif '9max' in group_key:
                        table_format = '9max'
                    elif 'pko' in group_key or 'mystery' in group_key:
                        table_format = 'PKO'
                    else:
                        table_format = group_key  # Fallback to full group key
                    
                    for stat_name, stat_data in stats.items():
                        if not isinstance(stat_data, dict):
                            continue
                        
                        stats_rows.append({
                            'processing_id': processing_id,
                            'token': token,
                            'month': None,  # NULL = aggregate data (all months)
                            'site': None,  # NULL = combined across all sites
                            'table_format': table_format,
                            'stat_name': stat_name,
                            'opportunities': stat_data.get('opportunities', 0),
                            'attempts': stat_data.get('attempts', 0),
                            'percentage': stat_data.get('percentage')
                        })
            
            elif pipeline_result.get('multi_site'):
                # Single-month multi-site: Use 'sites' key for aggregate stats
                sites_data = pipeline_result.get('sites', {})
                
                for site_name, site_data in sites_data.items():
                    for table_format, table_data in site_data.items():
                        if not isinstance(table_data, dict):
                            continue
                        
                        stats = table_data.get('stats', {})
                        
                        for stat_name, stat_data in stats.items():
                            if not isinstance(stat_data, dict):
                                continue
                            
                            stats_rows.append({
                                'processing_id': processing_id,
                                'token': token,
                                'month': None,  # NULL = aggregate data (single month)
                                'site': site_name,
                                'table_format': table_format,
                                'stat_name': stat_name,
                                'opportunities': stat_data.get('opportunities', 0),
                                'attempts': stat_data.get('attempts', 0),
                                'percentage': stat_data.get('percentage')
                            })
            
            # Batch insert stats
            if stats_rows:
                self.client.table('poker_stats_detail').insert(stats_rows).execute()
            
            return len(stats_rows)
            
        except Exception as e:
            logger.error(f"Error saving detailed stats: {e}", exc_info=True)
            return 0
    
    def get_user_history(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get processing history for a specific user
        
        Args:
            user_id: User identifier
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of processing history records
        """
        if not self.enabled:
            return []
        
        try:
            response = self.client.table('processing_history')\
                .select('*')\
                .eq('user_id', user_id)\
                .order('created_at', desc=True)\
                .limit(limit)\
                .offset(offset)\
                .execute()
            
            return response.data or []
            
        except Exception as e:
            logger.error(f"Error fetching user history: {e}", exc_info=True)
            return []
    
    def get_processing_details(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get full processing details including stats
        
        Args:
            token: Processing token
            
        Returns:
            Processing details with stats, or None if not found
        """
        if not self.enabled:
            return None
        
        try:
            # Get processing record
            response = self.client.table('processing_history')\
                .select('*')\
                .eq('token', token)\
                .single()\
                .execute()
            
            if not response.data:
                return None
            
            processing = response.data
            
            # Get detailed stats
            stats_response = self.client.table('poker_stats_detail')\
                .select('*')\
                .eq('token', token)\
                .execute()
            
            processing['detailed_stats'] = stats_response.data or []
            
            return processing
            
        except Exception as e:
            logger.error(f"Error fetching processing details: {e}", exc_info=True)
            return None
    
    def get_all_history(
        self,
        limit: int = 100,
        offset: int = 0,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all processing history (for admin view)
        
        Args:
            limit: Maximum number of records
            offset: Number of records to skip
            status: Filter by status (optional)
            
        Returns:
            List of processing history records
        """
        if not self.enabled:
            return []
        
        try:
            query = self.client.table('processing_history')\
                .select('*')\
                .order('created_at', desc=True)\
                .limit(limit)\
                .offset(offset)
            
            if status:
                query = query.eq('status', status)
            
            response = query.execute()
            return response.data or []
            
        except Exception as e:
            logger.error(f"Error fetching all history: {e}", exc_info=True)
            return []
    
    def update_processing_status(
        self,
        token: str,
        status: str,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update processing status
        
        Args:
            token: Processing token
            status: New status ('processing', 'completed', 'failed', 'cancelled')
            error_message: Error message if status is 'failed'
            
        Returns:
            True if updated successfully
        """
        if not self.enabled:
            return False
        
        try:
            update_data = {
                'status': status,
                'completed_at': datetime.now().isoformat() if status in ['completed', 'failed'] else None
            }
            
            if error_message and status == 'failed':
                # Store error in full_result
                current = self.client.table('processing_history')\
                    .select('full_result')\
                    .eq('token', token)\
                    .single()\
                    .execute()
                
                if current.data:
                    full_result = current.data.get('full_result', {})
                    full_result['error'] = error_message
                    update_data['full_result'] = full_result
            
            self.client.table('processing_history')\
                .update(update_data)\
                .eq('token', token)\
                .execute()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating processing status: {e}", exc_info=True)
            return False
    
    def find_by_file_hash(self, file_hash: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Find a previous processing by file hash (deduplication)
        
        Args:
            file_hash: SHA256 hash of the file
            user_id: Optional user_id to limit search to specific user
            
        Returns:
            Processing record if found, None otherwise
        """
        if not self.enabled or not file_hash:
            return None
        
        try:
            query = self.client.table('processing_history')\
                .select('*')\
                .eq('file_hash', file_hash)\
                .eq('status', 'completed')\
                .order('created_at', desc=True)
            
            # Optionally filter by user
            if user_id:
                query = query.eq('user_id', user_id)
            
            response = query.limit(1).execute()
            
            if response.data and len(response.data) > 0:
                logger.info(f"Found duplicate file with hash: {file_hash[:8]}...")
                return response.data[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding by file hash: {e}", exc_info=True)
            return None
