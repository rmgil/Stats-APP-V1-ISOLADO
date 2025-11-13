"""
Result Storage Service - Read job results from Object Storage or local filesystem
===============================================================================

This service provides a unified interface for reading pipeline results that works in:
- Production (Replit Autoscale): Reads from Object Storage (cloud-based, persistent)
- Development (local): Reads from local filesystem (work/ directory)

Key Features:
- Automatic fallback from cloud to local
- JSON parsing with error handling
- Memory-efficient for large result files
"""
import os
import logging
import json
import shutil
from typing import Optional, Dict, Any, List, Set, Tuple
from pathlib import Path
from .storage import get_storage

logger = logging.getLogger(__name__)

class ResultStorageService:
    """
    Service for reading job results from persistent storage
    
    Usage:
        result_service = ResultStorageService()
        
        # Get pipeline result JSON
        result = result_service.get_pipeline_result(token)
        
        # Get stats JSON
        stats = result_service.get_stats_json(token, "nonko_9max")
    """
    
    def __init__(self):
        self.storage = get_storage()
        self.local_work_dir = Path("work")
    
    def _read_json_from_storage(self, storage_path: str) -> Optional[Dict[str, Any]]:
        """
        Read JSON file from Object Storage
        
        Args:
            storage_path: Path in storage (e.g., "/results/abc123/pipeline_result.json")
        
        Returns:
            Parsed JSON dict or None if not found
        """
        try:
            file_data = self.storage.download_file(storage_path)
            if file_data:
                return json.loads(file_data.decode('utf-8'))
        except Exception as e:
            logger.debug(f"Could not read {storage_path} from storage: {e}")
        
        return None
    
    def _read_json_from_local(self, local_path: Path) -> Optional[Dict[str, Any]]:
        """
        Read JSON file from local filesystem
        
        Args:
            local_path: Path object to local file
        
        Returns:
            Parsed JSON dict or None if not found
        """
        try:
            if local_path.exists():
                return json.loads(local_path.read_text())
        except Exception as e:
            logger.debug(f"Could not read {local_path} from local: {e}")
        
        return None
    
    def get_pipeline_result(self, token: str, month: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get pipeline_result.json for a job
        
        Args:
            token: Job token (12 hex characters)
            month: Optional month in YYYY-MM format. If provided, loads month-specific results.
                   If None, loads aggregate results.
        
        Returns:
            Pipeline result dict or None if not found
        
        Raises:
            FileNotFoundError: If month is specified but monthly data doesn't exist
        """
        if month:
            logger.info(
                "[RESULT STORAGE] Attempting to load monthly pipeline_result_%s.json for %s",
                month,
                token,
            )
            # Prefer new root-level monthly result
            storage_path = f"/results/{token}/pipeline_result_{month}.json"
            result = self._read_json_from_storage(storage_path)

            if result:
                logger.info(
                    "[RESULT STORAGE] Loaded monthly pipeline_result_%s.json for %s from cloud storage",
                    month,
                    token,
                )
                return result

            local_path = self.local_work_dir / token / f"pipeline_result_{month}.json"
            result = self._read_json_from_local(local_path)

            if result:
                logger.info(
                    "[RESULT STORAGE] Loaded monthly pipeline_result_%s.json for %s from local filesystem",
                    month,
                    token,
                )
                return result

            # Fallback to legacy location
            legacy_storage = f"/results/{token}/months/{month}/pipeline_result.json"
            result = self._read_json_from_storage(legacy_storage)

            if result:
                logger.info(
                    "[RESULT STORAGE] Loaded legacy monthly pipeline_result for %s/%s from cloud storage",
                    token,
                    month,
                )
                return result

            legacy_local = self.local_work_dir / token / "months" / month / "pipeline_result.json"
            result = self._read_json_from_local(legacy_local)

            if result:
                logger.info(
                    "[RESULT STORAGE] Loaded legacy monthly pipeline_result for %s/%s from local filesystem",
                    token,
                    month,
                )
                return result

            logger.warning(
                "[RESULT STORAGE] Monthly pipeline_result missing for %s/%s (cloud and local)",
                token,
                month,
            )
            raise FileNotFoundError(f"Pipeline result for month {month} not found")

        # Load aggregate pipeline_result (default behavior)
        logger.info(
            "[RESULT STORAGE] Attempting to load pipeline_result_global.json for %s",
            token,
        )
        storage_path = f"/results/{token}/pipeline_result_global.json"
        result = self._read_json_from_storage(storage_path)

        if result:
            logger.info("[RESULT STORAGE] Loaded pipeline_result_global.json for %s from cloud storage", token)
            return result

        local_path = self.local_work_dir / token / "pipeline_result_global.json"
        result = self._read_json_from_local(local_path)

        if result:
            logger.info("[RESULT STORAGE] Loaded pipeline_result_global.json for %s from local filesystem", token)
            return result

        # Fallback to legacy aggregate file
        legacy_storage = f"/results/{token}/pipeline_result.json"
        result = self._read_json_from_storage(legacy_storage)

        if result:
            logger.info("[RESULT STORAGE] Loaded legacy pipeline_result.json for %s from cloud storage", token)
            return result

        legacy_local = self.local_work_dir / token / "pipeline_result.json"
        result = self._read_json_from_local(legacy_local)

        if result:
            logger.info("[RESULT STORAGE] Loaded legacy pipeline_result.json for %s from local filesystem", token)
            return result

        logger.warning("[RESULT STORAGE] Aggregate pipeline_result missing for %s (cloud and local)", token)
        return None

    def _check_month_file_exists(self, token: str, month: str) -> bool:
        """Check if a monthly pipeline result exists in storage or locally."""

        storage_path = f"/results/{token}/pipeline_result_{month}.json"

        try:
            if self.storage.use_cloud:
                if self.storage.file_exists(storage_path):
                    return True
                if self.storage.file_exists(storage_path + ".gz"):
                    return True
            else:
                local_path = self.local_work_dir / token / f"pipeline_result_{month}.json"
                if local_path.exists():
                    return True
        except Exception as exc:
            logger.debug("[RESULT STORAGE] Error checking month file %s: %s", storage_path, exc)

        # Legacy layout fallback (months/<month>/pipeline_result.json)
        legacy_storage = f"/results/{token}/months/{month}/pipeline_result.json"

        try:
            if self.storage.use_cloud:
                if self.storage.file_exists(legacy_storage):
                    return True
            else:
                legacy_local = self.local_work_dir / token / "months" / month / "pipeline_result.json"
                if legacy_local.exists():
                    return True
        except Exception as exc:
            logger.debug("[RESULT STORAGE] Error checking legacy month file %s: %s", legacy_storage, exc)

        return False

    def list_available_months(self, token: str) -> List[Dict[str, Any]]:
        """Return metadata about months that have stored pipeline results."""

        months_info: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        manifest = None
        try:
            manifest = self.get_months_manifest(token)
        except Exception as exc:
            logger.debug("[RESULT STORAGE] Could not load months manifest for %s: %s", token, exc)

        candidate_months: List[str] = []
        if manifest and isinstance(manifest, dict):
            candidate_months = [
                entry.get("month")
                for entry in manifest.get("months", [])
                if isinstance(entry, dict) and entry.get("month")
            ]

        # Fallback to scanning local files when manifest unavailable (dev/tests)
        if not candidate_months:
            local_dir = self.local_work_dir / token
            if local_dir.exists():
                for path in local_dir.glob("pipeline_result_*.json"):
                    name = path.stem.replace("pipeline_result_", "")
                    if name and name != "global":
                        candidate_months.append(name)

        def _sort_key(value: str) -> Tuple[str, str]:
            if value == 'unknown':
                return ('9999-99', value)
            return (value, '')

        candidate_months = sorted(dict.fromkeys(candidate_months), key=_sort_key)

        for month in candidate_months:
            if not month or month in seen:
                continue

            if not self._check_month_file_exists(token, month):
                logger.debug(
                    "[RESULT STORAGE] Skipping month %s for %s (no pipeline_result file)",
                    month,
                    token,
                )
                continue

            total_hands = None
            valid_hands = None
            has_data = False

            try:
                month_result = self.get_pipeline_result(token, month=month)
                if isinstance(month_result, dict):
                    total_hands = month_result.get("total_hands")
                    valid_hands = month_result.get("valid_hands")
                    combined = month_result.get("combined")
                    if isinstance(combined, dict):
                        for group in combined.values():
                            if isinstance(group, dict) and group.get("hand_count", 0) > 0:
                                has_data = True
                                break
            except FileNotFoundError:
                # Should not happen after existence check, but skip defensively
                logger.debug(
                    "[RESULT STORAGE] Month %s for %s disappeared during metadata load",
                    month,
                    token,
                )
                continue
            except Exception as exc:
                logger.debug(
                    "[RESULT STORAGE] Failed to load metadata for %s/%s: %s",
                    token,
                    month,
                    exc,
                )

            if has_data or (total_hands or valid_hands):
                months_info.append(
                    {
                        "month": month,
                        "total_hands": total_hands,
                        "valid_hands": valid_hands,
                        "has_data": has_data,
                    }
                )
                seen.add(month)

        return months_info
    
    def get_multi_site_manifest(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get multi_site_manifest.json for a job
        
        Args:
            token: Job token
        
        Returns:
            Manifest dict or None if not found
        """
        # Try cloud storage first
        storage_path = f"/results/{token}/multi_site_manifest.json"
        manifest = self._read_json_from_storage(storage_path)
        
        if manifest:
            return manifest
        
        # Fallback to local
        local_path = self.local_work_dir / token / "multi_site_manifest.json"
        return self._read_json_from_local(local_path)
    
    def get_group_manifest(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get group_manifest.json for a job (legacy single-site)
        
        Args:
            token: Job token
        
        Returns:
            Manifest dict or None if not found
        """
        # Try cloud storage first
        storage_path = f"/results/{token}/group_manifest.json"
        manifest = self._read_json_from_storage(storage_path)
        
        if manifest:
            return manifest
        
        # Fallback to local
        local_path = self.local_work_dir / token / "group_manifest.json"
        return self._read_json_from_local(local_path)
    
    def get_months_manifest(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get months_manifest.json for a job
        
        Args:
            token: Job token
        
        Returns:
            Months manifest dict or None if not found
        """
        # Try cloud storage first
        storage_path = f"/results/{token}/months_manifest.json"
        manifest = self._read_json_from_storage(storage_path)
        
        if manifest:
            return manifest
        
        # Fallback to local
        local_path = self.local_work_dir / token / "months_manifest.json"
        return self._read_json_from_local(local_path)
    
    def get_stats_json(self, token: str, group_key: str, is_multi_site: bool = True) -> Optional[Dict[str, Any]]:
        """Get statistics JSON for a group."""

        if is_multi_site:
            storage_path = f"/results/{token}/combined/stats/{group_key}_stats.json"
            stats = self._read_json_from_storage(storage_path)
            if stats:
                return stats

            local_path = self.local_work_dir / token / "combined" / "stats" / f"{group_key}_stats.json"
            return self._read_json_from_local(local_path)

        # Single-site uploads store stats directly inside pipeline_result.json
        result = self.get_pipeline_result(token)
        if result and isinstance(result.get('stats'), dict):
            return result['stats'].get(group_key)

        return None

    def delete_processing_results(self, token: str) -> Dict[str, Any]:
        """Delete stored results for a processing token from storage and local cache."""

        stats = {
            'storage_deleted': 0,
            'local_deleted': False
        }

        storage_prefix = f"/results/{token}"

        try:
            if self.storage.use_cloud:
                stats['storage_deleted'] = self.storage.delete_prefix(storage_prefix)
        except Exception as exc:
            logger.warning(f"Failed to delete storage prefix {storage_prefix}: {exc}")

        local_dir = self.local_work_dir / token
        if local_dir.exists():
            try:
                shutil.rmtree(local_dir)
                stats['local_deleted'] = True
            except Exception as exc:
                logger.warning(f"Failed to remove local directory {local_dir}: {exc}")

        return stats
    
    def job_exists(self, token: str) -> bool:
        """
        Check if job results exist in storage
        
        Args:
            token: Job token
        
        Returns:
            True if pipeline_result.json exists (cloud or local)
        """
        return self.get_pipeline_result(token) is not None


# Singleton instance
_result_storage_service = None

def get_result_storage() -> ResultStorageService:
    """Get singleton ResultStorageService instance"""
    global _result_storage_service
    if _result_storage_service is None:
        _result_storage_service = ResultStorageService()
    return _result_storage_service
