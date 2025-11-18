import logging
from collections import defaultdict
from typing import Dict, List

from app.services.result_storage import ResultStorageService
from app.services.upload_service import UploadService

logger = logging.getLogger(__name__)


class UserMonthsService:
    def __init__(
        self,
        upload_service: UploadService | None = None,
        result_storage_service: ResultStorageService | None = None,
    ) -> None:
        self.upload_service = upload_service or UploadService()
        self.result_storage = result_storage_service or ResultStorageService()

    def get_user_months_map(self, user_id: str) -> Dict[str, List[str]]:
        """
        Build a mapping of months to job tokens for a user.

        Returns a dict like {"2025-08": ["jobtoken1"], "2025-09": ["jobtoken2"]}
        based on uploads stored in the primary database and months_manifest.json
        entries stored under /results/<job_id>/.
        """

        logger.debug("[USER_MONTHS] Building months map for user %s", user_id)

        uploads = self.upload_service.list_all_uploads(user_id)
        tokens = [u.get("client_upload_token") for u in uploads if u.get("client_upload_token")]

        logger.debug("[USER_MONTHS] Found tokens for user %s: %s", user_id, tokens)

        months_map: Dict[str, List[str]] = defaultdict(list)

        for token in tokens:
            try:
                manifest = self.result_storage.get_months_manifest(token)
            except Exception as exc:  # noqa: BLE001 - bubble up debug info without stopping the loop
                logger.warning("[USER_MONTHS] Failed to load months_manifest for %s: %s", token, exc)
                continue

            if not manifest or not isinstance(manifest, dict):
                logger.debug("[USER_MONTHS] No months_manifest found for token %s", token)
                continue

            months = manifest.get("months", [])
            if not isinstance(months, list):
                logger.debug("[USER_MONTHS] Malformed months_manifest for token %s", token)
                continue

            for month_entry in months:
                if not isinstance(month_entry, dict):
                    continue

                month = month_entry.get("month")
                if not month:
                    continue

                if token not in months_map[month]:
                    months_map[month].append(token)

        final_map = dict(months_map)
        logger.debug("[USER_MONTHS] Final month-to-tokens map for user %s: %s", user_id, final_map)

        return final_map
