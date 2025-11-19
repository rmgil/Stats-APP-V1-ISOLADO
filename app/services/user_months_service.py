import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from app.services.master_result_builder import _merge_pipeline_results
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
        logger.debug("[USER_MONTHS] Upload entries for %s: %s", user_id, uploads)

        tokens = [u.get("token") for u in uploads if u.get("token")]

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
        logger.debug(
            "[USER_MONTHS] Months available for user %s: %s", user_id, sorted(final_map.keys())
        )

        return final_map


def build_user_month_pipeline_result(user_id: str, month: str) -> Optional[dict]:
    """
    Build a synthetic monthly ``pipeline_result`` by aggregating all job tokens for a
    given user/month pair.

    The aggregation mirrors the original pipeline behavior (including deduplication of
    hands and recalculation of scores) and returns a payload compatible with
    ``pipeline_result_<YYYY-MM>.json``.
    """

    logger.info("[USER_MONTHS] Building aggregated pipeline_result for user=%s month=%s", user_id, month)

    months_service = UserMonthsService()
    result_storage = ResultStorageService()

    user_months = months_service.get_user_months_map(user_id)
    tokens = user_months.get(month, [])

    if not tokens:
        logger.info("[USER_MONTHS] No tokens found for user=%s month=%s", user_id, month)
        return None

    cache_path = Path("results") / "users" / str(user_id) / "months" / month / "pipeline_result.json"
    try:
        if cache_path.exists():
            cached = json.loads(cache_path.read_text())
            logger.info("[USER_MONTHS] Returning cached pipeline_result for user=%s month=%s", user_id, month)
            return cached
    except Exception as exc:  # noqa: BLE001 - cache read failure should not block rebuild
        logger.debug("[USER_MONTHS] Failed to read cached pipeline_result for %s/%s: %s", user_id, month, exc)

    result_entries = []
    for token in tokens:
        try:
            pipeline_result = result_storage.get_pipeline_result(token, month=month)
        except FileNotFoundError:
            logger.warning("[USER_MONTHS] Missing pipeline_result for token=%s month=%s", token, month)
            continue
        except Exception as exc:  # noqa: BLE001 - continue with other tokens
            logger.warning("[USER_MONTHS] Error loading pipeline_result for %s/%s: %s", token, month, exc)
            continue

        if pipeline_result:
            result_entries.append((token, pipeline_result))

    if not result_entries:
        logger.info("[USER_MONTHS] No pipeline results available to merge for %s/%s", user_id, month)
        return None

    merged_result = _merge_pipeline_results(result_entries, month_key=month)

    logger.debug(
        "[USER_MONTHS] Aggregated %s tokens for %s/%s -> total_hands=%s valid_hands=%s discards=%s",
        len(result_entries),
        user_id,
        month,
        merged_result.get("total_hands"),
        merged_result.get("valid_hands"),
        (merged_result.get("aggregated_discards") or {}).get("total"),
    )

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(merged_result, indent=2), encoding="utf-8")
        logger.info(
            "[USER_MONTHS] Cached aggregated pipeline_result for user=%s month=%s at %s",
            user_id,
            month,
            cache_path,
        )
    except Exception as exc:  # noqa: BLE001 - caching is optional
        logger.debug("[USER_MONTHS] Failed to cache pipeline_result for %s/%s: %s", user_id, month, exc)

    return merged_result
