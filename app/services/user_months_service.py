import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from app.services.master_result_builder import _merge_pipeline_results
from app.services.result_storage import ResultStorageService
from app.services.upload_service import UploadService

logger = logging.getLogger(__name__)

LATEST_UPLOAD_KEY = "latest-upload"


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
            # Prefer the canonical global pipeline to discover month coverage so that
            # the mapping reflects the exact set of valid hands used by the pipeline.
            try:
                global_result = self.result_storage.get_pipeline_result(token)
            except Exception as exc:  # noqa: BLE001 - skip tokens without usable results
                logger.warning("[USER_MONTHS] Skipping token %s (global result unavailable: %s)", token, exc)
                continue

            hands_per_month = global_result.get("hands_per_month") or {}
            if not isinstance(hands_per_month, dict):
                hands_per_month = {}

            discovered_months: set[str] = set()
            for month, count in hands_per_month.items():
                if not self._is_valid_month(month):
                    continue
                if int(count or 0) <= 0:
                    continue
                discovered_months.add(month)

            # Fallback to months_manifest when hands_per_month is missing
            if not discovered_months:
                try:
                    manifest = self.result_storage.get_months_manifest(token)
                except Exception as exc:  # noqa: BLE001 - bubble up debug info without stopping the loop
                    logger.warning("[USER_MONTHS] Failed to load months_manifest for %s: %s", token, exc)
                    manifest = None

                months = manifest.get("months", []) if isinstance(manifest, dict) else []
                for month_entry in months:
                    if not isinstance(month_entry, dict):
                        continue

                    month = month_entry.get("month")
                    if not self._is_valid_month(month):
                        continue
                    discovered_months.add(month)

            for month in sorted(discovered_months):
                if not self._month_has_payload(token, month):
                    logger.debug(
                        "[USER_MONTHS] Skipping %s for %s (missing monthly payload)", month, token
                    )
                    continue

                if token not in months_map[month]:
                    months_map[month].append(token)

        final_map = dict(months_map)
        logger.debug("[USER_MONTHS] Final month-to-tokens map for user %s: %s", user_id, final_map)
        logger.debug(
            "[USER_MONTHS] Months available for user %s: %s", user_id, sorted(final_map.keys())
        )

        return final_map

    def list_user_months_with_hands(self, user_id: str) -> list[dict]:
        """Return months with a friendly hands count for dropdowns and selectors."""

        months_map = self.get_user_months_map(user_id)
        months_with_counts: list[dict] = []

        # Special entry: latest upload without month separation
        latest_upload = self.upload_service.get_master_or_latest_upload_for_user(user_id)
        if latest_upload and latest_upload.get("token"):
            try:
                latest_result = self.result_storage.get_pipeline_result(latest_upload["token"])
                if latest_result:
                    hands = int(latest_result.get("valid_hands") or latest_result.get("total_hands") or 0)
                    months_with_counts.append({
                        "month": LATEST_UPLOAD_KEY,
                        "hands": hands,
                    })
            except Exception as exc:  # noqa: BLE001 - optional helper
                logger.debug(
                    "[USER_MONTHS] Failed to load latest upload pipeline_result for %s: %s", user_id, exc
                )

        for month in sorted(months_map.keys(), reverse=True):
            payload: dict | None = None
            user_token = f"user-{user_id}"
            try:
                payload = self.result_storage.get_pipeline_result(user_token, month=month)
                logger.debug(
                    "[USER_MONTHS] Loaded cached month payload for %s/%s from %s",
                    user_id,
                    month,
                    user_token,
                )
            except FileNotFoundError:
                logger.debug(
                    "[USER_MONTHS] No cached month payload for %s/%s; falling back to merge",
                    user_id,
                    month,
                )
            except Exception as exc:  # noqa: BLE001 - continue with remaining months
                logger.debug(
                    "[USER_MONTHS] Failed to load cached payload for %s/%s: %s", user_id, month, exc
                )

            if payload is None:
                try:
                    payload = build_user_month_pipeline_result(user_id, month)
                except Exception as exc:  # noqa: BLE001 - continue with remaining months
                    logger.debug(
                        "[USER_MONTHS] Failed to build monthly payload for %s/%s: %s", user_id, month, exc
                    )
                    continue

            if not payload:
                continue

            hands_count = int(payload.get("valid_hands") or payload.get("total_hands") or 0)
            months_with_counts.append({"month": month, "hands": hands_count})

        return months_with_counts

    @staticmethod
    def _is_valid_month(month: str | None) -> bool:
        if not month or not isinstance(month, str):
            return False
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            return False
        if month.startswith("1970-"):
            return False
        return True

    def _month_has_payload(self, token: str, month: str) -> bool:
        try:
            result = self.result_storage.get_pipeline_result(token, month=month)
            return bool(result)
        except FileNotFoundError:
            logger.debug("[USER_MONTHS] Missing pipeline_result for token=%s month=%s", token, month)
            return False
        except Exception as exc:  # noqa: BLE001 - best effort skip on failures
            logger.debug("[USER_MONTHS] Error loading pipeline_result for %s/%s: %s", token, month, exc)
            return False


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

    aggregated_token = f"user-{user_id}"

    try:
        persisted_payload = result_storage.get_pipeline_result(aggregated_token, month=month)
        if persisted_payload:
            logger.info(
                "[USER_MONTHS] Loaded persisted aggregated pipeline_result for %s/%s",
                user_id,
                month,
            )
            return persisted_payload
    except FileNotFoundError:
        logger.debug("[USER_MONTHS] Aggregated payload for %s/%s not found yet", user_id, month)
    except Exception as exc:  # noqa: BLE001 - fallback to merge logic
        logger.debug("[USER_MONTHS] Failed to load aggregated payload for %s/%s: %s", user_id, month, exc)

    cache_path = Path("results") / "by_user" / str(user_id) / f"pipeline_result_{month}.json"
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
