"""Utilities for storing and reusing tournament files per user."""

from __future__ import annotations

import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class StoredTournament:
    """Represents a tournament file stored for a user."""

    month: str
    tournament_id: str
    path: Path
    source: str
    updated_at: str


class TournamentRepository:
    """Persist tournament files per user for monthly processing."""

    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = Path(base_dir or "user_datasets")
        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _sanitize_user(self, user_id: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9._-]", "_", user_id or "anonymous")
        return safe[:128]

    def _sanitize_tournament(self, tournament_id: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9._-]", "_", tournament_id or "tournament")
        return safe[:150] or "tournament"

    def _user_dir(self, user_id: str) -> Path:
        return self.base_dir / self._sanitize_user(user_id)

    def _manifest_path(self, user_id: str) -> Path:
        return self._user_dir(user_id) / "manifest.json"

    def _load_manifest(self, user_id: str) -> Dict[str, Dict[str, Dict[str, str]]]:
        manifest_path = self._manifest_path(user_id)
        if manifest_path.exists():
            try:
                return json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("Failed to read manifest for %s: %s", user_id, exc)

        return {"months": {}}

    def _save_manifest(self, user_id: str, manifest: Dict[str, Dict[str, Dict[str, str]]]):
        manifest_path = self._manifest_path(user_id)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def store_tournament(
        self,
        user_id: str,
        month: str,
        tournament_id: str,
        content: str,
        source_filename: str,
    ) -> Tuple[Path, bool]:
        """Persist tournament content for a user.

        Returns the stored path and whether the entry replaced an existing one.
        """

        safe_month = month or "unknown"
        safe_tournament = self._sanitize_tournament(tournament_id)

        user_dir = self._user_dir(user_id)
        month_dir = user_dir / safe_month
        month_dir.mkdir(parents=True, exist_ok=True)

        manifest = self._load_manifest(user_id)
        month_entries = manifest.setdefault("months", {}).setdefault(safe_month, {})

        # Always preserve existing entries by generating a unique identifier per file
        dest_path = month_dir / f"{safe_tournament}.txt"
        unique_id = tournament_id
        suffix = 1

        while dest_path.exists() or unique_id in month_entries:
            suffix += 1
            unique_id = f"{tournament_id}_{suffix}"
            dest_path = month_dir / f"{self._sanitize_tournament(unique_id)}.txt"

        replaced = False

        normalized = content.replace("\r\n", "\n").replace("\r", "\n")
        dest_path.write_text(normalized, encoding="utf-8")

        month_entries[unique_id] = {
            "filename": dest_path.name,
            "source": source_filename,
            "updated_at": datetime.utcnow().isoformat(),
            "tournament_id": tournament_id,
        }
        self._save_manifest(user_id, manifest)

        action = "updated" if replaced else "stored"
        logger.info(
            "[TOURNAMENT REPO] %s tournament %s/%s for user %s",
            action,
            safe_month,
            unique_id,
            user_id,
        )

        return dest_path, replaced

    def list_tournaments(self, user_id: str) -> List[StoredTournament]:
        manifest = self._load_manifest(user_id)
        tournaments: List[StoredTournament] = []
        user_dir = self._user_dir(user_id)

        for month, entries in manifest.get("months", {}).items():
            for tournament_id, info in entries.items():
                file_name = info.get("filename")
                path = user_dir / month / file_name if file_name else None
                if not path or not path.exists():
                    continue
                tournaments.append(
                    StoredTournament(
                        month=month,
                        tournament_id=tournament_id,
                        path=path,
                        source=info.get("source", ""),
                        updated_at=info.get("updated_at", ""),
                    )
                )

        return tournaments

    def export_dataset(self, user_id: str, target_dir: Path) -> int:
        """Copy all tournaments for a user into target_dir grouped by month."""

        tournaments = self.list_tournaments(user_id)

        if target_dir.exists():
            shutil.rmtree(target_dir)

        target_dir.mkdir(parents=True, exist_ok=True)

        for tournament in tournaments:
            month_dir = target_dir / tournament.month
            month_dir.mkdir(parents=True, exist_ok=True)
            dest_path = month_dir / tournament.path.name
            shutil.copy2(tournament.path, dest_path)

        logger.info(
            "[TOURNAMENT REPO] Exported %s tournaments for %s to %s",
            len(tournaments),
            user_id,
            target_dir,
        )

        return len(tournaments)

