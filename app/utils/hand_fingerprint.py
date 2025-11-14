"""Utility helpers for generating stable fingerprints for poker hands."""
from __future__ import annotations

import hashlib


def fingerprint_hand(hand_text: str) -> str:
    """Return a normalized SHA1 fingerprint for a hand history.

    The normalization matches the behaviour used in the multi-site aggregator so
    that deduplicated counts align across the pipeline.
    """
    normalized = (
        hand_text.strip()
        .replace("\r\n", "\n")
        .replace("\n\n", "\n")
        .lower()
    )
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()
