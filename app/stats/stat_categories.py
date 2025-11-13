"""Shared stat category definitions for NONKO, PKO and POSTFLOP analysis.

The production pipeline, dashboard aggregation and download helpers rely on the
exact same stat groupings.  Centralising these mappings avoids subtle drift
between NONKO/PKO logic paths while keeping the business rules untouched.
"""

from __future__ import annotations

from typing import Dict, Iterable, Mapping

# Shared category weights for NONKO and PKO score aggregation.
CATEGORY_WEIGHTS: Dict[str, float] = {
    "rfi": 0.25,
    "bvb": 0.15,
    "threbet_cc": 0.20,
    "vs_3bet": 0.10,
    "squeeze": 0.10,
    "bb_defense": 0.10,
    "sb_defense": 0.10,
}

# User-facing labels used in dashboard responses.
CATEGORY_LABELS: Dict[str, str] = {
    "rfi": "RFI",
    "bvb": "BvB",
    "threbet_cc": "3b & CC",
    "vs_3bet": "vs 3b IP/OOP",
    "squeeze": "Squeeze",
    "bb_defense": "Defesa da BB",
    "sb_defense": "Defesa da SB",
}

# Exact stat names grouped by category (shared by NONKO and PKO pipelines).
RFI_STATS = {
    "Early RFI",
    "Middle RFI",
    "CO Steal",
    "BTN Steal",
}

BVB_STATS = {
    "SB UO VPIP",
    "BB fold vs SB steal",
    "BB raise vs SB limp UOP",
    "SB Steal",
}

THREEBET_CC_STATS = {
    "EP 3bet",
    "EP Cold Call",
    "MP 3bet",
    "MP Cold Call",
    "CO 3bet",
    "CO Cold Call",
    "BTN 3bet",
    "BTN Cold Call",
    "BTN fold to CO steal",
}

VS_3BET_STATS = {
    "Fold to 3bet IP",
    "Fold to 3bet OOP",
    "Fold to 3bet",
}

SQUEEZE_STATS = {
    "Squeeze",
    "Squeeze vs BTN Raiser",
}

BB_DEFENSE_STATS = {
    "BB fold vs CO steal",
    "BB fold vs BTN steal",
    "BB resteal vs BTN steal",
}

SB_DEFENSE_STATS = {
    "SB fold to CO Steal",
    "SB fold to BTN Steal",
    "SB resteal vs BTN",
}

# Keywords used to identify postflop stats in aggregated outputs.
POSTFLOP_KEYWORDS = [
    "Flop CBet",
    "Flop fold",
    "Flop raise",
    "Check Raise",
    "Flop Bet vs",
    "Turn CBet",
    "Turn Donk",
    "Turn Fold",
    "Bet Turn",
    "WTSD",
    "W$SD",
    "W$WSF",
    "River Agg",
    "River Bet",
]


def filter_stats(stats: Mapping[str, dict], names: Iterable[str]) -> Dict[str, dict]:
    """Return the subset of ``stats`` whose keys exist in ``names``."""

    name_set = set(names)
    return {key: value for key, value in stats.items() if key in name_set}


def filter_stats_by_keyword(stats: Mapping[str, dict], keywords: Iterable[str]) -> Dict[str, dict]:
    """Filter stats whose name contains any of the provided ``keywords``."""

    lowered = [keyword.lower() for keyword in keywords]
    return {
        key: value
        for key, value in stats.items()
        if any(keyword in key.lower() for keyword in lowered)
    }
