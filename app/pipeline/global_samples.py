"""Helpers for building reusable global sampling summaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Any, Set


POSTFLOP_GROUP_KEY = "postflop_all"
PRIMARY_GROUP_KEYS = ("nonko_9max", "nonko_6max", "pko")


@dataclass
class GroupSample:
    """Collection of hand identifiers associated with a logical group."""

    key: str
    hand_ids: List[str] = field(default_factory=list)

    @property
    def hand_count(self) -> int:
        """Return the number of hands captured for this group."""

        return len(self.hand_ids)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the group sample into a JSON-friendly dictionary."""

        return {
            "key": self.key,
            "hand_ids": self.hand_ids,
            "hand_count": self.hand_count,
        }


@dataclass
class GlobalSamples:
    """Aggregated snapshot of the global classification results."""

    total_encontradas: int
    validas: int
    mystery: int
    lt4_players: int
    resumos: int
    discard_counts: Dict[str, int]
    groups: Dict[str, GroupSample]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the samples for inclusion in pipeline_result JSON."""

        return {
            "total_encontradas": self.total_encontradas,
            "validas": self.validas,
            "mystery": self.mystery,
            "less_than_4_players": self.lt4_players,
            "resumos": self.resumos,
            "discard_counts": self.discard_counts,
            "groups": {key: sample.to_dict() for key, sample in self.groups.items()},
        }

    @property
    def valid_hand_ids(self) -> List[str]:
        """Return the ordered list of valid hand identifiers."""

        postflop_sample = self.groups.get(POSTFLOP_GROUP_KEY)
        if not postflop_sample:
            return []
        return list(postflop_sample.hand_ids)

    def restrict_to(
        self,
        hand_ids: Iterable[str],
        discard_counts: Optional[Dict[str, int]] = None,
    ) -> "GlobalSamples":
        """Create a new :class:`GlobalSamples` limited to the provided hands.

        Args:
            hand_ids: Iterable of hand identifiers to retain.
            discard_counts: Optional discard mapping to associate with the
                restricted sample. When omitted, all discard counters default to
                zero.

        Returns:
            A new :class:`GlobalSamples` instance containing only the selected
            hands while keeping group integrity consistent with the global
            snapshot.
        """

        subset: Set[str] = {hand_id for hand_id in hand_ids if hand_id}

        global_valid_ids = set(self.valid_hand_ids)
        if global_valid_ids:
            subset &= global_valid_ids

        filtered_groups: Dict[str, GroupSample] = {}
        for key, sample in self.groups.items():
            filtered_ids = [hand_id for hand_id in sample.hand_ids if hand_id in subset]
            filtered_groups[key] = GroupSample(key=key, hand_ids=filtered_ids)

        postflop_sample = filtered_groups.get(POSTFLOP_GROUP_KEY)
        valid_count = postflop_sample.hand_count if postflop_sample else 0

        counted_valid = sum(
            sample.hand_count
            for key, sample in filtered_groups.items()
            if key != POSTFLOP_GROUP_KEY
        )

        assert counted_valid == valid_count, (
            "Mismatch between filtered group totals and valid hand count: "
            f"{counted_valid} != {valid_count}"
        )

        normalised_discards = _normalise_discards(discard_counts)
        total_encontradas = valid_count + normalised_discards.get("total", 0)

        restricted = GlobalSamples(
            total_encontradas=total_encontradas,
            validas=valid_count,
            mystery=normalised_discards.get("mystery", 0),
            lt4_players=normalised_discards.get("less_than_4_players", 0),
            resumos=normalised_discards.get("tournament_summary", 0),
            discard_counts=normalised_discards,
            groups=filtered_groups,
        )

        postflop_sample = restricted.groups.get(POSTFLOP_GROUP_KEY)
        if postflop_sample:
            assert postflop_sample.hand_count == restricted.validas, (
                "Restricted POSTFLOP group must match the number of valid hands"
            )

        return restricted


def _normalise_discards(discard_counts: Optional[Dict[str, int]]) -> Dict[str, int]:
    """Return a normalized discard dictionary with all expected keys."""

    base = discard_counts or {}
    tracked_keys = [
        "mystery",
        "less_than_4_players",
        "tournament_summary",
        "cash_game",
        "invalid_format",
        "other",
    ]

    normalised: Dict[str, int] = {key: int(base.get(key, 0) or 0) for key in tracked_keys}

    # Include any additional discard categories that might appear in the pipeline
    for key, value in base.items():
        if key in tracked_keys or key == "total":
            continue
        normalised[key] = normalised.get(key, 0) + int(value or 0)

    calculated_total = sum(normalised.values())
    provided_total = int(base.get("total", 0) or 0)

    if provided_total and provided_total != calculated_total:
        # Adjust the catch-all bucket so totals stay consistent with the pipeline
        diff = provided_total - calculated_total
        if diff:
            normalised["other"] = normalised.get("other", 0) + diff
            calculated_total += diff

    normalised["total"] = calculated_total

    # Guarantee presence of expected keys with integer values
    for key in tracked_keys:
        normalised.setdefault(key, 0)

    return normalised


def build_global_samples(
    hands: Optional[Iterable[Dict[str, Any]]],
    discard_counts: Optional[Dict[str, int]] = None,
) -> GlobalSamples:
    """Create a reusable summary of global hand counts and groups.

    Args:
        hands: Iterable of records describing each valid hand. Each record must
            contain at least ``hand_id`` and ``group`` keys.
        discard_counts: Mapping with discard reasons collected during
            classification.

    Returns:
        A :class:`GlobalSamples` object containing totals and per-group samples.
    """

    normalised_discards = _normalise_discards(discard_counts)

    group_samples: Dict[str, GroupSample] = {
        key: GroupSample(key) for key in PRIMARY_GROUP_KEYS
    }
    postflop_sample = GroupSample(POSTFLOP_GROUP_KEY)

    valid_count = 0

    if hands is not None:
        for record in hands:
            if not isinstance(record, dict):
                continue

            hand_id = record.get("hand_id")
            if not hand_id:
                continue

            group_key = record.get("group") or ""

            if group_key not in group_samples:
                group_samples[group_key] = GroupSample(group_key)

            group_samples[group_key].hand_ids.append(hand_id)
            postflop_sample.hand_ids.append(hand_id)
            valid_count += 1

    # Integrity checks – every valid hand must be accounted for exactly once
    counted_valid = sum(
        sample.hand_count
        for key, sample in group_samples.items()
        if key != POSTFLOP_GROUP_KEY
    )

    assert counted_valid == valid_count, (
        "Mismatch between valid hand records and grouped totals: "
        f"{counted_valid} != {valid_count}"
    )

    assert postflop_sample.hand_count == valid_count, (
        "POSTFLOP group must include exactly the set of valid hands: "
        f"{postflop_sample.hand_count} != {valid_count}"
    )

    group_samples[POSTFLOP_GROUP_KEY] = postflop_sample

    total_encontradas = valid_count + normalised_discards.get("total", 0)

    return GlobalSamples(
        total_encontradas=total_encontradas,
        validas=valid_count,
        mystery=normalised_discards.get("mystery", 0),
        lt4_players=normalised_discards.get("less_than_4_players", 0),
        resumos=normalised_discards.get("tournament_summary", 0),
        discard_counts=normalised_discards,
        groups=group_samples,
    )

