"""Shared pipeline result structures and builders.

This module centralizes the construction of ``pipeline_result`` payloads so
monthly and global outputs share the exact same shape and counting logic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.pipeline.global_samples import GlobalSamples, build_global_samples


@dataclass
class PipelineResult:
    """Canonical representation of the pipeline_result payload."""

    status: str
    multi_site: bool
    combined: Dict[str, Any]
    valid_hand_records: List[Dict[str, Any]]
    valid_hands: int
    total_hands: int
    aggregated_discards: Dict[str, int]
    classification: Dict[str, Any]
    sites: Dict[str, Any] = field(default_factory=dict)
    hands_per_month: Dict[str, int] = field(default_factory=dict)
    global_samples: Optional[Dict[str, Any]] = None
    month: Optional[str] = None
    postflop_hands_count: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": self.status,
            "multi_site": self.multi_site,
            "combined": self.combined,
            "valid_hand_records": self.valid_hand_records,
            "valid_hands": self.valid_hands,
            "total_hands": self.total_hands,
            "aggregated_discards": self.aggregated_discards,
            "classification": self.classification,
            "sites": self.sites,
            "hands_per_month": self.hands_per_month,
        }

        if self.global_samples is not None:
            payload["global_samples"] = self.global_samples
        if self.month is not None:
            payload["month"] = self.month
        if self.postflop_hands_count is not None:
            payload["postflop_hands_count"] = self.postflop_hands_count

        payload.update(self.extra)
        return payload


def build_pipeline_result_payload(
    *,
    combined: Dict[str, Any],
    valid_hand_records: List[Dict[str, Any]],
    aggregated_discards: Dict[str, int],
    sites: Dict[str, Any],
    hands_per_month: Optional[Dict[str, int]] = None,
    month: Optional[str] = None,
    postflop_hands_count: Optional[int] = None,
    samples: Optional[GlobalSamples] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the canonical pipeline_result payload used by dashboards.

    This is the single source of truth for computing totals, valid/mystery/<4
    counters, and embedding the GlobalSamples snapshot. All monthly results are
    produced by applying this same builder to a month-filtered subset of hands.
    """

    samples = samples or build_global_samples(valid_hand_records, aggregated_discards)

    payload = PipelineResult(
        status="completed",
        multi_site=True,
        combined=combined or {},
        valid_hand_records=valid_hand_records or [],
        valid_hands=samples.validas,
        total_hands=samples.total_encontradas,
        aggregated_discards=samples.discard_counts,
        classification={
            "discarded_hands": samples.discard_counts,
            "total_hands": samples.total_encontradas,
            "valid_hands": samples.validas,
        },
        sites=sites or {},
        hands_per_month=hands_per_month or {},
        global_samples=samples.to_dict(),
        month=month,
        postflop_hands_count=postflop_hands_count,
        extra=extra or {},
    )

    return payload.to_dict()
