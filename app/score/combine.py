"""
Combine stats across months and groups for scoring
"""
from typing import Dict, Tuple

NONKO_9 = "nonko_9max_pref"
NONKO_6 = "nonko_6max_pref"
NONKO_COMBINED = "nonko_pref"

def _node(data: dict, month: str, group: str, stat_id: str):
    """
    Navigate nested dict structure to get stat data
    
    Args:
        data: stat_counts dict
        month: Month string (e.g. "2025-06")
        group: Group name
        stat_id: Stat ID
        
    Returns:
        Stat data dict or None
    """
    return (data.get("counts", {})
               .get(month, {})
               .get(group, {})
               .get(stat_id))

def combine_nonko_stat(stat_counts: dict, month: str, stat_id: str, by: str = "opportunities") -> Tuple[int, int, float]:
    """
    Combine nonko_9max_pref and nonko_6max_pref stats
    
    Args:
        stat_counts: Stats data from Phase 5
        month: Month string
        stat_id: Stat ID to combine
        by: "opportunities" or "hands" (hands requires future integration)
        
    Returns:
        Tuple of (opportunities, attempts, percentage)
    """
    g9 = _node(stat_counts, month, NONKO_9, stat_id) or {}
    g6 = _node(stat_counts, month, NONKO_6, stat_id) or {}
    
    o9, a9 = g9.get("opportunities", 0), g9.get("attempts", 0)
    o6, a6 = g6.get("opportunities", 0), g6.get("attempts", 0)
    
    # FUTURO: se by == "hands", integrar com partitions; por ora usamos opportunities como proxy
    o = o9 + o6
    a = a9 + a6
    pct = (a / o * 100.0) if o > 0 else 0.0
    
    return o, a, pct