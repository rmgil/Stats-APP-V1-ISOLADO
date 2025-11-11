"""
Stats module for calculating poker statistics from enriched hands.
"""
from typing import Dict, List, Any

__version__ = "1.0.0"

# Main exports
__all__ = [
    "load_stats_catalog",
    "calculate_stats",
    "StatsEngine",
]

# Lazy imports to avoid circular dependencies
def load_stats_catalog():
    """Load the stats catalog from YAML."""
    from app.stats.dsl import load_catalog
    return load_catalog()

def calculate_stats(hands_jsonl: str, groups_filter: List[str] = None) -> Dict[str, Any]:
    """
    Calculate statistics from enriched hands.
    
    Args:
        hands_jsonl: Path to enriched hands JSONL file
        groups_filter: List of groups to calculate stats for
        
    Returns:
        Dictionary with calculated statistics
    """
    from app.stats.runner import run_stats
    return run_stats(hands_jsonl, groups_filter)

def StatsEngine():
    """Get the stats calculation engine."""
    from app.stats.engine import Engine
    return Engine()