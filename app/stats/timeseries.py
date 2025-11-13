"""
Phase 9.A - Time series and breakdown aggregation for stats
"""
import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def load_all_monthly_stats(stats_dir: str = "stats") -> Dict[str, Any]:
    """
    Load all monthly stat count files from stats directory
    Returns combined data from all available months
    """
    all_stats = {}
    
    # Try to load the main stat_counts.json
    counts_path = os.path.join(stats_dir, "stat_counts.json")
    if os.path.exists(counts_path):
        with open(counts_path, 'r') as f:
            data = json.load(f)
            if 'counts' in data:
                all_stats.update(data['counts'])
    
    # Also look for individual monthly files if they exist
    for file in os.listdir(stats_dir):
        if file.startswith("stat_counts_") and file.endswith(".json"):
            month_path = os.path.join(stats_dir, file)
            try:
                with open(month_path, 'r') as f:
                    month_data = json.load(f)
                    if 'counts' in month_data:
                        all_stats.update(month_data['counts'])
            except Exception as e:
                logger.warning(f"Failed to load {file}: {e}")
    
    return all_stats


def get_timeseries(
    stat: str,
    group: str,
    months: int = 12,
    stats_dir: str = "stats",
    apply_time_decay: bool = False
) -> Dict[str, Any]:
    """
    Get time series data for a specific stat over multiple months
    
    Args:
        stat: Stat name (e.g., "POST_CBET_FLOP_IP")
        group: Group name (e.g., "postflop_all")
        months: Number of months to retrieve (default 12)
        stats_dir: Directory containing stats files
        apply_time_decay: Whether to apply time decay weights
        
    Returns:
        Dictionary with timeseries data
    """
    all_stats = load_all_monthly_stats(stats_dir)
    
    # Sort months chronologically
    sorted_months = sorted(all_stats.keys())
    
    # Take the last N months
    if len(sorted_months) > months:
        sorted_months = sorted_months[-months:]
    
    timeseries = []
    
    for month in sorted_months:
        month_data = all_stats.get(month, {})
        group_data = month_data.get(group, {})
        stat_data = group_data.get(stat, {})
        
        if stat_data:
            point = {
                "month": month,
                "opportunities": stat_data.get("opportunities", 0),
                "attempts": stat_data.get("attempts", 0),
                "percentage": stat_data.get("percentage", 0.0)
            }
            
            # Apply time decay if requested
            if apply_time_decay:
                # More recent months get higher weight
                month_idx = sorted_months.index(month)
                total_months = len(sorted_months)
                weight = (month_idx + 1) / total_months
                point["weight"] = round(weight, 2)
            
            timeseries.append(point)
    
    # Calculate aggregates
    total_opps = sum(p["opportunities"] for p in timeseries)
    total_attempts = sum(p["attempts"] for p in timeseries)
    
    avg_percentage = 0.0
    if total_opps > 0:
        avg_percentage = (total_attempts / total_opps) * 100.0
    
    return {
        "stat": stat,
        "group": group,
        "months_requested": months,
        "months_available": len(timeseries),
        "timeseries": timeseries,
        "aggregates": {
            "total_opportunities": total_opps,
            "total_attempts": total_attempts,
            "average_percentage": round(avg_percentage, 2)
        },
        "time_decay_applied": apply_time_decay
    }


def get_breakdown(
    group: str,
    family: str,
    stats_dir: str = "stats",
    month: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get breakdown of stats by position and street for a family
    
    Args:
        group: Group name (e.g., "postflop_all")
        family: Family of stats (e.g., "POSTFLOP_CBET")
        stats_dir: Directory containing stats files
        month: Specific month to analyze (latest if None)
        
    Returns:
        Dictionary with breakdown by position and street
    """
    all_stats = load_all_monthly_stats(stats_dir)
    
    # Use specified month or latest
    if month:
        month_data = all_stats.get(month, {})
    else:
        # Get latest month
        sorted_months = sorted(all_stats.keys())
        if sorted_months:
            month = sorted_months[-1]
            month_data = all_stats.get(month, {})
        else:
            month_data = {}
    
    group_data = month_data.get(group, {})
    
    # Define position groups
    positions = {
        "EP": ["EP", "UTG", "UTG1", "UTG2"],
        "MP": ["MP", "MP1", "MP2", "LJ"],
        "CO": ["CO", "HJ"],
        "BTN": ["BTN", "BU"],
        "BLINDS": ["SB", "BB"]
    }
    
    # Define street mappings
    streets = ["FLOP", "TURN", "RIVER"]
    
    # Initialize breakdown structure
    breakdown = {
        "by_position": {},
        "by_street": {},
        "by_position_and_street": defaultdict(lambda: defaultdict(dict))
    }
    
    # Process all stats in the group
    for stat_name, stat_data in group_data.items():
        # Check if stat belongs to the family
        if not stat_name.startswith(family):
            continue
        
        # Extract position and street from stat name
        stat_upper = stat_name.upper()
        
        # Find position
        found_position = None
        for pos_group, pos_list in positions.items():
            for pos in pos_list:
                if f"_{pos}_" in stat_upper or stat_upper.endswith(f"_{pos}"):
                    found_position = pos_group
                    break
            if found_position:
                break
        
        # Find street
        found_street = None
        for street in streets:
            if f"_{street}_" in stat_upper or f"_{street}" in stat_upper:
                found_street = street
                break
        
        # Special handling for IP/OOP
        ip_oop = None
        if "_IP" in stat_upper and not "_SKIP" in stat_upper:
            ip_oop = "IP"
        elif "_OOP" in stat_upper:
            ip_oop = "OOP"
        
        # Aggregate by position
        if found_position:
            if found_position not in breakdown["by_position"]:
                breakdown["by_position"][found_position] = {
                    "opportunities": 0,
                    "attempts": 0,
                    "stats": []
                }
            
            breakdown["by_position"][found_position]["opportunities"] += stat_data.get("opportunities", 0)
            breakdown["by_position"][found_position]["attempts"] += stat_data.get("attempts", 0)
            breakdown["by_position"][found_position]["stats"].append(stat_name)
        
        # Aggregate by street
        if found_street:
            if found_street not in breakdown["by_street"]:
                breakdown["by_street"][found_street] = {
                    "opportunities": 0,
                    "attempts": 0,
                    "stats": []
                }
            
            breakdown["by_street"][found_street]["opportunities"] += stat_data.get("opportunities", 0)
            breakdown["by_street"][found_street]["attempts"] += stat_data.get("attempts", 0)
            breakdown["by_street"][found_street]["stats"].append(stat_name)
        
        # Aggregate by position and street
        if found_position and found_street:
            key = f"{found_position}_{found_street}"
            if ip_oop:
                key += f"_{ip_oop}"
            
            breakdown["by_position_and_street"][found_position][found_street] = {
                "opportunities": stat_data.get("opportunities", 0),
                "attempts": stat_data.get("attempts", 0),
                "percentage": stat_data.get("percentage", 0.0),
                "stat": stat_name,
                "ip_oop": ip_oop
            }
        
        # If no position/street found but belongs to family
        if not found_position and not found_street:
            # Try to extract from common patterns
            if "CBET" in stat_upper:
                street = "FLOP" if "FLOP" in stat_upper else "TURN" if "TURN" in stat_upper else "RIVER" if "RIVER" in stat_upper else None
                
                if street and street not in breakdown["by_street"]:
                    breakdown["by_street"][street] = {
                        "opportunities": 0,
                        "attempts": 0,
                        "stats": []
                    }
                
                if street:
                    breakdown["by_street"][street]["opportunities"] += stat_data.get("opportunities", 0)
                    breakdown["by_street"][street]["attempts"] += stat_data.get("attempts", 0)
                    breakdown["by_street"][street]["stats"].append(stat_name)
    
    # Calculate percentages
    for pos_data in breakdown["by_position"].values():
        if pos_data["opportunities"] > 0:
            pos_data["percentage"] = round((pos_data["attempts"] / pos_data["opportunities"]) * 100, 2)
        else:
            pos_data["percentage"] = 0.0
    
    for street_data in breakdown["by_street"].values():
        if street_data["opportunities"] > 0:
            street_data["percentage"] = round((street_data["attempts"] / street_data["opportunities"]) * 100, 2)
        else:
            street_data["percentage"] = 0.0
    
    # Convert defaultdict to regular dict for JSON serialization
    breakdown["by_position_and_street"] = dict(breakdown["by_position_and_street"])
    for pos in breakdown["by_position_and_street"]:
        breakdown["by_position_and_street"][pos] = dict(breakdown["by_position_and_street"][pos])
    
    return {
        "group": group,
        "family": family,
        "month": month,
        "breakdown": breakdown,
        "summary": {
            "positions_found": list(breakdown["by_position"].keys()),
            "streets_found": list(breakdown["by_street"].keys()),
            "total_stats_in_family": sum(len(v["stats"]) for v in breakdown["by_position"].values())
        }
    }


def get_trend_analysis(
    stat: str,
    group: str,
    months: int = 6,
    stats_dir: str = "stats"
) -> Dict[str, Any]:
    """
    Analyze trend for a specific stat over time
    
    Returns trend direction, rate of change, etc.
    """
    timeseries_data = get_timeseries(stat, group, months, stats_dir)
    
    if len(timeseries_data["timeseries"]) < 2:
        return {
            "stat": stat,
            "group": group,
            "trend": "insufficient_data",
            "message": "Need at least 2 months of data for trend analysis"
        }
    
    series = timeseries_data["timeseries"]
    
    # Calculate trend
    first_half = series[:len(series)//2]
    second_half = series[len(series)//2:]
    
    avg_first = sum(p["percentage"] for p in first_half) / len(first_half) if first_half else 0
    avg_second = sum(p["percentage"] for p in second_half) / len(second_half) if second_half else 0
    
    change = avg_second - avg_first
    
    # Determine trend direction
    if abs(change) < 1.0:  # Less than 1% change
        trend = "stable"
    elif change > 0:
        trend = "improving" if change > 5.0 else "slightly_improving"
    else:
        trend = "declining" if change < -5.0 else "slightly_declining"
    
    # Calculate rate of change
    if avg_first != 0:
        rate_of_change = ((avg_second - avg_first) / avg_first) * 100
    else:
        rate_of_change = 0
    
    return {
        "stat": stat,
        "group": group,
        "months_analyzed": len(series),
        "trend": trend,
        "first_period_avg": round(avg_first, 2),
        "second_period_avg": round(avg_second, 2),
        "absolute_change": round(change, 2),
        "rate_of_change": round(rate_of_change, 2),
        "latest_value": series[-1]["percentage"] if series else 0,
        "peak_value": max(p["percentage"] for p in series) if series else 0,
        "valley_value": min(p["percentage"] for p in series) if series else 0
    }