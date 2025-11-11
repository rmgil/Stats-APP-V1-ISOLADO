"""
Flat Data Contract API - Unified stats + scoring interface
"""
import os
import json
import tempfile
from typing import Dict, Any, Optional, List
from pathlib import Path
from app.ui.display import stat_label, group_label, subgroup_label

def get_workspace_path(token: str) -> Optional[Path]:
    """Get workspace path for token, checking multiple possible locations"""
    # Check work dir first (pipeline runner)
    work_path = Path("work") / token
    if work_path.exists():
        return work_path
    
    # Check jobs dir (v2 system)
    base_jobs = os.environ.get("MTT_JOBS_DIR", os.path.join(tempfile.gettempdir(), "mtt_jobs"))
    job_path = Path(base_jobs) / token
    if job_path.exists():
        return job_path
    
    # Check runs dir (v1 system)
    runs_path = Path("runs") / token
    if runs_path.exists():
        return runs_path
    
    return None

def load_json_safe(file_path: Path) -> Optional[Dict]:
    """Load JSON file with error handling"""
    try:
        if file_path.exists():
            with open(file_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
    return None

def calculate_note(pct: Optional[float], ideal_min: Optional[float], ideal_max: Optional[float], 
                   opportunities: int = 0, sample_min: Optional[int] = None) -> str:
    """Calculate note based on percentage vs ideal range and sample size"""
    notes = []
    
    # Check sample size first
    if sample_min and opportunities < sample_min:
        notes.append("amostra baixa")
    
    # Calculate deviation from ideal
    if pct is not None and ideal_min is not None and ideal_max is not None:
        if ideal_min <= pct <= ideal_max:
            notes.append("dentro do ideal")
        elif pct < ideal_min:
            diff = ideal_min - pct
            notes.append(f"−{diff:.2f}pp abaixo do ideal")
        else:  # pct > ideal_max
            diff = pct - ideal_max
            notes.append(f"+{diff:.2f}pp acima do ideal")
    
    return "; ".join(notes) if notes else ""

def get_stat_label(stat_code: str, scorecard_stats: Optional[Dict]) -> str:
    """Get friendly label for stat"""
    # Try scorecard config first
    if scorecard_stats and stat_code in scorecard_stats:
        config = scorecard_stats[stat_code]
        if "display_name" in config:
            return config["display_name"]
    
    # Fallback to display helper
    return stat_label(stat_code)

def build_flat(token: str) -> Dict[str, Any]:
    """Build flat data contract combining stats and scoring"""
    
    # Find workspace
    workspace = get_workspace_path(token)
    if not workspace:
        return {
            "error": f"Token {token} not found",
            "token": token,
            "groups": [],
            "sample": {"hands": 0, "opportunities_total": 0}
        }
    
    # Load stats
    stats_path = workspace / "stats" / "stat_counts.json"
    if not stats_path.exists():
        stats_path = workspace / "stat_counts.json"  # Fallback to root
    
    stats_data = load_json_safe(stats_path)
    if not stats_data:
        return {
            "error": "No stats found",
            "token": token,
            "groups": [],
            "sample": {"hands": 0, "opportunities_total": 0}
        }
    
    # Load scorecard (optional)
    scorecard_path = workspace / "scores" / "scorecard.json"
    if not scorecard_path.exists():
        scorecard_path = workspace / "scorecard.json"  # Fallback to root
    
    scorecard_data = load_json_safe(scorecard_path)
    
    # Extract config from scorecard
    scorecard_config = scorecard_data.get("config", {}) if scorecard_data else {}
    scorecard_stats = scorecard_config.get("stats", {})
    scorecard_subgroups = scorecard_config.get("subgroups", {})
    scorecard_groups = scorecard_config.get("groups", {})
    
    # Extract scoring data
    scoring_data = scorecard_data.get("scoring", {}) if scorecard_data else {}
    overall_score = scoring_data.get("overall", {}).get("score")
    
    # Build flat structure
    result_groups = []
    total_opportunities = 0
    total_hands = stats_data.get("total_hands", 0)
    
    # Get month info
    months_list = stats_data.get("months", [])
    month_latest = months_list[0] if months_list else None
    
    # Process each group
    for group_key, group_data in stats_data.get("groups", {}).items():
        # Get group config
        group_config = scorecard_groups.get(group_key, {})
        group_weight = group_config.get("weight", 0.0)
        
        # Get group scoring
        group_scoring = scoring_data.get("groups", {}).get(group_key, {})
        
        # Build group structure
        flat_group = {
            "key": group_key,
            "label": group_config.get("display_name", group_label(group_key)),
            "weight": group_weight,
            "score": group_scoring.get("score"),
            "subgroups": []
        }
        
        # Process subgroups
        for subgroup_key, subgroup_data in group_data.get("subgroups", {}).items():
            # Get subgroup config
            subgroup_config = scorecard_subgroups.get(subgroup_key, {})
            subgroup_weight = subgroup_config.get("weight", 0.0)
            
            # Get subgroup scoring
            subgroup_scoring = group_scoring.get("subgroups", {}).get(subgroup_key, {})
            
            # Build subgroup structure
            flat_subgroup = {
                "key": subgroup_key,
                "label": subgroup_config.get("display_name", subgroup_label(subgroup_key)),
                "weight": subgroup_weight,
                "score": subgroup_scoring.get("score"),
                "rows": []
            }
            
            # Process stats
            for stat_code, stat_data in subgroup_data.get("stats", {}).items():
                opportunities = stat_data.get("opportunities", 0)
                attempts = stat_data.get("attempts", 0)
                total_opportunities += opportunities
                
                # Calculate percentage with 2 decimal places
                pct = (attempts / opportunities * 100) if opportunities > 0 else None
                
                # Get stat config
                stat_config = scorecard_stats.get(stat_code, {})
                ideal_min = stat_config.get("ideal_min")
                ideal_max = stat_config.get("ideal_max")
                stat_weight = stat_config.get("weight", 0.0)
                sample_min = stat_config.get("sample_min")
                
                # Get stat scoring
                stat_scoring = subgroup_scoring.get("stats", {}).get(stat_code, {})
                stat_score = stat_scoring.get("score")
                
                # Calculate note with sample size check
                note = calculate_note(pct, ideal_min, ideal_max, opportunities, sample_min)
                
                # Build stat row with proper rounding
                stat_row = {
                    "code": stat_code,
                    "label": get_stat_label(stat_code, scorecard_stats),
                    "opps": int(round(opportunities)),  # Round to integer
                    "att": int(round(attempts)),  # Round to integer
                    "pct": round(pct, 2) if pct is not None else None,  # 2 decimal places
                    "score": round(stat_score, 1) if stat_score is not None else None,
                    "ideal_min": ideal_min,
                    "ideal_max": ideal_max,
                    "weight_stat": stat_weight,
                    "weight_subgroup": subgroup_weight,
                    "weight_group": group_weight,
                    "note": note,
                    "ids": {
                        "opportunities": stat_data.get("opps", {}).get("ids", []),
                        "attempts": stat_data.get("attempts_ids", [])
                    }
                }
                
                flat_subgroup["rows"].append(stat_row)
            
            if flat_subgroup["rows"]:  # Only add non-empty subgroups
                flat_group["subgroups"].append(flat_subgroup)
        
        if flat_group["subgroups"]:  # Only add non-empty groups
            result_groups.append(flat_group)
    
    # Build final result with proper rounding
    return {
        "token": token,
        "month_latest": month_latest,
        "overall_score": round(overall_score, 1) if overall_score is not None else None,
        "groups": result_groups,
        "sample": {
            "hands": int(round(total_hands)),  # Round to integer
            "opportunities_total": int(round(total_opportunities))  # Round to integer
        }
    }