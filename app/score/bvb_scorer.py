"""
BvB (Battle of the Blinds) scoring module
Calculates scores based on ideal values and acceptable deviations for BvB stats
Uses the ScoringCalculator for consistent scoring across all stats
"""

def calculate_bvb_scores(stats, table_format="9max"):
    """
    Calculate BvB scores for given stats using ScoringCalculator
    
    Args:
        stats: Dictionary with BvB stats (opportunities and attempts)
        table_format: "9max", "6max", or "pko"
    
    Returns:
        Dictionary with scores for each stat and overall BvB score
    """
    from app.stats.scoring_calculator import ScoringCalculator
    from app.stats.scoring_config import get_stat_config
    
    # Map table format to group key
    if table_format == "9max":
        group_key = "nonko_9max"
    elif table_format == "6max":
        group_key = "nonko_6max"
    elif table_format.lower() == "pko":
        group_key = "pko"
    else:
        group_key = "nonko_9max"  # Default
    
    scoring_calc = ScoringCalculator()
    scores = {}
    
    # Calculate score for each BvB stat
    total_weight = 0
    weighted_score = 0
    
    # BvB stat names
    bvb_stat_names = ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]
    
    for stat_name in bvb_stat_names:
        stat_data = stats.get(stat_name, {})
        opportunities = stat_data.get("opportunities", 0)
        attempts = stat_data.get("attempts", 0)
        
        # Get configuration for this stat
        config = get_stat_config(group_key, stat_name)
        
        if config and opportunities >= 1:  # Generate score from first occurrence
            # Calculate percentage
            percentage = (attempts / opportunities) * 100
            
            # Get detailed score info
            detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
            
            scores[stat_name] = {
                "score": detailed['score'],
                "percentage": detailed['actual'],
                "ideal": detailed['ideal'],
                "deviation": detailed['deviation'],
                "trend": detailed['trend'],
                "opportunities": opportunities,
                "attempts": attempts
            }
            
            # Add to weighted average (all BvB stats have equal weight of 0.25)
            weight = config.get('weight', 0.25)
            weighted_score += detailed['score'] * weight
            total_weight += weight
        else:
            # Insufficient data or no config
            scores[stat_name] = {
                "score": None,
                "percentage": None,
                "ideal": config.get('ideal') if config else None,
                "opportunities": opportunities,
                "attempts": attempts
            }
    
    # Calculate overall BvB score
    if total_weight > 0:
        overall_score = weighted_score / total_weight
    else:
        overall_score = None
    
    scores["overall_score"] = round(overall_score, 1) if overall_score is not None else None
    
    return scores