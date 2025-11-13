"""
3bet & Cold Call scoring module
Calculates scores for VPIP and BTN fold to CO steal stats
3bet and Cold Call stats don't have scores, only percentages
"""

def calculate_3bet_cc_scores(stats, table_format="9max"):
    """
    Calculate 3bet/CC scores for given stats using ScoringCalculator
    
    Args:
        stats: Dictionary with 3bet/CC stats (opportunities and attempts)
        table_format: "9max", "6max", or "pko"
    
    Returns:
        Dictionary with scores for VPIP and BTN fold to CO steal, percentages for 3bet/CC
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
    
    # Define weights per format for VPIP and BTN fold to CO steal
    weight_map = {
        "9max": {
            "EP VPIP": 0.10,  # 10%
            "MP VPIP": 0.10,  # 10%
            "CO VPIP": 0.30,  # 30%
            "BTN VPIP": 0.30,  # 30%
            "BTN fold to CO steal": 0.20  # 20%
        },
        "6max": {
            "EP VPIP": 0.0,   # 0% - não gera
            "MP VPIP": 0.10,  # 10%
            "CO VPIP": 0.35,  # 35%
            "BTN VPIP": 0.35,  # 35%
            "BTN fold to CO steal": 0.20  # 20%
        },
        "pko": {
            "EP VPIP": 0.0,   # 0% - não contribui
            "MP VPIP": 0.10,  # 10%
            "CO VPIP": 0.35,  # 35%
            "BTN VPIP": 0.35,  # 35%
            "BTN fold to CO steal": 0.20  # 20%
        }
    }
    
    # Get weights for current format
    format_weights = weight_map.get(table_format.lower() if table_format else "9max", weight_map["9max"])
    
    # Calculate score for each stat
    total_weight = 0
    weighted_score = 0
    
    # Process all 3bet/CC related stats
    positions = ["EP", "MP", "CO", "BTN"]
    
    for pos in positions:
        # Process 3bet (no score, only percentage)
        three_bet_stat = f"{pos} 3bet"
        three_bet_data = stats.get(three_bet_stat, {})
        opportunities = three_bet_data.get("opportunities", 0)
        attempts = three_bet_data.get("attempts", 0)
        
        if opportunities >= 1:
            percentage = (attempts / opportunities) * 100
            scores[three_bet_stat] = {
                "score": None,  # No score for 3bet
                "percentage": round(percentage, 1),
                "ideal": None,
                "opportunities": opportunities,
                "attempts": attempts
            }
        else:
            scores[three_bet_stat] = {
                "score": None,
                "percentage": None,
                "ideal": None,
                "opportunities": opportunities,
                "attempts": attempts
            }
        
        # Process Cold Call (no score, only percentage)
        cold_call_stat = f"{pos} Cold Call"
        cold_call_data = stats.get(cold_call_stat, {})
        opportunities = cold_call_data.get("opportunities", 0)
        attempts = cold_call_data.get("attempts", 0)
        
        if opportunities >= 1:
            percentage = (attempts / opportunities) * 100
            scores[cold_call_stat] = {
                "score": None,  # No score for Cold Call
                "percentage": round(percentage, 1),
                "ideal": None,
                "opportunities": opportunities,
                "attempts": attempts
            }
        else:
            scores[cold_call_stat] = {
                "score": None,
                "percentage": None,
                "ideal": None,
                "opportunities": opportunities,
                "attempts": attempts
            }
        
        # Process VPIP (has score)
        vpip_stat = f"{pos} VPIP"
        vpip_data = stats.get(vpip_stat, {})
        opportunities = vpip_data.get("opportunities", 0)
        attempts = vpip_data.get("attempts", 0)
        
        # Get configuration for VPIP
        config = get_stat_config(group_key, vpip_stat)
        
        # For 6max, EP doesn't exist
        if table_format == "6max" and pos == "EP":
            scores[vpip_stat] = {
                "score": None,
                "percentage": None,
                "ideal": None,
                "opportunities": 0,
                "attempts": 0
            }
        elif config and opportunities >= 1:
            # Calculate percentage
            percentage = (attempts / opportunities) * 100
            
            # Get detailed score info
            detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
            
            scores[vpip_stat] = {
                "score": detailed['score'],
                "percentage": detailed['actual'],
                "ideal": detailed['ideal'],
                "deviation": detailed['deviation'],
                "trend": detailed['trend'],
                "opportunities": opportunities,
                "attempts": attempts
            }
            
            # Add to weighted average using format-specific weights
            weight = format_weights.get(vpip_stat, 0)
            if weight > 0:
                weighted_score += detailed['score'] * weight
                total_weight += weight
        else:
            scores[vpip_stat] = {
                "score": None,
                "percentage": None,
                "ideal": config.get('ideal') if config else None,
                "opportunities": opportunities,
                "attempts": attempts
            }
    
    # Process BTN fold to CO steal (has score)
    btn_fold_stat = "BTN fold to CO steal"
    btn_fold_data = stats.get(btn_fold_stat, {})
    opportunities = btn_fold_data.get("opportunities", 0)
    attempts = btn_fold_data.get("attempts", 0)
    
    config = get_stat_config(group_key, btn_fold_stat)
    
    if config and opportunities >= 1:
        percentage = (attempts / opportunities) * 100
        detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
        
        scores[btn_fold_stat] = {
            "score": detailed['score'],
            "percentage": detailed['actual'],
            "ideal": detailed['ideal'],
            "deviation": detailed['deviation'],
            "trend": detailed['trend'],
            "opportunities": opportunities,
            "attempts": attempts
        }
        
        # Use format-specific weight for BTN fold to CO steal
        weight = format_weights.get(btn_fold_stat, 0.20)
        if weight > 0:
            weighted_score += detailed['score'] * weight
            total_weight += weight
    else:
        scores[btn_fold_stat] = {
            "score": None,
            "percentage": None,
            "ideal": config.get('ideal') if config else None,
            "opportunities": opportunities,
            "attempts": attempts
        }
    
    # Calculate overall 3bet/CC score (weighted average)
    # Note: total_weight might be < 1.0 if some stats have 0 weight
    if total_weight > 0:
        overall_score = weighted_score  # Already weighted properly
    else:
        overall_score = None
    
    scores["overall_score"] = round(overall_score, 1) if overall_score is not None else None
    
    return scores