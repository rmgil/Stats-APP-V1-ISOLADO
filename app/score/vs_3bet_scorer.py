"""
vs 3bet IP/OOP scoring module
Calculates scores for Fold to 3bet IP and OOP stats
Fold to 3bet general stat doesn't have a score
"""

def calculate_vs_3bet_scores(stats, table_format="9max"):
    """
    Calculate vs 3bet scores for given stats using ScoringCalculator
    
    Args:
        stats: Dictionary with vs 3bet stats (opportunities and attempts)
        table_format: "9max", "6max", or "pko"
    
    Returns:
        Dictionary with scores for Fold to 3bet IP and OOP, percentage for general
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
    
    # Calculate score for each stat with equal weights (50/50)
    total_weight = 0
    weighted_score = 0
    
    # Process Fold to 3bet IP (has score)
    ip_stat = "Fold to 3bet IP"
    ip_data = stats.get(ip_stat, {})
    opportunities = ip_data.get("opportunities", 0)
    attempts = ip_data.get("attempts", 0)
    
    config = get_stat_config(group_key, ip_stat)
    
    if config and opportunities >= 1:
        percentage = (attempts / opportunities) * 100
        detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
        
        scores[ip_stat] = {
            "score": detailed['score'],
            "percentage": detailed['actual'],
            "ideal": detailed['ideal'],
            "deviation": detailed['deviation'],
            "trend": detailed['trend'],
            "opportunities": opportunities,
            "attempts": attempts
        }
        
        # Weight is 50%
        weight = 0.50
        weighted_score += detailed['score'] * weight
        total_weight += weight
    else:
        scores[ip_stat] = {
            "score": None,
            "percentage": None,
            "ideal": config.get('ideal') if config else None,
            "opportunities": opportunities,
            "attempts": attempts
        }
    
    # Process Fold to 3bet OOP (has score)
    oop_stat = "Fold to 3bet OOP"
    oop_data = stats.get(oop_stat, {})
    opportunities = oop_data.get("opportunities", 0)
    attempts = oop_data.get("attempts", 0)
    
    config = get_stat_config(group_key, oop_stat)
    
    if config and opportunities >= 1:
        percentage = (attempts / opportunities) * 100
        detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
        
        scores[oop_stat] = {
            "score": detailed['score'],
            "percentage": detailed['actual'],
            "ideal": detailed['ideal'],
            "deviation": detailed['deviation'],
            "trend": detailed['trend'],
            "opportunities": opportunities,
            "attempts": attempts
        }
        
        # Weight is 50%
        weight = 0.50
        weighted_score += detailed['score'] * weight
        total_weight += weight
    else:
        scores[oop_stat] = {
            "score": None,
            "percentage": None,
            "ideal": config.get('ideal') if config else None,
            "opportunities": opportunities,
            "attempts": attempts
        }
    
    # Process Fold to 3bet general (no score, only percentage)
    general_stat = "Fold to 3bet"
    general_data = stats.get(general_stat, {})
    opportunities = general_data.get("opportunities", 0)
    attempts = general_data.get("attempts", 0)
    
    if opportunities >= 1:
        percentage = (attempts / opportunities) * 100
        scores[general_stat] = {
            "score": None,  # No score for general stat
            "percentage": round(percentage, 1),
            "ideal": None,  # No ideal for general stat
            "opportunities": opportunities,
            "attempts": attempts
        }
    else:
        scores[general_stat] = {
            "score": None,
            "percentage": None,
            "ideal": None,
            "opportunities": opportunities,
            "attempts": attempts
        }
    
    # Calculate overall vs 3bet score (50/50 weighted average)
    if total_weight > 0:
        overall_score = weighted_score  # Already weighted properly
    else:
        overall_score = None
    
    scores["overall_score"] = round(overall_score, 1) if overall_score is not None else None
    
    return scores