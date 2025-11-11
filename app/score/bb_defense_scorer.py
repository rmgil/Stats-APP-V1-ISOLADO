"""
BB Defense scoring implementation
"""

from typing import Dict, Optional
from app.stats.scoring_calculator import ScoringCalculator
from app.stats.scoring_config import get_stat_config

class BBDefenseScorer:
    """Calculates scores for BB Defense statistics"""
    
    def __init__(self):
        self.calculator = ScoringCalculator()
    
    def calculate_bb_defense_scores(self, stats: Dict, bvb_stats: Dict, group_key: str) -> Dict:
        """
        Calculate BB Defense scores for a given group
        
        Args:
            stats: Dictionary with BB defense statistics
            bvb_stats: Dictionary with BvB statistics (for BB fold vs SB steal)
            group_key: The group key (nonko_9max, nonko_6max, pko)
            
        Returns:
            Dictionary with BB Defense scores and weighted average
        """
        scores = {}
        
        # BB fold vs CO steal (30% weight)
        bb_co_config = get_stat_config(group_key, "BB fold vs CO steal")
        bb_co_stat = stats.get("BB fold vs CO steal", {})
        bb_co_pct = self._calculate_percentage(bb_co_stat)
        
        if bb_co_config and bb_co_pct is not None:
            config = {
                'ideal': bb_co_config["ideal"],
                'oscillation_down': bb_co_config["oscillation_down"],
                'oscillation_up': bb_co_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(bb_co_pct, config)
            scores["BB fold vs CO steal"] = {
                "percentage": bb_co_pct,
                "score": score,
                "ideal": bb_co_config["ideal"],
                "weight": bb_co_config["weight"]
            }
        else:
            scores["BB fold vs CO steal"] = {
                "percentage": bb_co_pct if bb_co_pct is not None else 0,
                "score": None,
                "weight": 0.30
            }
        
        # BB fold vs BTN steal (35% weight)
        bb_btn_config = get_stat_config(group_key, "BB fold vs BTN steal")
        bb_btn_stat = stats.get("BB fold vs BTN steal", {})
        bb_btn_pct = self._calculate_percentage(bb_btn_stat)
        
        if bb_btn_config and bb_btn_pct is not None:
            config = {
                'ideal': bb_btn_config["ideal"],
                'oscillation_down': bb_btn_config["oscillation_down"],
                'oscillation_up': bb_btn_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(bb_btn_pct, config)
            scores["BB fold vs BTN steal"] = {
                "percentage": bb_btn_pct,
                "score": score,
                "ideal": bb_btn_config["ideal"],
                "weight": bb_btn_config["weight"]
            }
        else:
            scores["BB fold vs BTN steal"] = {
                "percentage": bb_btn_pct if bb_btn_pct is not None else 0,
                "score": None,
                "weight": 0.35
            }
        
        # BB fold vs SB steal (15% weight) - comes from BvB stats
        bb_sb_config = get_stat_config(group_key, "BB fold vs SB steal")
        bb_sb_stat = bvb_stats.get("BB fold vs SB steal", {})
        bb_sb_pct = bb_sb_stat.get("percentage") if "percentage" in bb_sb_stat else self._calculate_percentage(bb_sb_stat)
        
        if bb_sb_config and bb_sb_pct is not None:
            config = {
                'ideal': bb_sb_config["ideal"],
                'oscillation_down': bb_sb_config["oscillation_down"],
                'oscillation_up': bb_sb_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(bb_sb_pct, config)
            scores["BB fold vs SB steal"] = {
                "percentage": bb_sb_pct,
                "score": score,
                "ideal": bb_sb_config["ideal"],
                "weight": 0.15  # Fixed weight as specified
            }
        else:
            scores["BB fold vs SB steal"] = {
                "percentage": bb_sb_pct if bb_sb_pct is not None else 0,
                "score": None,
                "weight": 0.15
            }
        
        # BB resteal vs BTN steal (20% weight)
        bb_resteal_config = get_stat_config(group_key, "BB resteal vs BTN steal")
        bb_resteal_stat = stats.get("BB resteal vs BTN steal", {})
        bb_resteal_pct = self._calculate_percentage(bb_resteal_stat)
        
        if bb_resteal_config and bb_resteal_pct is not None:
            config = {
                'ideal': bb_resteal_config["ideal"],
                'oscillation_down': bb_resteal_config["oscillation_down"],
                'oscillation_up': bb_resteal_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(bb_resteal_pct, config)
            scores["BB resteal vs BTN steal"] = {
                "percentage": bb_resteal_pct,
                "score": score,
                "ideal": bb_resteal_config["ideal"],
                "weight": bb_resteal_config["weight"]
            }
        else:
            scores["BB resteal vs BTN steal"] = {
                "percentage": bb_resteal_pct if bb_resteal_pct is not None else 0,
                "score": None,
                "weight": 0.20
            }
        
        # Calculate weighted average for BB Defense subgroup
        total_weight = 0
        weighted_sum = 0
        
        for stat_name, stat_data in scores.items():
            if stat_data["score"] is not None:
                weighted_sum += stat_data["score"] * stat_data["weight"]
                total_weight += stat_data["weight"]
        
        if total_weight > 0:
            scores["_weighted_average"] = round(weighted_sum / total_weight, 1)
        else:
            scores["_weighted_average"] = None
        
        return scores
    
    def _calculate_percentage(self, stat: Dict) -> Optional[float]:
        """Calculate percentage from opportunities and attempts"""
        opportunities = stat.get("opportunities", 0)
        attempts = stat.get("attempts", 0)
        
        if opportunities > 0:
            return round((attempts / opportunities) * 100, 1)
        return None