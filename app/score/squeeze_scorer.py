"""
Squeeze scoring implementation
"""

from typing import Dict, Optional
from app.stats.scoring_calculator import ScoringCalculator
from app.stats.scoring_config import get_stat_config

class SqueezeScorer:
    """Calculates scores for Squeeze statistics"""
    
    def __init__(self):
        self.calculator = ScoringCalculator()
    
    def calculate_squeeze_scores(self, stats: Dict, group_key: str) -> Dict:
        """
        Calculate Squeeze scores for a given group
        
        Args:
            stats: Dictionary with all statistics
            group_key: The group key (nonko_9max, nonko_6max, pko)
            
        Returns:
            Dictionary with Squeeze scores and weighted average
        """
        scores = {}
        
        # Squeeze geral (70% weight)
        squeeze_config = get_stat_config(group_key, "Squeeze")
        squeeze_stat = stats.get("Squeeze", {})
        squeeze_pct = self._calculate_percentage(squeeze_stat)
        
        if squeeze_config and squeeze_pct is not None:
            config = {
                'ideal': squeeze_config["ideal"],
                'oscillation_down': squeeze_config["oscillation_down"],
                'oscillation_up': squeeze_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(squeeze_pct, config)
            scores["Squeeze"] = {
                "percentage": squeeze_pct,
                "score": score,
                "ideal": squeeze_config["ideal"],
                "weight": squeeze_config["weight"]
            }
        else:
            scores["Squeeze"] = {
                "percentage": squeeze_pct if squeeze_pct is not None else 0,
                "score": None,
                "weight": 0.70
            }
        
        # Squeeze vs BTN Raiser (30% weight)
        squeeze_btn_config = get_stat_config(group_key, "Squeeze vs BTN Raiser")
        squeeze_btn_stat = stats.get("Squeeze vs BTN Raiser", {})
        squeeze_btn_pct = self._calculate_percentage(squeeze_btn_stat)
        
        if squeeze_btn_config and squeeze_btn_pct is not None:
            config = {
                'ideal': squeeze_btn_config["ideal"],
                'oscillation_down': squeeze_btn_config["oscillation_down"],
                'oscillation_up': squeeze_btn_config["oscillation_up"]
            }
            score = self.calculator.calculate_single_score(squeeze_btn_pct, config)
            scores["Squeeze vs BTN Raiser"] = {
                "percentage": squeeze_btn_pct,
                "score": score,
                "ideal": squeeze_btn_config["ideal"],
                "weight": squeeze_btn_config["weight"]
            }
        else:
            scores["Squeeze vs BTN Raiser"] = {
                "percentage": squeeze_btn_pct if squeeze_btn_pct is not None else 0,
                "score": None,
                "weight": 0.30
            }
        
        # Calculate weighted average for Squeeze subgroup
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