"""SB Defense Scorer - scores Small Blind defense statistics"""

from typing import Dict, Any, List
from app.stats.scoring_calculator import ScoringCalculator


class SBDefenseScorer:
    """Calculates scores for SB defense statistics with format-specific ideal values"""
    
    def __init__(self):
        self.calculator = ScoringCalculator()
        
        # Format-specific ideal values for SB defense stats
        self.ideal_values = {
            "9max": {
                "SB fold to CO Steal": {
                    "ideal": 76,
                    "oscillation_up": 3,    # 3% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB fold to BTN Steal": {
                    "ideal": 72,
                    "oscillation_up": 3,    # 3% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB resteal vs BTN": {
                    "ideal": 14,
                    "oscillation_up": 18,   # 18% para cima
                    "oscillation_down": 3   # 3% para baixo
                }
            },
            "6max": {
                "SB fold to CO Steal": {
                    "ideal": 81,
                    "oscillation_up": 3,    # 3% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB fold to BTN Steal": {
                    "ideal": 77,
                    "oscillation_up": 3,    # 3% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB resteal vs BTN": {
                    "ideal": 14,
                    "oscillation_up": 18,   # 18% para cima
                    "oscillation_down": 3   # 3% para baixo
                }
            },
            "PKO": {
                "SB fold to CO Steal": {
                    "ideal": 70,
                    "oscillation_up": 1.5,  # 1.5% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB fold to BTN Steal": {
                    "ideal": 65,
                    "oscillation_up": 1.5,  # 1.5% para cima
                    "oscillation_down": 3   # 3% para baixo
                },
                "SB resteal vs BTN": {
                    "ideal": 15,
                    "oscillation_up": 18,   # 18% para cima
                    "oscillation_down": 3   # 3% para baixo
                }
            }
        }
        
        # Weights for each statistic in the SB defense subgroup
        self.weights = {
            "SB fold to CO Steal": 0.40,    # 40% weight
            "SB fold to BTN Steal": 0.40,   # 40% weight
            "SB resteal vs BTN": 0.20       # 20% weight
        }
    
    def calculate_group_score(self, stats: Dict[str, Any], table_format: str) -> Dict[str, Any]:
        """Calculate SB defense group score with format-specific ideal values"""
        
        # Validate format
        if table_format not in self.ideal_values:
            return {
                "score": 0,
                "details": {},
                "error": f"Unknown format: {table_format}"
            }
        
        format_ideals = self.ideal_values[table_format]
        stat_scores = {}
        weighted_sum = 0
        total_weight = 0
        
        # Define which stats belong to this group
        sb_defense_stats = ["SB fold to CO Steal", "SB fold to BTN Steal", "SB resteal vs BTN"]
        
        for stat_name in sb_defense_stats:
            if stat_name in stats and stat_name in format_ideals:
                stat_data = stats[stat_name]
                
                # Skip if no opportunities
                if stat_data.get("opportunities", 0) == 0:
                    continue
                
                # Calculate percentage
                attempts = stat_data.get("attempts", 0)
                opportunities = stat_data["opportunities"]
                percentage = (attempts / opportunities) * 100 if opportunities > 0 else 0
                
                # Get ideal values for this stat and format
                ideal_config = format_ideals[stat_name]
                
                # Calculate score using the oscillation system
                score = self.calculator.calculate_single_score(
                    percentage, 
                    {
                        "ideal": ideal_config["ideal"],
                        "oscillation_up": ideal_config["oscillation_up"],
                        "oscillation_down": ideal_config["oscillation_down"]
                    }
                )
                
                # Store individual stat score
                stat_scores[stat_name] = {
                    "percentage": round(percentage, 2),
                    "score": round(score, 2),
                    "ideal": ideal_config["ideal"],
                    "opportunities": opportunities,
                    "attempts": attempts,
                    "weight": self.weights[stat_name]
                }
                
                # Add to weighted sum
                weight = self.weights[stat_name]
                weighted_sum += score * weight
                total_weight += weight
        
        # Calculate final weighted score
        final_score = weighted_sum / total_weight if total_weight > 0 else 0
        
        return {
            "score": round(final_score, 2),
            "details": stat_scores,
            "group": "SB Defense",
            "format": table_format,
            "total_weight": round(total_weight, 2)
        }