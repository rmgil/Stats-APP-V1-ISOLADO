"""
Score calculator that evaluates statistics against ideal values.
Generates scores from 0-100 based on deviation from ideal.
"""

from app.stats.scoring_config import get_stat_config
import math

class ScoringCalculator:
    def __init__(self):
        self.scores = {}
        
    def calculate_single_score(self, actual_percentage: float, config: dict) -> float:
        """
        Calculate score for a single statistic based on its configuration.
        Linear scoring with discrete steps: each oscillation step (as % of ideal) reduces score by 10 points.
        
        Example for ideal=19%, osc_down=2%:
        - 2% of 19 = 0.38
        - > 18.62 to 19.00 = 100 points
        - >= 18.24 to < 18.62 = 90 points  
        - >= 17.86 to < 18.24 = 80 points
        - etc.
        
        Args:
            actual_percentage: The actual percentage achieved
            config: Configuration dict with ideal, oscillation_down, oscillation_up
            
        Returns:
            Score from 0 to 100
        """
        if config is None:
            return 0
            
        ideal = config['ideal']
        osc_down_percent = config['oscillation_down']
        osc_up_percent = config['oscillation_up']
        
        # Calculate the actual oscillation values based on percentage of ideal
        osc_down_value = ideal * (osc_down_percent / 100)
        osc_up_value = ideal * (osc_up_percent / 100)
        
        # Calculate deviation from ideal
        deviation = actual_percentage - ideal
        
        # Special case: exactly at ideal or very close
        if abs(deviation) < 0.001:
            return 100.0
        
        if deviation < 0:
            # Below ideal - use oscillation_down
            abs_deviation = abs(deviation)
            
            # First check if within first oscillation (still 100 points)
            if abs_deviation < osc_down_value:
                return 100.0
            
            # Calculate which tier we're in
            # Each osc_down_value width is one tier
            tier = int(abs_deviation / osc_down_value)
            
            # Score decreases by 10 for each tier
            score = 100 - (tier * 10)
            
        else:
            # Above ideal - use oscillation_up
            # First check if within first oscillation (still 100 points)
            if deviation < osc_up_value:
                return 100.0
            
            # Calculate which tier we're in
            tier = int(deviation / osc_up_value)
            
            # Score decreases by 10 for each tier
            score = 100 - (tier * 10)
        
        # Ensure score stays within 0-100 range
        return max(0, min(100, score))
    
    def calculate_detailed_score(self, actual_percentage: float, opportunities: int, 
                                config: dict) -> dict:
        """
        Calculate detailed score with additional context.
        
        Returns dict with:
        - score: 0-100 score
        - deviation: How far from ideal
        - trend: 'below', 'ideal', or 'above'
        - confidence: Based on sample size
        """
        if config is None:
            return {
                'score': 0,
                'deviation': 0,
                'trend': 'unknown',
                'confidence': 'low'
            }
        
        score = self.calculate_single_score(actual_percentage, config)
        ideal = config['ideal']
        deviation = actual_percentage - ideal
        
        # Determine trend
        if abs(deviation) < 0.5:
            trend = 'ideal'
        elif deviation < 0:
            trend = 'below'
        else:
            trend = 'above'
        
        # Calculate confidence based on sample size
        if opportunities >= 100:
            confidence = 'high'
        elif opportunities >= 50:
            confidence = 'medium'
        else:
            confidence = 'low'
        
        return {
            'score': round(score, 1),
            'deviation': round(deviation, 1),
            'trend': trend,
            'confidence': confidence,
            'ideal': ideal,
            'actual': round(actual_percentage, 1)
        }
    
    def calculate_group_scores(self, group_key: str, stats_data: dict) -> dict:
        """
        Calculate scores for all statistics in a group.
        
        Args:
            group_key: 'nonko_9max', 'nonko_6max', or 'pko'
            stats_data: Dict with stat names as keys, containing 'opportunities' and 'attempts'
            
        Returns:
            Dict with scores for each stat and overall score
        """
        results = {
            'stats': {},
            'overall_score': 0
        }
        
        total_weighted_score = 0
        total_weight = 0
        
        for stat_name, stat_data in stats_data.items():
            config = get_stat_config(group_key, stat_name)
            
            if config and stat_data.get('opportunities', 0) > 0:
                # Calculate percentage
                attempts = stat_data.get('attempts', 0)
                opportunities = stat_data.get('opportunities', 0)
                percentage = (attempts / opportunities) * 100 if opportunities > 0 else 0
                
                # Get detailed score
                detailed = self.calculate_detailed_score(percentage, opportunities, config)
                results['stats'][stat_name] = detailed
                
                # Add to weighted total
                weight = config.get('weight', 1.0)
                total_weighted_score += detailed['score'] * weight
                total_weight += weight
        
        # Calculate overall score
        if total_weight > 0:
            overall_score = total_weighted_score / total_weight
            results['overall_score'] = round(overall_score, 1)
        
        return results
    
    def get_score_color(self, score: float) -> str:
        """Get color code for score visualization."""
        if score >= 90:
            return '#22c55e'  # Green
        elif score >= 75:
            return '#84cc16'  # Light green
        elif score >= 60:
            return '#eab308'  # Yellow
        elif score >= 40:
            return '#f97316'  # Orange
        else:
            return '#ef4444'  # Red