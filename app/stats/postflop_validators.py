"""
Postflop opportunity validator with stack validation (≥16bb) and all-in detection.
Mirrors the preflop validation architecture for consistency.

Created: Nov 11, 2025
Purpose: Add stack/all-in validations to postflop stats
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from app.stats.validator_base import ValidatorBase

logger = logging.getLogger(__name__)


class PostflopOpportunityValidator(ValidatorBase):
    """
    Validator for postflop opportunities with stack/all-in rules.
    
    Applies ≥16bb stack validation and all-in detection to all 20 postflop stats,
    ensuring consistency with preflop validation architecture.
    """
    
    # Rule map: stat_name → (required actors, allow_allin)
    # Actors can be: hero, villain, bettor, aggressor, pfr, raiser, caller
    # CRITICAL: Keys must match EXACTLY the stat names in PostflopCalculatorV4.stats dict
    STAT_RULES = {
        # Flop CBet Group
        "Flop CBet IP %": {"actors": ["hero", "villain"], "allow_allin": False},
        "Flop CBet 3BetPot IP": {"actors": ["hero", "villain"], "allow_allin": False},
        "Flop CBet OOP%": {"actors": ["hero", "villain"], "allow_allin": False},
        
        # Vs CBet Group
        "Flop fold vs Cbet IP": {"actors": ["hero", "bettor"], "allow_allin": False},
        "Flop raise Cbet IP": {"actors": ["hero", "bettor"], "allow_allin": False},
        "Flop raise Cbet OOP": {"actors": ["hero", "bettor"], "allow_allin": False},
        "Fold vs Check Raise": {"actors": ["hero", "raiser"], "allow_allin": False},
        
        # Vs Skipped CBet
        "Flop bet vs missed Cbet SRP": {"actors": ["hero", "pfr"], "allow_allin": False},
        
        # Turn Play
        "Turn CBet IP%": {"actors": ["hero", "villain"], "allow_allin": False},
        "Turn Cbet OOP%": {"actors": ["hero", "villain"], "allow_allin": False},
        "Turn donk bet": {"actors": ["hero", "aggressor"], "allow_allin": False},
        "Turn donk bet SRP vs PFR": {"actors": ["hero", "pfr"], "allow_allin": False},
        "Bet turn vs Missed Flop Cbet OOP SRP": {"actors": ["hero", "pfr"], "allow_allin": False},
        "Turn Fold vs CBet OOP": {"actors": ["hero", "bettor"], "allow_allin": False},
        
        # River & Showdown (ALIGNED with stats dict keys)
        "WTSD%": {"actors": ["hero"], "allow_allin": True},
        "W$SD%": {"actors": ["hero"], "allow_allin": True},
        "W$SD% B River": {"actors": ["hero"], "allow_allin": True},
        "W$WSF Rating": {"actors": ["hero", "villain"], "allow_allin": False},  # FIXED: was "W$WSF"
        "River Agg %": {"actors": ["hero", "villain"], "allow_allin": False},  # FIXED: was "River Agg"
        "River bet - Single Rsd Pot": {"actors": ["hero", "caller"], "allow_allin": False},  # FIXED: was "River bet Single Raised Pot"
    }
    
    def validate_cbet(
        self, 
        hero_stack_bb: float,
        villain_stack_bb: float,
        street_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate CBet opportunity (IP or OOP, flop or turn).
        
        Requirements:
        - Hero ≥16bb
        - Villain (who can respond) ≥16bb
        - No all-in raise/bet before Hero acts
        
        Args:
            hero_stack_bb: Hero's stack in BB
            villain_stack_bb: Villain's stack in BB
            street_actions: Actions on this street
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        # 1. Check all-in before Hero
        if self.check_allin_before_hero(street_actions):
            return False, "all-in raise/bet before hero"
        
        # 2. Validate Hero stack
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        # 3. Validate Villain stack
        if not self.validate_stack(villain_stack_bb, player_name="Villain"):
            return False, f"villain stack {villain_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_vs_cbet(
        self,
        hero_stack_bb: float,
        bettor_stack_bb: float,
        street_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate vs CBet opportunity (fold/raise/call facing cbet).
        
        Requirements:
        - Hero ≥16bb
        - Bettor ≥16bb
        - No all-in raise/bet before Hero acts
        
        Args:
            hero_stack_bb: Hero's stack in BB
            bettor_stack_bb: Bettor's stack in BB
            street_actions: Actions on this street
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        # 1. All-in check
        if self.check_allin_before_hero(street_actions):
            return False, "all-in raise/bet before hero"
        
        # 2. Hero stack
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        # 3. Bettor stack
        if not self.validate_stack(bettor_stack_bb, player_name="Bettor"):
            return False, f"bettor stack {bettor_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_donk(
        self,
        hero_stack_bb: float,
        aggressor_stack_bb: float,
        street_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate donk bet opportunity.
        
        Requirements:
        - Hero ≥16bb
        - Prior street aggressor ≥16bb
        - No all-in on this street before Hero
        
        Args:
            hero_stack_bb: Hero's stack in BB
            aggressor_stack_bb: Prior aggressor's stack in BB
            street_actions: Actions on this street
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        if self.check_allin_before_hero(street_actions):
            return False, "all-in raise/bet before hero"
        
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        if not self.validate_stack(aggressor_stack_bb, player_name="Aggressor"):
            return False, f"aggressor stack {aggressor_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_vs_missed_cbet(
        self,
        hero_stack_bb: float,
        pfr_stack_bb: float,
        street_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate opportunity when PFR misses cbet and Hero bets.
        
        Requirements:
        - Hero ≥16bb
        - PFR (who checked) ≥16bb
        - No all-in before Hero
        
        Args:
            hero_stack_bb: Hero's stack in BB
            pfr_stack_bb: PFR's stack in BB
            street_actions: Actions on this street
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        if self.check_allin_before_hero(street_actions):
            return False, "all-in raise/bet before hero"
        
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        if not self.validate_stack(pfr_stack_bb, player_name="PFR"):
            return False, f"pfr stack {pfr_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_check_raise(
        self,
        hero_stack_bb: float,
        bettor_stack_bb: float,
        street_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate check-raise opportunity.
        
        Requirements:
        - Hero ≥16bb
        - Bettor (who bet after Hero checked) ≥16bb
        - No all-in before Hero's check-raise
        
        Args:
            hero_stack_bb: Hero's stack in BB
            bettor_stack_bb: Bettor's stack in BB
            street_actions: Actions on this street
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        if self.check_allin_before_hero(street_actions):
            return False, "all-in raise/bet before hero"
        
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        if not self.validate_stack(bettor_stack_bb, player_name="Bettor"):
            return False, f"bettor stack {bettor_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_showdown(
        self,
        hero_stack_bb: float,
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate showdown stats (WTSD%, W$SD%).
        
        Requirements:
        - Hero ≥16bb (only requirement)
        - All-in allowed (showdown happens regardless)
        
        Args:
            hero_stack_bb: Hero's stack in BB
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def validate_river_action(
        self,
        hero_stack_bb: float,
        opponent_stack_bb: float,
        river_actions: List[Dict],
        stat_name: str
    ) -> Tuple[bool, str]:
        """
        Validate river action opportunity (aggression, betting, etc.).
        
        Requirements:
        - Hero ≥16bb
        - Opponent facing decision ≥16bb
        - No all-in before Hero (for most stats)
        
        Args:
            hero_stack_bb: Hero's stack in BB
            opponent_stack_bb: Opponent's stack in BB
            river_actions: Actions on river
            stat_name: Name of the stat (for logging)
            
        Returns:
            (valid: bool, reason: str)
        """
        # Allow all-in for some showdown stats
        if "W$SD" not in stat_name and "WTSD" not in stat_name:
            if self.check_allin_before_hero(river_actions):
                return False, "all-in raise/bet before hero"
        
        if not self.validate_stack(hero_stack_bb, player_name="Hero"):
            return False, f"hero stack {hero_stack_bb:.1f}bb < 16bb"
        
        if not self.validate_stack(opponent_stack_bb, player_name="Opponent"):
            return False, f"opponent stack {opponent_stack_bb:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
    
    def _get_opponent_stack(self, context: Dict[str, Any], actor_type: str) -> float:
        """
        Get opponent stack from context based on actor type.
        
        Maps actor types to context keys:
        - villain → villain_stack_bb
        - bettor → bettor_stack_bb
        - aggressor → aggressor_stack_bb
        - pfr → pfr_stack_bb
        - raiser → raiser_stack_bb
        - caller → caller_stack_bb
        
        Args:
            context: Validation context dict
            actor_type: Type of actor (villain, bettor, etc.)
            
        Returns:
            Stack in BB, or 0 if not found
        """
        key_map = {
            'villain': 'villain_stack_bb',
            'bettor': 'bettor_stack_bb',
            'aggressor': 'aggressor_stack_bb',
            'pfr': 'pfr_stack_bb',
            'raiser': 'raiser_stack_bb',
            'caller': 'caller_stack_bb'
        }
        
        key = key_map.get(actor_type.lower())
        if key:
            return context.get(key, 0)
        
        # Fallback: try direct key
        return context.get(f'{actor_type}_stack_bb', 0)
    
    def validate(
        self,
        stat_name: str,
        context: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Generic validation dispatcher using STAT_RULES mapping.
        
        Routes to appropriate validation method based on stat name and required actors.
        Uses STAT_RULES to determine which opponent stacks to validate and all-in policy.
        
        Args:
            stat_name: Name of the stat being validated
            context: Dict with validation context:
                - hero_stack_bb: float (required)
                - villain_stack_bb, bettor_stack_bb, aggressor_stack_bb, etc. (optional)
                - street_actions: List[Dict] (optional, for all-in check)
                - river_actions: List[Dict] (optional, for river stats)
        
        Returns:
            (valid: bool, reason: str)
        """
        hero_stack = context.get('hero_stack_bb', 0)
        street_actions = context.get('street_actions', [])
        river_actions = context.get('river_actions', [])
        
        # Get rule for this stat
        rule = self.STAT_RULES.get(stat_name)
        
        if not rule:
            # No specific rule - default to Hero ≥16bb only
            if not self.validate_stack(hero_stack, player_name="Hero"):
                return False, f"hero stack {hero_stack:.1f}bb < 16bb"
            return True, "no specific rule (default validation)"
        
        # Extract required actors and all-in policy
        required_actors = rule['actors']
        allow_allin = rule.get('allow_allin', False)
        
        # 1. Check all-in if not allowed
        if not allow_allin:
            actions_to_check = river_actions if 'river' in stat_name.lower() else street_actions
            if self.check_allin_before_hero(actions_to_check):
                return False, "all-in raise/bet before hero"
        
        # 2. Validate Hero stack (always required)
        if not self.validate_stack(hero_stack, player_name="Hero"):
            return False, f"hero stack {hero_stack:.1f}bb < 16bb"
        
        # 3. Validate opponent stacks based on required actors
        for actor in required_actors:
            if actor.lower() == 'hero':
                continue  # Already validated
            
            opponent_stack = self._get_opponent_stack(context, actor)
            
            # If stack is 0, it might be missing data - be lenient
            if opponent_stack == 0:
                logger.warning(
                    f"[VALIDATOR] Hand {self.hand_id}: {stat_name} - "
                    f"{actor} stack missing in context (lenient: allowing)"
                )
                continue
            
            # Validate opponent stack
            if not self.validate_stack(opponent_stack, player_name=actor.capitalize()):
                return False, f"{actor} stack {opponent_stack:.1f}bb < 16bb"
        
        self.log_validation(stat_name, True)
        return True, "validated"
