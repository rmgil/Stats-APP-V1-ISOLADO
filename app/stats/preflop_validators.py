"""
Centralized validator for preflop statistics opportunities.
Normalizes actions and enforces stack/all-in rules across all stats.

Created: Oct 20, 2025
Purpose: Eliminate validation duplications and fix multisite action classification bugs
"""
import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class PreflopOpportunityValidator:
    """
    Centralized validator for preflop stat opportunities.
    
    Handles:
    - Action normalization (fix all-in standalone detection)
    - Stack validations per stat type (≥16bb rules)
    - All-in blocking rules (raise/bet blocks, call/limp doesn't)
    """
    
    def __init__(self, hand_id: Optional[str] = None):
        """
        Initialize validator.
        
        Args:
            hand_id: Optional hand ID for logging
        """
        self.hand_id = hand_id
    
    def normalize_action(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize action dictionary to ensure consistent is_raise, is_call flags.
        
        CRITICAL FIX: All-in actions without explicit call/fold should be treated as raises
        because call all-ins always have "calls" in the action text.
        
        Args:
            action: Raw action dict from parser or extractor
            
        Returns:
            Normalized action dict with correct is_raise, is_call, is_fold flags
        """
        action_type = action.get('action', '').lower()
        is_allin = action.get('is_allin', False)
        
        # Detect is_call first (before is_raise)
        is_call = action.get('is_call', False)
        if not is_call:
            is_call = 'call' in action_type or 'limp' in action_type
        
        # Detect is_fold
        is_fold = action.get('is_fold', False)
        if not is_fold:
            is_fold = 'fold' in action_type
        
        # Detect is_raise
        is_raise = action.get('is_raise', False)
        if not is_raise:
            # Check action type string
            is_raise = 'raise' in action_type or 'bet' in action_type
            
            # CRITICAL FIX: If is_allin is True and NOT call/fold, treat as raise
            # This covers all all-in variants: 'all-in', 'allin', 'allin shove', etc.
            # Logic: call all-ins always have "call" in text, fold all-ins have "fold"
            # So any other all-in must be a raise/bet all-in
            if is_allin and not is_call and not is_fold:
                is_raise = True
        
        return {
            **action,  # Preserve original fields
            'is_raise': is_raise,
            'is_call': is_call,
            'is_fold': is_fold,
            'is_allin': is_allin
        }
    
    def check_allin_before_hero(self, actions: List[Dict]) -> bool:
        """
        Check if there was an all-in RAISE/BET before Hero acted.
        
        CRITICAL: All-in calls/limps are allowed (they don't block opportunities)
        because the villain cannot act after calling.
        
        Args:
            actions: List of action dictionaries (must be normalized)
            
        Returns:
            True if blocking all-in (raise/bet) occurred before Hero
        """
        for action in actions:
            # Stop when we reach Hero
            if action.get("player") == "Hero":
                return False  # No blocking all-in before Hero
            
            # Only block if all-in AND raise/bet (not call/limp all-in)
            if action.get("is_allin", False) and action.get("is_raise", False):
                if self.hand_id:
                    logger.debug(f"[VALIDATOR] Hand {self.hand_id}: All-in RAISE/BET before Hero by {action.get('player')}")
                return True  # Blocking all-in raise/bet occurred before Hero
        
        return False
    
    def validate_rfi_steal(
        self,
        actions: List[Dict],
        hero_position: str,
        stacks_bb: Dict[str, float],
        positions: Dict[str, str]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate RFI/Steal opportunity.
        
        Rules:
        - Hero stack ≥16bb
        - At least one stack acting AFTER hero ≥16bb
        - No all-in raise/bet before Hero
        
        Args:
            actions: Normalized action list
            hero_position: Hero's position
            stacks_bb: Player stacks in BB
            positions: Player positions
            
        Returns:
            (is_valid, reason) - reason is None if valid, error message if invalid
        """
        # VALIDATION 1: Check for all-in before Hero acts
        if self.check_allin_before_hero(actions):
            return False, "All-in RAISE/BET before Hero"
        
        # VALIDATION 2: Hero stack >= 16bb
        hero_stack = stacks_bb.get("Hero", 0)
        if hero_stack < 16.0:
            logger.info(f"[AUDIT] Hand {self.hand_id}: RFI REJECTED - Hero stack {hero_stack:.1f}bb < 16bb")
            return False, f"Hero stack {hero_stack:.1f}bb < 16bb"
        
        # VALIDATION 3: At least one stack acting after hero >= 16bb
        # LENIENT RULE: If we cannot enumerate opponent stacks (888poker parsing quirks),
        # allow the opportunity. This restores pre-ab5bd5c behavior for 888poker.
        pos_order_9max = ["EP", "EP2", "MP1", "MP2", "MP3", "HJ", "CO", "BTN", "SB", "BB"]
        pos_order_6max = ["EP", "MP", "CO", "BTN", "SB", "BB"]
        
        # Find hero's index in position order
        positions_after = []
        try:
            hero_idx = pos_order_9max.index(hero_position)
            positions_after = pos_order_9max[hero_idx + 1:]
        except ValueError:
            try:
                hero_idx = pos_order_6max.index(hero_position)
                positions_after = pos_order_6max[hero_idx + 1:]
            except ValueError:
                # Position not recognized, allow by default
                logger.info(f"[AUDIT] Hand {self.hand_id}: RFI ALLOWED - Position {hero_position} not recognized")
                return True, None
        
        # Get stacks of players still to act
        stacks_after = []
        for player, pos in positions.items():
            if player != "Hero" and pos in positions_after:
                player_stack = stacks_bb.get(player, 0)
                if player_stack > 0:
                    stacks_after.append(player_stack)
        
        # AUDIT LOG: Show all available stack data
        logger.info(f"[AUDIT] Hand {self.hand_id}: RFI check - Hero {hero_position} {hero_stack:.1f}bb, positions_after={positions_after}, stacks_after={[f'{s:.1f}bb' for s in stacks_after]}, all_stacks={dict((k, f'{v:.1f}bb') for k, v in stacks_bb.items())}")
        
        # LENIENT FALLBACK: If no opponent stacks found (888poker often has incomplete stack data),
        # allow the opportunity by default (pre-ab5bd5c behavior)
        if not stacks_after:
            logger.warning(f"[AUDIT] Hand {self.hand_id}: RFI LENIENT FALLBACK USED - No opponent stacks found, allowing opportunity")
            return True, None
        
        # If we have opponent stacks, check if at least one has >= 16bb
        has_valid_stack = any(stack >= 16.0 for stack in stacks_after)
        if not has_valid_stack:
            logger.info(f"[AUDIT] Hand {self.hand_id}: RFI REJECTED - No opponent after Hero has >=16bb (stacks: {[f'{s:.1f}bb' for s in stacks_after]})")
            return False, f"No player after hero has >= 16bb (stacks: {stacks_after})"
        
        logger.info(f"[AUDIT] Hand {self.hand_id}: RFI ALLOWED - Valid stacks found")
        return True, None
    
    def validate_3bet_defense(
        self,
        actions: List[Dict],
        raiser_name: Optional[str],
        stacks_bb: Dict[str, float]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate 3bet/Cold Call/Defense opportunity.
        
        Rules:
        - Hero stack ≥16bb
        - Raiser stack ≥16bb
        - No all-in raise/bet before Hero
        
        Args:
            actions: Normalized action list
            raiser_name: Name of the original raiser
            stacks_bb: Player stacks in BB
            
        Returns:
            (is_valid, reason) - reason is None if valid, error message if invalid
        """
        # VALIDATION 1: Check for all-in before Hero acts
        if self.check_allin_before_hero(actions):
            return False, "All-in RAISE/BET before Hero"
        
        # VALIDATION 2: Hero stack >= 16bb
        hero_stack = stacks_bb.get("Hero", 0)
        if hero_stack < 16.0:
            logger.info(f"[AUDIT] Hand {self.hand_id}: 3BET/CC REJECTED - Hero stack {hero_stack:.1f}bb < 16bb")
            return False, f"Hero stack {hero_stack:.1f}bb < 16bb"
        
        # VALIDATION 3: Raiser stack >= 16bb (LENIENT: allow if raiser stack unknown)
        raiser_stack = 0
        if raiser_name:
            raiser_stack = stacks_bb.get(raiser_name, 0)
            # If raiser stack is 0 (missing data), allow by default (888poker quirk)
            if raiser_stack > 0 and raiser_stack < 16.0:
                logger.info(f"[AUDIT] Hand {self.hand_id}: 3BET/CC REJECTED - Raiser ({raiser_name}) stack {raiser_stack:.1f}bb < 16bb")
                return False, f"Raiser ({raiser_name}) stack {raiser_stack:.1f}bb < 16bb"
            
            if raiser_stack == 0:
                logger.warning(f"[AUDIT] Hand {self.hand_id}: 3BET/CC LENIENT FALLBACK - Raiser ({raiser_name}) stack unknown, allowing")
        
        logger.info(f"[AUDIT] Hand {self.hand_id}: 3BET/CC ALLOWED - Hero {hero_stack:.1f}bb, Raiser ({raiser_name}) {raiser_stack:.1f}bb")
        return True, None
    
    def validate_squeeze(
        self,
        actions: List[Dict],
        raiser_name: Optional[str],
        caller_names: List[str],
        stacks_bb: Dict[str, float]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate Squeeze opportunity.
        
        Rules:
        - Hero stack ≥16bb
        - At least one of (raiser + callers) stacks ≥16bb
        - No all-in raise/bet before Hero
        
        Args:
            actions: Normalized action list
            raiser_name: Name of the original raiser
            caller_names: Names of players who called the raise
            stacks_bb: Player stacks in BB
            
        Returns:
            (is_valid, reason) - reason is None if valid, error message if invalid
        """
        # VALIDATION 1: Check for all-in before Hero acts
        if self.check_allin_before_hero(actions):
            return False, "All-in RAISE/BET before Hero"
        
        # VALIDATION 2: Hero stack >= 16bb
        hero_stack = stacks_bb.get("Hero", 0)
        if hero_stack < 16.0:
            return False, f"Hero stack {hero_stack:.1f}bb < 16bb"
        
        # VALIDATION 3: At least one of (raiser + callers) >= 16bb
        # LENIENT: allow if no stack data found (888poker quirk)
        if raiser_name and caller_names:
            raiser_stack = stacks_bb.get(raiser_name, 0)
            caller_stacks = [stacks_bb.get(caller, 0) for caller in caller_names]
            all_stacks = [s for s in [raiser_stack] + caller_stacks if s > 0]
            
            # LENIENT FALLBACK: If no stacks found, allow by default
            if not all_stacks:
                return True, None
            
            # If we have stacks, check at least one >= 16bb
            has_valid_stack = any(stack >= 16.0 for stack in all_stacks)
            if not has_valid_stack:
                return False, f"No raiser/caller has >= 16bb (stacks: {all_stacks})"
        
        return True, None
    
    def validate_fold_to_3bet(
        self,
        actions_before_raise: List[Dict],
        threebetter_name: Optional[str],
        threebetter_action: Optional[Dict],
        stacks_bb: Dict[str, float]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate Fold to 3bet opportunity.
        
        Rules:
        - Hero stack ≥16bb
        - 3-bettor stack ≥16bb
        - 3bet action itself cannot be all-in
        - No all-in raise/bet before Hero's initial raise
        
        Args:
            actions_before_raise: Actions before Hero raised (for checking prior all-in)
            threebetter_name: Name of the 3-bettor
            threebetter_action: The 3bet action dictionary (normalized)
            stacks_bb: Player stacks in BB
            
        Returns:
            (is_valid, reason) - reason is None if valid, error message if invalid
        """
        # VALIDATION 1: Check for all-in before Hero raised
        if self.check_allin_before_hero(actions_before_raise):
            return False, "All-in RAISE/BET before Hero raised"
        
        # VALIDATION 2: Hero stack >= 16bb
        hero_stack = stacks_bb.get("Hero", 0)
        if hero_stack < 16.0:
            return False, f"Hero stack {hero_stack:.1f}bb < 16bb"
        
        # VALIDATION 3: 3-bettor stack >= 16bb
        if threebetter_name:
            threebetter_stack = stacks_bb.get(threebetter_name, 0)
            if threebetter_stack < 16.0:
                return False, f"3-bettor ({threebetter_name}) stack {threebetter_stack:.1f}bb < 16bb"
        
        # VALIDATION 4: The 3bet action itself CANNOT be all-in
        if threebetter_action and threebetter_action.get("is_allin", False):
            return False, "3bet was all-in (no decision for Hero)"
        
        return True, None
    
    def validate_bvb(
        self,
        actions: List[Dict],
        villain_player: str,
        stacks_bb: Dict[str, float]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate Blind vs Blind opportunity.
        
        Rules:
        - Hero stack ≥16bb
        - Villain (other blind) stack ≥16bb
        - No all-in raise/bet before Hero
        
        Args:
            actions: Normalized action list
            villain_player: Name of the villain blind
            stacks_bb: Player stacks in BB
            
        Returns:
            (is_valid, reason) - reason is None if valid, error message if invalid
        """
        # VALIDATION 1: Check for all-in before Hero acts
        if self.check_allin_before_hero(actions):
            return False, "All-in RAISE/BET before Hero"
        
        # VALIDATION 2: Hero stack >= 16bb
        hero_stack = stacks_bb.get("Hero", 0)
        if hero_stack < 16.0:
            return False, f"Hero stack {hero_stack:.1f}bb < 16bb"
        
        # VALIDATION 3: Villain (other blind) stack >= 16bb
        villain_stack = stacks_bb.get(villain_player, 0)
        if villain_stack < 16.0:
            return False, f"Villain ({villain_player}) stack {villain_stack:.1f}bb < 16bb"
        
        return True, None
