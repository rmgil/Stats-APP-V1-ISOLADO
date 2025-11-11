"""
Base class for opportunity validators (preflop + postflop).
Provides shared utilities for action normalization, all-in detection, and stack validation.

Created: Nov 11, 2025
Purpose: Share common validation logic between preflop and postflop validators
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ValidatorBase:
    """
    Base validator with shared logic for preflop and postflop validators.
    
    Provides:
    - Action normalization (fix all-in standalone detection)
    - All-in blocking detection (raise/bet blocks, call/limp doesn't)
    - Stack validation utilities
    - Logging helpers
    """
    
    def __init__(self, hand_id: Optional[str] = None):
        """
        Initialize validator.
        
        Args:
            hand_id: Optional hand ID for logging context
        """
        self.hand_id = hand_id
    
    def normalize_action(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize action dictionary to ensure consistent is_raise, is_call, is_fold flags.
        
        CRITICAL FIX: All-in actions without explicit call/fold should be treated as raises
        because call all-ins always have "calls" in the action text.
        
        This handles site-specific quirks where all-in standalone actions need to be
        classified correctly as raises vs. calls.
        
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
        because the villain cannot act after calling all-in.
        
        This is used across all stats (preflop + postflop) to filter out scenarios
        where Hero's decision is forced/limited by a prior all-in.
        
        Args:
            actions: List of action dictionaries (should be normalized first)
            
        Returns:
            True if blocking all-in (raise/bet) occurred before Hero
            False if no blocking all-in or Hero acts first
        """
        for action in actions:
            # Stop when we reach Hero
            if action.get("player") == "Hero":
                return False  # No blocking all-in before Hero
            
            # Normalize action to ensure correct flags
            normalized = self.normalize_action(action)
            
            # Only block if all-in AND raise/bet (not call/limp all-in)
            if normalized.get("is_allin", False) and normalized.get("is_raise", False):
                if self.hand_id:
                    logger.debug(
                        f"[VALIDATOR] Hand {self.hand_id}: All-in RAISE/BET before Hero "
                        f"by {action.get('player')} - {action.get('action')}"
                    )
                return True  # Blocking all-in raise/bet occurred before Hero
        
        return False
    
    def validate_stack(
        self, 
        stack_bb: float, 
        min_bb: float = 16.0, 
        player_name: str = ""
    ) -> bool:
        """
        Validate single player stack meets minimum threshold.
        
        Used across all stats to enforce ≥16bb requirement for Hero and relevant opponents.
        
        Args:
            stack_bb: Stack in big blinds
            min_bb: Minimum threshold (default 16bb)
            player_name: Player name for logging
        
        Returns:
            True if stack >= min_bb, False otherwise
        """
        if stack_bb < min_bb:
            if self.hand_id and player_name:
                logger.debug(
                    f"[VALIDATOR] Hand {self.hand_id}: {player_name} stack "
                    f"{stack_bb:.1f}bb < {min_bb}bb threshold"
                )
            return False
        return True
    
    def log_validation(self, stat_name: str, valid: bool, reason: str = ""):
        """
        Log validation result for debugging.
        
        Args:
            stat_name: Name of the stat being validated
            valid: Whether validation passed
            reason: Reason for rejection (if not valid)
        """
        if valid:
            logger.debug(f"[VALIDATOR] Hand {self.hand_id}: {stat_name} ALLOWED")
        else:
            logger.info(f"[VALIDATOR] Hand {self.hand_id}: {stat_name} REJECTED - {reason}")
