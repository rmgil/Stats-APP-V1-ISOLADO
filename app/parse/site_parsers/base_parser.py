"""Base parser class for all poker site parsers."""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
import re


class BaseParser(ABC):
    """Abstract base class for site-specific poker hand parsers."""
    
    def __init__(self):
        self.site_name = "Unknown"
        self.currency_symbols = ['$', '€', '£', '¥']
        
    @abstractmethod
    def can_parse(self, text: str) -> bool:
        """Check if this parser can handle the given text format."""
        pass
    
    @abstractmethod
    def is_tournament(self, text: str) -> bool:
        """Check if the hand is from a tournament (not cash game)."""
        pass
    
    @abstractmethod
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        pass
    
    @abstractmethod
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all relevant information from a hand.
        
        Returns a dict with:
        - hand_id: unique identifier
        - tournament_id: tournament identifier (if tournament)
        - is_pko: whether it's a PKO/Progressive KO tournament
        - table_size: max players at table (6 or 9)
        - button_seat: seat number of the button
        - players: list of player dicts with name, seat, stack, bounty
        - hero: name of the hero player
        - hero_cards: tuple of hero's hole cards
        - blinds: dict with sb and bb amounts
        - ante: ante amount
        - actions: list of action dicts by street
        - board: community cards
        - winners: list of winners
        """
        pass
    
    @abstractmethod
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract the hero's name from the hand text."""
        pass
    
    @abstractmethod
    def extract_actions(self, hand_text: str) -> Dict[str, List[Dict]]:
        """Extract all actions by street.
        
        Returns dict with keys: 'preflop', 'flop', 'turn', 'river'
        Each value is a list of action dicts with:
        - player: player name
        - action: action type (fold, call, raise, check, bet, all-in)
        - amount: amount if applicable
        """
        pass
    
    def normalize_action(self, action: str, amount: Optional[float] = None) -> Tuple[str, Optional[float]]:
        """Normalize action types across different sites."""
        action_lower = action.lower()
        
        # Normalize to standard action types
        if 'fold' in action_lower:
            return 'fold', None
        elif 'check' in action_lower:
            return 'check', None
        elif any(x in action_lower for x in ['call', 'limp']):
            return 'call', amount
        elif any(x in action_lower for x in ['raise', 'bet']):
            return 'raise', amount
        elif 'all' in action_lower and 'in' in action_lower:
            return 'all-in', amount
        else:
            return action, amount
    
    def is_pko_tournament(self, hand_text: str) -> bool:
        """Check if tournament is PKO/Progressive KO/Bounty (but not Mystery)."""
        text_lower = hand_text.lower()
        
        # Mystery tournaments are NOT PKO
        if 'mystery' in text_lower:
            return False
            
        pko_patterns = [
            r'bounty',
            r'knockout',
            r'\bko\b',
            r'pko',
            r'progressive',
            r'€\s*\d+\s*€\s*\d+',  # Winamax format with double buy-in
            r'\$\d+\+\$\d+\+\$\d+',  # PokerStars PKO format
        ]
        
        return any(re.search(pattern, text_lower, re.IGNORECASE) for pattern in pko_patterns)
    
    def extract_tournament_id(self, hand_text: str) -> Optional[str]:
        """Extract tournament ID from hand text."""
        # Each site will override with specific pattern
        return None
    
    def clean_player_name(self, name: str) -> str:
        """Clean and standardize player names."""
        return name.strip()
    
    def parse_amount(self, text: str) -> float:
        """Parse monetary amount from text, removing currency symbols."""
        # Remove currency symbols and commas
        cleaned = re.sub(r'[€$£¥,]', '', text)
        cleaned = cleaned.replace(' ', '')
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    
    def _mark_mathematical_allins(self, players: List[Dict], ante: float, actions: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Mark all-in actions based on mathematical stack calculations.
        
        CRITICAL: Ante does NOT count as available stack - it's already committed at hand start.
        If a player has 8,845 chips and raises 8,785, they're all-in (60 remaining is just the ante).
        
        Args:
            players: List of player dicts with 'name' and 'stack'
            ante: Ante amount per player
            actions: Dict of actions by street {'preflop': [...], 'flop': [...], ...}
            
        Returns:
            Updated actions dict with is_allin correctly marked
        """
        # Initialize available stacks (starting stack - ante)
        available_stacks = {}
        for player in players:
            available_stacks[player['name']] = player['stack'] - ante
        
        # Track committed amounts in current betting round
        committed_in_round = {}
        
        # Process each street
        for street in ['preflop', 'flop', 'turn', 'river']:
            if street not in actions:
                continue
            
            # Reset committed amounts for new street (except preflop which includes blinds/ante)
            if street != 'preflop':
                committed_in_round = {name: 0 for name in available_stacks}
            
            for action in actions[street]:
                player_name = action['player']
                action_type = action['action']
                amount = action.get('amount', 0) or 0
                
                # Skip if player not in our stack tracking (defensive)
                if player_name not in available_stacks:
                    continue
                
                # For actions that commit chips (call, raise, bet)
                if action_type in ['call', 'raise', 'bet', 'all-in'] and amount > 0:
                    available_stack = available_stacks[player_name]
                    
                    # Calculate additional chips needed for this action
                    # (total amount to reach minus what already committed in round)
                    already_committed = committed_in_round.get(player_name, 0)
                    additional_needed = amount - already_committed
                    
                    # CRITICAL: If additional_needed >= available_stack, it's an all-in
                    # Allow for small rounding errors (0.01 tolerance)
                    if additional_needed >= available_stack - 0.01:
                        action['is_allin'] = True
                    
                    # Update tracking
                    available_stacks[player_name] -= additional_needed
                    committed_in_round[player_name] = amount
        
        return actions