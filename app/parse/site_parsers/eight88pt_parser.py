"""888.pt specific parser - handles Portuguese variant with different format."""
import re
from typing import List, Dict, Optional
from .eight88_parser import Eight88PokerParser


class Eight88PtParser(Eight88PokerParser):
    """Parser for 888.pt hand histories (Portuguese variant with different format)."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "888.pt"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is an 888.pt hand (not 888poker)."""
        return bool(re.search(r'888\.pt Hand History', text[:500]))
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from an 888.pt hand."""
        # Call parent method to get base info
        info = super().extract_hand_info(hand_text)
        
        # Override site name to distinguish from 888poker
        info['site'] = '888.pt'
        
        return info
    
    def _extract_players(self, hand_text: str) -> List[Dict]:
        """
        Extract player information from 888.pt format.
        888.pt uses: "Seat 1: Player ( 1500 )" instead of "Seat 1: Player (1500 in chips)"
        """
        players = []
        
        # 888.pt format: "Seat 1: PlayerName ( chips )"
        # Note: no "in chips" text, just parentheses with amount
        pattern = r'Seat\s+(\d+):\s+(.+?)\s+\(\s*([\d,.€]+)\s*\)'
        
        for match in re.finditer(pattern, hand_text):
            seat_num = int(match.group(1))
            player_name = match.group(2).strip()
            stack_str = match.group(3)
            
            # Normalize stack value (handle European number format and € symbol)
            stack = self._normalize_stack_value(stack_str)
            
            players.append({
                'name': player_name,
                'seat': seat_num,
                'stack': stack
            })
        
        return players
    
    def _normalize_stack_value(self, value: str) -> float:
        """
        Normalize stack value from 888.pt format.
        Handles: 30.000 (European format with . as thousands separator)
        """
        if not value:
            return 0.0
        
        # Remove € symbol and whitespace
        clean = value.replace('€', '').strip()
        
        # European format: dot is thousands separator, comma is decimal
        # Convert: 30.000 → 30000, 1.234,56 → 1234.56
        if ',' in clean:
            # Has comma - it's the decimal separator
            clean = clean.replace('.', '').replace(',', '.')
        elif '.' in clean:
            # Check if it's thousands or decimal
            parts = clean.split('.')
            if len(parts[-1]) == 3 and len(parts) > 1:
                # Format: 30.000 - dot is thousands separator
                clean = clean.replace('.', '')
            # else: format like 30.5 - dot is decimal, keep it
        
        try:
            return float(clean)
        except ValueError:
            return 0.0
    
    def extract_actions(self, hand_text: str) -> Dict[str, List[Dict]]:
        """
        Extract actions from 888.pt format.
        888.pt uses: "PlayerName folds" instead of "PlayerName: folds"
        """
        actions = {
            'preflop': [],
            'flop': [],
            'turn': [],
            'river': []
        }
        
        lines = hand_text.split('\n')
        current_street = None
        
        for line in lines:
            line = line.strip()
            
            # Detect street markers (888.pt format)
            if '** Dealing down cards **' in line or 'Dealt to' in line:
                current_street = 'preflop'
                continue
            elif '** Dealing flop **' in line:
                current_street = 'flop'
                continue
            elif '** Dealing turn **' in line:
                current_street = 'turn'
                continue
            elif '** Dealing river **' in line:
                current_street = 'river'
                continue
            elif '** Summary **' in line or '** Showdown **' in line:
                current_street = None
                continue
            
            if current_street is None:
                continue
            
            # Parse action - 888.pt format WITHOUT colon
            # Format: "PlayerName folds" or "PlayerName raises [amount]"
            action_match = re.match(r'^(\S+(?:\s+\S+)*?)\s+(folds|calls|raises|bets|checks|posts)\s*(.*)$', line)
            
            if action_match:
                player = action_match.group(1).strip()
                action_type = action_match.group(2).strip()
                amount_str = action_match.group(3).strip()
                
                # Skip blind posts
                if action_type == 'posts':
                    continue
                
                # Detect all-in
                is_allin = 'all-in' in line.lower() or 'all in' in line.lower()
                
                # Extract amount if present
                amount = None
                if amount_str:
                    # Amount in brackets: raises [2800]
                    amount_match = re.search(r'\[([\d,.€]+)\]', amount_str)
                    if amount_match:
                        amount = self._normalize_stack_value(amount_match.group(1))
                
                action_dict = {
                    'player': player,
                    'action': action_type,
                    'is_raise': action_type in ['raises', 'bets'],
                    'is_call': action_type == 'calls',
                    'is_fold': action_type == 'folds',
                    'is_allin': is_allin
                }
                
                if amount is not None:
                    action_dict['amount'] = amount
                
                actions[current_street].append(action_dict)
        
        return actions
    
    def _extract_blinds(self, hand_text: str) -> Dict:
        """Extract blind amounts from 888.pt format."""
        blinds = {'sb': 0, 'bb': 0}
        
        # 888.pt format: "700/1.400 Blinds"
        # Note: European format with . as thousands separator
        blinds_match = re.search(r'([\d,.]+)/([\d,.]+)\s+Blinds', hand_text)
        if blinds_match:
            blinds['sb'] = self._normalize_stack_value(blinds_match.group(1))
            blinds['bb'] = self._normalize_stack_value(blinds_match.group(2))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante from 888.pt format."""
        # 888.pt format: "Player posts ante [175]"
        ante_match = re.search(r'posts\s+ante\s+\[([\d,.€]+)\]', hand_text)
        if ante_match:
            return self._normalize_stack_value(ante_match.group(1))
        return 0.0
