"""Party Poker specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class PartyPokerParser(BaseParser):
    """Parser for Party Poker hand histories."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "PartyPoker"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is a Party Poker hand."""
        return bool(re.search(r'\*{5}\s*Hand History For Game|Tourney Texas Holdem', text[:500]))
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament."""
        # Party tournaments have "MTT Tournament" or "Tourney" format
        has_tournament = bool(re.search(r'MTT Tournament|Tourney', text[:500]))
        # Cash games have specific markers
        is_cash = bool(re.search(r'Cash Game|Ring Game', text[:500]))
        return has_tournament and not is_cash
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # Party hands start with "***** Hand History For Game"
        pattern = r'(?=\*{5}\s*Hand History For Game)'
        hands = re.split(pattern, text)
        return [hand.strip() for hand in hands if hand.strip() and 'Hand History For Game' in hand]
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from a Party Poker hand."""
        info = {
            'site': 'partypoker',
            'original_text': hand_text,
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        if not lines:
            return info
        
        # Extract hand ID from first line
        header = lines[0]
        hand_match = re.search(r'Game\s+([0-9a-z]+)', header)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament ID and buy-in
        tourn_match = re.search(r'Tournament\s+#(\d+)', hand_text)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO
        info['is_pko'] = self.is_pko_tournament(hand_text)
        
        # Extract table size
        # Party uses "Total number of players : 8/8" format
        size_match = re.search(r'Total number of players\s*:\s*\d+/(\d+)', hand_text)
        if size_match:
            max_players = int(size_match.group(1))
            info['table_size'] = max_players
        else:
            # Count seats to determine
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract button position
        button_match = re.search(r'Seat\s+(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        
        # Extract players
        info['players'] = self._extract_players(hand_text)
        
        # Extract hero (Party uses "Hero" directly)
        info['hero'] = self._extract_hero_name(hand_text)
        
        # Extract hero cards
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)\s*\[\s*([^,]+),\s*([^\]]+)\s*\]', hand_text)
        if dealt_match:
            info['hero_cards'] = f"{dealt_match.group(2).strip()} {dealt_match.group(3).strip()}"
        
        # Extract blinds and ante
        info['blinds'] = self._extract_blinds(hand_text)
        info['ante'] = self._extract_ante(hand_text)
        
        # Extract actions
        info['actions'] = self.extract_actions(hand_text)
        
        # Extract board
        info['board'] = self._extract_board(hand_text)
        
        return info
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract hero name from 'Dealt to' line or find 'Hero'."""
        # Party Poker often uses "Hero" as the player name
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        
        # Check if "Hero" is in players
        if re.search(r'Seat\s+\d+:\s*Hero\s*\(', hand_text):
            return "Hero"
        
        return None
    
    def _extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Internal method to extract hero name."""
        return self.extract_hero_name(hand_text)
    
    def extract_actions(self, hand_text: str) -> Dict[str, List[Dict]]:
        """Extract all actions by street."""
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
            
            # Detect street markers
            if '** Dealing down cards **' in line:
                current_street = 'preflop'
                continue
            elif '** Dealing Flop **' in line:
                current_street = 'flop'
                continue
            elif '** Dealing Turn **' in line:
                current_street = 'turn'
                continue
            elif '** Dealing River **' in line:
                current_street = 'river'
                continue
            elif '** Summary **' in line:
                break
            
            # Parse action if we're in a street
            if current_street:
                action = self._parse_action_line(line)
                if action:
                    actions[current_street].append(action)
        
        return actions
    
    def _extract_players(self, hand_text: str) -> List[Dict]:
        """Extract player information."""
        players = []
        # Party format: Seat 1: Player1 (68297)
        # Note: Party sometimes uses generic names like Player1, Player2, etc.
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9]+)\)'
        
        for match in re.finditer(pattern, hand_text, re.MULTILINE):
            seat = int(match.group(1))
            name = match.group(2).strip()
            stack = float(match.group(3))
            
            players.append({
                'seat': seat,
                'name': name,
                'stack': stack
            })
        
        return players
    
    def _extract_blinds(self, hand_text: str) -> Dict[str, float]:
        """Extract blind amounts."""
        blinds = {'sb': 0, 'bb': 0}
        
        # Try from header (e.g., "600/1200 Tourney")
        header_match = re.search(r'(\d+)/(\d+)\s+Tourney', hand_text[:200])
        if header_match:
            blinds['sb'] = float(header_match.group(1))
            blinds['bb'] = float(header_match.group(2))
        else:
            # Extract from posts
            sb_match = re.search(r'posts\s+small\s+blind\s+\((\d+)\)', hand_text)
            if sb_match:
                blinds['sb'] = float(sb_match.group(1))
            
            bb_match = re.search(r'posts\s+big\s+blind\s+\((\d+)\)', hand_text)
            if bb_match:
                blinds['bb'] = float(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        # Party format: "Player posts ante (150)"
        ante_match = re.search(r'posts\s+ante\s+\((\d+)\)', hand_text)
        if ante_match:
            return float(ante_match.group(1))
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # Party format: ** Dealing Flop ** : [ Qs, 4d, 5h ]
        flop_match = re.search(r'\*\* Dealing Flop \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        # Turn: ** Dealing Turn ** : [ Js ]
        turn_match = re.search(r'\*\* Dealing Turn \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        # River: ** Dealing River ** : [ Jd ]
        river_match = re.search(r'\*\* Dealing River \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line."""
        # Skip non-action lines
        if not line or line.startswith('**') or 'Dealt to' in line:
            return None
        
        # Skip posts, balance info
        if any(x in line.lower() for x in ['posts', 'balance', 'collected', 'lost']):
            return None
        
        # Party format: "Player action [amount]"
        
        # Fold
        if ' folds' in line:
            match = re.search(r'^([^\s]+)\s+folds', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'fold',
                    'amount': None
                }
        
        # Check
        if ' checks' in line:
            match = re.search(r'^([^\s]+)\s+checks', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'check',
                    'amount': None
                }
        
        # Call
        if ' calls' in line:
            # Format: "Player calls (1000)"
            match = re.search(r'^([^\s]+)\s+calls\s+\((\d+)\)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'call',
                    'amount': float(match.group(2))
                }
        
        # Raise
        if ' raises' in line:
            # Format: "Player raises 2400 to 2400"
            match = re.search(r'^([^\s]+)\s+raises\s+\d+\s+to\s+(\d+)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'raise',
                    'amount': float(match.group(2))
                }
        
        # Bet
        if ' bets' in line:
            # Format: "Player bets (1000)"
            match = re.search(r'^([^\s]+)\s+bets\s+\((\d+)\)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'bet',
                    'amount': float(match.group(2))
                }
        
        # All-in
        if 'is all-in' in line.lower() or 'all-in' in line.lower():
            # Can be various formats
            player_match = re.search(r'^([^\s]+)', line)
            if player_match:
                amount_match = re.search(r'\((\d+)\)', line)
                return {
                    'player': player_match.group(1).strip(),
                    'action': 'all-in',
                    'amount': float(amount_match.group(1)) if amount_match else None
                }
        
        return None