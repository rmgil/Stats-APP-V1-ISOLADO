"""WPN (Winning Poker Network) specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class WPNParser(BaseParser):
    """Parser for WPN hand histories."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "WPN"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is a WPN hand."""
        # Look for the specific WPN format: "Game Hand #xxxx - Tournament #xxxx - Holdem (No Limit)"
        return bool(re.search(r'Game Hand #\d+\s*-\s*Tournament #\d+.*Holdem \(No Limit\)', text[:500]))
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament."""
        # WPN tournaments have "Tournament #" format
        has_tournament = bool(re.search(r'Tournament\s+#\d+', text[:500]))
        # Cash games have specific markers
        is_cash = bool(re.search(r'Cash Game|Ring Game', text[:500]))
        return has_tournament and not is_cash
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # WPN hands start with "Game Hand #"
        pattern = r'(?=Game\s+Hand\s+#\d+)'
        hands = re.split(pattern, text)
        return [hand.strip() for hand in hands if hand.strip() and 'Game Hand #' in hand]
    
    def extract_hand_info(self, hand_text: str, filename: str = "") -> Dict:
        """Extract all information from a WPN hand."""
        info = {
            'site': 'wpn',
            'original_text': hand_text,
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        if not lines:
            return info
        
        # Parse header
        header = lines[0]
        
        # Extract hand ID
        hand_match = re.search(r'Game Hand #(\d+)', header)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament ID
        tourn_match = re.search(r'Tournament\s+#(\d+)', header)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO (for compatibility)
        info['is_pko'] = self.is_pko_tournament(hand_text)
        
        # Detect tournament type from filename
        info['tournament_class'] = self._detect_tournament_type(filename)
        
        # Extract table size from table line
        table_match = re.search(r"Table\s+'[^']+'\s+(\d+)-max", hand_text)
        if table_match:
            info['table_size'] = int(table_match.group(1))
        else:
            # Count seats to determine size
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract button position - WPN format: "Table '48' 8-max Seat #1 is the button"
        button_match = re.search(r'Seat\s+#(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        
        # Extract players
        info['players'] = self._extract_players(hand_text)
        
        # Extract hero
        info['hero'] = self.extract_hero_name(hand_text)
        
        # Extract hero cards
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)\s*\[([^\]]+)\]', hand_text)
        if dealt_match:
            info['hero_cards'] = dealt_match.group(2).strip()
        
        # Extract blinds and ante
        info['blinds'] = self._extract_blinds(hand_text)
        info['ante'] = self._extract_ante(hand_text)
        
        # Extract actions
        info['actions'] = self.extract_actions(hand_text)
        
        # CRITICAL: Mark mathematical all-ins (ante doesn't count as available stack)
        info['actions'] = self._mark_mathematical_allins(
            players=info['players'],
            ante=info['ante'],
            actions=info['actions']
        )
        
        # Extract board
        info['board'] = self._extract_board(hand_text)
        
        # Extract showdown information
        info['showdown'] = self._extract_showdown(hand_text)
        
        # Extract winners and pot collection
        winners_info = self._extract_winners(hand_text)
        info['winners'] = winners_info['winners']
        info['pot_collected'] = winners_info['pot_collected']
        info['total_pot'] = winners_info['total_pot']
        
        return info
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract hero name from 'Dealt to' line."""
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        return None
    
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
            if '*** HOLE CARDS ***' in line:
                current_street = 'preflop'
                continue
            elif '*** FLOP ***' in line:
                current_street = 'flop'
                continue
            elif '*** TURN ***' in line:
                current_street = 'turn'
                continue
            elif '*** RIVER ***' in line:
                current_street = 'river'
                continue
            elif '*** SUMMARY ***' in line or '*** SHOW DOWN ***' in line:
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
        # WPN format: Seat 1: PlayerName (100000.00)
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9.,]+)\)'
        
        for match in re.finditer(pattern, hand_text, re.MULTILINE):
            seat = int(match.group(1))
            name = match.group(2).strip()
            stack = self.parse_amount(match.group(3))
            
            players.append({
                'seat': seat,
                'name': name,
                'stack': stack
            })
        
        return players
    
    def _extract_blinds(self, hand_text: str) -> Dict[str, float]:
        """Extract blind amounts."""
        blinds: Dict[str, float] = {'sb': 0.0, 'bb': 0.0}
        
        # Try from level info (e.g., "Level 13 (2250.00/4500.00)")
        level_match = re.search(r'Level\s+\d+\s*\(([0-9.,]+)/([0-9.,]+)\)', hand_text)
        if level_match:
            blinds['sb'] = self.parse_amount(level_match.group(1))
            blinds['bb'] = self.parse_amount(level_match.group(2))
        else:
            # Extract from posts
            sb_match = re.search(r'posts\s+the\s+small\s+blind\s+([0-9.,]+)', hand_text)
            if sb_match:
                blinds['sb'] = self.parse_amount(sb_match.group(1))
            
            bb_match = re.search(r'posts\s+the\s+big\s+blind\s+([0-9.,]+)', hand_text)
            if bb_match:
                blinds['bb'] = self.parse_amount(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        ante_match = re.search(r'posts\s+ante\s+([0-9.,]+)', hand_text)
        if ante_match:
            return self.parse_amount(ante_match.group(1))
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # WPN format similar to PokerStars
        flop_match = re.search(r'\*\*\* FLOP \*\*\*\s*\[([^\]]+)\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        turn_match = re.search(r'\*\*\* TURN \*\*\*\s*\[[^\]]+\]\s*\[([^\]]+)\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        river_match = re.search(r'\*\*\* RIVER \*\*\*\s*\[[^\]]+\]\s*\[([^\]]+)\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line."""
        # Skip non-action lines
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Skip posts, main pot, and other info lines
        if any(x in line.lower() for x in ['posts', 'main pot', 'collected', 'shows', 'mucks']):
            return None
        
        # WPN format: "PlayerName action [amount]"
        
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
        
        # Detect all-in first, then determine if it's raise/bet/call all-in
        is_allin = 'all-in' in line.lower() or 'allin' in line.lower()
        
        # Call (including call all-in)
        if ' calls' in line:
            match = re.search(r'^([^\s]+)\s+calls\s+([0-9.,]+)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'call',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Bet (including bet all-in)
        if ' bets' in line:
            match = re.search(r'^([^\s]+)\s+bets\s+([0-9.,]+)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'bet',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Raise (including raise all-in)
        if ' raises' in line:
            # Format: "Player raises to 1000"
            match = re.search(r'^([^\s]+)\s+raises\s+to\s+([0-9.,]+)', line)
            if not match:
                # Alternative: "Player raises 1000"
                match = re.search(r'^([^\s]+)\s+raises\s+([0-9.,]+)', line)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'raise',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Pure all-in (standalone all-in action)
        if is_allin:
            match = re.search(r'^([^\s]+)', line)
            if match:
                amount_match = re.search(r'([0-9.,]+)', line)
                return {
                    'player': match.group(1).strip(),
                    'action': 'all-in',
                    'amount': self.parse_amount(amount_match.group(1)) if amount_match else None,
                    'is_allin': True
                }
        
        return None
    
    def _detect_tournament_type(self, filename: str) -> str:
        """
        Detect tournament type based on filename keywords for WPN.
        Returns: 'pko', 'mystery', or 'non-ko'
        """
        filename_lower = filename.lower()
        
        # Check for Mystery first (highest priority)
        if 'mystery' in filename_lower:
            return 'mystery'
        
        # Check for PKO keywords
        if any(keyword in filename_lower for keyword in ['pko', 'bounty', 'knockout', 'ko ']):
            return 'pko'
        
        # Default to NON-KO
        return 'non-ko'
    
    def _extract_showdown(self, hand_text: str) -> Dict:
        """Extract showdown information."""
        showdown = {
            'players_showed': [],
            'players_mucked': [],
            'hands_shown': {}
        }
        
        # WPN format (similar to PokerStars):
        # "PlayerName shows [Ah Kd] (pair of Aces)"
        # "PlayerName: shows [Ah Kd]"
        # "PlayerName mucks hand"
        # "PlayerName: mucks"
        
        # Extract players who showed cards
        # Pattern 1: "PlayerName shows [cards]"
        for match in re.finditer(r'^([^:\n]+)\s+shows\s+\[([^\]]+)\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            cards = match.group(2).strip()
            showdown['players_showed'].append(player)
            showdown['hands_shown'][player] = cards
        
        # Pattern 2: "PlayerName: shows [cards]" (with colon)
        for match in re.finditer(r'^([^:\n]+):\s*shows\s+\[([^\]]+)\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            cards = match.group(2).strip()
            if player not in showdown['players_showed']:
                showdown['players_showed'].append(player)
                showdown['hands_shown'][player] = cards
        
        # Extract players who mucked
        # Pattern 1: "PlayerName mucks"
        for match in re.finditer(r'^([^:\n]+)\s+mucks?(?:\s+hand)?', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            # Only add if not already in showed list
            if player not in showdown['players_showed']:
                showdown['players_mucked'].append(player)
        
        # Pattern 2: "PlayerName: mucks" (with colon)
        for match in re.finditer(r'^([^:\n]+):\s*mucks?(?:\s+hand)?', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            # Only add if not already in showed or mucked list
            if player not in showdown['players_showed'] and player not in showdown['players_mucked']:
                showdown['players_mucked'].append(player)
        
        return showdown
    
    def _extract_winners(self, hand_text: str) -> Dict:
        """Extract winners and pot collection information."""
        winners = []
        pot_collected = {}
        total_pot = 0.0
        
        # WPN format:
        # "PlayerName wins Pot (#1) (amount) with..."
        # "PlayerName collected amount from pot"
        # "PlayerName wins amount"
        
        # Pattern 1: "wins Pot (#X) (amount)"
        for match in re.finditer(r'^([^:\n]+)\s+wins\s+Pot\s+\(#?\d+\)\s+\(([0-9.,]+)\)', hand_text, re.MULTILINE):
            player = self._normalize_player_name(match.group(1))
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            # Accumulate amounts if player won multiple pots
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Pattern 2: "collected X from pot"
        for match in re.finditer(r'^([^:\n]+)\s+collected\s+([0-9.,]+)\s+from', hand_text, re.MULTILINE):
            player = self._normalize_player_name(match.group(1))
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Pattern 3: "wins X" (simple format)
        for match in re.finditer(r'^([^:\n]+)\s+wins?\s+([0-9.,]+)(?:\s|$)', hand_text, re.MULTILINE):
            player = self._normalize_player_name(match.group(1))
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Also check for "Total pot" line in summary
        pot_match = re.search(r'Total\s+pot\s+([0-9.,]+)', hand_text)
        if pot_match:
            # Use the explicitly stated total pot if available
            stated_total = self.parse_amount(pot_match.group(1))
            if stated_total > total_pot:
                total_pot = stated_total
        
        return {
            'winners': winners,
            'pot_collected': pot_collected,
            'total_pot': total_pot
        }
    
    def _normalize_player_name(self, name: str) -> str:
        """Normalize player name - strip and handle multiple spaces."""
        name = name.strip()
        return ' '.join(name.split())