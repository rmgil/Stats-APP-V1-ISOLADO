"""888 Poker specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class Eight88PokerParser(BaseParser):
    """Parser for 888 Poker hand histories (888poker and 888.pt)."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "888poker"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is an 888 Poker hand (888poker or 888.pt)."""
        return bool(re.search(r'888(?:poker|\.pt) Hand History|Game No :', text[:500]))
    
    def parse_amount(self, text: str) -> float:
        """
        Parse monetary amount from 888poker format (European number format).
        888poker uses: 34.069 (dot as thousands separator) = 34,069 chips
        """
        if not text:
            return 0.0
        
        # Remove currency symbols and whitespace
        clean = text.strip()
        clean = re.sub(r'[€$£¥]', '', clean)
        clean = clean.strip()
        
        # European format: dot is thousands separator, comma is decimal
        # Handle formats: "30.000", "1.234,56", "30", "30.5"
        if ',' in clean:
            # Has comma - it's the decimal separator
            # Example: "1.234,56" → "1234.56"
            clean = clean.replace('.', '').replace(',', '.')
        elif '.' in clean:
            # Check if it's thousands or decimal by counting digits after dot
            parts = clean.split('.')
            if len(parts[-1]) == 3 and len(parts) > 1:
                # Format: "30.000" - dot is thousands separator
                # Example: "34.069" → "34069"
                clean = clean.replace('.', '')
            # else: format like "30.5" - dot is decimal, keep it
        
        try:
            return float(clean)
        except ValueError:
            return 0.0
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament."""
        # 888 tournaments have "Tournament #" format
        has_tournament = bool(re.search(r'Tournament\s+#', text[:500]))
        # Cash games have specific markers
        is_cash = bool(re.search(r'Cash Game|Ring Game', text[:500]))
        return has_tournament and not is_cash
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # 888 hands start with "#Game No :" or similar pattern (888poker or 888.pt)
        pattern = r'(?=#Game No\s*:|^\*{5}\s+888(?:poker|\.pt))'
        hands = re.split(pattern, text, flags=re.MULTILINE)
        return [hand.strip() for hand in hands if hand.strip() and ('Game No' in hand or '888poker' in hand or '888.pt' in hand)]
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from an 888 Poker hand (888poker or 888.pt)."""
        info = {
            'site': '888poker',
            'original_text': hand_text,
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        
        # Extract hand ID
        hand_match = re.search(r'Game No\s*:\s*(\d+)', hand_text)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament ID
        tourn_match = re.search(r'Tournament\s+#(\d+)', hand_text)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO
        info['is_pko'] = self.is_pko_tournament(hand_text)
        
        # Extract table size
        table_match = re.search(r'(\d+)\s*Max', hand_text, re.IGNORECASE)
        if table_match:
            info['table_size'] = int(table_match.group(1))
        else:
            # Count players to determine size
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract button position  
        button_match = re.search(r'Seat\s+(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        
        # Extract players
        info['players'] = self._extract_players(hand_text)
        
        # Extract hero
        info['hero'] = self.extract_hero_name(hand_text)
        
        # Extract hero cards
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)\s*\[\s*([^,]+),\s*([^\]]+)\s*\]', hand_text)
        if dealt_match:
            info['hero_cards'] = f"{dealt_match.group(2).strip()} {dealt_match.group(3).strip()}"
        
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
        # 888 format: Seat 1: PlayerName ( 1000 ) or Seat 1: PlayerName ( 10.000 )
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(\s*([0-9,. ]+)\s*\)'
        
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
        """Extract blind amounts from header or posts."""
        blinds = {'sb': 0, 'bb': 0}
        
        # Try to extract from header (e.g., "350/700 Blinds" or "30/60 Blinds")
        header_match = re.search(r'(\d+)/(\d+)\s+Blinds', hand_text)
        if header_match:
            blinds['sb'] = float(header_match.group(1))
            blinds['bb'] = float(header_match.group(2))
        else:
            # Extract from posts
            sb_match = re.search(r'posts\s+small\s+blind\s+\[([0-9,. ]+)\]', hand_text, re.IGNORECASE)
            if sb_match:
                blinds['sb'] = self.parse_amount(sb_match.group(1))
            
            bb_match = re.search(r'posts\s+big\s+blind\s+\[([0-9,. ]+)\]', hand_text, re.IGNORECASE)
            if bb_match:
                blinds['bb'] = self.parse_amount(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        ante_match = re.search(r'posts\s+ante\s+\[([0-9,. ]+)\]', hand_text, re.IGNORECASE)
        if ante_match:
            return self.parse_amount(ante_match.group(1))
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # 888 format: ** Dealing Flop ** : [ Qs, 4d, 5h ]
        flop_match = re.search(r'\*\* Dealing Flop \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        turn_match = re.search(r'\*\* Dealing Turn \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        river_match = re.search(r'\*\* Dealing River \*\*\s*:?\s*\[\s*([^\]]+)\s*\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line."""
        # Skip non-action lines
        if not line or line.startswith('**') or 'Dealt to' in line:
            return None
        
        # Skip blind/ante posts as they're handled separately
        if 'posts' in line.lower() and any(x in line.lower() for x in ['blind', 'ante']):
            return None
        
        # 888 format: PlayerName action [amount]
        
        # Fold
        if ' folds' in line.lower():
            match = re.search(r'^([^\s]+)\s+folds', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'fold',
                    'amount': None
                }
        
        # Check
        if ' checks' in line.lower():
            match = re.search(r'^([^\s]+)\s+checks', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'check',
                    'amount': None
                }
        
        # Detect all-in first, then determine if it's raise/bet/call all-in
        is_allin = 'all-in' in line.lower() or 'allin' in line.lower()
        
        # Call (including call all-in)
        if ' calls' in line.lower():
            match = re.search(r'^([^\s]+)\s+calls\s+\[?([0-9,. ]+)\]?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'call',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Raise (including raise all-in)
        if ' raises' in line.lower():
            match = re.search(r'^([^\s]+)\s+raises\s+\[?([0-9,. ]+)\]?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'raise',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Bet (including bet all-in)
        if ' bets' in line.lower():
            match = re.search(r'^([^\s]+)\s+bets\s+\[?([0-9,. ]+)\]?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'bet',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Pure all-in (standalone all-in action)
        if is_allin:
            match = re.search(r'^([^\s]+)', line)
            if match:
                # Try to find amount
                amount_match = re.search(r'\[?([0-9,. ]+)\]?', line)
                return {
                    'player': match.group(1).strip(),
                    'action': 'all-in',
                    'amount': self.parse_amount(amount_match.group(1)) if amount_match else None,
                    'is_allin': True
                }
        
        return None
    
    def _extract_showdown(self, hand_text: str) -> Dict:
        """Extract showdown information."""
        showdown = {
            'players_showed': [],
            'players_mucked': [],
            'hands_shown': {}
        }
        
        # 888 format:
        # "PlayerName shows [ Ah, Kd ]" (note spaces around brackets and commas)
        # "PlayerName mucks"
        # "PlayerName did not show his hand"
        
        # Extract players who showed cards
        # Pattern: PlayerName shows [ cards ]
        for match in re.finditer(r'^([^\n]+?)\s+shows\s+\[\s*([^\]]+)\s*\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            cards = match.group(2).strip()
            # Normalize card format: remove commas and extra spaces
            cards = ' '.join(cards.replace(',', ' ').split())
            showdown['players_showed'].append(player)
            showdown['hands_shown'][player] = cards
        
        # Extract players who mucked
        # Pattern: PlayerName mucks
        for match in re.finditer(r'^([^\n]+?)\s+mucks?(?:\s+hand)?', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            # Only add if not already in showed list
            if player not in showdown['players_showed']:
                showdown['players_mucked'].append(player)
        
        # Also check for "did not show" pattern
        for match in re.finditer(r'^([^\n]+?)\s+did\s+not\s+show', hand_text, re.MULTILINE | re.IGNORECASE):
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
        
        # 888 format:
        # "PlayerName collected [ amount ]"
        # "PlayerName wins amount chips"
        
        # Pattern 1: "collected [ X ]"
        for match in re.finditer(r'^([^\n]+?)\s+collected\s+\[\s*([0-9,. ]+)\s*\]', hand_text, re.MULTILINE):
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
        
        # Pattern 2: "wins X chips" (alternative format)
        for match in re.finditer(r'^([^\n]+?)\s+wins?\s+([0-9,. ]+)\s+chips?', hand_text, re.MULTILINE):
            player = self._normalize_player_name(match.group(1))
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Also check for "Total pot" line in summary (if available)
        pot_match = re.search(r'Total\s+pot\s+([0-9,. ]+)', hand_text)
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