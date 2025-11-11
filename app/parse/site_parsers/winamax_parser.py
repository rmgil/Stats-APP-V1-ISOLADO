"""Winamax specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class WinamaxParser(BaseParser):
    """Parser for Winamax hand histories."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "Winamax"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is a Winamax hand."""
        return bool(re.search(r'Winamax Poker', text[:500]))
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament."""
        # Winamax tournaments have "Tournament" in header
        has_tournament = bool(re.search(r'Tournament|buyIn:', text[:500]))
        # Cash games have specific markers
        is_cash = bool(re.search(r'Cash Game|Ring Game', text[:500]))
        return has_tournament and not is_cash
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # Winamax hands start with "Winamax Poker - Tournament"
        pattern = r'(?=Winamax\s+Poker\s+-\s+Tournament)'
        hands = re.split(pattern, text)
        # Filter out summary files
        return [hand.strip() for hand in hands 
                if hand.strip() and 'Winamax Poker' in hand and 'Tournament summary' not in hand]
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from a Winamax hand."""
        info = {
            'site': 'winamax',
            'original_text': hand_text,
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        if not lines:
            return info
        
        # Parse header
        header = lines[0]
        
        # Extract hand ID
        hand_match = re.search(r'HandId:\s*#([0-9-]+)', header)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament name/ID
        tourn_match = re.search(r'Tournament\s+"([^"]+)"', header)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO - Winamax PKO has "bounty" in player stacks
        # Example: Seat 1: Enaifos (21338, 9€ bounty)
        info['is_pko'] = ' bounty)' in hand_text
        
        # Extract table size
        table_match = re.search(r"Table:\s+'[^']+'\s+(\d+)-max", hand_text)
        if table_match:
            info['table_size'] = int(table_match.group(1))
        else:
            # Count seats to determine size
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract button position (support both "Seat #X" and "Seat X" formats)
        button_match = re.search(r'Seat\s+#?(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        else:
            # Fallback: infer button from SB position
            info['button_seat'] = self._infer_button_from_sb(hand_text)
        
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
            if '*** PRE-FLOP ***' in line:
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
        # Winamax format: Seat 1: PlayerName (20000, 9€ bounty)
        # or: Seat 1: PlayerName (20000)
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9]+)(?:,\s*[^)]+)?\)'
        
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
        blinds = {'sb': 0.0, 'bb': 0.0}
        
        # Try from header (e.g., "(25/100/200)" or "(100/200)")
        header_match = re.search(r'\((\d+)/(\d+)(?:/\d+)?\)', hand_text[:200])
        if header_match:
            # In Winamax, sometimes format is ante/sb/bb or just sb/bb
            groups = header_match.groups()
            if header_match.group(0).count('/') == 2:
                # Format is ante/sb/bb, skip first number
                blinds_match = re.search(r'\(\d+/(\d+)/(\d+)\)', hand_text[:200])
                if blinds_match:
                    blinds['sb'] = float(blinds_match.group(1))
                    blinds['bb'] = float(blinds_match.group(2))
            else:
                # Format is sb/bb
                blinds['sb'] = float(groups[0])
                blinds['bb'] = float(groups[1])
        else:
            # Extract from posts
            sb_match = re.search(r'posts\s+small\s+blind\s+(\d+)', hand_text)
            if sb_match:
                blinds['sb'] = float(sb_match.group(1))
            
            bb_match = re.search(r'posts\s+big\s+blind\s+(\d+)', hand_text)
            if bb_match:
                blinds['bb'] = float(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        ante_match = re.search(r'posts\s+ante\s+(\d+)', hand_text)
        if ante_match:
            return float(ante_match.group(1))
        
        # Also check header format (ante/sb/bb)
        header_match = re.search(r'\((\d+)/\d+/\d+\)', hand_text[:200])
        if header_match:
            return float(header_match.group(1))
        
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # Winamax format: *** FLOP *** [Kh Qs 7s]
        flop_match = re.search(r'\*\*\* FLOP \*\*\*\s*\[([^\]]+)\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        # Turn format: *** TURN *** [Kh Qs 7s][5h]
        turn_match = re.search(r'\*\*\* TURN \*\*\*\s*\[[^\]]*\]\[([^\]]+)\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        # River format: *** RIVER *** [Kh Qs 7s 5h][6s]
        river_match = re.search(r'\*\*\* RIVER \*\*\*\s*\[[^\]]*\]\[([^\]]+)\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _infer_button_from_sb(self, hand_text: str) -> Optional[int]:
        """Infer button position from small blind poster."""
        # Find who posts small blind
        sb_match = re.search(r'^([^\s]+)\s+posts\s+small\s+blind', hand_text, re.MULTILINE)
        if not sb_match:
            return None
        
        sb_player = sb_match.group(1).strip()
        
        # Get all seats and players
        players = self._extract_players(hand_text)
        if not players:
            return None
        
        # Find SB player's seat
        sb_seat = None
        for player in players:
            if player['name'] == sb_player:
                sb_seat = player['seat']
                break
        
        if sb_seat is None:
            return None
        
        # Button is the seat before SB in circular order
        seats = sorted([p['seat'] for p in players])
        sb_index = seats.index(sb_seat)
        button_index = (sb_index - 1) % len(seats)
        return seats[button_index]
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line - supports both English and French."""
        # Skip non-action lines
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Skip posts, collected, board info
        if any(x in line.lower() for x in ['posts', 'collected', 'board:', 'total pot']):
            return None
        
        # Winamax format: "PlayerName action [amount]"
        # Support both English and French terms
        
        # Fold - English: "folds", French: "se couche"
        if re.search(r'\s+(?:folds?|se\s+couche)', line, re.IGNORECASE):
            match = re.search(r'^(.+?)\s+(?:folds?|se\s+couche)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'fold',
                    'amount': None
                }
        
        # Check - English: "checks", French: "checke"
        if re.search(r'\s+(?:checks?|checke)', line, re.IGNORECASE):
            match = re.search(r'^(.+?)\s+(?:checks?|checke)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'check',
                    'amount': None
                }
        
        # Detect all-in first - English: "all-in"/"allin", French: "fait tapis"
        is_allin = 'all-in' in line.lower() or 'allin' in line.lower() or 'fait tapis' in line.lower()
        
        # Call - English: "calls", French: "suit" (including call all-in)
        if re.search(r'\s+(?:calls?|suit)', line, re.IGNORECASE):
            match = re.search(r'^(.+?)\s+(?:calls?|suit)\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'call',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Bet - English: "bets", French: "mise" (including bet all-in)
        if re.search(r'\s+(?:bets?|mise)', line, re.IGNORECASE):
            match = re.search(r'^(.+?)\s+(?:bets?|mise)\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'bet',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Raise - English: "raises X to Y", French: "relance à Y" (including raise all-in)
        if re.search(r'\s+(?:raises?|relance)', line, re.IGNORECASE):
            # Try English format: "raises X to Y"
            match = re.search(r'^(.+?)\s+raises?\s+[0-9,. ]+\s+to\s+([0-9,. ]+)', line, re.IGNORECASE)
            if not match:
                # Try French format: "relance à Y"
                match = re.search(r'^(.+?)\s+relance\s+à\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'raise',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Pure all-in - English: "all-in", French: "fait tapis"
        if is_allin:
            match = re.search(r'^(.+?)\s+(?:all-?in|fait\s+tapis)(?:\s+([0-9,. ]+))?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'all-in',
                    'amount': self.parse_amount(match.group(2)) if match.group(2) else None,
                    'is_allin': True
                }
        
        return None
    
    def _extract_showdown(self, hand_text: str) -> Dict:
        """Extract showdown information - supports both English and French."""
        showdown = {
            'players_showed': [],
            'players_mucked': [],
            'hands_shown': {}
        }
        
        # Winamax showdown format:
        # English: "PlayerName shows [Ah Kd]" or "PlayerName: shows [Ah Kd]"
        # French: "PlayerName montre [Ah Kd]"
        
        # Extract players who showed cards
        # Support both with and without colon after player name
        for match in re.finditer(r'^([^:\[]+?)(?::)?\s+(?:shows?|montre)\s+\[([^\]]+)\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            cards = match.group(2).strip()
            showdown['players_showed'].append(player)
            showdown['hands_shown'][player] = cards
        
        # Extract players who mucked
        # English: "PlayerName mucks" or "PlayerName: mucks hand"
        # French: "PlayerName passe" or similar
        for match in re.finditer(r'^([^:\[]+?)(?::)?\s+(?:mucks?(?:\s+hand)?|passe)', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            # Only add if not already in showed list
            if player not in showdown['players_showed']:
                showdown['players_mucked'].append(player)
        
        return showdown
    
    def _extract_winners(self, hand_text: str) -> Dict:
        """Extract winners and pot collection information - supports both English and French."""
        winners = []
        pot_collected = {}
        total_pot = 0.0
        
        # Winamax winner formats:
        # English: "PlayerName collected 10000 from pot" or "PlayerName wins 10000"
        # French: "PlayerName remporte 10000 du pot"
        
        # Pattern 1: "collected X from pot" (English)
        # Use non-greedy match to capture only player name on same line
        for match in re.finditer(r'^(.+?)\s+collected\s+([0-9,. ]+)\s+from', hand_text, re.MULTILINE):
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
        
        # Pattern 2: "remporte X du pot" (French)
        # Use non-greedy match to capture only player name on same line
        for match in re.finditer(r'^(.+?)\s+remporte\s+([0-9,. ]+)\s+du\s+pot', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Pattern 3: "wins X" (alternative English format)
        # Use non-greedy match to capture only player name on same line
        for match in re.finditer(r'^(.+?)\s+wins?\s+([0-9,. ]+)', hand_text, re.MULTILINE):
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
        pot_match = re.search(r'Total pot\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
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