"""GG Poker specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class GGPokerParser(BaseParser):
    """Parser for GG Poker hand histories."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "GGPoker"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is a GG Poker hand."""
        return bool(re.search(r'Poker\s+Hand\s+#\w+:|PokerTime\.eu|GGPoker', text[:1000], re.IGNORECASE))
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament (not cash game)."""
        # GG tournaments have "Tournament" or specific tournament formats
        has_tournament = bool(re.search(r'Tournament|Hold\'em No Limit', text[:500]))
        # Cash games typically have "Ring Game" or specific cash markers
        is_cash = bool(re.search(r'Ring Game|Cash Game|\$\d+/\$\d+\s+USD', text[:500]))
        return has_tournament and not is_cash
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # GG Poker hands start with "Poker Hand #"
        pattern = r'(?=Poker\s+Hand\s+#\w+)'
        hands = re.split(pattern, text)
        # Filter out empty strings and keep complete hands
        return [hand.strip() for hand in hands if hand.strip() and 'Poker Hand #' in hand]
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from a GG Poker hand."""
        info = {
            'site': 'ggpoker',
            'original_text': hand_text,  # Keep original text intact
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        if not lines:
            return info
        
        # Extract hand ID from first line
        header = lines[0]
        hand_match = re.search(r'Hand\s*#(\w+)', header)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament ID
        tourn_match = re.search(r'Tournament\s*#(\w+)', header)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO
        info['is_pko'] = self.is_pko_tournament(hand_text)
        
        # Extract table info and size
        table_match = re.search(r'(\d+)-max', hand_text)
        if table_match:
            info['table_size'] = int(table_match.group(1))
        else:
            # Count seats to determine size
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract players first (we'll need this for button detection)
        info['players'] = self._extract_players(hand_text)
        
        # Extract button position
        # Try PokerStars format first
        button_match = re.search(r'Seat\s+#(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        else:
            # GG Poker doesn't have "is the button" line, infer from SB
            # The button is the player immediately before the SB
            sb_match = re.search(r'^([^\n:]+)\s+posts?\s+(?:the\s+)?(?:small\s+blind|SB)\s+', hand_text, re.MULTILINE | re.IGNORECASE)
            if sb_match:
                sb_player = sb_match.group(1).strip()
                # Normalize the SB player name
                sb_player = ' '.join(sb_player.split())
                
                # Find SB player's seat from players list
                players = info['players']
                if players:
                    # Find SB seat
                    sb_seat = None
                    for p in players:
                        if p['name'] == sb_player:
                            sb_seat = p['seat']
                            break
                    
                    if sb_seat:
                        # Get all seat numbers sorted
                        all_seats = sorted([p['seat'] for p in players])
                        
                        # Find the seat before SB (that's the button)
                        sb_idx = all_seats.index(sb_seat)
                        # The button is the seat before SB (wrap around if needed)
                        button_idx = (sb_idx - 1) % len(all_seats)
                        info['button_seat'] = all_seats[button_idx]
        
        # Extract hero
        info['hero'] = self.extract_hero_name(hand_text)
        
        # Extract hero cards
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)\s*\[([^\]]+)\]', hand_text)
        if dealt_match:
            info['hero_cards'] = dealt_match.group(2).strip()
        
        # Extract blinds and ante
        info['blinds'] = self._extract_blinds(hand_text)
        info['ante'] = self._extract_ante(hand_text)
        
        # Extract actions by street
        info['actions'] = self.extract_actions(hand_text)
        
        # CRITICAL: Mark mathematical all-ins (ante doesn't count as available stack)
        info['actions'] = self._mark_mathematical_allins(
            players=info['players'],
            ante=info['ante'],
            actions=info['actions']
        )
        
        # Extract board cards
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
            # Normalize: strip whitespace and multiple spaces
            name = dealt_match.group(1).strip()
            # Replace multiple spaces with single space
            name = ' '.join(name.split())
            return name
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
            
            # Detect street markers (case-insensitive for compatibility)
            line_upper = line.upper()
            if '*** HOLE CARDS ***' in line_upper:
                current_street = 'preflop'
                continue
            elif '*** FLOP ***' in line_upper:
                current_street = 'flop'
                continue
            elif '*** TURN ***' in line_upper:
                current_street = 'turn'
                continue
            elif '*** RIVER ***' in line_upper:
                current_street = 'river'
                continue
            elif '*** SUMMARY ***' in line_upper or '*** SHOW DOWN ***' in line_upper or '*** SHOWDOWN ***' in line_upper:
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
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9,. ]+)(?:\s+in\s+chips)?\)'
        
        for match in re.finditer(pattern, hand_text, re.MULTILINE):
            seat = int(match.group(1))
            # Normalize: strip whitespace and multiple spaces (same as hero name)
            name = match.group(2).strip()
            # Replace multiple spaces with single space
            name = ' '.join(name.split())
            stack = self.parse_amount(match.group(3))
            
            players.append({
                'seat': seat,
                'name': name,
                'stack': stack
            })
        
        return players
    
    def _extract_blinds(self, hand_text: str) -> Dict[str, float]:
        """Extract blind amounts."""
        blinds = {'sb': 0, 'bb': 0}
        
        # Small blind
        sb_match = re.search(r'posts?\s+(?:the\s+)?small\s+blind\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
        if sb_match:
            blinds['sb'] = self.parse_amount(sb_match.group(1))
        
        # Big blind
        bb_match = re.search(r'posts?\s+(?:the\s+)?big\s+blind\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
        if bb_match:
            blinds['bb'] = self.parse_amount(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        ante_match = re.search(r'posts?\s+(?:the\s+)?ante\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
        if ante_match:
            return self.parse_amount(ante_match.group(1))
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # Flop
        flop_match = re.search(r'\*\*\* FLOP \*\*\*\s*\[([^\]]+)\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        # Turn
        turn_match = re.search(r'\*\*\* TURN \*\*\*\s*\[[^\]]*\]\s*\[([^\]]+)\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        # River
        river_match = re.search(r'\*\*\* RIVER \*\*\*\s*\[[^\]]*\]\s*\[([^\]]+)\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line."""
        # Skip non-action lines
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Helper to normalize player names
        def normalize_name(name: str) -> str:
            name = name.strip()
            # Replace multiple spaces with single space
            return ' '.join(name.split())
        
        # Pattern for player actions in GG format
        # Format: "PlayerName: action [amount]"
        
        # Fold
        if re.search(r'^([^:]+):\s*folds?', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*folds?', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'fold',
                    'amount': None
                }
        
        # Check
        if re.search(r'^([^:]+):\s*checks?', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*checks?', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'check',
                    'amount': None
                }
        
        # Detect all-in first, then determine if it's raise/bet/call all-in
        is_allin = 'all-in' in line.lower() or 'allin' in line.lower()
        
        # Call (including call all-in)
        if re.search(r'^([^:]+):\s*calls?\s+([0-9,. ]+)', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*calls?\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'call',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Bet (including bet all-in)
        if re.search(r'^([^:]+):\s*bets?\s+([0-9,. ]+)', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*bets?\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'bet',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Raise (including raise all-in)
        if re.search(r'^([^:]+):\s*raises?\s+.*to\s+([0-9,. ]+)', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*raises?\s+.*to\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'raise',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Pure all-in (standalone all-in action)
        if is_allin:
            match = re.search(r'^([^:]+):\s*(?:all-?in|allin)\s*(?:for\s+)?([0-9,. ]+)?', line, re.IGNORECASE)
            if match:
                return {
                    'player': normalize_name(match.group(1)),
                    'action': 'all-in',
                    'amount': self.parse_amount(match.group(2)) if match.group(2) else None,
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
        
        # GG format (similar to PokerStars):
        # "PlayerName: shows [Ah Kd] (pair of Aces)"
        # "PlayerName: mucks hand"
        # "PlayerName shows [Ah Kd]"
        
        # Extract players who showed cards
        for match in re.finditer(r'^([^:]+):\s*shows?\s+\[([^\]]+)\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            cards = match.group(2).strip()
            showdown['players_showed'].append(player)
            showdown['hands_shown'][player] = cards
        
        # Extract players who mucked
        for match in re.finditer(r'^([^:]+):\s*mucks?(?:\s+hand)?', hand_text, re.MULTILINE | re.IGNORECASE):
            player = self._normalize_player_name(match.group(1))
            # Only add if not already in showed list
            if player not in showdown['players_showed']:
                showdown['players_mucked'].append(player)
        
        return showdown
    
    def _extract_winners(self, hand_text: str) -> Dict:
        """Extract winners and pot collection information."""
        winners = []
        pot_collected = {}
        total_pot = 0.0
        
        # GG format (similar to PokerStars):
        # "PlayerName collected 10000 from pot"
        # "PlayerName wins 10000"
        
        # Pattern 1: "collected X from pot"
        # Exclude "Uncalled bet" lines
        for match in re.finditer(r'^([^:]+)\s+collected\s+([0-9,. ]+)\s+from', hand_text, re.MULTILINE):
            player = self._normalize_player_name(match.group(1))
            # Skip if this is an "Uncalled bet" line
            if 'uncalled bet' in player.lower() or 'uncalled' in player.lower():
                continue
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            # Accumulate amounts if player won multiple pots
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Pattern 2: "wins X" (alternative format)
        for match in re.finditer(r'^([^:]+)\s+wins?\s+([0-9,. ]+)', hand_text, re.MULTILINE):
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
        pot_match = re.search(r'Total pot\s+([0-9,. ]+)', hand_text)
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