"""
Multi-site aware RFI calculator
Works with any poker site by detecting hero name dynamically
"""
import re
from typing import Dict, List, Tuple, Optional
from app.parse.site_parsers.site_detector import detect_poker_site, get_parser
from app.stats.position_mapping import get_position_map, get_position_category as centralized_position_category

class MultiSiteRFICalculator:
    def __init__(self):
        self.stats = {
            "Early RFI": {"opportunities": 0, "attempts": 0},
            "Middle RFI": {"opportunities": 0, "attempts": 0},
            "CO Steal": {"opportunities": 0, "attempts": 0},
            "BTN Steal": {"opportunities": 0, "attempts": 0}
        }
        self.hero_name = None
    
    def identify_hero(self, hand_text: str) -> Optional[str]:
        """
        Identify the HERO player from hand text using site-specific parser
        """
        site = detect_poker_site(hand_text)
        if site:
            parser = get_parser(site)
            if parser:
                info = parser.extract_hand_info(hand_text)
                hero = info.get('hero')
                if hero:
                    return hero
        
        # Fallback to looking for "Hero" literal
        if re.search(r'Dealt to Hero\b', hand_text, re.IGNORECASE):
            return "Hero"
        
        return None
    
    def get_button_position(self, hand_text: str) -> Optional[int]:
        """Get the button seat number"""
        button_match = re.search(r'Seat #?(\d+) is the button', hand_text)
        if button_match:
            return int(button_match.group(1))
        return None
    
    def extract_players_and_positions(self, hand_text: str) -> Dict[str, Dict]:
        """
        Extract all players and their positions relative to button
        Works with multiple poker sites
        """
        players = {}
        
        # Use site-specific parser for better accuracy
        site = detect_poker_site(hand_text)
        if site:
            parser = get_parser(site)
            if parser:
                info = parser.extract_hand_info(hand_text)
                player_list = info.get('players', [])
                button_seat = info.get('button_seat')
                
                if player_list and button_seat is not None:
                    # Sort by seat
                    player_list.sort(key=lambda x: x['seat'])
                    
                    # Find button index
                    button_idx = None
                    for i, p in enumerate(player_list):
                        if p['seat'] == button_seat:
                            button_idx = i
                            break
                    
                    if button_idx is not None:
                        num_players = len(player_list)
                        
                        for i, p in enumerate(player_list):
                            relative_pos = (i - button_idx) % num_players
                            position = self.get_position_name(num_players, relative_pos)
                            
                            players[p['name']] = {
                                "seat": p['seat'],
                                "position": position,
                                "relative_to_button": relative_pos
                            }
                    
                    return players
        
        # Fallback to regex extraction (less accurate)
        # Updated pattern to accept names with spaces and make "in chips" optional
        # This works for both GG Poker and other sites
        seat_pattern = r'Seat\s+(\d+):\s+([^(]+?)\s+\([\d,.]+(?: in chips)?(?:,.*?)?\)'
        seats = re.findall(seat_pattern, hand_text)
        
        if not seats:
            return players
        
        button_seat = self.get_button_position(hand_text)
        if not button_seat:
            return players
        
        active_seats = [(int(seat), player) for seat, player in seats]
        active_seats.sort(key=lambda x: x[0])
        
        button_idx = None
        for i, (seat_num, player) in enumerate(active_seats):
            if seat_num == button_seat:
                button_idx = i
                break
        
        if button_idx is None:
            return players
        
        num_players = len(active_seats)
        
        for i, (seat_num, player) in enumerate(active_seats):
            relative_pos = (i - button_idx) % num_players
            position = self.get_position_name(num_players, relative_pos)
            
            players[player] = {
                "seat": seat_num,
                "position": position,
                "relative_to_button": relative_pos
            }
        
        return players
    
    def get_position_name(self, num_players: int, relative_pos: int) -> str:
        """
        Get position name based on number of players and relative position to button
        relative_pos: 0 = BTN, 1 = SB, 2 = BB, etc.
        Uses centralized position mapping for consistency (GG Poker standard).
        """
        position_map = get_position_map(num_players)
        if not position_map:
            # Unsupported player count - use generic naming
            return f"P{relative_pos}" if relative_pos > 2 else ["BTN", "SB", "BB"][relative_pos]
        
        return position_map.get(relative_pos, "Unknown")
    
    def get_position_category(self, position: str) -> Optional[str]:
        """
        Categorize position for RFI statistics
        Returns: "Early", "Middle", "CO", "BTN", or None
        Uses centralized categorization (GG Poker standard).
        """
        category = centralized_position_category(position)
        
        # For RFI stats, we need specific handling of CO and BTN
        if position == "CO":
            return "CO"
        elif position == "BTN":
            return "BTN"
        elif category == "Late":
            # Other late positions (shouldn't happen with standard positions)
            return None
        else:
            # Early and Middle categories map directly
            return category
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str]]:
        """
        Extract pre-flop actions (player, action)
        Returns list of tuples: [(player, action), ...]
        """
        actions = []
        
        # Use site-specific parser when possible
        site = detect_poker_site(hand_text)
        if site:
            parser = get_parser(site)
            if parser:
                info = parser.extract_hand_info(hand_text)
                action_dict = info.get('actions', {})
                preflop_actions = action_dict.get('preflop', [])
                
                for action in preflop_actions:
                    player = action.get('player')
                    action_type = action.get('action')
                    
                    if player and action_type:
                        # Normalize action types
                        if 'raise' in action_type.lower():
                            action_type = 'raise'
                        elif 'fold' in action_type.lower():
                            action_type = 'fold'
                        elif 'call' in action_type.lower():
                            action_type = 'call'
                        elif 'check' in action_type.lower():
                            action_type = 'check'
                        
                        actions.append((player, action_type))
                
                if actions:
                    return actions
        
        # Fallback to regex extraction
        lines = hand_text.split('\\n')
        in_preflop = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line:
                in_preflop = True
                continue
            elif '*** FLOP ***' in line or '*** SUMMARY ***' in line:
                break
            elif in_preflop:
                # Parse action lines
                if 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold'))
                elif 'raises' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'raise'))
                elif 'calls' in line and 'blind' not in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'call'))
                elif 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check'))
        
        return actions
    
    def is_rfi_opportunity(self, position: str, actions_before: List[Tuple[str, str]]) -> bool:
        """
        Check if this is an RFI opportunity
        RFI = Raise First In (opportunity to be first raiser when everyone folds)
        """
        # Check if anyone raised or limped before hero
        for player, action in actions_before:
            if action == "raise":
                return False  # No RFI if someone raised
            if action == "call":
                return False  # No RFI if someone limped (RFI requires all folds)
        
        # RFI opportunity = no raises AND no calls before hero (everyone folded)
        return True
    
    def analyze_hand(self, hand_text: str) -> None:
        """
        Analyze a single hand and update RFI statistics
        Works with any poker site
        """
        # Identify hero dynamically
        hero = self.identify_hero(hand_text)
        if not hero:
            return
        
        # Set hero name
        self.hero_name = hero
        
        # Get all players and positions
        players = self.extract_players_and_positions(hand_text)
        if not players or hero not in players:
            return
        
        hero_position = players[hero]["position"]
        position_category = self.get_position_category(hero_position)
        
        # Only interested in positions that can RFI
        if not position_category:
            return
        
        # Get pre-flop actions
        actions = self.extract_preflop_actions(hand_text)
        if not actions:
            return
        
        # Find actions before hero
        actions_before_hero = []
        hero_action = None
        
        for player, action in actions:
            if player == hero:
                hero_action = action
                break
            actions_before_hero.append((player, action))
        
        # Check if this is an RFI opportunity
        if not self.is_rfi_opportunity(hero_position, actions_before_hero):
            return
        
        # This is an RFI opportunity - record it
        stat_name = f"{position_category} {'RFI' if position_category in ['Early', 'Middle'] else 'Steal'}"
        
        if stat_name in self.stats:
            self.stats[stat_name]["opportunities"] += 1
            
            # Check if hero raised (attempted RFI)
            if hero_action == "raise":
                self.stats[stat_name]["attempts"] += 1
    
    def get_stats(self) -> Dict:
        """Get calculated statistics"""
        return self.stats
    
    def reset(self):
        """Reset statistics for new analysis"""
        self.stats = {
            "Early RFI": {"opportunities": 0, "attempts": 0},
            "Middle RFI": {"opportunities": 0, "attempts": 0},
            "CO Steal": {"opportunities": 0, "attempts": 0},
            "BTN Steal": {"opportunities": 0, "attempts": 0}
        }