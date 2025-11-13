"""
RFI (Raise First In) statistics calculator
Calculates Early RFI, Middle RFI, CO Steal, and BTN Steal
"""
import re
from typing import Dict, List, Tuple, Optional
from app.stats.position_mapping import get_position_map, get_position_category as centralized_position_category

class RFICalculator:
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
        Identify the HERO player from hand text
        HERO is identified by "Dealt to" line
        """
        # Look specifically for "Hero" as the player name
        # Pattern: "Dealt to Hero" or variations
        if re.search(r'Dealt to Hero\b', hand_text, re.IGNORECASE):
            return "Hero"
        
        # If not found, return None
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
        Returns dict with player info including position
        """
        players = {}
        
        # Extract all seats with players
        seat_pattern = r'Seat\s+(\d+):\s+(\S+)\s+\(\$?[\d,.]+ in chips\)'
        seats = re.findall(seat_pattern, hand_text)
        
        if not seats:
            return players
        
        # Get button position
        button_seat = self.get_button_position(hand_text)
        if not button_seat:
            return players
        
        # Create ordered list of active seats
        active_seats = [(int(seat), player) for seat, player in seats]
        active_seats.sort(key=lambda x: x[0])
        
        # Find button index
        button_idx = None
        for i, (seat_num, player) in enumerate(active_seats):
            if seat_num == button_seat:
                button_idx = i
                break
        
        if button_idx is None:
            return players
        
        num_players = len(active_seats)
        
        # Assign positions based on number of players and position relative to button
        for i, (seat_num, player) in enumerate(active_seats):
            relative_pos = (i - button_idx) % num_players
            
            # Map to standard positions based on player count
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
        Uses centralized position mapping for consistency.
        """
        position_map = get_position_map(num_players)
        if not position_map:
            # Unsupported player count - use generic naming
            if relative_pos == 0:
                return "BTN"
            elif relative_pos == 1:
                return "SB"
            elif relative_pos == 2:
                return "BB"
            else:
                return f"MP{relative_pos-2}"
        
        return position_map.get(relative_pos, "Unknown")
    
    def get_position_category(self, position: str) -> Optional[str]:
        """
        Categorize position for RFI stats
        Returns: 'Early', 'Middle', 'CO', 'BTN', or None
        Uses centralized categorization for consistency.
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
        Extract pre-flop actions in order
        Returns list of (player, action) tuples
        """
        actions = []
        
        # Find pre-flop section
        preflop_start = hand_text.find("*** HOLE CARDS ***")
        if preflop_start == -1:
            return actions
        
        # Find end of pre-flop (start of flop or end of hand)
        flop_start = hand_text.find("*** FLOP ***")
        summary_start = hand_text.find("*** SUMMARY ***")
        
        if flop_start != -1:
            preflop_end = flop_start
        elif summary_start != -1:
            preflop_end = summary_start
        else:
            preflop_end = len(hand_text)
        
        preflop_section = hand_text[preflop_start:preflop_end]
        
        # Parse actions (excluding blind posts and antes)
        action_pattern = r'(\S+):\s+(folds|calls|raises|bets|checks)'
        matches = re.findall(action_pattern, preflop_section)
        
        for player, action in matches:
            # Clean up action
            if "raises" in action or "bets" in action:
                action = "raise"
            elif "calls" in action:
                action = "call"
            elif "folds" in action:
                action = "fold"
            elif "checks" in action:
                action = "check"
            
            actions.append((player, action))
        
        return actions
    
    def is_rfi_opportunity(self, hero_position: str, actions_before_hero: List[Tuple[str, str]]) -> bool:
        """
        Check if this is an RFI opportunity for the hero
        RFI = Raise First In (no one has raised before)
        """
        # Check if anyone raised or called before hero
        for player, action in actions_before_hero:
            if action in ["raise", "call"]:
                return False
        
        # All players before hero folded = RFI opportunity
        return True
    
    def analyze_hand(self, hand_text: str) -> None:
        """
        Analyze a single hand and update RFI statistics
        """
        # Identify hero - always look for "Hero"
        hero = self.identify_hero(hand_text)
        if not hero:
            return
        
        # Hero should always be "Hero"
        if hero != "Hero":
            return
        
        # Set hero name
        self.hero_name = "Hero"
        
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
        """Get calculated statistics with percentages"""
        result = {}
        for stat_name, counts in self.stats.items():
            result[stat_name] = {
                "opportunities": counts["opportunities"],
                "attempts": counts["attempts"]
            }
        return result
    
    def reset(self):
        """Reset statistics for new analysis"""
        self.stats = {
            "Early RFI": {"opportunities": 0, "attempts": 0},
            "Middle RFI": {"opportunities": 0, "attempts": 0},
            "CO Steal": {"opportunities": 0, "attempts": 0},
            "BTN Steal": {"opportunities": 0, "attempts": 0}
        }
        self.hero_name = None