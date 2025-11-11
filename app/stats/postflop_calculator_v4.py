"""
Postflop Calculator V4 - With stack validation (≥16bb) and all-in detection
Integrates PostflopOpportunityValidator to ensure data quality consistency with preflop stats

Created: Nov 11, 2025
Changes from V3:
- Added PostflopOpportunityValidator integration
- Validates stack ≥16bb and all-in rules before counting opportunities
- Caches stacks_bb per hand for efficient validation
"""
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
from app.stats.position_mapping import get_position_map, get_position_category
from app.stats.postflop_validators import PostflopOpportunityValidator

logger = logging.getLogger(__name__)

class PostflopCalculatorV4:
    """Postflop calculator with stack/all-in validation (V4)"""
    
    def __init__(self, hand_collector=None):
        self.hands_processed = 0
        self.hands_with_flop = 0
        self.hands_with_turn = 0
        self.hands_with_river = 0
        self.hands_with_showdown = 0
        
        # Hand collection support (like PreflopStats)
        self.hand_collector = hand_collector
        self.current_hand_text = None
        self.current_hand_id = None
        
        # V4: Validator for opportunity validation (CRITICAL: needed for normalize_action + stack checks)
        from app.stats.postflop_validators import PostflopOpportunityValidator
        self.validator = PostflopOpportunityValidator()
        
        # V4: Cache stacks_bb per hand to avoid re-extraction
        self.cached_stacks_bb = None
        self.cached_hero_name = None
        
        # DEBUG: Rejection counters for Flop Cbet IP
        self.flop_cbet_ip_debug = {
            "pfr_checked": 0,
            "rejected_not_unopened": 0,  # NEW: Hero didn't face unopened pot
            "rejected_position": 0,
            "rejected_3bet": 0,
            "rejected_called_3bet": 0,
            "rejected_4bet": 0,
            "rejected_allin_pf": 0,
            "rejected_not_heads_up": 0,
            "rejected_not_ip": 0,
            "rejected_ip_none": 0,  # Track None separately
            "rejected_ip_false": 0,  # Track False separately
            "rejected_no_check": 0,
            "accepted": 0
        }
        
        # All 20 postflop statistics
        self.stats = {
            # Flop CBet Group
            "Flop CBet IP %": {"opportunities": 0, "attempts": 0},
            "Flop CBet 3BetPot IP": {"opportunities": 0, "attempts": 0},
            "Flop CBet OOP%": {"opportunities": 0, "attempts": 0},
            # Vs CBet Group
            "Flop fold vs Cbet IP": {"opportunities": 0, "attempts": 0},
            "Flop raise Cbet IP": {"opportunities": 0, "attempts": 0},
            "Flop raise Cbet OOP": {"opportunities": 0, "attempts": 0},
            "Fold vs Check Raise": {"opportunities": 0, "attempts": 0},
            # Vs Skipped CBet
            "Flop bet vs missed Cbet SRP": {"opportunities": 0, "attempts": 0},
            # Turn Play
            "Turn CBet IP%": {"opportunities": 0, "attempts": 0},
            "Turn Cbet OOP%": {"opportunities": 0, "attempts": 0},
            "Turn donk bet": {"opportunities": 0, "attempts": 0},
            "Turn donk bet SRP vs PFR": {"opportunities": 0, "attempts": 0},
            "Bet turn vs Missed Flop Cbet OOP SRP": {"opportunities": 0, "attempts": 0},
            "Turn Fold vs CBet OOP": {"opportunities": 0, "attempts": 0},
            # River & Showdown
            "WTSD%": {"opportunities": 0, "attempts": 0},
            "W$SD%": {"opportunities": 0, "attempts": 0},
            "W$WSF Rating": {"opportunities": 0, "attempts": 0, "player_sum": 0},  # opportunities=flops, attempts=wins, player_sum=sum of players
            "River Agg %": {"opportunities": 0, "attempts": 0, "total_hands": 0},  # opportunities=calls, attempts=bets/raises, total_hands=unique hands
            "River bet - Single Rsd Pot": {"opportunities": 0, "attempts": 0},
            "W$SD% B River": {"opportunities": 0, "attempts": 0},
        }
    
    def _extract_hand_id(self, hand_text: str) -> Optional[str]:
        """Extract hand ID from hand text"""
        # Try multiple patterns to catch different formats
        patterns = [
            r'Poker Hand\s*#([A-Z0-9]+):',  # PokerStars/GGPoker format with alphanumeric IDs
            r'Poker Hand\s*#(\d+):',        # PokerStars format with numeric IDs
            r'Game\s*#([A-Z0-9]+)',          # Other sites with alphanumeric
            r'Game\s*#(\d+)',                # Other sites with numeric
            r'Hand\s*#([A-Z0-9]+)',          # Generic format with alphanumeric
            r'Hand\s*#(\d+)',                # Generic format with numeric
        ]
        
        for pattern in patterns:
            match = re.search(pattern, hand_text)
            if match:
                return match.group(1)
        
        # Fallback: use first 50 chars as unique identifier
        return hand_text[:50].replace('\n', ' ').strip()
    
    def _detect_hero(self, hand_text: str) -> Optional[str]:
        """Detect Hero from 'Dealt to' line (same as PreflopStats)"""
        dealt_match = re.search(r'Dealt to ([^\[]+?)\s*\[', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        return None
    
    def _extract_positions(self, hand_text: str) -> Dict[str, str]:
        """
        Extract player positions from hand text (copied from PreflopStats)
        Returns mapping of player name to position
        """
        positions = {}
        
        # Look for button designation (# is optional for 888poker/WPN)
        button_match = re.search(r"Seat #?(\d+) is the button", hand_text)
        button_seat = int(button_match.group(1)) if button_match else None
        
        # Extract all seats with players - try all known formats automatically
        # This makes it work for ANY poker site without site-specific detection
        is_pokerstars = hand_text.startswith('PokerStars')
        
        seat_lines = []
        for line in hand_text.split('\n'):
            if 'Seat ' not in line:
                continue
                
            # Skip "out of hand" lines for PokerStars
            if is_pokerstars and 'out of hand' in line:
                continue
            
            # Try all known seat line formats (order matters: most specific first)
            patterns = [
                # PokerStars/GG: Seat X: PlayerName (chips in chips, $X.XX bounty)
                r'Seat\s+(\d+):\s+(.+?)\s+\([\d,]+(?:\.\d+)?\s+in\s+chips(?:,\s+\$[\d.]+\s+bounty)?\)',
                # Winamax: Seat X: PlayerName (stack, bounty)
                r'Seat\s+(\d+):\s+(.+?)\s+\([\d,]+(?:\.\d+)?(?:,\s+[^\)]+)?\)',
                # 888poker: Seat X: PlayerName ( stack )
                r'Seat\s+(\d+):\s*([^(]+?)\s*\(\s*[\d,. ]+\s*\)',
                # WPN: Seat X: PlayerName (stack)
                r'Seat\s+(\d+):\s*([^(]+?)\s*\([\d,. ]+\)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    seat_lines.append((match.group(1), match.group(2).strip()))
                    break  # Stop after first successful match
        
        seats = seat_lines
        
        if not seats or not button_seat:
            return positions
        
        # Create ordered list of active seats
        active_seats = [(int(seat), player) for seat, player in seats]
        active_seats.sort(key=lambda x: x[0])
        
        # Find button index - button seat may not have an active player
        # In that case, find the closest active seat clockwise from button
        button_idx = None
        for i, (seat_num, player) in enumerate(active_seats):
            if seat_num == button_seat:
                button_idx = i
                break
        
        # If button seat has no active player, find closest active seat clockwise
        if button_idx is None and button_seat is not None:
            # Find seat immediately clockwise from button
            # Create list of all seat numbers for wraparound logic
            active_seat_nums = [seat for seat, _ in active_seats]
            max_seat = max(active_seat_nums)
            
            # Start from button seat and move clockwise until we find an active seat
            for offset in range(1, max_seat + 1):
                test_seat = ((button_seat - 1 + offset) % max_seat) + 1  # 1-indexed seats
                if test_seat in active_seat_nums:
                    # Found closest active seat clockwise from button
                    button_idx = active_seat_nums.index(test_seat)
                    break
            
            # Last resort: if still no button, just use first active seat
            if button_idx is None and active_seats:
                button_idx = 0
        
        # Assign positions based on button
        num_players = len(active_seats)
        
        # Safety check: if still no button_idx, can't assign positions
        if button_idx is None:
            return positions
        
        for i, (seat_num, player) in enumerate(active_seats):
            # Calculate position relative to button
            relative_pos = (i - button_idx) % num_players
            
            # Use centralized position mapping
            position_map = get_position_map(num_players)
            if not position_map:
                continue
            
            positions[player] = position_map.get(relative_pos, "Unknown")
        
        return positions
    
    def _extract_preflop_actions(self, hand_text: str) -> List[Dict[str, Any]]:
        """
        Extract pre-flop actions from hand text (copied from PreflopStats)
        Returns list of actions in order
        """
        actions = []
        
        # Find pre-flop section (multi-site compatible)
        # Try all known preflop markers
        preflop_markers = [
            "*** HOLE CARDS ***",  # PokerStars, GG
            "*** PRE-FLOP ***",    # Winamax
            "** Dealing down cards **",  # 888poker
        ]
        
        preflop_start = -1
        for marker in preflop_markers:
            pos = hand_text.find(marker)
            if pos != -1:
                preflop_start = pos
                break
        
        # Find flop start (try multiple markers)
        flop_markers = ["*** FLOP ***", "** Dealing Flop **", "** Dealing flop **"]
        flop_start = -1
        for marker in flop_markers:
            pos = hand_text.find(marker)
            if pos != -1:
                flop_start = pos
                break
        
        if preflop_start == -1:
            return actions
        
        if flop_start == -1:
            preflop_section = hand_text[preflop_start:]
        else:
            preflop_section = hand_text[preflop_start:flop_start]
        
        # Parse actions - multi-site compatible (with or without colon)
        # EXTENDED to capture bet amounts for mathematical all-in detection
        # Supports 888poker bracketed format: "raises [1.400]", "bets [6.593]"
        colon_pattern = r'^(.*):\s+(folds|calls|raises|bets|checks|posts|is all-in)(?:\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?(?:\s+to\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?'
        space_pattern = r'^(.+?)\s+(folds|calls|raises|bets|checks|posts)(?:\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?(?:\s+to\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?'
        
        for line in preflop_section.split('\n'):
            line_stripped = line.strip()
            
            # Try colon format first
            match = re.match(colon_pattern, line_stripped)
            if not match:
                # Try space format (Winamax)
                match = re.match(space_pattern, line_stripped)
            
            if match:
                player = match.group(1).strip()
                action_text = match.group(2).strip()
                amount_str = match.group(3) if len(match.groups()) >= 3 else None
                to_amount_str = match.group(4) if len(match.groups()) >= 4 else None
                
                # Skip blind posts
                if "posts" in action_text:
                    continue
                
                # Parse amount (use "to" amount for raises, otherwise use first amount)
                amount = 0.0
                if to_amount_str:
                    amount = self._parse_amount(to_amount_str)
                elif amount_str:
                    amount = self._parse_amount(amount_str)
                
                # Detect if this is an all-in (text-based first, mathematical later)
                is_allin = "all-in" in line.lower() or "all in" in line.lower()
                
                actions.append({
                    "player": player,
                    "action": action_text,
                    "amount": amount,  # NEW: Store amount for mathematical all-in detection
                    "is_raise": "raises" in action_text or "bets" in action_text,
                    "is_call": "calls" in action_text,
                    "is_fold": "folds" in action_text,
                    "is_allin": is_allin
                })
        
        return actions
    
    def _extract_street_actions(self, hand_text: str, street: str) -> List[Dict[str, Any]]:
        """
        Extract actions from a specific street (flop, turn, river) - multi-site compatible
        Returns list of actions in order
        
        Works for: PokerStars, GGPoker, Winamax, 888poker, WPN
        """
        actions = []
        
        # Find street section (start marker) - try multiple formats
        start_markers_by_street = {
            "flop": ["*** FLOP ***", "** Dealing Flop **", "** Dealing flop **"],
            "turn": ["*** TURN ***", "** Dealing Turn **", "** Dealing turn **"],
            "river": ["*** RIVER ***", "** Dealing River **", "** Dealing river **"]
        }
        
        if street not in start_markers_by_street:
            return actions
        
        # Try each possible start marker for this street
        start_pos = -1
        for marker in start_markers_by_street[street]:
            pos = hand_text.find(marker)
            if pos != -1:
                start_pos = pos
                break
        
        if start_pos == -1:
            return actions
        
        # Find end of street section (multi-site compatible)
        # Try multiple end markers for each street
        end_markers_by_street = {
            "flop": ["*** TURN ***", "** Dealing Turn **", "** Dealing turn **"],
            "turn": ["*** RIVER ***", "** Dealing River **", "** Dealing river **"],
            "river": ["*** SHOW DOWN ***", "*** SHOWDOWN ***", "*** SUMMARY ***", "** Summary **"]
        }
        
        end_pos = -1
        for end_marker in end_markers_by_street[street]:
            pos = hand_text.find(end_marker, start_pos)
            if pos != -1:
                if end_pos == -1 or pos < end_pos:
                    end_pos = pos
        
        if end_pos == -1:
            # No next street, hand ended on this street
            street_section = hand_text[start_pos:]
        else:
            street_section = hand_text[start_pos:end_pos]
        
        # Parse actions - multi-site compatible (with or without colon)
        # EXTENDED to capture bet amounts for mathematical all-in detection
        # Patterns support: "raises 500 to 1500", "calls 1000", "bets 750", "raises [1.400]" (888poker)
        colon_pattern = r'^(.*):\s+(folds|calls|raises|bets|checks)(?:\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?(?:\s+to\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?'
        space_pattern = r'^(.+?)\s+(folds|calls|raises|bets|checks)(?:\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?(?:\s+to\s+(?:€|£|\$|¥|R\$)?\[?([0-9,.]+)\]?)?'
        
        for line in street_section.split('\n'):
            match = re.match(colon_pattern, line.strip())
            if not match:
                # Try space-delimited format (no colon)
                match = re.match(space_pattern, line.strip())
            
            if match:
                player = match.group(1).strip()
                action_text = match.group(2).strip()
                amount_str = match.group(3)  # First number (bet/call amount or raise from)
                to_amount_str = match.group(4)  # Second number (raise to amount)
                
                # Parse amount (use "to" amount for raises, otherwise use first amount)
                amount = 0.0
                if to_amount_str:  # "raises 500 to 1500" -> amount = 1500
                    amount = self._parse_amount(to_amount_str)
                elif amount_str:  # "calls 1000" or "bets 750" -> amount = that value
                    amount = self._parse_amount(amount_str)
                
                # Detect if this is an all-in (text-based first, mathematical later)
                is_allin = "all-in" in line.lower() or "all in" in line.lower()
                
                actions.append({
                    "player": player,
                    "action": action_text,
                    "amount": amount,  # NEW: Store amount for mathematical all-in detection
                    "is_raise": "raises" in action_text or "bets" in action_text,
                    "is_call": "calls" in action_text,
                    "is_fold": "folds" in action_text,
                    "is_check": "checks" in action_text,
                    "is_allin": is_allin
                })
        
        return actions
    
    def _parse_amount(self, text: str) -> float:
        """Parse monetary amount from text, removing currency symbols, brackets, and handling decimals."""
        if not text:
            return 0.0
        
        # Remove currency symbols, brackets, commas, and spaces (888poker uses brackets)
        cleaned = re.sub(r'[€$£¥R\$,\s\[\]]', '', text)
        
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    
    def _extract_stacks_and_ante(self, hand_text: str) -> Tuple[Dict[str, float], float]:
        """
        Extract player stacks and ante from hand text (multi-site compatible).
        Returns: (stacks_dict, ante_amount)
        """
        stacks = {}
        ante = 0.0
        
        # Extract stacks from seat lines - use multi-pattern approach like _extract_positions()
        # to support ALL poker sites (PokerStars, GG, 888, WPN, Winamax)
        for line in hand_text.split('\n'):
            if 'Seat ' not in line:
                continue
            
            # Skip "out of hand" lines
            if 'out of hand' in line:
                continue
            
            # Try all known seat line formats (order matters: most specific first)
            patterns = [
                # PokerStars/GG: Seat X: PlayerName (chips in chips, $X.XX bounty)
                r'Seat\s+\d+:\s+(.+?)\s+\((?:R\$|€|£|\$|¥|₹)?([0-9,.]+)\s+in\s+chips(?:,\s*[^\)]+)?\)',
                # Winamax: Seat X: PlayerName (stack, bounty) or PlayerName (€12345)
                r'Seat\s+\d+:\s+(.+?)\s+\((?:R\$|€|£|\$|¥|₹)?([0-9,.]+)(?:,\s*[^\)]+)?\)',
                # 888poker: Seat X: PlayerName ( stack )
                r'Seat\s+\d+:\s*([^(]+?)\s*\(\s*(?:R\$|€|£|\$|¥|₹)?\s*([0-9,. ]+)\s*\)',
                # WPN: Seat X: PlayerName (stack)
                r'Seat\s+\d+:\s*([^(]+?)\s*\((?:R\$|€|£|\$|¥|₹)?\s*([0-9,. ]+)\)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    player_name = match.group(1).strip()
                    stack_str = match.group(2)
                    stack_value = self._parse_amount(stack_str)
                    if stack_value > 0:
                        stacks[player_name] = stack_value
                    break  # Stop after first successful match
        
        # Extract ante (if present)
        # Patterns: "PlayerName: posts the ante 60", "posts ante €0.05"
        ante_patterns = [
            r'posts (?:the )?ante (?:€|£|\$|¥|R\$|₹)?([0-9,.]+)',
            r'Ante: (?:€|£|\$|¥|R\$|₹)?([0-9,.]+)'
        ]
        for pattern in ante_patterns:
            match = re.search(pattern, hand_text)
            if match:
                ante = self._parse_amount(match.group(1))
                break
        
        return stacks, ante
    
    def _extract_stacks_in_bb(self, hand_text: str, hero_name: str) -> Dict[str, float]:
        """
        Extract player stacks converted to big blinds (V4 helper for validation).
        
        Args:
            hand_text: Raw hand text
            hero_name: Hero's real name (for mapping to 'Hero')
            
        Returns:
            Dict mapping player names to stack sizes in BB
        """
        # Extract stacks in chips and ante
        stacks_chips, ante = self._extract_stacks_and_ante(hand_text)
        
        # Extract big blind size
        bb_size = 0.0
        bb_pattern = r'posts (?:the )?big blind \[?(?:R\$|€|£|\$|¥|₹)?([0-9,.]+)\]?'
        blinds_pattern = r'([0-9,.]+)/([0-9,.]+)\s+Blinds'
        
        match = re.search(bb_pattern, hand_text)
        if match:
            bb_size = self._parse_amount(match.group(1))
        elif not bb_size:
            match = re.search(blinds_pattern, hand_text)
            if match:
                bb_size = self._parse_amount(match.group(2))
        
        if bb_size <= 0:
            logger.warning(f"[V4] Hand {self.current_hand_id}: Could not extract BB size, validation may fail")
            return {}
        
        # Convert to BB and replace hero name with "Hero"
        stacks_bb = {}
        for player, chips in stacks_chips.items():
            player_key = "Hero" if player == hero_name else player
            stacks_bb[player_key] = round(chips / bb_size, 2)
        
        return stacks_bb
    
    def _mark_mathematical_allins_postflop(
        self, 
        hand_text: str,
        preflop_actions: List[Dict[str, Any]], 
        flop_actions: List[Dict[str, Any]], 
        turn_actions: List[Dict[str, Any]], 
        river_actions: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Mark all-in actions mathematically across all streets.
        
        CRITICAL: Ante does NOT count as available stack.
        If player has 8,845 chips with 60 ante, available = 8,785.
        If they raise 8,785, they're all-in.
        
        Args:
            hand_text: Raw hand text for stack/ante extraction
            preflop_actions, flop_actions, turn_actions, river_actions: Action lists
            
        Returns:
            Tuple of updated action lists (preflop, flop, turn, river)
        """
        # Extract initial stacks and ante
        stacks, ante = self._extract_stacks_and_ante(hand_text)
        
        if not stacks:
            # Can't do mathematical detection without stacks, return as-is
            return preflop_actions, flop_actions, turn_actions, river_actions
        
        # Initialize available stacks (stack - ante)
        available_stacks = {name: stack - ante for name, stack in stacks.items()}
        
        # Track committed amounts in current betting round
        committed_in_round = {name: 0.0 for name in available_stacks}
        
        # Process each street
        all_streets = [
            ('preflop', preflop_actions),
            ('flop', flop_actions),
            ('turn', turn_actions),
            ('river', river_actions)
        ]
        
        for street_name, actions in all_streets:
            # Reset committed amounts for new street (except preflop)
            if street_name != 'preflop':
                committed_in_round = {name: 0.0 for name in available_stacks}
            
            for action in actions:
                player_name = action.get('player')
                action_type = action.get('action', '')
                amount = action.get('amount', 0.0) or 0.0
                
                # Skip if player not tracked (shouldn't happen)
                if player_name not in available_stacks:
                    continue
                
                # For chip-committing actions (call, raise, bet)
                if action_type in ['calls', 'raises', 'bets'] and amount > 0:
                    available_stack = available_stacks[player_name]
                    already_committed = committed_in_round.get(player_name, 0.0)
                    
                    # Additional chips needed for this action
                    additional_needed = amount - already_committed
                    
                    # CRITICAL: Mathematical all-in detection
                    # Allow 0.01 tolerance for rounding
                    if additional_needed >= available_stack - 0.01:
                        action['is_allin'] = True
                    
                    # Update tracking
                    available_stacks[player_name] -= additional_needed
                    committed_in_round[player_name] = amount
        
        return preflop_actions, flop_actions, turn_actions, river_actions
    
    def _first_raiser(self, actions: List[Dict[str, Any]]) -> Optional[str]:
        """Find the first player to raise (PFR)"""
        for action in actions:
            if action.get("is_raise"):
                return action["player"]
        return None
    
    def _who_3bet_4bet(self, actions: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine who 3-bet and 4-bet
        Returns (three_bettor, four_bettor)
        """
        raise_count = 0
        three_bettor = None
        four_bettor = None
        
        for action in actions:
            if action.get("is_raise"):
                raise_count += 1
                if raise_count == 2:
                    three_bettor = action["player"]
                elif raise_count == 3:
                    four_bettor = action["player"]
        
        return three_bettor, four_bettor
    
    def _has_allin_preflop(self, actions: List[Dict[str, Any]]) -> bool:
        """Check if HERO was all-in preflop (excludes hands where Hero has no postflop decisions)"""
        return any(action.get("is_allin", False) and action.get("player") == "Hero" for action in actions)
    
    
    def _players_on_flop(self, positions: Dict[str, str], preflop_actions: List[Dict[str, Any]], flop_actions: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """
        Determine which players SAW the flop (did not fold preflop).
        This is the standard count for NumberOfPlayersSawFlop.
        
        CRITICAL FIX (Oct 2025): Use flop_actions to determine who actually participated,
        not just "all_players - preflop_folds" which incorrectly counts blinds.
        """
        # If flop_actions available, use players who actually acted on flop (most accurate)
        if flop_actions:
            flop_participants = set()
            for action in flop_actions:
                flop_participants.add(action["player"])
            return list(flop_participants)
        
        # Fallback: Players who saw flop = all players - preflop folds
        # NOTE: This is less accurate as it may include blinds who folded but aren't in preflop_actions
        all_players = set(positions.keys())
        folded_preflop = {action["player"] for action in preflop_actions if action.get("is_fold")}
        return list(all_players - folded_preflop)
    
    def _determine_ip_on_flop(self, positions: Dict[str, str], flop_players: List[str], hero: str) -> Optional[bool]:
        """
        Determine if hero is in position on flop (multiway-aware)
        Returns True if hero is IP (last to act), False if OOP, None if position unknown
        
        IP = Hero's position index >= all other players' position indices
        
        CRITICAL FIX (Nov 2025): Filter positions to only include flop_players, excluding
        players who folded preflop (e.g., BB who folded to BTN raise). This prevents
        false OOP detection when Hero is actually IP against remaining opponents.
        """
        if hero not in flop_players:
            return None
        
        # Filter positions to only include players who actually saw the flop
        # This excludes folded blinds and other players who mucked preflop
        active_positions = {p: positions.get(p) for p in flop_players if positions.get(p)}
        
        hero_pos = active_positions.get(hero)
        if not hero_pos:
            return None
        
        # Position order for postflop action (acts FIRST to LAST on flop)
        # SB acts first, then BB, then UTG, etc. BTN acts last
        # BTN/SB is heads-up button (acts last postflop but is also SB preflop)
        position_order = ["SB", "BB", "UTG", "UTG+1", "UTG+2", "MP", "MP+1", "MP+2", "HJ", "CO", "BTN", "BTN/SB"]
        
        try:
            hero_idx = position_order.index(hero_pos)
        except ValueError:
            return None  # Hero position unknown
        
        # Check if hero has highest position index among all active flop players
        for player in flop_players:
            if player == hero:
                continue
            
            player_pos = active_positions.get(player)
            if not player_pos:
                continue  # Skip players with unknown position
            
            try:
                player_idx = position_order.index(player_pos)
                if player_idx > hero_idx:
                    return False  # Hero is not last to act (OOP)
            except ValueError:
                continue  # Skip players with unknown position
        
        # Hero has highest position index among all active players (or tied for highest)
        return True
    
    def _players_on_turn(self, positions: Dict[str, str], flop_actions: List[Dict[str, Any]], turn_actions: List[Dict[str, Any]]) -> List[str]:
        """
        Determine which players reached the turn
        Players who were on flop and didn't fold on flop
        """
        # Start with players who reached the flop (not all seated players!)
        # This prevents including players who folded preflop
        from app.stats.postflop_calculator_v3 import PostflopCalculatorV3
        
        # Get players who reached flop first
        flop_players = self._players_on_flop(positions, [])  # Pass empty preflop actions since we'll determine from flop presence
        
        # More robust: check who actually has actions on flop
        flop_participants = set()
        for action in flop_actions:
            flop_participants.add(action["player"])
        
        # If we have flop participants from actions, use those
        if flop_participants:
            flop_players = list(flop_participants)
        
        # Track who folded on flop
        folded_on_flop = set()
        for action in flop_actions:
            if action.get("is_fold"):
                folded_on_flop.add(action["player"])
        
        # Players on turn = flop participants - who folded on flop
        turn_players = [p for p in flop_players if p not in folded_on_flop]
        
        return turn_players
    
    def _players_on_river(self, positions: Dict[str, str], turn_actions: List[Dict[str, Any]], river_actions: List[Dict[str, Any]]) -> List[str]:
        """
        Determine which players reached the river
        Players who were on turn and didn't fold on turn
        """
        # Get players who participated in turn
        turn_participants = set()
        for action in turn_actions:
            turn_participants.add(action["player"])
        
        if not turn_participants:
            # If no turn actions, fall back to extracting from river
            river_participants = set()
            for action in river_actions:
                river_participants.add(action["player"])
            return list(river_participants)
        
        # Track who folded on turn
        folded_on_turn = set()
        for action in turn_actions:
            if action.get("is_fold"):
                folded_on_turn.add(action["player"])
        
        # Players on river = turn participants - who folded on turn
        river_players = [p for p in turn_participants if p not in folded_on_turn]
        
        return river_players
    
    def _determine_ip_on_turn(self, positions: Dict[str, str], turn_players: List[str], hero: str) -> Optional[bool]:
        """
        Determine if hero is in position on turn (multiway-aware)
        Returns True if hero is IP (last to act), False if OOP, None if position unknown
        Same logic as flop IP determination
        
        IP = Hero's position index >= all other players' position indices
        """
        if hero not in turn_players:
            return None
        
        hero_pos = positions.get(hero)
        if not hero_pos:
            return None
        
        # Position order for postflop action (acts FIRST to LAST on turn)
        # Same as flop: SB acts first, then BB, then UTG, etc. BTN acts last
        # BTN/SB is heads-up button (acts last postflop but is also SB preflop)
        position_order = ["SB", "BB", "UTG", "UTG+1", "UTG+2", "MP", "MP+1", "MP+2", "HJ", "CO", "BTN", "BTN/SB"]
        
        try:
            hero_idx = position_order.index(hero_pos)
        except ValueError:
            return None  # Hero position unknown
        
        # Check if hero has highest position index among all turn players
        for player in turn_players:
            if player == hero:
                continue
            
            player_pos = positions.get(player)
            if not player_pos:
                continue  # Skip players with unknown position
            
            try:
                player_idx = position_order.index(player_pos)
                if player_idx > hero_idx:
                    return False  # Hero is not last to act (OOP)
            except ValueError:
                continue  # Skip players with unknown position
        
        # Hero has highest position index among all players (or tied for highest)
        return True
    
    def _has_flop_multisite(self, hand_text: str) -> bool:
        """Detect if flop was reached - multi-site compatible"""
        # PokerStars, GGPoker, Winamax, WPN
        if "*** FLOP ***" in hand_text:
            return True
        # 888poker (case variations)
        if "** Dealing Flop **" in hand_text or "** Dealing flop **" in hand_text:
            return True
        return False
    
    def _has_turn_multisite(self, hand_text: str) -> bool:
        """Detect if turn was reached - multi-site compatible"""
        # PokerStars, GGPoker, Winamax, WPN
        if "*** TURN ***" in hand_text:
            return True
        # 888poker (case variations)
        if "** Dealing Turn **" in hand_text or "** Dealing turn **" in hand_text:
            return True
        return False
    
    def _has_river_multisite(self, hand_text: str) -> bool:
        """Detect if river was reached - multi-site compatible"""
        # PokerStars, GGPoker, Winamax, WPN
        if "*** RIVER ***" in hand_text:
            return True
        # 888poker (case variations)
        if "** Dealing River **" in hand_text or "** Dealing river **" in hand_text:
            return True
        return False
    
    def _get_actor_stack(self, actor_name: str, default: float = 0.0) -> float:
        """
        Get actor stack from cached stacks_bb (V4 validation helper).
        
        Args:
            actor_name: Player name (or "Hero")
            default: Default value if not found
            
        Returns:
            Stack in BB, or default if not found
        """
        if not self.cached_stacks_bb:
            return default
        return self.cached_stacks_bb.get(actor_name, default)
    
    def _build_validation_context(
        self,
        stat_name: str,
        actors: Dict[str, str],
        street_actions: List[Dict] = None,
        river_actions: List[Dict] = None
    ) -> Dict[str, Any]:
        """
        Build validation context dict for PostflopOpportunityValidator (V4 helper).
        
        Args:
            stat_name: Name of the stat being validated
            actors: Dict mapping roles to player names, e.g.:
                {"hero": "Hero", "villain": "Opponent1", "bettor": "Opponent2"}
            street_actions: Actions on current street (for all-in check)
            river_actions: River actions (for river stats)
            
        Returns:
            Context dict ready for validator.validate()
        """
        context = {"hero_stack_bb": self._get_actor_stack("Hero", 0.0)}
        
        # Add actor stacks
        for role, player_name in actors.items():
            if role != "hero":
                context[f"{role}_stack_bb"] = self._get_actor_stack(player_name, 0.0)
        
        # Add action lists
        if street_actions is not None:
            context["street_actions"] = street_actions
        if river_actions is not None:
            context["river_actions"] = river_actions
        
        return context
    
    def _validate_opportunity(
        self,
        stat_name: str,
        context: Dict[str, Any]
    ) -> bool:
        """
        Validate opportunity using PostflopOpportunityValidator (V4 helper).
        
        Args:
            stat_name: Name of the stat being validated
            context: Validation context from _build_validation_context()
            
        Returns:
            True if valid, False if rejected
        """
        if not self.validator:
            # No validator initialized - allow opportunity (shouldn't happen)
            logger.warning(f"[V4] Hand {self.current_hand_id}: Validator not initialized for {stat_name}")
            return True
        
        valid, reason = self.validator.validate(stat_name, context)
        
        if not valid:
            logger.debug(f"[V4] Hand {self.current_hand_id}: {stat_name} rejected - {reason}")
        
        return valid
    
    def analyze_hand(self, hand_text: str) -> None:
        """Analyze a single hand using raw hand text (same input as PreflopStats)"""
        if not hand_text or not hand_text.strip():
            return
        
        # Track current hand for sample collection
        self.current_hand_text = hand_text
        self.current_hand_id = self._extract_hand_id(hand_text)
        
        # Detect hero
        hero = self._detect_hero(hand_text)
        if not hero:
            return
        
        # V4: Initialize validator and cache stacks for this hand
        self.validator = PostflopOpportunityValidator(hand_id=self.current_hand_id)
        self.cached_hero_name = hero
        self.cached_stacks_bb = self._extract_stacks_in_bb(hand_text, hero)
        
        # Extract positions and preflop actions
        positions = self._extract_positions(hand_text)
        preflop_actions = self._extract_preflop_actions(hand_text)
        
        # Replace hero's actual name with "Hero" for consistency
        if hero in positions:
            positions["Hero"] = positions.pop(hero)
        for action in preflop_actions:
            if action["player"] == hero:
                action["player"] = "Hero"
        
        # Count EVERY valid hand with hero
        self.hands_processed += 1
        
        # Log every 1000 hands to track progress
        if self.hands_processed % 1000 == 0:
            logger.info(f"PostflopCalculatorV3: Processed {self.hands_processed} hands")
        
        # Check which streets were reached by looking for markers (multi-site compatible)
        has_flop = self._has_flop_multisite(hand_text)
        has_turn = self._has_turn_multisite(hand_text)
        has_river = self._has_river_multisite(hand_text)
        has_showdown = self._has_showdown_multisite(hand_text)
        
        if has_flop:
            self.hands_with_flop += 1
        if has_turn:
            self.hands_with_turn += 1
        if has_river:
            self.hands_with_river += 1
        if has_showdown:
            self.hands_with_showdown += 1
        
        # Extract ALL street actions up front (flop, turn, river)
        flop_actions = self._extract_street_actions(hand_text, "flop") if has_flop else []
        turn_actions = self._extract_street_actions(hand_text, "turn") if has_turn else []
        river_actions = self._extract_street_actions(hand_text, "river") if has_river else []
        
        # CRITICAL: Apply mathematical all-in detection to ALL streets (including preflop)
        # This catches cases like "raises 8,785 with stack 8,845 and ante 60" → all-in
        preflop_actions, flop_actions, turn_actions, river_actions = self._mark_mathematical_allins_postflop(
            hand_text, preflop_actions, flop_actions, turn_actions, river_actions
        )
        
        # CRITICAL FIX (Nov 2025): Normalize preflop actions to ensure is_raise flag is present
        # This is required for _first_raiser() to detect PFR correctly
        preflop_actions = [self.validator.normalize_action(a) for a in preflop_actions]
        
        # Normalize hero name in ALL actions (critical for detection)
        for action in flop_actions:
            if action.get("player") == hero:
                action["player"] = "Hero"
        for action in turn_actions:
            if action.get("player") == hero:
                action["player"] = "Hero"
        for action in river_actions:
            if action.get("player") == hero:
                action["player"] = "Hero"
        
        # Get hero position
        hero_position = positions.get("Hero")
        
        # Check if Hero faced UNOPENED pot (PreflopActionFacingPlayer=Unopened)
        # This means NO calls/limps before Hero's first action
        hero_faced_unopened = False
        if hero_is_pfr:
            # Find Hero's first action
            for i, action in enumerate(preflop_actions):
                if action["player"] == "Hero" and action.get("is_raise"):
                    # Check if any calls/limps before this action
                    had_action_before = any(
                        preflop_actions[j].get("is_call") or preflop_actions[j].get("is_raise")
                        for j in range(i)
                    )
                    hero_faced_unopened = not had_action_before
                    break
        
        # Check if hero is 3bettor or 4bettor
        three_bettor, four_bettor = self._who_3bet_4bet(preflop_actions)
        hero_did_3bet = (three_bettor == "Hero")
        hero_did_4bet = (four_bettor == "Hero")
        
        # Check if hero called a 3bet
        hero_called_3bet = False
        if three_bettor and three_bettor != "Hero":
            seen_3bet = False
            for action in preflop_actions:
                if action["player"] == three_bettor and action.get("is_raise"):
                    seen_3bet = True
                elif seen_3bet and action["player"] == "Hero" and action.get("is_call"):
                    hero_called_3bet = True
                    break
        
        # Check if there was an all-in preflop
        allin_preflop = self._has_allin_preflop(preflop_actions)
        
        # Get flop players and determine IP/OOP status
        # IMPORTANT: Pass flop_actions to count active players when Hero acts (HM logic)
        # Extract flop actions FIRST to ensure they're available for player counting
        if has_flop:
            if not flop_actions:  # May not have been extracted yet
                flop_actions = self._extract_street_actions(hand_text, "flop")
        
        flop_players = self._players_on_flop(positions, preflop_actions, flop_actions) if has_flop else []
        hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero") if has_flop and flop_players else None
        
        # DEBUG: Track player counts for PokerStars
        if has_flop and hero_is_pfr and self.current_hand_text and "PokerStars" in self.current_hand_text:
            flop_players_no_actions = self._players_on_flop(positions, preflop_actions, None)
            if len(flop_actions) > 0:
                logger.debug(f"[FLOP_ACTIONS] {self.current_hand_id}: {len(flop_actions)} actions, players: {len(flop_players_no_actions)}→{len(flop_players)}")
            if len(flop_players) != len(flop_players_no_actions):
                logger.debug(f"[PLAYER_COUNT] {self.current_hand_id}: {len(flop_players_no_actions)}→{len(flop_players)} players (fold before hero)")
        
        # ============== FLOP CBet IP % - HM FILTER IMPLEMENTATION ==============
        # HM Filter criteria (ALL must be true):
        # 1. Hero is PFR (first raiser) - DidPFR=true
        # 2. Hero position in (EP, MP, CO, BTN) - NOT SB/BB
        # 3. Hero did NOT 3bet - DidThreeBet=false
        # 4. Hero did NOT call 3bet - CalledPreflopThreeBet=false
        # 5. Hero did NOT 4bet - DidFourBet=false
        # 6. Hand reached flop - SawFlop=true
        # 7. NO all-in preflop
        # 8. Hero is IP on flop (heads-up) - InPositionOnFlop=true, NumberOfPlayersSawFlop=2
        # 9. NO all-in on flop before hero acts
        # 10. Action checked to Hero - FlopActionFacingBet=false
        
        if has_flop and hero_position:
            # Check hero position: MUST be EP, MP, CO, or BTN (NOT pure SB/BB)
            # Valid positions from get_position_map: Early (UTG, UTG+1, UTG+2), Middle (MP, HJ), Late (CO, BTN)
            # BTN/SB is heads-up button (counts as valid - acts last postflop)
            valid_positions = ["UTG", "UTG+1", "UTG+2", "MP", "HJ", "CO", "BTN", "BTN/SB"]
            hero_in_valid_position = hero_position in valid_positions
            
            # Check all opportunity criteria for "Flop CBet IP %"
            # CRITICAL: Must be heads-up only (exactly 2 players on flop)
            is_heads_up = len(flop_players) == 2
            
            # DEBUG: Log rejection reasons for PokerStars hands
            debug_reject = False
            if hero_is_pfr and self.current_hand_text and "PokerStars" in self.current_hand_text:
                self.flop_cbet_ip_debug["pfr_checked"] += 1
                reject_reasons = []
                if not hero_faced_unopened:
                    reject_reasons.append("not_unopened")
                    self.flop_cbet_ip_debug["rejected_not_unopened"] += 1
                if not hero_in_valid_position:
                    reject_reasons.append(f"position={hero_position}")
                    self.flop_cbet_ip_debug["rejected_position"] += 1
                if hero_did_3bet:
                    reject_reasons.append("did_3bet")
                    self.flop_cbet_ip_debug["rejected_3bet"] += 1
                if hero_called_3bet:
                    reject_reasons.append("called_3bet")
                    self.flop_cbet_ip_debug["rejected_called_3bet"] += 1
                if hero_did_4bet:
                    reject_reasons.append("did_4bet")
                    self.flop_cbet_ip_debug["rejected_4bet"] += 1
                if allin_preflop:
                    reject_reasons.append("allin_preflop")
                    self.flop_cbet_ip_debug["rejected_allin_pf"] += 1
                if not is_heads_up:
                    reject_reasons.append(f"players={len(flop_players)}")
                    self.flop_cbet_ip_debug["rejected_not_heads_up"] += 1
                if hero_ip_flop is not True:
                    if hero_ip_flop is None:
                        reject_reasons.append(f"ip=None")
                        self.flop_cbet_ip_debug["rejected_ip_none"] += 1
                    else:
                        reject_reasons.append(f"ip=False")
                        self.flop_cbet_ip_debug["rejected_ip_false"] += 1
                    self.flop_cbet_ip_debug["rejected_not_ip"] += 1
                
                if reject_reasons:
                    logger.debug(f"[FLOP_CBET_IP] REJECTED {self.current_hand_id}: {', '.join(reject_reasons)}")
                    debug_reject = True
            
            if (hero_is_pfr and
                hero_faced_unopened and  # PreflopActionFacingPlayer=Unopened (HM filter requirement)
                hero_in_valid_position and
                not hero_did_3bet and 
                not hero_called_3bet and
                not hero_did_4bet and
                not allin_preflop and 
                is_heads_up and  # Must be exactly 2 players
                hero_ip_flop is True):  # Must be IP (last to act)
                
                # Find hero's first action index on flop
                hero_action_index = None
                for i, action in enumerate(flop_actions):
                    if action["player"] == "Hero":
                        hero_action_index = i
                        break
                
                # Check for all-in BEFORE hero acts
                allin_before_hero = False
                if hero_action_index is not None:
                    for i in range(hero_action_index):
                        if flop_actions[i].get("is_allin"):
                            allin_before_hero = True
                            break
                
                # Check if action was checked to hero (and no all-in before)
                checked_to_hero = False
                if hero_action_index is not None and hero_action_index > 0 and not allin_before_hero:
                    prev_action = flop_actions[hero_action_index - 1]
                    if prev_action.get("is_check"):
                        checked_to_hero = True
                
                # DEBUG: Log final check rejection
                if not checked_to_hero and not debug_reject and self.current_hand_text and "PokerStars" in self.current_hand_text:
                    self.flop_cbet_ip_debug["rejected_no_check"] += 1
                    if hero_action_index is None:
                        logger.debug(f"[FLOP_CBET_IP] REJECTED {self.current_hand_id}: no_hero_action")
                    elif hero_action_index == 0:
                        logger.debug(f"[FLOP_CBET_IP] REJECTED {self.current_hand_id}: hero_first_to_act")
                    elif allin_before_hero:
                        logger.debug(f"[FLOP_CBET_IP] REJECTED {self.current_hand_id}: allin_before_hero")
                    else:
                        logger.debug(f"[FLOP_CBET_IP] REJECTED {self.current_hand_id}: no_check_to_hero")
                
                # This is an opportunity (all conditions met)
                if checked_to_hero:
                    # V4: Validate stacks before counting opportunity
                    villain_name = next((p for p in flop_players if p != "Hero"), None)
                    if villain_name:  # Guard: ensure villain exists (should always be true in heads-up)
                        context = self._build_validation_context(
                            stat_name="Flop CBet IP %",
                            actors={"villain": villain_name},
                            street_actions=flop_actions,
                            river_actions=None
                        )
                        
                        if self._validate_opportunity("Flop CBet IP %", context):
                            self.stats["Flop CBet IP %"]["opportunities"] += 1
                            
                            # DEBUG: Log acceptance
                            if self.current_hand_text and "PokerStars" in self.current_hand_text:
                                self.flop_cbet_ip_debug["accepted"] += 1
                                logger.debug(f"[FLOP_CBET_IP] ACCEPTED {self.current_hand_id}: pos={hero_position}, players={len(flop_players)}, ip={hero_ip_flop}")
                            
                            # Collect hand sample for this opportunity (always collect on opportunity)
                            if self.hand_collector:
                                self.hand_collector.add_hand("Flop CBet IP %", self.current_hand_text, self.current_hand_id)
                            
                            # Check if hero bet (attempt criteria)
                            hero_bet = any(a["player"] == "Hero" and a.get("is_raise") 
                                           for a in flop_actions)
                            if hero_bet:
                                self.stats["Flop CBet IP %"]["attempts"] += 1
            
            # ============== FLOP CBet 3BetPot IP ==============
            # Criteria:
            # 1. Hero made 3bet preflop (not all-in)
            # 2. Only original raiser (PFR) called the 3bet, all others folded
            # 3. Hand reached flop (no all-in preflop)
            # 4. Hero is last to act (IP) on flop
            # 5. Original raiser checks (action checked to Hero)
            # Attempt: Hero bets
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # Check if hero is 3bettor
                three_bettor, four_bettor = self._who_3bet_4bet(preflop_actions)
                hero_did_3bet = (three_bettor == "Hero")
                
                # Find the original raiser (PFR)
                pfr = self._first_raiser(preflop_actions)
                
                # Check if there was all-in preflop
                allin_preflop = self._has_allin_preflop(preflop_actions)
                
                # Check if hero is IP on flop (heads-up)
                hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                
                if hero_did_3bet and pfr and pfr != "Hero" and not allin_preflop and hero_ip_flop is True:
                    # Verify only PFR called the 3bet (heads-up on flop)
                    if len(flop_players) == 2 and pfr in flop_players:
                        # Find hero's first action index on flop
                        hero_action_index = None
                        for i, action in enumerate(flop_actions):
                            if action["player"] == "Hero":
                                hero_action_index = i
                                break
                        
                        # Check for all-in BEFORE hero acts
                        allin_before_hero = False
                        if hero_action_index is not None:
                            for i in range(hero_action_index):
                                if flop_actions[i].get("is_allin"):
                                    allin_before_hero = True
                                    break
                        
                        # Check if action was checked to hero (and no all-in before)
                        checked_to_hero = False
                        if hero_action_index is not None and hero_action_index > 0 and not allin_before_hero:
                            prev_action = flop_actions[hero_action_index - 1]
                            if prev_action.get("is_check"):
                                checked_to_hero = True
                        
                        # This is an opportunity
                        if checked_to_hero and pfr:  # Guard: pfr must exist
                            # V4: Validate stacks before counting opportunity
                            context = self._build_validation_context(
                                stat_name="Flop CBet 3BetPot IP",
                                actors={"villain": pfr},
                                street_actions=flop_actions,
                                river_actions=None
                            )
                            
                            if self._validate_opportunity("Flop CBet 3BetPot IP", context):
                                self.stats["Flop CBet 3BetPot IP"]["opportunities"] += 1
                                
                                # Collect hand sample
                                if self.hand_collector:
                                    self.hand_collector.add_hand("Flop CBet 3BetPot IP", self.current_hand_text, self.current_hand_id)
                                
                                # Check if hero bet (attempt)
                                hero_bet = any(a["player"] == "Hero" and a.get("is_raise") 
                                               for a in flop_actions)
                                if hero_bet:
                                    self.stats["Flop CBet 3BetPot IP"]["attempts"] += 1
            
            # ============== FLOP CBet OOP% ==============
            # Same as "Flop CBet IP %" but Hero is OOP (first to act) instead of IP
            # Criteria:
            # 1. Hero is PFR (first raiser)
            # 2. PreflopActionFacingPlayer=Unopened (no limpers before hero's raise)
            # 3. Hero position in (EP, MP, CO, BTN) - NOT SB/BB
            # 4. Hero did NOT 3bet
            # 5. Hero did NOT call 3bet
            # 6. Hero did NOT 4bet
            # 7. Hand reached flop
            # 8. NO all-in preflop
            # 9. Exactly 2 players on flop (heads-up)
            # 10. Hero is OOP on flop (first to act in heads-up)
            # Attempt: Hero bets
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up = len(flop_players) == 2
                
                # Check if hero is PFR
                pfr = self._first_raiser(preflop_actions)
                hero_is_pfr = (pfr == "Hero")
                
                # Check PreflopActionFacingPlayer=Unopened (same as CBet IP)
                # This means NO calls/limps before Hero's first action
                hero_faced_unopened = False
                if hero_is_pfr:
                    # Find Hero's first action
                    for i, action in enumerate(preflop_actions):
                        if action["player"] == "Hero" and action.get("is_raise"):
                            # Check if any calls/limps before this action
                            had_action_before = any(
                                preflop_actions[j].get("is_call") or preflop_actions[j].get("is_raise")
                                for j in range(i)
                            )
                            hero_faced_unopened = not had_action_before
                            break
                
                # Check if hero is 3bettor or 4bettor
                three_bettor, four_bettor = self._who_3bet_4bet(preflop_actions)
                hero_did_3bet = (three_bettor == "Hero")
                hero_did_4bet = (four_bettor == "Hero")
                
                # Check if hero called a 3bet
                hero_called_3bet = False
                if three_bettor and three_bettor != "Hero":
                    seen_3bet = False
                    for action in preflop_actions:
                        if action["player"] == three_bettor and action.get("is_raise"):
                            seen_3bet = True
                        elif seen_3bet and action["player"] == "Hero" and action.get("is_call"):
                            hero_called_3bet = True
                            break
                
                # Check hero position: MUST be EP, MP, CO, or BTN (NOT SB/BB)
                valid_positions = ["UTG", "UTG+1", "UTG+2", "MP", "MP+1", "MP+2", "HJ", "CO", "BTN"]
                hero_in_valid_position = hero_position in valid_positions
                
                # Check if there was an all-in preflop
                allin_preflop = self._has_allin_preflop(preflop_actions)
                
                # Determine if hero is OOP on flop (first to act in heads-up)
                hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                hero_oop_flop = (hero_ip_flop is False)  # OOP means not IP (False)
                
                # Check all opportunity criteria for "Flop CBet OOP%"
                if (hero_is_pfr and 
                    hero_faced_unopened and  # PreflopActionFacingPlayer=Unopened
                    hero_in_valid_position and
                    not hero_did_3bet and 
                    not hero_called_3bet and
                    not hero_did_4bet and
                    not allin_preflop and
                    is_heads_up and  # Exactly 2 players on flop
                    hero_oop_flop):  # Must be OOP (first to act)
                    
                    # Hero must be first to act (index 0)
                    if flop_actions and flop_actions[0]["player"] == "Hero":
                        # Check for all-in before hero acts (shouldn't happen if hero is first)
                        allin_before_hero = False
                        
                        # This is an opportunity (Hero is first to act)
                        # V4: Validate stacks before counting opportunity
                        villain_name = next((p for p in flop_players if p != "Hero"), None)
                        if villain_name:  # Guard: ensure villain exists
                            context = self._build_validation_context(
                                stat_name="Flop CBet OOP%",
                                actors={"villain": villain_name},
                                street_actions=flop_actions,
                                river_actions=None
                            )
                            
                            if self._validate_opportunity("Flop CBet OOP%", context):
                                self.stats["Flop CBet OOP%"]["opportunities"] += 1
                                
                                # Collect hand sample
                                if self.hand_collector:
                                    self.hand_collector.add_hand("Flop CBet OOP%", self.current_hand_text, self.current_hand_id)
                                
                                # Check if hero bet (attempt)
                                hero_bet = (flop_actions[0].get("is_raise"))
                                if hero_bet:
                                    self.stats["Flop CBet OOP%"]["attempts"] += 1
            
            # ============== VS CBET GROUP ==============
            
            # ============== Flop fold vs Cbet IP ==============
            # Criteria:
            # 1. Hero is IP on flop (last to act in heads-up)
            # 2. Pre-flop: Hero called a raise (did NOT raise himself)
            # 3. NO all-in before hero's decision on flop (including villain's bet must NOT be all-in)
            # 4. Exactly 2 players on flop (heads-up)
            # 5. Villain bet on flop (cbet)
            # Opportunity: Villain bets (not all-in) and action is on Hero
            # Attempt: Hero folds
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up = len(flop_players) == 2
                
                # Check if hero is PFR or 3bettor
                pfr = self._first_raiser(preflop_actions)
                three_bettor, _ = self._who_3bet_4bet(preflop_actions)
                hero_is_pfr = (pfr == "Hero")
                hero_did_3bet = (three_bettor == "Hero")
                
                # Hero must have called a raise preflop (not be aggressor)
                hero_called_raise = False
                if not hero_is_pfr and not hero_did_3bet:
                    # Verify Hero actually called a raise from an OPPONENT (not Hero's own raise)
                    for i, action in enumerate(preflop_actions):
                        if action["player"] == "Hero" and action.get("is_call"):
                            # Check if there was a raise from an OPPONENT before this specific call
                            has_opponent_raise_before = any(
                                prev_action.get("is_raise") and prev_action["player"] != "Hero"
                                for prev_action in preflop_actions[:i]
                            )
                            if has_opponent_raise_before:
                                hero_called_raise = True
                                break  # Found a valid call of opponent's raise, can stop
                
                # Check if there was an all-in preflop
                allin_preflop = self._has_allin_preflop(preflop_actions)
                
                # Determine if hero is IP on flop (heads-up)
                hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                
                if hero_called_raise and not allin_preflop and is_heads_up and hero_ip_flop is True:
                    # Find hero's first action index on flop
                    hero_action_index = None
                    for i, action in enumerate(flop_actions):
                        if action["player"] == "Hero":
                            hero_action_index = i
                            break
                    
                    # Check if villain bet (not all-in) before hero acts
                    villain_bet_not_allin = False
                    if hero_action_index is not None and hero_action_index > 0:
                        prev_action = flop_actions[hero_action_index - 1]
                        if prev_action.get("is_raise") and not prev_action.get("is_allin"):
                            villain_bet_not_allin = True
                    
                    # This is an opportunity
                    if villain_bet_not_allin and hero_action_index is not None and hero_action_index > 0:
                        # V4: Validate stacks before counting opportunity
                        bettor_name = flop_actions[hero_action_index - 1]["player"]
                        if bettor_name:  # Guard: ensure bettor exists
                            context = self._build_validation_context(
                                stat_name="Flop fold vs Cbet IP",
                                actors={"bettor": bettor_name},
                                street_actions=flop_actions,
                                river_actions=None
                            )
                            
                            if self._validate_opportunity("Flop fold vs Cbet IP", context):
                                self.stats["Flop fold vs Cbet IP"]["opportunities"] += 1
                                
                                # Collect hand sample
                                if self.hand_collector:
                                    self.hand_collector.add_hand("Flop fold vs Cbet IP", self.current_hand_text, self.current_hand_id)
                                
                                # Check if hero folded (attempt)
                                hero_fold = any(a["player"] == "Hero" and a.get("is_fold") 
                                               for a in flop_actions)
                                if hero_fold:
                                    self.stats["Flop fold vs Cbet IP"]["attempts"] += 1
            
            # ============== Flop raise Cbet IP ==============
            # Same as "Flop fold vs Cbet IP" but Hero raises instead of folds
            # Criteria:
            # 1. Hero is IP on flop
            # 2. Pre-flop: Hero called a raise (did NOT raise himself)
            # 3. NO all-in before hero's decision on flop (including villain's bet must NOT be all-in)
            # 4. Exactly 2 players on flop (heads-up)
            # 5. Villain bet on flop (cbet)
            # Opportunity: Villain bets (not all-in) and action is on Hero
            # Attempt: Hero raises (including all-in)
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up = len(flop_players) == 2
                
                # Check if hero is PFR or 3bettor
                pfr = self._first_raiser(preflop_actions)
                three_bettor, _ = self._who_3bet_4bet(preflop_actions)
                hero_is_pfr = (pfr == "Hero")
                hero_did_3bet = (three_bettor == "Hero")
                
                # Hero must have called a raise preflop (not be aggressor)
                hero_called_raise = False
                if not hero_is_pfr and not hero_did_3bet:
                    # Verify Hero actually called a raise from an OPPONENT (not Hero's own raise)
                    for i, action in enumerate(preflop_actions):
                        if action["player"] == "Hero" and action.get("is_call"):
                            # Check if there was a raise from an OPPONENT before this specific call
                            has_opponent_raise_before = any(
                                prev_action.get("is_raise") and prev_action["player"] != "Hero"
                                for prev_action in preflop_actions[:i]
                            )
                            if has_opponent_raise_before:
                                hero_called_raise = True
                                break  # Found a valid call of opponent's raise, can stop
                
                # Check if there was an all-in preflop
                allin_preflop = self._has_allin_preflop(preflop_actions)
                
                # Determine if hero is IP on flop
                hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                
                if hero_called_raise and not allin_preflop and is_heads_up and hero_ip_flop is True:
                    # Find hero's first action index on flop
                    hero_action_index = None
                    for i, action in enumerate(flop_actions):
                        if action["player"] == "Hero":
                            hero_action_index = i
                            break
                    
                    # Check if villain bet (not all-in) before hero acts
                    villain_bet_not_allin = False
                    if hero_action_index is not None and hero_action_index > 0:
                        prev_action = flop_actions[hero_action_index - 1]
                        if prev_action.get("is_raise") and not prev_action.get("is_allin"):
                            villain_bet_not_allin = True
                    
                    # This is an opportunity
                    if villain_bet_not_allin and hero_action_index is not None and hero_action_index > 0:
                        # V4: Validate stacks before counting opportunity
                        bettor_name = flop_actions[hero_action_index - 1]["player"]
                        if bettor_name:  # Guard: ensure bettor exists
                            context = self._build_validation_context(
                                stat_name="Flop raise Cbet IP",
                                actors={"bettor": bettor_name},
                                street_actions=flop_actions,
                                river_actions=None
                            )
                            
                            if self._validate_opportunity("Flop raise Cbet IP", context):
                                self.stats["Flop raise Cbet IP"]["opportunities"] += 1
                                
                                # Collect hand sample
                                if self.hand_collector:
                                    self.hand_collector.add_hand("Flop raise Cbet IP", self.current_hand_text, self.current_hand_id)
                                
                                # Check if hero raised (including all-in)
                                hero_raise = any(a["player"] == "Hero" and a.get("is_raise") 
                                                for a in flop_actions)
                                if hero_raise:
                                    self.stats["Flop raise Cbet IP"]["attempts"] += 1
            
            # ============== Flop raise Cbet OOP ==============
            # Criteria:
            # 1. Hero CALLED preflop (is NOT PFR) - CRITICAL FILTER
            # 2. Hero is OOP on flop (first to act)
            # 3. Hero checks
            # 4. Villain bets (NOT all-in)
            # 5. Exactly 2 players on flop (heads-up)
            # 6. Action back to Hero
            # Opportunity: Hero can raise after villain's bet
            # Attempt: Hero raises (including all-in)
            
            if has_flop and hero_position:
                # CRITICAL: Hero must NOT be PFR (must be caller preflop)
                pfr = self._first_raiser(preflop_actions)
                hero_is_caller = (pfr != "Hero")
                
                if hero_is_caller:
                    # Get flop players
                    flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                    
                    # HEADS-UP FILTER: Must be exactly 2 players
                    is_heads_up = len(flop_players) == 2
                    
                    # Determine if hero is OOP on flop
                    hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                    hero_oop_flop = (hero_ip_flop is False)
                    
                    if is_heads_up and hero_oop_flop:
                        # Hero must be first to act and check
                        if flop_actions and flop_actions[0]["player"] == "Hero" and flop_actions[0].get("is_check"):
                            # Check if villain bet (and it's NOT all-in)
                            villain_bet_not_allin = False
                            if len(flop_actions) > 1:
                                villain_action = flop_actions[1]
                                if villain_action.get("is_raise") and not villain_action.get("is_allin"):
                                    villain_bet_not_allin = True
                            
                            # This is an opportunity if villain bet (not all-in)
                            if villain_bet_not_allin and len(flop_actions) > 1:
                                # V4: Validate stacks before counting opportunity
                                bettor_name = flop_actions[1]["player"]
                                if bettor_name:  # Guard: ensure bettor exists
                                    context = self._build_validation_context(
                                        stat_name="Flop raise Cbet OOP",
                                        actors={"bettor": bettor_name},
                                        street_actions=flop_actions,
                                        river_actions=None
                                    )
                                    
                                    if self._validate_opportunity("Flop raise Cbet OOP", context):
                                        self.stats["Flop raise Cbet OOP"]["opportunities"] += 1
                                        
                                        # Collect hand sample
                                        if self.hand_collector:
                                            self.hand_collector.add_hand("Flop raise Cbet OOP", self.current_hand_text, self.current_hand_id)
                                        
                                        # Check if hero raised after villain's bet
                                        hero_raise = False
                                        for i, action in enumerate(flop_actions[2:], start=2):
                                            if action["player"] == "Hero" and action.get("is_raise"):
                                                hero_raise = True
                                                break
                                        
                                        if hero_raise:
                                            self.stats["Flop raise Cbet OOP"]["attempts"] += 1
            
            # ============== Fold vs Check Raise ==============
            # Criteria:
            # 1. Exactly 2 players on flop (heads-up)
            # 2. Villain checks
            # 3. Hero bets
            # 4. Villain check-raises
            # 5. Action back to Hero
            # Opportunity: Hero faces a check-raise
            # Attempt: Hero folds
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up = len(flop_players) == 2
                
                # Only process if heads-up
                if is_heads_up:
                    # Look for pattern: villain check -> hero bet -> villain raise
                    for i in range(len(flop_actions) - 2):
                        action1 = flop_actions[i]
                        action2 = flop_actions[i + 1]
                        action3 = flop_actions[i + 2]
                        
                        # Check pattern: villain check, hero bet, villain raise (check-raise)
                        if (action1["player"] != "Hero" and action1.get("is_check") and
                            action2["player"] == "Hero" and action2.get("is_raise") and
                            action3["player"] == action1["player"] and action3.get("is_raise")):
                            
                            # This is an opportunity (hero faces check-raise)
                            # V4: Validate stacks before counting opportunity
                            raiser_name = action3["player"]
                            if raiser_name:  # Guard: ensure raiser exists
                                context = self._build_validation_context(
                                    stat_name="Fold vs Check Raise",
                                    actors={"raiser": raiser_name},
                                    street_actions=flop_actions,
                                    river_actions=None
                                )
                                
                                if self._validate_opportunity("Fold vs Check Raise", context):
                                    self.stats["Fold vs Check Raise"]["opportunities"] += 1
                                    
                                    # Collect hand sample
                                    if self.hand_collector:
                                        self.hand_collector.add_hand("Fold vs Check Raise", self.current_hand_text, self.current_hand_id)
                                    
                                    # Check if hero folded after check-raise
                                    hero_fold = False
                                    for j in range(i + 3, len(flop_actions)):
                                        if flop_actions[j]["player"] == "Hero" and flop_actions[j].get("is_fold"):
                                            hero_fold = True
                                            break
                                    
                                    if hero_fold:
                                        self.stats["Fold vs Check Raise"]["attempts"] += 1
                                
                            # Only count first check-raise opportunity in this hand
                            break
            
            # ============== Flop bet vs missed Cbet SRP ==============
            # Criteria:
            # 1. Single Raised Pot (SRP) - only 1 raise preflop (no 3bet)
            # 2. Hero called the raise preflop (not the PFR)
            # 3. Hero is IP on flop (acts after PFR)
            # 4. Exactly 2 players on flop (heads-up)
            # 5. PFR checks on flop (missed Cbet opportunity)
            # 6. Action is on Hero
            # Opportunity: Hero has chance to bet after PFR checks
            # Attempt: Hero bets
            
            if has_flop and hero_position:
                # Get flop players
                flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up = len(flop_players) == 2
                
                # Check if it's a Single Raised Pot (no 3bet)
                three_bettor, _ = self._who_3bet_4bet(preflop_actions)
                is_srp = (three_bettor is None)
                
                # Get the PFR (first raiser)
                pfr = self._first_raiser(preflop_actions)
                hero_is_pfr = (pfr == "Hero")
                
                # Hero must have called a raise preflop (same logic as VS CBET stats)
                hero_called_raise = False
                if not hero_is_pfr and three_bettor != "Hero":
                    # Verify Hero actually called a raise from an OPPONENT
                    for i, action in enumerate(preflop_actions):
                        if action["player"] == "Hero" and action.get("is_call"):
                            # Check if there was a raise from an OPPONENT before this call
                            has_opponent_raise_before = any(
                                prev_action.get("is_raise") and prev_action["player"] != "Hero"
                                for prev_action in preflop_actions[:i]
                            )
                            if has_opponent_raise_before:
                                hero_called_raise = True
                                break
                
                # Check if there was an all-in preflop
                allin_preflop = self._has_allin_preflop(preflop_actions)
                
                # Determine if hero is IP on flop
                hero_ip_flop = self._determine_ip_on_flop(positions, flop_players, "Hero")
                
                # All conditions must be true:
                # - SRP (no 3bet)
                # - Hero called a raise preflop (not PFR or 3bettor)
                # - No all-in preflop
                # - Heads-up (exactly 2 players)
                # - Hero is IP on flop
                if is_srp and hero_called_raise and not allin_preflop and is_heads_up and hero_ip_flop is True:
                    # Check if PFR checked on flop (first to act and checks)
                    if flop_actions and flop_actions[0]["player"] == pfr and flop_actions[0].get("is_check"):
                        # Find hero's action index
                        hero_action_index = None
                        for i, action in enumerate(flop_actions):
                            if action["player"] == "Hero":
                                hero_action_index = i
                                break
                        
                        # Hero must act after PFR's check
                        if hero_action_index is not None and hero_action_index > 0:
                            # Check if there was any all-in before hero acts
                            allin_before_hero = any(
                                flop_actions[j].get("is_allin") 
                                for j in range(hero_action_index)
                            )
                            
                            # This is an opportunity if no all-in before hero
                            if not allin_before_hero and pfr:
                                # V4: Validate stacks before counting opportunity
                                context = self._build_validation_context(
                                    stat_name="Flop bet vs missed Cbet SRP",
                                    actors={"pfr": pfr},
                                    street_actions=flop_actions,
                                    river_actions=None
                                )
                                
                                if self._validate_opportunity("Flop bet vs missed Cbet SRP", context):
                                    self.stats["Flop bet vs missed Cbet SRP"]["opportunities"] += 1
                                    
                                    # Collect hand sample
                                    if self.hand_collector:
                                        self.hand_collector.add_hand("Flop bet vs missed Cbet SRP", self.current_hand_text, self.current_hand_id)
                                    
                                    # Check if hero bet (is_raise indicates a bet or raise)
                                    hero_bet = any(
                                        a["player"] == "Hero" and a.get("is_raise") 
                                        for a in flop_actions[hero_action_index:]
                                    )
                                    
                                    if hero_bet:
                                        self.stats["Flop bet vs missed Cbet SRP"]["attempts"] += 1
        
        # ============== TURN STATS ==============
        # Note: turn_actions already extracted and normalized at top of analyze_hand()
        
        if has_turn and has_flop and hero_position:
            # ============== Turn CBet IP% ==============
            # Criteria:
            # 1. Hero made Cbet on flop while IP
            # 2. Villain called the flop Cbet
            # 3. Exactly 2 players on turn (heads-up)
            # 4. Hero is IP on turn
            # 5. Action checks to Hero = opportunity
            # 6. Hero bets = attempt
            
            # Check if Hero made Cbet on flop IP
            hero_cbet_flop_ip = False
            if hero_is_pfr and not hero_did_3bet and not hero_called_3bet and not hero_did_4bet and hero_ip_flop is True:
                # Check if hero bet on flop
                for action in flop_actions:
                    if action["player"] == "Hero" and action.get("is_raise"):
                        hero_cbet_flop_ip = True
                        break
            
            if hero_cbet_flop_ip:
                # Check if villain called the flop Cbet
                villain_called_flop = False
                hero_bet_found = False
                for action in flop_actions:
                    if action["player"] == "Hero" and action.get("is_raise"):
                        hero_bet_found = True
                    elif hero_bet_found and action["player"] != "Hero" and action.get("is_call"):
                        villain_called_flop = True
                        break
                
                if villain_called_flop:
                    # Determine if hero is still IP on turn
                    turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                    
                    # HEADS-UP FILTER: Must be exactly 2 players
                    is_heads_up_turn = len(turn_players) == 2
                    
                    hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                    
                    if is_heads_up_turn and hero_ip_turn is True:
                        # Check if action checks to hero
                        if turn_actions and turn_actions[0].get("is_check"):
                            # Find hero's action
                            hero_turn_action = None
                            for action in turn_actions:
                                if action["player"] == "Hero":
                                    hero_turn_action = action
                                    break
                            
                            if hero_turn_action:
                                # V4: Validate stacks before counting opportunity
                                villain_name = next((p for p in turn_players if p != "Hero"), None)
                                if villain_name:  # Guard: ensure villain exists
                                    context = self._build_validation_context(
                                        stat_name="Turn CBet IP%",
                                        actors={"villain": villain_name},
                                        street_actions=turn_actions,
                                        river_actions=None
                                    )
                                    
                                    if self._validate_opportunity("Turn CBet IP%", context):
                                        self.stats["Turn CBet IP%"]["opportunities"] += 1
                                        
                                        if self.hand_collector:
                                            self.hand_collector.add_hand("Turn CBet IP%", self.current_hand_text, self.current_hand_id)
                                        
                                        if hero_turn_action.get("is_raise"):
                                            self.stats["Turn CBet IP%"]["attempts"] += 1
            
            # ============== Turn Cbet OOP% ==============
            # Criteria:
            # 1. Hero made Cbet on flop while OOP
            # 2. Villain called the flop Cbet
            # 3. Exactly 2 players on turn (heads-up)
            # 4. Hero is OOP on turn
            # 5. Hero has opportunity to bet = opportunity
            # 6. Hero bets = attempt
            
            # Check if Hero made Cbet on flop OOP
            hero_cbet_flop_oop = False
            if hero_is_pfr and not hero_did_3bet and not hero_called_3bet and not hero_did_4bet and hero_ip_flop is False:
                # Check if hero bet on flop
                for action in flop_actions:
                    if action["player"] == "Hero" and action.get("is_raise"):
                        hero_cbet_flop_oop = True
                        break
            
            if hero_cbet_flop_oop:
                # Check if villain called the flop Cbet
                villain_called_flop = False
                hero_bet_found = False
                for action in flop_actions:
                    if action["player"] == "Hero" and action.get("is_raise"):
                        hero_bet_found = True
                    elif hero_bet_found and action["player"] != "Hero" and action.get("is_call"):
                        villain_called_flop = True
                        break
                
                if villain_called_flop:
                    # Determine if hero is still OOP on turn
                    turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                    
                    # HEADS-UP FILTER: Must be exactly 2 players
                    is_heads_up_turn = len(turn_players) == 2
                    
                    hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                    
                    if is_heads_up_turn and hero_ip_turn is False:
                        # Hero is OOP, first to act
                        if turn_actions and turn_actions[0]["player"] == "Hero":
                            # V4: Validate stacks before counting opportunity
                            villain_name = next((p for p in turn_players if p != "Hero"), None)
                            if villain_name:  # Guard: ensure villain exists
                                context = self._build_validation_context(
                                    stat_name="Turn Cbet OOP%",
                                    actors={"villain": villain_name},
                                    street_actions=turn_actions,
                                    river_actions=None
                                )
                                
                                if self._validate_opportunity("Turn Cbet OOP%", context):
                                    self.stats["Turn Cbet OOP%"]["opportunities"] += 1
                                    
                                    if self.hand_collector:
                                        self.hand_collector.add_hand("Turn Cbet OOP%", self.current_hand_text, self.current_hand_id)
                                    
                                    if turn_actions[0].get("is_raise"):
                                        self.stats["Turn Cbet OOP%"]["attempts"] += 1
            
            # ============== Turn donk bet ==============
            # Criteria:
            # 1. Hero was caller on flop (not aggressor)
            # 2. Exactly 2 players on turn (heads-up)
            # 3. Hero is OOP on turn
            # 4. Hero bets first (takes initiative) = opportunity and attempt
            
            # Check if hero called on flop (not the aggressor)
            hero_called_flop = False
            for action in flop_actions:
                if action["player"] == "Hero" and action.get("is_call"):
                    hero_called_flop = True
                    break
            
            if hero_called_flop:
                # Determine if hero is OOP on turn
                turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up_turn = len(turn_players) == 2
                
                hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                
                if is_heads_up_turn and hero_ip_turn is False:
                    # Hero is OOP, check if first to act on turn
                    if turn_actions and turn_actions[0]["player"] == "Hero":
                        # V4: Validate stacks before counting opportunity
                        # Aggressor is the non-Hero player who bet/raised on flop
                        aggressor_name = None
                        for action in flop_actions:
                            if action["player"] != "Hero" and action.get("is_raise"):
                                aggressor_name = action["player"]
                                break
                        
                        if aggressor_name:  # Guard: ensure aggressor exists
                            context = self._build_validation_context(
                                stat_name="Turn donk bet",
                                actors={"aggressor": aggressor_name},
                                street_actions=turn_actions,
                                river_actions=None
                            )
                            
                            if self._validate_opportunity("Turn donk bet", context):
                                self.stats["Turn donk bet"]["opportunities"] += 1
                                
                                if self.hand_collector:
                                    self.hand_collector.add_hand("Turn donk bet", self.current_hand_text, self.current_hand_id)
                                
                                if turn_actions[0].get("is_raise"):
                                    self.stats["Turn donk bet"]["attempts"] += 1
            
            # ============== Turn donk bet SRP vs PFR ==============
            # Criteria:
            # 1. SRP preflop (no 3bet)
            # 2. Hero called PFR preflop
            # 3. Hero called on flop
            # 4. Exactly 2 players on turn (heads-up)
            # 5. Hero is OOP on turn
            # 6. Hero bets first = opportunity and attempt
            
            three_bettor, _ = self._who_3bet_4bet(preflop_actions)
            is_srp = (three_bettor is None)
            
            # Check if hero called PFR preflop
            hero_called_pfr = False
            pfr = self._first_raiser(preflop_actions)
            if pfr and pfr != "Hero":
                for action in preflop_actions:
                    if action["player"] == "Hero" and action.get("is_call"):
                        hero_called_pfr = True
                        break
            
            if is_srp and hero_called_pfr and hero_called_flop:
                # Determine if hero is OOP on turn
                turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                
                # HEADS-UP FILTER: Must be exactly 2 players
                is_heads_up_turn = len(turn_players) == 2
                
                hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                
                if is_heads_up_turn and hero_ip_turn is False:
                    # Hero is OOP, check if first to act on turn
                    if turn_actions and turn_actions[0]["player"] == "Hero" and pfr:
                        # V4: Validate stacks before counting opportunity
                        context = self._build_validation_context(
                            stat_name="Turn donk bet SRP vs PFR",
                            actors={"pfr": pfr},
                            street_actions=turn_actions,
                            river_actions=None
                        )
                        
                        if self._validate_opportunity("Turn donk bet SRP vs PFR", context):
                            self.stats["Turn donk bet SRP vs PFR"]["opportunities"] += 1
                            
                            if self.hand_collector:
                                self.hand_collector.add_hand("Turn donk bet SRP vs PFR", self.current_hand_text, self.current_hand_id)
                            
                            if turn_actions[0].get("is_raise"):
                                self.stats["Turn donk bet SRP vs PFR"]["attempts"] += 1
            
            # ============== Bet turn vs Missed Flop Cbet OOP SRP ==============
            # Criteria:
            # 1. Hero called PFR preflop in SRP
            # 2. Hero is OOP on flop, checks
            # 3. Villain (PFR) checks behind on flop
            # 4. Exactly 2 players on turn (heads-up)
            # 5. Hero is OOP on turn and bets = opportunity and attempt
            
            if is_srp and hero_called_pfr:
                # Check if hero was OOP on flop
                if hero_ip_flop is False:
                    # Check if hero checked on flop
                    hero_checked_flop = False
                    if flop_actions and flop_actions[0]["player"] == "Hero" and flop_actions[0].get("is_check"):
                        hero_checked_flop = True
                    
                    if hero_checked_flop:
                        # Check if villain (PFR) checked behind
                        villain_checked_behind = False
                        for action in flop_actions[1:]:
                            if action["player"] == pfr and action.get("is_check"):
                                villain_checked_behind = True
                                break
                        
                        if villain_checked_behind:
                            # Determine if hero is OOP on turn
                            turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                            
                            # HEADS-UP FILTER: Must be exactly 2 players
                            is_heads_up_turn = len(turn_players) == 2
                            
                            hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                            
                            if is_heads_up_turn and hero_ip_turn is False:
                                # Hero is OOP on turn
                                if turn_actions and turn_actions[0]["player"] == "Hero" and pfr:
                                    # V4: Validate stacks
                                    context = self._build_validation_context(
                                        stat_name="Bet turn vs Missed Flop Cbet OOP SRP",
                                        actors={"pfr": pfr},
                                        street_actions=turn_actions,
                                        river_actions=None
                                    )
                                    
                                    if self._validate_opportunity("Bet turn vs Missed Flop Cbet OOP SRP", context):
                                        self.stats["Bet turn vs Missed Flop Cbet OOP SRP"]["opportunities"] += 1
                                        
                                        if self.hand_collector:
                                            self.hand_collector.add_hand("Bet turn vs Missed Flop Cbet OOP SRP", self.current_hand_text, self.current_hand_id)
                                        
                                        if turn_actions[0].get("is_raise"):
                                            self.stats["Bet turn vs Missed Flop Cbet OOP SRP"]["attempts"] += 1
            
            # ============== Turn Fold vs CBet OOP ==============
            # Criteria:
            # 1. Hero called PFR preflop in SRP
            # 2. Hero OOP on flop, checks
            # 3. Villain bets (Cbet), hero calls
            # 4. Exactly 2 players on turn (heads-up)
            # 5. Hero OOP on turn, checks
            # 6. Villain bets (turn Cbet)
            # 7. Hero folds = opportunity and attempt
            
            if is_srp and hero_called_pfr:
                # Check if hero was OOP on flop and checked
                hero_checked_flop_oop = False
                if hero_ip_flop is False and flop_actions and flop_actions[0]["player"] == "Hero" and flop_actions[0].get("is_check"):
                    hero_checked_flop_oop = True
                
                if hero_checked_flop_oop:
                    # Check if villain bet and hero called
                    villain_cbet_flop = False
                    hero_called_cbet_flop = False
                    
                    for i, action in enumerate(flop_actions):
                        if action["player"] != "Hero" and action.get("is_raise"):
                            villain_cbet_flop = True
                        elif villain_cbet_flop and action["player"] == "Hero" and action.get("is_call"):
                            hero_called_cbet_flop = True
                            break
                    
                    if villain_cbet_flop and hero_called_cbet_flop:
                        # Determine if hero is OOP on turn
                        turn_players = self._players_on_turn(positions, flop_actions, turn_actions)
                        
                        # HEADS-UP FILTER: Must be exactly 2 players
                        is_heads_up_turn = len(turn_players) == 2
                        
                        hero_ip_turn = self._determine_ip_on_turn(positions, turn_players, "Hero")
                        
                        if is_heads_up_turn and hero_ip_turn is False:
                            # Check if hero checks on turn
                            if turn_actions and turn_actions[0]["player"] == "Hero" and turn_actions[0].get("is_check"):
                                # Check if villain bets on turn
                                villain_bet_turn = False
                                for action in turn_actions[1:]:
                                    if action["player"] != "Hero" and action.get("is_raise"):
                                        villain_bet_turn = True
                                        break
                                
                                if villain_bet_turn:
                                    # V4: Validate stacks - bettor is the player who bet on turn
                                    bettor_name = None
                                    for action in turn_actions[1:]:
                                        if action["player"] != "Hero" and action.get("is_raise"):
                                            bettor_name = action["player"]
                                            break
                                    
                                    if bettor_name:
                                        context = self._build_validation_context(
                                            stat_name="Turn Fold vs CBet OOP",
                                            actors={"bettor": bettor_name},
                                            street_actions=turn_actions,
                                            river_actions=None
                                        )
                                        
                                        if self._validate_opportunity("Turn Fold vs CBet OOP", context):
                                            self.stats["Turn Fold vs CBet OOP"]["opportunities"] += 1
                                            
                                            if self.hand_collector:
                                                self.hand_collector.add_hand("Turn Fold vs CBet OOP", self.current_hand_text, self.current_hand_id)
                                            
                                            # Check if hero folded
                                            for action in turn_actions:
                                                if action["player"] == "Hero" and action.get("is_fold"):
                                                    self.stats["Turn Fold vs CBet OOP"]["attempts"] += 1
                                                    break
        
        # ============== RIVER STATS ==============
        # Note: river_actions already extracted and normalized at top of analyze_hand()
        
        # ============== WTSD% (Went To ShowDown %) ==============
        # Opportunities: Hands where Hero participated in flop (didn't fold preflop) AND no all-in before river
        # Attempts: hands where Hero participated in showdown (showed/mucked cards)
        # NOTE: All-in on river with call is OK, but no all-ins before river
        if has_flop and "Hero" in flop_players:
            # Check no all-in before river
            allin_before_river = self._has_allin_preflop(preflop_actions)
            
            if not allin_before_river and flop_actions:
                for action in flop_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            if not allin_before_river and turn_actions:
                for action in turn_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            # Only count if no all-in before river
            if not allin_before_river:
                # V4: Validate stacks (hero-only stat, allow_allin: True)
                context = self._build_validation_context(
                    stat_name="WTSD%",
                    actors={},
                    street_actions=river_actions if river_actions else [],
                    river_actions=river_actions
                )
                
                if self._validate_opportunity("WTSD%", context):
                    self.stats["WTSD%"]["opportunities"] += 1
                    
                    # Check if Hero reached showdown (participated in card reveal)
                    if has_showdown and self._hero_at_showdown(hand_text, hero):
                        self.stats["WTSD%"]["attempts"] += 1
                        
                        # Collect hand sample (only for attempts - showdowns)
                        if self.hand_collector:
                            self.hand_collector.add_hand("WTSD%", self.current_hand_text, self.current_hand_id)
        
        # ============== W$SD% (Won $ at ShowDown %) ==============
        # Opportunities: hands where Hero participated in showdown (showed/mucked cards) AND saw flop AND no all-in before river
        # Attempts: hands where Hero won at showdown ("Hero collected X from pot")
        # NOTE: All-in on river with call is OK, but no all-ins before river
        if has_flop and "Hero" in flop_players and has_showdown and self._hero_at_showdown(hand_text, hero):
            # Check no all-in before river (same logic as WTSD%)
            allin_before_river = self._has_allin_preflop(preflop_actions)
            
            if not allin_before_river and flop_actions:
                for action in flop_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            if not allin_before_river and turn_actions:
                for action in turn_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            # Only count if no all-in before river
            if not allin_before_river:
                # V4: Validate stacks (hero-only stat, allow_allin: True)
                context = self._build_validation_context(
                    stat_name="W$SD%",
                    actors={},
                    street_actions=river_actions if river_actions else [],
                    river_actions=river_actions
                )
                
                if self._validate_opportunity("W$SD%", context):
                    self.stats["W$SD%"]["opportunities"] += 1
                    
                    if self.hand_collector:
                        self.hand_collector.add_hand("W$SD%", self.current_hand_text, self.current_hand_id)
                    
                    # Check if hero won
                    if self._hero_won_pot(hand_text, hero):
                        self.stats["W$SD%"]["attempts"] += 1
        
        # ============== W$WSF Rating (Won $ When Saw Flop) ==============
        # Opportunities: Total hands where Hero saw flop (integer count)
        # Attempts: Number of wins
        # Player_sum: Sum of number of players on flop (for average calculation)
        if has_flop and "Hero" in flop_players:
            # V4: Validate stacks (villain needed for multi-player context)
            villain_name = next((p for p in flop_players if p != "Hero"), None) if len(flop_players) == 2 else None
            actors = {"villain": villain_name} if villain_name else {}
            
            context = self._build_validation_context(
                stat_name="W$WSF Rating",
                actors=actors,
                street_actions=flop_actions,
                river_actions=None
            )
            
            if self._validate_opportunity("W$WSF Rating", context):
                # Count this hand as an opportunity
                self.stats["W$WSF Rating"]["opportunities"] += 1
                
                # Track number of players for average calculation
                num_players_on_flop = len(flop_players)
                self.stats["W$WSF Rating"]["player_sum"] += num_players_on_flop
                
                if self.hand_collector:
                    self.hand_collector.add_hand("W$WSF Rating", self.current_hand_text, self.current_hand_id)
                
                # Check if hero won (any street, any method)
                if self._hero_won_pot(hand_text, hero):
                    self.stats["W$WSF Rating"]["attempts"] += 1
        
        # ============== River Agg % (River Aggression %) ==============
        # Ratio: (bets + raises) / calls
        # Opportunities: hands reaching river without all-in, where Hero has bets/raises/calls (exclude check-only)
        # Attempts: count bets+raises, opportunities: count calls
        # This is stored differently - we'll calculate ratio in aggregation
        if has_river and river_actions:
            # Check no all-in before river
            allin_before_river = self._has_allin_preflop(preflop_actions)
            
            if not allin_before_river and flop_actions:
                for action in flop_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            if not allin_before_river and turn_actions:
                for action in turn_actions:
                    if action.get("is_allin"):
                        allin_before_river = True
                        break
            
            if not allin_before_river:
                # Check if Hero has any action other than just checks
                hero_river_actions = [a for a in river_actions if a["player"] == "Hero"]
                has_action = any(a.get("is_raise") or a.get("is_call") for a in hero_river_actions)
                
                # Also check if there's at least a check followed by other action
                if not has_action and hero_river_actions:
                    for i, action in enumerate(river_actions):
                        if action["player"] == "Hero" and action.get("is_check"):
                            # Check if there's a bet after and hero responds
                            for j in range(i+1, len(river_actions)):
                                if river_actions[j]["player"] == "Hero" and river_actions[j].get("is_call"):
                                    has_action = True
                                    break
                
                if has_action:
                    # V4: Validate stacks (villain needed)
                    river_players = [a["player"] for a in river_actions]
                    villain_name = next((p for p in river_players if p != "Hero"), None)
                    
                    if villain_name:
                        context = self._build_validation_context(
                            stat_name="River Agg %",
                            actors={"villain": villain_name},
                            street_actions=river_actions,
                            river_actions=river_actions
                        )
                        
                        if self._validate_opportunity("River Agg %", context):
                            # Count Hero's bets+raises and calls on river
                            # Ratio: (bets+raises) / calls = aggression level
                            # Higher ratio = more aggressive (more bets/raises than calls)
                            # Count as 1 if Hero had ANY bet/raise in this hand (not multiple actions)
                            has_bet_raise = any(a.get("is_raise") for a in hero_river_actions)
                            has_call = any(a.get("is_call") for a in hero_river_actions)
                            
                            # Store bets+raises as attempts (numerator), calls as opportunities (denominator)
                            # Each hand counts max 1 for bets/raises and max 1 for calls
                            # This gives us: (bets+raises) / calls ratio
                            if has_bet_raise:
                                self.stats["River Agg %"]["attempts"] += 1
                            if has_call:
                                self.stats["River Agg %"]["opportunities"] += 1
                            
                            # Count total unique hands ONLY if Hero had bet/raise or call (not check-only)
                            # Collect hand sample - save ALL hands with bet/raise or call
                            if has_bet_raise or has_call:
                                self.stats["River Agg %"]["total_hands"] += 1
                                if self.hand_collector:
                                    self.hand_collector.add_hand("River Agg %", self.current_hand_text, self.current_hand_id)
        
        # ============== River bet - Single Rsd Pot ==============
        # Opportunities: SRP hands reaching river without all-in, heads-up SINCE FLOP, ACTION CHECKS TO HERO
        # Attempts: Hero bets on river
        # CRITICAL: Only count when Hero HAS OPPORTUNITY to bet (not facing bet)
        # HEADS-UP FILTER: Must be exactly 2 players from flop onwards (not just on river)
        if has_river and river_actions and flop_actions:
            # Check if SRP
            three_bettor, _ = self._who_3bet_4bet(preflop_actions)
            is_srp = (three_bettor is None)
            
            if is_srp:
                # Check no all-in before river
                allin_before_river = self._has_allin_preflop(preflop_actions)
                
                if not allin_before_river and flop_actions:
                    for action in flop_actions:
                        if action.get("is_allin"):
                            allin_before_river = True
                            break
                
                if not allin_before_river and turn_actions:
                    for action in turn_actions:
                        if action.get("is_allin"):
                            allin_before_river = True
                            break
                
                if not allin_before_river:
                    # HEADS-UP FILTER: Check flop players (must be exactly 2 FROM FLOP)
                    # Only count hands that were heads-up since the flop (not just on river)
                    flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
                    is_heads_up_from_flop = len(flop_players) == 2
                    
                    if is_heads_up_from_flop:
                        # CRITICAL: Check if action checks to Hero (Hero has opportunity to bet)
                        # Find Hero's first action on river
                        hero_action_index = None
                        for i, action in enumerate(river_actions):
                            if action["player"] == "Hero":
                                hero_action_index = i
                                break
                        
                        # Check if villain bet BEFORE hero acts (if so, Hero is FACING bet, not opportunity to bet)
                        villain_bet_before_hero = False
                        if hero_action_index is not None and hero_action_index > 0:
                            # Check actions before Hero
                            for action in river_actions[:hero_action_index]:
                                if action.get("is_raise"):  # Villain bet before Hero
                                    villain_bet_before_hero = True
                                    break
                        
                        # Only count as opportunity if action checks to Hero (no bet before Hero)
                        if not villain_bet_before_hero and hero_action_index is not None:
                            # V4: Validate stacks (caller is the opponent in this heads-up SRP)
                            caller_name = next((p for p in flop_players if p != "Hero"), None)
                            if caller_name:
                                context = self._build_validation_context(
                                    stat_name="River bet - Single Rsd Pot",
                                    actors={"caller": caller_name},
                                    street_actions=river_actions,
                                    river_actions=river_actions
                                )
                                
                                if self._validate_opportunity("River bet - Single Rsd Pot", context):
                                    self.stats["River bet - Single Rsd Pot"]["opportunities"] += 1
                                    
                                    if self.hand_collector:
                                        self.hand_collector.add_hand("River bet - Single Rsd Pot", self.current_hand_text, self.current_hand_id)
                                    
                                    # Check if Hero bet on river
                                    for action in river_actions:
                                        if action["player"] == "Hero" and action.get("is_raise"):
                                            self.stats["River bet - Single Rsd Pot"]["attempts"] += 1
                                            break
        
        # ============== W$SD% B River (Won $ at ShowDown after Betting River) ==============
        # Opportunities: hands where Hero bet/raised river (NOT call), went to showdown, heads-up from flop
        # Attempts: Hero wins at showdown
        # CRITICAL FILTERS: Hero must BET/RAISE (never call), heads-up from flop (2 players)
        if has_river and has_showdown and river_actions and has_flop and flop_actions:
            # HEADS-UP FILTER: Must be exactly 2 players from flop
            flop_players = self._players_on_flop(positions, preflop_actions, flop_actions)
            is_heads_up_from_flop = len(flop_players) == 2
            
            if is_heads_up_from_flop:
                # Check if Hero bet/raised on river (NOT call)
                hero_bet_river = False
                for action in river_actions:
                    if action["player"] == "Hero" and action.get("is_raise"):
                        hero_bet_river = True
                        break
                
                if hero_bet_river:
                    # V4: Validate stacks (hero-only stat, allow_allin: True)
                    context = self._build_validation_context(
                        stat_name="W$SD% B River",
                        actors={},
                        street_actions=river_actions,
                        river_actions=river_actions
                    )
                    
                    if self._validate_opportunity("W$SD% B River", context):
                        self.stats["W$SD% B River"]["opportunities"] += 1
                        
                        if self.hand_collector:
                            self.hand_collector.add_hand("W$SD% B River", self.current_hand_text, self.current_hand_id)
                        
                        # Check if Hero won
                        if self._hero_won_pot(hand_text, hero):
                            self.stats["W$SD% B River"]["attempts"] += 1
    
    def _has_showdown_multisite(self, hand_text: str) -> bool:
        """
        Detect REAL showdown (when players actually show/muck cards) across all poker sites
        
        A real showdown happens when:
        - Players reveal their cards (": shows [" or " shows [")
        - Players muck/hide cards ("does not show", "doesn't show hand", " mucks [")
        - Summary shows cards were revealed ("showed [")
        - Explicit showdown section ("*** SHOW DOWN ***" or "*** SHOWDOWN ***")
        
        IMPORTANT: "does not show" alone is NOT sufficient - it can mean player won without 
        showdown (everyone folded). Must have positive evidence of showdown.
        
        Works for: PokerStars, GGPoker, Winamax, 888poker, WPN
        """
        # Pattern 1a: Player shows cards - with colon (PokerStars/GG/Winamax)
        # Example: "Hero: shows [Ah Kd]" or "Player1: shows [9c 9d]"
        if ": shows [" in hand_text:
            return True
        
        # Pattern 1b: Player shows cards - WITHOUT colon (888poker/WPN)
        # Example: "Hero shows [ 9d, 9s ]" or "Player shows [Ah Kd] (pair)"
        if " shows [" in hand_text:
            return True
        
        # Pattern 2: Summary shows player revealed cards - "showed ["
        # Example: "Seat 5: Hero (button) showed [Td Ad] and lost"
        if "showed [" in hand_text:
            return True
        
        # Pattern 3: Explicit showdown section marker
        # PokerStars/GG/WPN: "*** SHOW DOWN ***"
        # 888poker: "** Showdown **"
        if "*** SHOW DOWN ***" in hand_text or "*** SHOWDOWN ***" in hand_text or "** Showdown **" in hand_text:
            return True
        
        # Pattern 4: Muck patterns - only count if we have evidence of showdown
        # Check if there's positive evidence first
        has_positive_evidence = (
            ": shows [" in hand_text or 
            " shows [" in hand_text or 
            "showed [" in hand_text or
            "*** SHOW DOWN ***" in hand_text or
            "*** SHOWDOWN ***" in hand_text or
            "** Showdown **" in hand_text
        )
        
        if has_positive_evidence:
            # Now it's safe to count muck patterns as showdown indicators
            # Pattern 4a: WPN/PokerStars style muck
            if "does not show" in hand_text or "doesn't show" in hand_text:
                return True
            
            # Pattern 4b: Explicit muck with colon (PokerStars/GG)
            if "mucks hand" in hand_text or ": mucks" in hand_text.lower():
                return True
            
            # Pattern 4c: Explicit muck WITHOUT colon (888poker)
            # Example: "Player mucks [ cards ]"
            if " mucks [" in hand_text.lower():
                return True
        
        return False
    
    def _hero_at_showdown(self, hand_text: str, hero_name: str) -> bool:
        """
        Check if Hero was involved in showdown (showed cards, mucked, or won)
        Must check both hero_name and "Hero" since code normalizes player names
        
        Works for: PokerStars, GGPoker, Winamax, 888poker, WPN
        """
        # Normalize for case-insensitive matching
        hand_text_lower = hand_text.lower()
        hero_name_lower = hero_name.lower()
        
        # Pattern 1: Hero shows cards - with colon (PokerStars/GG/Winamax style)
        if f"{hero_name}: shows [" in hand_text or "Hero: shows [" in hand_text:
            return True
        
        # Pattern 1b: Hero shows cards - WITHOUT colon (888poker style)
        # 888poker format: "Player shows [ cards ]"
        if f"{hero_name} shows [" in hand_text or "Hero shows [" in hand_text:
            return True
        
        # Pattern 2: Hero in summary with "showed" (check both)
        hero_showed_pattern1 = f"{re.escape(hero_name)}.*showed \\["
        hero_showed_pattern2 = r"Hero.*showed \["
        if re.search(hero_showed_pattern1, hand_text) or re.search(hero_showed_pattern2, hand_text):
            return True
        
        # Pattern 3: Hero mucked - with colon (PokerStars/GG style)
        if f"{hero_name_lower}: mucks" in hand_text_lower or "hero: mucks" in hand_text_lower:
            return True
        
        # Pattern 3b: Hero mucked - WITHOUT colon (888poker style)
        # 888poker format: "Player mucks [ cards ]"
        if f"{hero_name_lower} mucks [" in hand_text_lower or "hero mucks [" in hand_text_lower:
            return True
        
        # Pattern 4: Hero doesn't show (case-insensitive, check both)
        if (f"{hero_name_lower} does not show" in hand_text_lower or 
            f"{hero_name_lower} doesn't show" in hand_text_lower or
            "hero does not show" in hand_text_lower or
            "hero doesn't show" in hand_text_lower):
            return True
        
        # Pattern 5: Hero collected pot (check both)
        if f"{hero_name} collected" in hand_text or "Hero collected" in hand_text:
            return True
        
        return False
    
    def _hero_won_pot(self, hand_text: str, hero_name: str) -> bool:
        """
        Determine if Hero won the pot from hand history text (multi-site compatible)
        
        Works for: PokerStars, GGPoker, Winamax, 888poker, WPN
        """
        # Pattern 1: "Hero collected" or "[hero_name] collected"
        if f"{hero_name} collected" in hand_text or "Hero collected" in hand_text:
            return True
        
        # Pattern 2: Check SUMMARY section for winner (PokerStars, GGPoker, Winamax, WPN)
        if "*** SUMMARY ***" in hand_text:
            summary_start = hand_text.find("*** SUMMARY ***")
            summary_section = hand_text[summary_start:]
            
            # Look for "Seat X: [hero_name] ... collected"
            if f"{hero_name}" in summary_section and "collected" in summary_section:
                # Verify it's about the hero collecting
                hero_line_pattern = f"Seat \\d+: {re.escape(hero_name)}.*collected"
                if re.search(hero_line_pattern, summary_section):
                    return True
        
        # Pattern 2b: 888poker summary (different format)
        if "** Summary **" in hand_text:
            summary_start = hand_text.find("** Summary **")
            summary_section = hand_text[summary_start:]
            
            # 888poker: look for hero name and "collected" or "wins"
            # Check both original hero_name and normalized "Hero"
            if f"{hero_name}" in summary_section and ("collected" in summary_section or "wins" in summary_section.lower()):
                hero_line_pattern = f"{re.escape(hero_name)}.*(collected|wins)"
                if re.search(hero_line_pattern, summary_section, re.IGNORECASE):
                    return True
            
            # Also check for normalized "Hero" in 888poker summary
            if "Hero" in summary_section and ("collected" in summary_section or "wins" in summary_section.lower()):
                hero_line_pattern = r"Hero.*(collected|wins)"
                if re.search(hero_line_pattern, summary_section, re.IGNORECASE):
                    return True
        
        # Pattern 3: Showdown winner (PokerStars, Winamax, WPN)
        if "*** SHOW DOWN ***" in hand_text:
            showdown_start = hand_text.find("*** SHOW DOWN ***")
            showdown_section = hand_text[showdown_start:]
            
            # Look for "[hero_name] showed" followed by "won"
            lines = showdown_section.split('\n')
            for i, line in enumerate(lines):
                if hero_name in line and "showed" in line:
                    # Check next few lines for "won"
                    for j in range(i, min(i+3, len(lines))):
                        if "won" in lines[j].lower() and hero_name in lines[j]:
                            return True
        
        # Pattern 3b: GGPoker showdown (alternative format without space)
        if "*** SHOWDOWN ***" in hand_text:
            showdown_start = hand_text.find("*** SHOWDOWN ***")
            showdown_section = hand_text[showdown_start:]
            
            # Look for "[hero_name] showed" followed by "won"
            lines = showdown_section.split('\n')
            for i, line in enumerate(lines):
                if hero_name in line and "showed" in line:
                    # Check next few lines for "won"
                    for j in range(i, min(i+3, len(lines))):
                        if "won" in lines[j].lower() and hero_name in lines[j]:
                            return True
        
        # Pattern 4: Generic "won" pattern (fallback for any site)
        # Look for "[hero_name] won" anywhere in the hand
        if f"{hero_name} won" in hand_text.lower():
            return True
        
        return False
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Return statistics summary in the expected format for aggregation"""
        result = {}
        
        for stat_name, stat_data in self.stats.items():
            result[stat_name] = {
                'opportunities': stat_data["opportunities"],
                'attempts': stat_data["attempts"]
            }
            
            # Include player_sum for W$WSF Rating
            if stat_name == 'W$WSF Rating' and 'player_sum' in stat_data:
                result[stat_name]['player_sum'] = stat_data['player_sum']
            
            # Include total_hands for River Agg %
            if stat_name == 'River Agg %' and 'total_hands' in stat_data:
                result[stat_name]['total_hands'] = stat_data['total_hands']
        
        return result
    
    def get_hands_count(self) -> int:
        """Return total number of hands processed"""
        return self.hands_processed
    
    def get_detailed_counts(self) -> Dict[str, int]:
        """Return detailed counts for debugging"""
        return {
            "total_hands": self.hands_processed,
            "hands_with_flop": self.hands_with_flop,
            "hands_with_turn": self.hands_with_turn,
            "hands_with_river": self.hands_with_river,
            "hands_with_showdown": self.hands_with_showdown,
        }
    
    def print_flop_cbet_ip_debug(self):
        """Print debug statistics for Flop Cbet IP rejections"""
        print("\n" + "="*80)
        print("FLOP CBET IP DEBUG STATISTICS (PokerStars only)")
        print("="*80)
        total = self.flop_cbet_ip_debug["pfr_checked"]
        if total == 0:
            print("No PokerStars PFR hands checked")
            return
            
        print(f"\nTotal PokerStars PFR hands checked: {total}")
        print(f"Accepted: {self.flop_cbet_ip_debug['accepted']} ({self.flop_cbet_ip_debug['accepted']/total*100:.1f}%)\n")
        
        print("Rejection reasons:")
        for reason, count in sorted(self.flop_cbet_ip_debug.items(), key=lambda x: -x[1]):
            if reason not in ['pfr_checked', 'accepted'] and count > 0:
                pct = count/total*100
                print(f"  {reason:30s}: {count:4d} ({pct:5.1f}%)")
        print("="*80)
    
    def log_final_summary(self):
        """Log comprehensive summary for verification"""
        logger.info("="*60)
        logger.info("PostflopCalculatorV3 FINAL SUMMARY")
        logger.info(f"Total hands processed: {self.hands_processed}")
        logger.info(f"Hands that reached flop: {self.hands_with_flop}")
        logger.info(f"Hands that reached turn: {self.hands_with_turn}")
        logger.info(f"Hands that reached river: {self.hands_with_river}")
        logger.info(f"Hands that reached showdown: {self.hands_with_showdown}")
        logger.info("-"*60)
        logger.info("All 20 Postflop Statistics:")
        for stat_name, stat_data in self.stats.items():
            percentage = (stat_data['attempts'] / stat_data['opportunities'] * 100) if stat_data['opportunities'] > 0 else 0
            logger.info(f"{stat_name}: {stat_data['attempts']}/{stat_data['opportunities']} = {percentage:.2f}%")
        logger.info("="*60)
